"""
データベース操作モジュール

DuckDBデータベースの初期化、処理済みファイル管理などの機能を提供します。
"""

import datetime
import enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import duckdb
import polars as pl

from src.utils.error_handlers import DatabaseOperationError, safe_operation
from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("db_utils")


class ProcessStatus(enum.Enum):
    """ファイル処理状態を表す列挙型"""

    PENDING = "PENDING"  # 処理前
    IN_PROGRESS = "IN_PROGRESS"  # 処理中
    COMPLETED = "COMPLETED"  # 正常終了
    FAILED = "FAILED"  # エラーで失敗
    TIMEOUT = "TIMEOUT"  # タイムアウトで中断


class DatabaseManager:
    """データベース操作を行うクラス"""

    def __init__(self, db_path: Union[str, Path]) -> None:
        """
        初期化

        Parameters:
            db_path (str or Path): データベースファイルのパス
        """
        self.db_path = Path(db_path)
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.read_only: bool = False
        self.setup_database()

    def setup_database(self) -> duckdb.DuckDBPyConnection:
        """
        データベースを初期化する

        Returns:
            duckdb.DuckDBPyConnection: データベース接続
        """
        try:
            # 通常モードで接続を試みる
            self.conn = duckdb.connect(str(self.db_path))
            self.read_only = False
            logger.debug(f"データベースに接続しました: {self.db_path}")
        except duckdb.IOException as e:
            if "File is already open" in str(e):
                logger.warning(
                    f"データベースファイル {self.db_path} は既に別のプロセスで開かれています。"
                )
                logger.info("読み取り専用モードで接続を試みます...")
                try:
                    # 読み取り専用モードで接続を試みる
                    self.conn = duckdb.connect(str(self.db_path), read_only=True)
                    self.read_only = True
                    logger.info(
                        "読み取り専用モードで接続しました。データの変更はできません。"
                    )
                except Exception as e2:
                    logger.error(f"読み取り専用モードでの接続にも失敗しました: {e2}")
                    raise DatabaseOperationError(
                        "データベース接続に失敗しました", operation="connect"
                    ) from e2
            else:
                logger.error(f"データベース接続エラー: {str(e)}")
                raise DatabaseOperationError(
                    "データベース接続に失敗しました", operation="connect"
                ) from e

        if self.conn is None:
            raise DatabaseOperationError(
                "データベース接続が確立できませんでした", operation="connect"
            )

        # 処理状態を管理するための列を追加したprocessed_filesテーブルを作成
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_files (
                file_path VARCHAR NOT NULL,
                file_hash VARCHAR NOT NULL,
                source_zip VARCHAR,
                processed_date TIMESTAMP,
                status VARCHAR NOT NULL DEFAULT 'COMPLETED',
                status_updated_at TIMESTAMP,
                PRIMARY KEY (file_path, source_zip)
            )
        """
        )

        # file_hashのインデックスを削除して再作成（トランザクションで囲む）
        try:
            self.conn.execute("BEGIN TRANSACTION")

            # インデックスが存在するか確認して削除
            result = self.conn.execute(
                """
                SELECT COUNT(*) 
                FROM duckdb_indexes() 
                WHERE table_name = 'processed_files' AND index_name = 'idx_processed_files_hash'
            """
            ).fetchone()

            if result[0] > 0:
                self.conn.execute("DROP INDEX idx_processed_files_hash")
                logger.debug(
                    "既存のインデックス idx_processed_files_hash を削除しました"
                )

            # インデックスを再作成（UNIQUEではなく普通のインデックスとして）
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_processed_files_hash 
                ON processed_files(file_hash)
            """
            )
            self.conn.execute("COMMIT")
            logger.debug("インデックス idx_processed_files_hash を作成しました")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.warning(f"インデックス再作成中にエラー: {str(e)}")

        # センサーデータ格納テーブル
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sensor_data (
                Time TIMESTAMP,
                value VARCHAR,
                sensor_id VARCHAR,
                sensor_name VARCHAR,
                unit VARCHAR,
                source_file VARCHAR,
                source_zip VARCHAR,
                factory VARCHAR,
                machine_id VARCHAR,
                data_label VARCHAR
            )
        """
        )
        logger.debug("テーブル構造を確認しました")

        return cast(duckdb.DuckDBPyConnection, self.conn)

    def close(self) -> None:
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.debug(f"データベース接続を閉じました: {self.db_path}")

    def is_file_processed_by_path(
        self, file_path: Union[str, Path], source_zip: Optional[Union[str, Path]] = None
    ) -> bool:
        """
        ファイルパスに基づいて処理済みかどうかを確認する
        注: 完了状態（COMPLETED）のファイルのみを処理済みとみなします

        Parameters:
            file_path (str or Path): ファイルパス
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            bool: 処理済みの場合はTrue
        """
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return False

        # ファイルパスからファイル名を抽出
        file_name = Path(file_path).name
        source_zip_value = "" if source_zip is None else str(source_zip)

        try:
            result = self.conn.execute(
                """
                SELECT COUNT(*) 
                FROM processed_files 
                WHERE file_path = ? AND source_zip = ? AND status = ?
                """,
                [file_name, source_zip_value, ProcessStatus.COMPLETED.value],
            ).fetchone()

            is_processed = result[0] > 0
            logger.debug(
                f"ファイルパスによる処理済みチェック: {file_name} "
                f"(source_zip: {source_zip_value}) -> {is_processed}"
            )
            return is_processed
        except Exception as e:
            logger.error(f"ファイル処理状態チェック中にエラー: {str(e)}")
            return False

    def is_file_processed_by_hash(self, file_hash: str) -> bool:
        """
        ファイルハッシュに基づいて処理済みかどうかを確認する
        注: 完了状態（COMPLETED）のファイルのみを処理済みとみなします

        Parameters:
            file_hash (str): ファイルハッシュ

        Returns:
            bool: 処理済みの場合はTrue
        """
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return False

        try:
            result = self.conn.execute(
                """
                SELECT COUNT(*) 
                FROM processed_files 
                WHERE file_hash = ? AND status = ?
                """,
                [file_hash, ProcessStatus.COMPLETED.value],
            ).fetchone()

            is_processed = result[0] > 0
            logger.debug(
                f"ファイルハッシュによる処理済みチェック: {file_hash} -> {is_processed}"
            )
            return is_processed
        except Exception as e:
            logger.error(f"ファイルハッシュチェック中にエラー: {str(e)}")
            return False

    def update_file_status(
        self,
        file_path: Union[str, Path],
        file_hash: str,
        source_zip: Optional[Union[str, Path]],
        status: ProcessStatus,
    ) -> bool:
        """
        ファイルの処理状態を更新する

        Parameters:
            file_path (str or Path): ファイルパス
            file_hash (str): ファイルハッシュ
            source_zip (str or Path, optional): ZIPファイルパス
            status (ProcessStatus): 処理状態

        Returns:
            bool: 成功した場合はTrue
        """
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return False

        # 読み取り専用モードの場合は何もせずにTrueを返す
        if self.read_only:
            logger.info(
                f"読み取り専用モードのため、状態更新はスキップします: {file_path}"
            )
            return True

        now = datetime.datetime.now()
        source_zip_value = "" if source_zip is None else str(source_zip)
        # ファイルパスからファイル名を抽出
        file_name = Path(file_path).name

        try:
            # 既存のレコードを確認
            result = self.conn.execute(
                """
                SELECT COUNT(*) 
                FROM processed_files 
                WHERE file_path = ? AND source_zip = ?
                """,
                [file_name, source_zip_value],
            ).fetchone()

            if result[0] > 0:
                # 既存のレコードが存在する場合は更新
                self.conn.execute(
                    """
                    UPDATE processed_files 
                    SET file_hash = ?, processed_date = ?, status = ?, status_updated_at = ?
                    WHERE file_path = ? AND source_zip = ?
                    """,
                    [file_hash, now, status.value, now, file_name, source_zip_value],
                )
                logger.debug(
                    f"ファイル状態を更新しました: {file_name} "
                    f"(source_zip: {source_zip_value}) -> {status.value}"
                )
            else:
                # 新規レコードの場合は挿入
                self.conn.execute(
                    """
                    INSERT INTO processed_files 
                    (file_path, file_hash, source_zip, processed_date, status, status_updated_at) 
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [file_name, file_hash, source_zip_value, now, status.value, now],
                )
                logger.debug(
                    f"ファイル状態を新規登録しました: {file_name} "
                    f"(source_zip: {source_zip_value}) -> {status.value}"
                )

            return True
        except Exception as e:
            logger.error(f"状態更新中にエラー ({file_name}): {str(e)}")
            return False

    def mark_file_as_in_progress(
        self,
        file_path: Union[str, Path],
        file_hash: str,
        source_zip: Optional[Union[str, Path]] = None,
    ) -> bool:
        """
        ファイルを処理中としてマークする

        Parameters:
            file_path (str or Path): ファイルパス
            file_hash (str): ファイルハッシュ
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            bool: 成功した場合はTrue
        """
        return self.update_file_status(
            file_path, file_hash, source_zip, ProcessStatus.IN_PROGRESS
        )

    def mark_file_as_completed(
        self,
        file_path: Union[str, Path],
        file_hash: str,
        source_zip: Optional[Union[str, Path]] = None,
    ) -> bool:
        """
        ファイルを正常終了としてマークする

        Parameters:
            file_path (str or Path): ファイルパス
            file_hash (str): ファイルハッシュ
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            bool: 成功した場合はTrue
        """
        return self.update_file_status(
            file_path, file_hash, source_zip, ProcessStatus.COMPLETED
        )

    def mark_file_as_failed(
        self,
        file_path: Union[str, Path],
        file_hash: str,
        source_zip: Optional[Union[str, Path]] = None,
    ) -> bool:
        """
        ファイルを失敗としてマークする

        Parameters:
            file_path (str or Path): ファイルパス
            file_hash (str): ファイルハッシュ
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            bool: 成功した場合はTrue
        """
        return self.update_file_status(
            file_path, file_hash, source_zip, ProcessStatus.FAILED
        )

    def mark_file_as_timeout(
        self,
        file_path: Union[str, Path],
        file_hash: str,
        source_zip: Optional[Union[str, Path]] = None,
    ) -> bool:
        """
        ファイルをタイムアウトとしてマークする

        Parameters:
            file_path (str or Path): ファイルパス
            file_hash (str): ファイルハッシュ
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            bool: 成功した場合はTrue
        """
        return self.update_file_status(
            file_path, file_hash, source_zip, ProcessStatus.TIMEOUT
        )

    def mark_file_as_processed(
        self,
        file_path: Union[str, Path],
        file_hash: str,
        source_zip: Optional[Union[str, Path]] = None,
    ) -> bool:
        """
        ファイルを処理済み（COMPLETED）としてデータベースに記録する
        注: 後方互換性のために残されています。新しいコードでは mark_file_as_completed を使用してください。

        Parameters:
            file_path (str or Path): ファイルパス
            file_hash (str): ファイルハッシュ
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            bool: 成功した場合はTrue
        """
        logger.debug(
            f"mark_file_as_processed は非推奨です。代わりに mark_file_as_completed を使用してください。"
        )
        return self.mark_file_as_completed(file_path, file_hash, source_zip)

    def unmark_file_as_processed(
        self, file_path: Union[str, Path], source_zip: Optional[Union[str, Path]] = None
    ) -> bool:
        """
        ファイルの処理済みマークをデータベースから削除する

        Parameters:
            file_path (str or Path): ファイルパス
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            bool: 成功した場合はTrue
        """
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return False

        # 読み取り専用モードの場合は何もせずにTrueを返す
        if self.read_only:
            logger.info(
                f"読み取り専用モードのため、処理済み記録の削除はスキップします: {file_path}"
            )
            return True

        source_zip_value = "" if source_zip is None else str(source_zip)
        # ファイルパスからファイル名を抽出
        file_name = Path(file_path).name

        try:
            # ファイルパスとソースZIPに基づいて処理済み記録を削除
            self.conn.execute(
                """
                DELETE FROM processed_files 
                WHERE file_path = ? AND source_zip = ?
            """,
                [file_name, source_zip_value],
            )
            logger.info(f"{file_name} の処理済みマークを解除しました")
            return True
        except Exception as e:
            logger.error(f"処理済みマーク解除中にエラー ({file_name}): {str(e)}")
            return False

    def get_file_status(
        self, file_path: Union[str, Path], source_zip: Optional[Union[str, Path]] = None
    ) -> Optional[ProcessStatus]:
        """
        ファイルの処理状態を取得する

        Parameters:
            file_path (str or Path): ファイルパス
            source_zip (str or Path, optional): ZIPファイルパス

        Returns:
            ProcessStatus or None: ファイルの処理状態、存在しない場合はNone
        """
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return None

        # ファイルパスからファイル名を抽出
        file_name = Path(file_path).name
        source_zip_value = "" if source_zip is None else str(source_zip)

        try:
            result = self.conn.execute(
                """
                SELECT status 
                FROM processed_files 
                WHERE file_path = ? AND source_zip = ?
                """,
                [file_name, source_zip_value],
            ).fetchone()

            if result and result[0]:
                # 文字列から列挙型に変換
                for status in ProcessStatus:
                    if status.value == result[0]:
                        logger.debug(
                            f"ファイル状態を取得: {file_name} "
                            f"(source_zip: {source_zip_value}) -> {status.value}"
                        )
                        return status

            logger.debug(
                f"ファイル状態が見つかりません: {file_name} (source_zip: {source_zip_value})"
            )
            return None
        except Exception as e:
            logger.error(f"ファイル状態取得中にエラー ({file_name}): {str(e)}")
            return None

    def insert_sensor_data(self, data_df: pl.DataFrame) -> int:
        """
        センサーデータをデータベースに挿入する

        Parameters:
            data_df (pl.DataFrame): Polars DataFrame

        Returns:
            int: 挿入された行数
        """
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return 0

        # 読み取り専用モードの場合は何もせずに行数を返す
        if self.read_only:
            logger.info(
                "読み取り専用モードのため、センサーデータの挿入はスキップします"
            )
            return len(data_df) if data_df is not None else 0

        # データフレームが空の場合は何もしない
        if data_df is None or len(data_df) == 0:
            logger.warning("挿入するデータがありません")
            return 0

        try:
            # エンコーディング問題を回避するため、文字列データを事前にクリーニング
            logger.debug("データフレームの文字列カラムをクリーニング")
            string_columns = [
                col
                for col in data_df.columns
                if data_df[col].dtype == pl.Utf8 or data_df[col].dtype == pl.String
            ]

            # 文字列カラムの無効な文字を置換
            if string_columns:
                clean_df = data_df.with_columns(
                    [
                        pl.col(col).str.replace_all(
                            r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", ""
                        )
                        for col in string_columns
                    ]
                )
            else:
                clean_df = data_df

            # DataFrameをArrowテーブルに変換
            try:
                logger.debug("DataFrameをArrowテーブルに変換")
                arrow_table = clean_df.to_arrow()
            except Exception as arrow_err:
                logger.error(f"Arrowテーブル変換中にエラー: {str(arrow_err)}")
                # フォールバック: 問題のある行を特定して除外
                logger.warning("問題のある行を特定して除外します")
                valid_rows = []
                for i in range(len(clean_df)):
                    try:
                        # 1行ずつArrowに変換を試みる
                        row_df = clean_df.slice(i, 1)
                        row_df.to_arrow()
                        valid_rows.append(i)
                    except Exception:
                        logger.warning(f"行 {i} は変換できないためスキップします")

                if not valid_rows:
                    logger.error("有効な行がありません")
                    return 0

                # 有効な行だけのデータフレームを作成
                clean_df = clean_df.select(valid_rows)
                arrow_table = clean_df.to_arrow()
                logger.info(f"クリーニング後の行数: {len(clean_df)}")

            # 一時テーブルとして登録
            self.conn.register("temp_sensor_data", arrow_table)

            # トランザクションを開始
            self.conn.execute("BEGIN TRANSACTION")

            try:
                # SQLで一括挿入（Arrow形式からの直接挿入）
                self.conn.execute(
                    """
                    INSERT INTO sensor_data 
                    SELECT * FROM temp_sensor_data
                """
                )

                # 一時テーブルを削除
                self.conn.execute("DROP VIEW IF EXISTS temp_sensor_data")

                # コミット
                self.conn.execute("COMMIT")

                # 挿入された行数を取得
                row_count = len(clean_df)
                logger.info(f"センサーデータを {row_count} 行挿入しました")

                return row_count
            except Exception as e:
                # エラーが発生した場合はロールバック
                self.conn.execute("ROLLBACK")
                logger.error(f"センサーデータ挿入中にエラー: {str(e)}")
                raise DatabaseOperationError(
                    "センサーデータの挿入に失敗しました", operation="insert_sensor_data"
                ) from e
        except Exception as e:
            logger.error(f"データ準備中にエラー: {str(e)}")
            raise DatabaseOperationError(
                "センサーデータの準備に失敗しました", operation="insert_sensor_data"
            ) from e

    def commit(self) -> None:
        """変更をコミットする"""
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return

        if not self.read_only:
            try:
                self.conn.execute("COMMIT")
                logger.debug("トランザクションをコミットしました")
            except duckdb.duckdb.TransactionException:
                # トランザクションがアクティブでない場合は無視
                logger.debug(
                    "コミットをスキップ: アクティブなトランザクションがありません"
                )
                pass

    def rollback(self) -> None:
        """変更をロールバックする"""
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            return

        if not self.read_only:
            try:
                self.conn.execute("ROLLBACK")
                logger.debug("トランザクションをロールバックしました")
            except duckdb.duckdb.TransactionException:
                # トランザクションがアクティブでない場合は無視
                logger.debug(
                    "ロールバックをスキップ: アクティブなトランザクションがありません"
                )
                pass

    def execute(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """
        SQLクエリを実行する

        Parameters:
            query (str): SQLクエリ
            params (list, optional): クエリパラメータ

        Returns:
            duckdb.DuckDBPyResult: クエリ結果
        """
        if self.conn is None:
            logger.error("データベース接続が確立されていません")
            raise DatabaseOperationError(
                "データベース接続が確立されていません", operation="execute"
            )

        try:
            if params:
                logger.debug(f"SQLクエリを実行: {query} (パラメータあり)")
                return self.conn.execute(query, params)
            logger.debug(f"SQLクエリを実行: {query}")
            return self.conn.execute(query)
        except Exception as e:
            logger.error(f"SQLクエリ実行中にエラー: {str(e)}")
            raise DatabaseOperationError(
                f"SQLクエリの実行に失敗しました: {query}", operation="execute"
            ) from e
