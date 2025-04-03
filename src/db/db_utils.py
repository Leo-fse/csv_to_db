"""
データベース操作モジュール

DuckDBデータベースの初期化、処理済みファイル管理などの機能を提供します。
"""

import datetime
import enum
from pathlib import Path

import duckdb


class ProcessStatus(enum.Enum):
    """ファイル処理状態を表す列挙型"""

    PENDING = "PENDING"  # 処理前
    IN_PROGRESS = "IN_PROGRESS"  # 処理中
    COMPLETED = "COMPLETED"  # 正常終了
    FAILED = "FAILED"  # エラーで失敗
    TIMEOUT = "TIMEOUT"  # タイムアウトで中断


class DatabaseManager:
    """データベース操作を行うクラス"""

    def __init__(self, db_path):
        """
        初期化

        Parameters:
        db_path (str or Path): データベースファイルのパス
        """
        self.db_path = Path(db_path)
        self.conn = None
        self.read_only = False
        self.setup_database()

    def setup_database(self):
        """
        データベースを初期化する

        Returns:
        duckdb.DuckDBPyConnection: データベース接続
        """
        try:
            # 通常モードで接続を試みる
            self.conn = duckdb.connect(str(self.db_path))
            self.read_only = False
        except duckdb.IOException as e:
            if "File is already open" in str(e):
                print(
                    f"警告: データベースファイル {self.db_path} は既に別のプロセスで開かれています。"
                )
                print("読み取り専用モードで接続を試みます...")
                try:
                    # 読み取り専用モードで接続を試みる
                    self.conn = duckdb.connect(str(self.db_path), read_only=True)
                    self.read_only = True
                    print(
                        "読み取り専用モードで接続しました。データの変更はできません。"
                    )
                except Exception as e2:
                    print(f"エラー: 読み取り専用モードでの接続にも失敗しました: {e2}")
                    raise
            else:
                raise

        # 処理状態を管理するための列を追加したprocessed_filesテーブルを作成
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_path VARCHAR NOT NULL,
                file_hash VARCHAR NOT NULL,
                source_zip VARCHAR,
                processed_date TIMESTAMP,
                status VARCHAR NOT NULL DEFAULT 'COMPLETED',
                status_updated_at TIMESTAMP,
                PRIMARY KEY (file_path, source_zip)
            )
        """)

        # file_hashのインデックスを削除して再作成（トランザクションで囲む）
        try:
            self.conn.execute("BEGIN TRANSACTION")

            # インデックスが存在するか確認して削除
            result = self.conn.execute("""
                SELECT COUNT(*) 
                FROM duckdb_indexes() 
                WHERE table_name = 'processed_files' AND index_name = 'idx_processed_files_hash'
            """).fetchone()

            if result[0] > 0:
                self.conn.execute("DROP INDEX idx_processed_files_hash")

            # インデックスを再作成（UNIQUEではなく普通のインデックスとして）
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_processed_files_hash 
                ON processed_files(file_hash)
            """)
            self.conn.execute("COMMIT")
        except Exception as e:
            self.conn.execute("ROLLBACK")
            print(f"警告: インデックス再作成中にエラー: {str(e)}")

        # センサーデータ格納テーブル
        self.conn.execute("""
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
        """)

        return self.conn

    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def is_file_processed_by_path(self, file_path, source_zip=None):
        """
        ファイルパスに基づいて処理済みかどうかを確認する
        注: 完了状態（COMPLETED）のファイルのみを処理済みとみなします

        Parameters:
        file_path (str or Path): ファイルパス
        source_zip (str or Path, optional): ZIPファイルパス

        Returns:
        bool: 処理済みの場合はTrue
        """
        # ファイルパスからファイル名を抽出
        file_name = Path(file_path).name
        source_zip_value = "" if source_zip is None else str(source_zip)
        result = self.conn.execute(
            """
            SELECT COUNT(*) 
            FROM processed_files 
            WHERE file_path = ? AND source_zip = ? AND status = ?
            """,
            [file_name, source_zip_value, ProcessStatus.COMPLETED.value],
        ).fetchone()

        return result[0] > 0

    def is_file_processed_by_hash(self, file_hash):
        """
        ファイルハッシュに基づいて処理済みかどうかを確認する
        注: 完了状態（COMPLETED）のファイルのみを処理済みとみなします

        Parameters:
        file_hash (str): ファイルハッシュ

        Returns:
        bool: 処理済みの場合はTrue
        """
        result = self.conn.execute(
            """
            SELECT COUNT(*) 
            FROM processed_files 
            WHERE file_hash = ? AND status = ?
            """,
            [file_hash, ProcessStatus.COMPLETED.value],
        ).fetchone()

        return result[0] > 0

    def update_file_status(self, file_path, file_hash, source_zip, status):
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
        # 読み取り専用モードの場合は何もせずにTrueを返す
        if self.read_only:
            print(
                f"  情報: 読み取り専用モードのため、状態更新はスキップします: {file_path}"
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

            print(f"  情報: {file_name} の状態を {status.value} に更新しました")
            return True
        except Exception as e:
            print(f"  警告: 状態更新中にエラー ({file_name}): {str(e)}")
            return False

    def mark_file_as_in_progress(self, file_path, file_hash, source_zip=None):
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

    def mark_file_as_completed(self, file_path, file_hash, source_zip=None):
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

    def mark_file_as_failed(self, file_path, file_hash, source_zip=None):
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

    def mark_file_as_timeout(self, file_path, file_hash, source_zip=None):
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

    def mark_file_as_processed(self, file_path, file_hash, source_zip=None):
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
        return self.mark_file_as_completed(file_path, file_hash, source_zip)

    def unmark_file_as_processed(self, file_path, source_zip=None):
        """
        ファイルの処理済みマークをデータベースから削除する

        Parameters:
        file_path (str or Path): ファイルパス
        source_zip (str or Path, optional): ZIPファイルパス

        Returns:
        bool: 成功した場合はTrue
        """
        # 読み取り専用モードの場合は何もせずにTrueを返す
        if self.read_only:
            print(
                f"  情報: 読み取り専用モードのため、処理済み記録の削除はスキップします: {file_path}"
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
            print(f"  情報: {file_name} の処理済みマークを解除しました")
            return True
        except Exception as e:
            print(f"  警告: 処理済みマーク解除中にエラー ({file_name}): {str(e)}")
            return False

    def get_file_status(self, file_path, source_zip=None):
        """
        ファイルの処理状態を取得する

        Parameters:
        file_path (str or Path): ファイルパス
        source_zip (str or Path, optional): ZIPファイルパス

        Returns:
        ProcessStatus or None: ファイルの処理状態、存在しない場合はNone
        """
        # ファイルパスからファイル名を抽出
        file_name = Path(file_path).name
        source_zip_value = "" if source_zip is None else str(source_zip)

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
                    return status

        return None

    def insert_sensor_data(self, data_df):
        """
        センサーデータをデータベースに挿入する

        Parameters:
        data_df: Polars DataFrame

        Returns:
        int: 挿入された行数
        """
        # 読み取り専用モードの場合は何もせずに行数を返す
        if self.read_only:
            print(
                "  情報: 読み取り専用モードのため、センサーデータの挿入はスキップします"
            )
            return len(data_df) if data_df is not None else 0

        # DataFrameをArrowテーブルに変換
        arrow_table = data_df.to_arrow()

        # 一時テーブルとして登録
        self.conn.register("temp_sensor_data", arrow_table)

        # トランザクションを開始
        self.conn.execute("BEGIN TRANSACTION")

        try:
            # SQLで一括挿入（Arrow形式からの直接挿入）
            result = self.conn.execute("""
                INSERT INTO sensor_data 
                SELECT * FROM temp_sensor_data
            """)

            # 一時テーブルを削除
            self.conn.execute("DROP VIEW IF EXISTS temp_sensor_data")

            # 挿入された行数を取得（DuckDBでは直接取得できないため、データフレームの行数を使用）
            row_count = len(data_df) if data_df is not None else 0

            return row_count
        except Exception as e:
            # エラーが発生した場合はロールバック
            self.conn.execute("ROLLBACK")
            raise e

    def commit(self):
        """変更をコミットする"""
        if self.conn and not self.read_only:
            try:
                self.conn.execute("COMMIT")
            except duckdb.duckdb.TransactionException:
                # トランザクションがアクティブでない場合は無視
                pass

    def rollback(self):
        """変更をロールバックする"""
        if self.conn and not self.read_only:
            try:
                self.conn.execute("ROLLBACK")
            except duckdb.duckdb.TransactionException:
                # トランザクションがアクティブでない場合は無視
                pass

    def execute(self, query, params=None):
        """
        SQLクエリを実行する

        Parameters:
        query (str): SQLクエリ
        params (list, optional): クエリパラメータ

        Returns:
        duckdb.DuckDBPyResult: クエリ結果
        """
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)
