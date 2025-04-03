"""
ファイル処理モジュール

CSVファイルの検索、抽出、処理を統合的に行います。
"""

import concurrent.futures
import multiprocessing
import os
import re
import shutil
import signal
import tempfile
import threading
import time
from multiprocessing import Manager
from pathlib import Path

from src.config.config import config
from src.db.db_utils import DatabaseManager
from src.file.file_utils import FileFinder, FileHasher
from src.file.zip_handler import ZipHandler
from src.processor.csv_processor import CsvProcessor
from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("file_processor")


# スタンドアロン関数（プロセス間で共有しない）
def process_file_standalone(
    file_path, actual_file_path, file_hash, source_zip, meta_info, db_path
):
    """
    スタンドアロンで実行できるファイル処理関数（プロセス間共有なし）

    Parameters:
    file_path (str): ファイルのパス
    actual_file_path (str): 実際のファイルパス（ZIP展開後など）
    file_hash (str): ファイルハッシュ
    source_zip (str): 元のZIPファイルパス（なければNone）
    meta_info (dict): メタ情報
    db_path (str): データベースファイルパス

    Returns:
    dict: 処理結果
    """
    import os
    import random
    import time
    from pathlib import Path

    from src.db.db_utils import DatabaseManager
    from src.processor.csv_processor import CsvProcessor

    # プロセス固有のデータベースファイル名を生成
    process_id = os.getpid()
    # 競合を避けるためにランダム要素と時間を追加
    random_suffix = random.randint(1000, 9999)
    timestamp = int(time.time() * 1000) % 10000

    # 元のパスからディレクトリとファイル名を分離
    db_dir = os.path.dirname(db_path)
    db_name = os.path.basename(db_path)
    base_name, ext = os.path.splitext(db_name)

    # 一時データベースのパスを生成
    temp_db_name = f"{base_name}_{process_id}_{timestamp}_{random_suffix}{ext}"
    temp_db_path = os.path.join(db_dir, temp_db_name) if db_dir else temp_db_name

    # 独立したデータベース接続とCSVプロセッサを作成（エンコーディングを強制）
    db_manager = DatabaseManager(temp_db_path)
    csv_processor = CsvProcessor(force_encoding=True)

    result = {
        "success": False,
        "file_path": file_path,
        "source_zip": source_zip,
        "file_hash": file_hash,
        "temp_db_path": temp_db_path,  # 一時データベースのパスを結果に含める
    }

    try:
        # ファイル処理前に二重チェック
        if db_manager.is_file_processed_by_hash(file_hash):
            result["already_processed"] = True
            return result

        # ファイルを処理
        data_df = csv_processor.process_csv_file(actual_file_path)

        if data_df is not None:
            # メタ情報を追加
            file_info = {"file_path": file_path, "source_zip": source_zip}
            data_df = csv_processor.add_meta_info(data_df, file_info, meta_info)

            # データベースに保存
            rows_inserted = db_manager.insert_sensor_data(data_df)

            # 処理済みに記録
            source_zip_str = str(source_zip) if source_zip else None
            db_manager.mark_file_as_completed(file_path, file_hash, source_zip_str)

            # コミット
            db_manager.commit()
            result["success"] = True
            result["rows_inserted"] = rows_inserted
        else:
            print(f"エラー: {file_path} の処理結果がNoneです")
            # 失敗状態にマーク
            source_zip_str = str(source_zip) if source_zip else None
            db_manager.mark_file_as_failed(file_path, file_hash, source_zip_str)
    except Exception as e:
        # ロールバック
        db_manager.rollback()

        file_name = Path(file_path).name
        source_zip_str = f" (in {source_zip})" if source_zip else ""
        print(f"エラー処理中 {file_name}{source_zip_str}: {str(e)}")

        # 失敗状態にマーク
        source_zip_str = str(source_zip) if source_zip else None
        db_manager.mark_file_as_failed(file_path, file_hash, source_zip_str)

        result["error"] = str(e)
    finally:
        # データベース接続を閉じる
        db_manager.close()

    return result


class FileProcessor:
    """ファイル処理を行うクラス"""

    def __init__(self, db_path=None, meta_info=None):
        """
        初期化

        Parameters:
        db_path (str or Path, optional): データベースファイルのパス
        meta_info (dict, optional): メタ情報
        """
        self.db_path = db_path or config.get("db")
        self.meta_info = meta_info or config.get_meta_info()
        self.db_manager = DatabaseManager(self.db_path)
        self.csv_processor = CsvProcessor(force_encoding=True)

        # ファイルロックを管理するための辞書
        self.file_locks = {}
        self.lock_dict_lock = threading.Lock()

        # プロセス間通信用のマネージャー
        self.manager = Manager()
        # キャンセルフラグを管理する共有辞書
        self.cancel_flags = self.manager.dict()

    def __del__(self):
        """デストラクタ"""
        if hasattr(self, "db_manager"):
            self.db_manager.close()

    def get_file_lock(self, file_path):
        """
        ファイルロックを取得する

        Parameters:
        file_path (str): ファイルパス

        Returns:
        threading.Lock: ファイルロック
        """
        with self.lock_dict_lock:
            if file_path not in self.file_locks:
                self.file_locks[file_path] = threading.Lock()
            return self.file_locks[file_path]

    def find_csv_files(self, folder_path, pattern):
        """
        フォルダ内およびZIPファイル内から正規表現パターンに一致するCSVファイルを抽出する

        Parameters:
        folder_path (str or Path): 検索対象のフォルダパス
        pattern (str): 正規表現パターン

        Returns:
        list: [{'path': ファイルパス, 'source_zip': ZIPファイルパス（ない場合はNone）}]
        """
        # コンパイル済み正規表現パターン
        regex = re.compile(pattern)

        # ファイル検索オブジェクト
        file_finder = FileFinder(pattern)

        # 通常のCSVファイルを検索
        found_files = file_finder.find_csv_files(folder_path)

        # ZIPファイルを検索して中身を確認
        for zip_file in Path(folder_path).rglob("*.zip"):
            zip_files = ZipHandler.find_csv_files_in_zip(zip_file, regex)
            found_files.extend(zip_files)

        return found_files

    def process_single_file(self, file_info, temp_dir):
        """
        単一のCSVファイルを処理する関数

        Parameters:
        file_info (dict): 処理するファイルの情報
        temp_dir (Path): 一時ディレクトリのパス

        Returns:
        dict: 処理結果
        """
        result = {
            "success": False,
            "file_path": file_info["file_path"],
            "source_zip": file_info["source_zip"],
            "file_hash": file_info["file_hash"],
        }

        try:
            # ファイルを処理
            data_df = self.csv_processor.process_csv_file(file_info["actual_file_path"])
            if data_df is not None:
                # メタ情報を追加
                data_df = self.csv_processor.add_meta_info(
                    data_df, file_info, self.meta_info
                )

                # トランザクションを開始（insert_sensor_data内で開始されるため不要）
                # データベースに保存
                rows_inserted = self.db_manager.insert_sensor_data(data_df)

                # 処理済みに記録
                self.db_manager.mark_file_as_processed(
                    file_info["file_path"],
                    file_info["file_hash"],
                    file_info["source_zip_str"],
                )

                # コミット
                self.db_manager.commit()
                result["success"] = True
                result["rows_inserted"] = rows_inserted
            else:
                print(f"エラー: {file_info['file_path']} の処理結果がNoneです")
        except Exception as e:
            # ロールバック
            self.db_manager.rollback()

            print(
                f"エラー処理中 {file_info['file_path']}"
                + (
                    f" (in {file_info['source_zip']})"
                    if file_info["source_zip"]
                    else ""
                )
                + f": {str(e)}"
            )

        return result

    def process_file_in_subprocess(
        self, file_info, process_id, db_path, meta_info, cancel_key
    ):
        """
        サブプロセスでファイルを処理する関数

        Parameters:
        file_info (dict): 処理するファイルの情報
        process_id (int): プロセスID
        db_path (str or Path): データベースファイルのパス
        meta_info (dict): メタ情報
        cancel_key (str): キャンセルフラグのキー

        Returns:
        dict: 処理結果
        """

        # プロセス開始時にシグナルハンドラを設定
        def handle_termination(signum, frame):
            print(
                f"プロセス {process_id} がシグナル {signum} を受信しました。終了します。"
            )
            # 強制終了
            os._exit(1)

        # SIGTERMシグナルのハンドラを設定
        signal.signal(signal.SIGTERM, handle_termination)

        # 各プロセス用の独立したデータベース接続を作成
        process_db_manager = DatabaseManager(db_path)

        # CSVプロセッサを作成（エンコーディングを強制）
        csv_processor = CsvProcessor(force_encoding=True)

        try:
            # 結果オブジェクトを初期化
            result = {
                "success": False,
                "file_path": file_info["file_path"],
                "source_zip": file_info["source_zip"],
                "file_hash": file_info["file_hash"],
                "process_id": process_id,
            }

            # キャンセルされていないか定期的にチェックする関数
            # 注: この関数はダミーです。実際のキャンセルチェックはメインプロセスで行われます
            def check_cancelled():
                # 常にFalseを返す（キャンセルされていない）
                # 実際のキャンセル処理はメインプロセスで行われ、タイムアウト時にプロセスが強制終了されます
                return False

            # ファイルを処理
            data_df = csv_processor.process_csv_file(
                file_info["actual_file_path"],
                check_cancelled,  # キャンセルチェック関数を渡す
            )

            # キャンセルされたかチェック
            if check_cancelled():
                print(f"プロセス {process_id}: 処理がキャンセルされました")
                # タイムアウト状態にマーク
                process_db_manager.mark_file_as_timeout(
                    file_info["file_path"],
                    file_info["file_hash"],
                    file_info["source_zip_str"],
                )
                result["cancelled"] = True
                return result

            if data_df is not None:
                # メタ情報を追加
                data_df = csv_processor.add_meta_info(data_df, file_info, meta_info)

                # キャンセルされたかチェック
                if check_cancelled():
                    print(
                        f"プロセス {process_id}: メタ情報追加後にキャンセルされました"
                    )
                    # タイムアウト状態にマーク
                    process_db_manager.mark_file_as_timeout(
                        file_info["file_path"],
                        file_info["file_hash"],
                        file_info["source_zip_str"],
                    )
                    result["cancelled"] = True
                    return result

                # データベースに保存
                rows_inserted = process_db_manager.insert_sensor_data(data_df)

                # 処理済み（完了）に記録
                process_db_manager.mark_file_as_completed(
                    file_info["file_path"],
                    file_info["file_hash"],
                    file_info["source_zip_str"],
                )

                # コミット
                process_db_manager.commit()
                result["success"] = True
                result["rows_inserted"] = rows_inserted
            else:
                print(f"エラー: {file_info['file_path']} の処理結果がNoneです")
                # 失敗状態にマーク
                process_db_manager.mark_file_as_failed(
                    file_info["file_path"],
                    file_info["file_hash"],
                    file_info["source_zip_str"],
                )
        except Exception as e:
            # ロールバック
            process_db_manager.rollback()

            file_name = Path(file_info["file_path"]).name
            print(
                f"エラー処理中 {file_name}"
                + (
                    f" (in {file_info['source_zip']})"
                    if file_info["source_zip"]
                    else ""
                )
                + f": {str(e)}"
            )

            # 失敗状態にマーク
            process_db_manager.mark_file_as_failed(
                file_info["file_path"],
                file_info["file_hash"],
                file_info["source_zip_str"],
            )

        finally:
            # データベース接続を閉じる
            process_db_manager.close()

        return result

    def process_csv_files(self, csv_files, process_all=False):
        """
        CSVファイルのリストを処理する

        Parameters:
        csv_files (list): 処理するCSVファイルのリスト
        process_all (bool): 処理済みファイルも再処理するかどうか

        Returns:
        dict: 処理結果の統計情報
        """
        # 結果統計
        stats = {
            "total_found": len(csv_files),
            "already_processed_by_path": 0,
            "already_processed_by_hash": 0,
            "newly_processed": 0,
            "failed": 0,
            "timeout": 0,  # タイムアウトによる失敗件数を追加
        }

        # 一時ディレクトリを作成
        temp_dir = Path(tempfile.mkdtemp())

        try:
            # 処理対象ファイルのリストを作成
            files_to_process = []

            # 前処理：処理済みファイルのフィルタリング
            for file_info in csv_files:
                file_path = file_info["path"]
                source_zip = file_info["source_zip"]
                source_zip_str = str(source_zip) if source_zip else None

                # パスベースで既に処理済みかチェック
                if not process_all and self.db_manager.is_file_processed_by_path(
                    file_path, source_zip_str
                ):
                    stats["already_processed_by_path"] += 1
                    file_name = Path(file_path).name
                    print(
                        f"スキップ (既処理 - ファイル名一致): {file_name}"
                        + (f" (in {source_zip})" if source_zip else "")
                    )
                    continue

                # ZIPファイル内のファイルなら抽出
                try:
                    if source_zip:
                        # 一時ファイルにZIPから抽出
                        actual_file_path = ZipHandler.extract_file(
                            source_zip, file_path, temp_dir
                        )
                        # 抽出したファイルが存在するか確認
                        if not Path(actual_file_path).exists():
                            logger.error(
                                f"抽出されたファイルが見つかりません: {actual_file_path}"
                            )
                            stats["failed"] += 1
                            continue
                    else:
                        actual_file_path = file_path
                        # 通常のファイルが存在するか確認
                        if not Path(actual_file_path).exists():
                            logger.error(
                                f"ファイルが見つかりません: {actual_file_path}"
                            )
                            stats["failed"] += 1
                            continue

                    # ファイルハッシュを計算
                    try:
                        file_hash = FileHasher.get_file_hash(actual_file_path)
                    except Exception as e:
                        logger.error(f"ファイルハッシュ計算中にエラー: {str(e)}")
                        stats["failed"] += 1
                        continue

                    # ハッシュベースで既に処理済みかチェック
                    if not process_all and self.db_manager.is_file_processed_by_hash(
                        file_hash
                    ):
                        stats["already_processed_by_hash"] += 1
                        file_name = Path(file_path).name
                        print(
                            f"スキップ (既処理 - 内容一致): {file_name}"
                            + (f" (in {source_zip})" if source_zip else "")
                        )
                        continue

                    # 処理対象リストに追加
                    files_to_process.append(
                        {
                            "file_path": file_path,
                            "actual_file_path": actual_file_path,
                            "source_zip": source_zip,
                            "source_zip_str": source_zip_str,
                            "file_hash": file_hash,
                        }
                    )
                except Exception as e:
                    file_name = Path(file_path).name
                    print(
                        f"エラー前処理中 {file_name}"
                        + (f" (in {source_zip})" if source_zip else "")
                        + f": {str(e)}"
                    )
                    stats["failed"] += 1

            # 並列処理の方法を選択
            # ファイル数が少ない場合は逐次処理、多い場合は並列処理
            if len(files_to_process) <= 1:
                # 逐次処理
                print("逐次処理を開始")
                for file_info in files_to_process:
                    result = self.process_single_file(file_info, temp_dir)
                    if result["success"]:
                        stats["newly_processed"] += 1
                    else:
                        stats["failed"] += 1
            else:
                # 並列処理（ProcessPoolExecutorを使用）
                max_workers = min(
                    4, multiprocessing.cpu_count(), len(files_to_process)
                )  # 最大値は手元のCPUコア数と4の小さい方
                print(f"並列処理を開始: {max_workers}プロセス")

                # 事前に処理済みファイルを再確認
                for file_info in files_to_process[:]:
                    try:
                        # 二重チェック - 別プロセスによって処理されていないか確認
                        if self.db_manager.is_file_processed_by_hash(
                            file_info["file_hash"]
                        ):
                            stats["already_processed_by_hash"] += 1
                            files_to_process.remove(file_info)
                            file_name = Path(file_info["file_path"]).name
                            print(f"スキップ (既処理 - 内容一致): {file_name}")
                            continue
                    except Exception as e:
                        print(f"警告: ファイル重複チェック中にエラー: {str(e)}")

                # プロセス間で共有するキャンセルフラグをクリア
                self.cancel_flags.clear()

                # 並列処理実行部分を修正
                with concurrent.futures.ProcessPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    # 各ファイルを並列処理
                    futures = []
                    for file_info in files_to_process:
                        # ProcessPoolExecutorに渡すのは単純なデータのみ
                        future = executor.submit(
                            process_file_standalone,  # モジュールレベルの関数を使用
                            file_info["file_path"],
                            file_info["actual_file_path"],
                            file_info["file_hash"],
                            file_info["source_zip"],
                            self.meta_info,
                            self.db_path,
                        )
                        futures.append(future)
                        print(f"処理開始: {file_info['file_path']}")

                    # 処理中のファイル数を追跡
                    completed = 0
                    total = len(futures)

                    # 成功した処理の一時データベースパスを保存するリスト
                    successful_temp_dbs = []

                    # 結果を集計
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            result = future.result(timeout=60)  # 個別のタイムアウト
                            completed += 1

                            if (
                                "already_processed" in result
                                and result["already_processed"]
                            ):
                                stats["already_processed_by_hash"] += 1
                                print(
                                    f"処理済みスキップ ({completed}/{total}): {result['file_path']}"
                                )
                            elif result["success"]:
                                stats["newly_processed"] += 1
                                print(
                                    f"処理成功 ({completed}/{total}): {result['file_path']}"
                                )
                                # 成功した場合、一時データベースパスを保存
                                if "temp_db_path" in result:
                                    successful_temp_dbs.append(result["temp_db_path"])
                            else:
                                stats["failed"] += 1
                                print(
                                    f"処理失敗 ({completed}/{total}): {result['file_path']}"
                                )
                                if "error" in result:
                                    print(f"  エラー内容: {result['error']}")
                        except concurrent.futures.TimeoutError:
                            completed += 1
                            stats["timeout"] += 1
                            print(f"処理タイムアウト ({completed}/{total})")
                        except Exception as e:
                            completed += 1
                            stats["failed"] += 1
                            print(f"処理例外 ({completed}/{total}): {str(e)}")

                    # すべてのタスクが完了したことを確認
                    print(f"すべてのファイル処理が完了しました: {completed}/{total}")

                    # 一時データベースからメインデータベースにデータをマージ
                    if successful_temp_dbs:
                        total_dbs = len(successful_temp_dbs)
                        print(
                            f"{total_dbs}個の一時データベースからデータをマージします..."
                        )
                        merged_count = 0
                        total_rows_merged = 0

                        # メインデータベースのコネクションを取得
                        main_conn = self.db_manager.conn

                        for idx, temp_db_path in enumerate(successful_temp_dbs, 1):
                            try:
                                print(
                                    f"  データベース {idx}/{total_dbs} をマージ中... ({os.path.basename(temp_db_path)})"
                                )

                                # ATTACH DATABASE コマンドを使用して一時データベースを接続
                                temp_db_name = f"temp_db_{idx}"
                                main_conn.execute(
                                    f"ATTACH DATABASE '{temp_db_path}' AS {temp_db_name}"
                                )

                                try:
                                    # トランザクションを開始
                                    main_conn.execute("BEGIN TRANSACTION")

                                    # processed_filesテーブルのデータをマージ（SQLを使用して直接コピー）
                                    processed_count = main_conn.execute(f"""
                                        INSERT OR REPLACE INTO main.processed_files 
                                        SELECT * FROM {temp_db_name}.processed_files
                                    """).fetchone()[0]

                                    print(
                                        f"    processed_filesテーブル: {processed_count}行をマージしました"
                                    )

                                    # sensor_dataテーブルのデータをマージ（SQLを使用して直接コピー）
                                    # 行数をカウントするためのクエリ
                                    row_count_query = f"SELECT COUNT(*) FROM {temp_db_name}.sensor_data"
                                    total_sensor_rows = main_conn.execute(
                                        row_count_query
                                    ).fetchone()[0]

                                    if total_sensor_rows > 0:
                                        print(
                                            f"    sensor_dataテーブル: 合計{total_sensor_rows}行をマージします..."
                                        )

                                        # 大量データの場合はバッチ処理
                                        batch_size = 10000  # バッチサイズを増やす
                                        offset = 0
                                        rows_processed = 0

                                        while offset < total_sensor_rows:
                                            # バッチごとにデータをコピー
                                            batch_query = f"""
                                                INSERT INTO main.sensor_data 
                                                SELECT * FROM {temp_db_name}.sensor_data 
                                                LIMIT {batch_size} OFFSET {offset}
                                            """
                                            main_conn.execute(batch_query)

                                            # 進捗を更新
                                            offset += batch_size
                                            rows_processed += min(
                                                batch_size,
                                                total_sensor_rows
                                                - (offset - batch_size),
                                            )
                                            progress = min(
                                                100,
                                                int(
                                                    rows_processed
                                                    * 100
                                                    / total_sensor_rows
                                                ),
                                            )
                                            # 進捗表示をシンプルにして余分な文字が表示されないようにする
                                            print(
                                                f"    データ処理: {rows_processed}/{total_sensor_rows}行 ({progress}%完了)"
                                            )

                                        total_rows_merged += total_sensor_rows
                                        print(
                                            f"    sensor_dataテーブル: {total_sensor_rows}行のマージが完了しました"
                                        )

                                    # トランザクションをコミット
                                    main_conn.execute("COMMIT")
                                    print(
                                        f"    データベース {idx}/{total_dbs} のマージが完了しました"
                                    )

                                except Exception as e:
                                    # エラーが発生した場合はロールバック
                                    main_conn.execute("ROLLBACK")
                                    print(
                                        f"    エラー: データベースマージ中に例外が発生しました: {str(e)}"
                                    )
                                    raise

                                finally:
                                    # 一時データベースをデタッチ
                                    try:
                                        main_conn.execute(
                                            f"DETACH DATABASE {temp_db_name}"
                                        )
                                    except Exception as e:
                                        print(
                                            f"    警告: データベースのデタッチに失敗しました: {str(e)}"
                                        )

                                # 一時データベースファイルを削除
                                try:
                                    os.remove(temp_db_path)
                                    print(
                                        f"    一時データベース {os.path.basename(temp_db_path)} を削除しました"
                                    )
                                except Exception as e:
                                    print(
                                        f"    警告: 一時データベースファイルの削除中にエラー: {str(e)}"
                                    )

                                merged_count += 1

                            except Exception as e:
                                print(
                                    f"  警告: 一時データベース {temp_db_path} からのマージ中にエラー: {str(e)}"
                                )

                        # 最終結果を表示
                        print(
                            f"{merged_count}/{total_dbs}個の一時データベースからのマージが完了しました"
                        )
                        print(f"合計 {total_rows_merged} 行のデータがマージされました")

        finally:
            # 一時ディレクトリを削除
            try:
                shutil.rmtree(temp_dir)
            except Exception as e:
                print(f"一時ディレクトリの削除中にエラー: {str(e)}")

        return stats

    def process_folder(self, folder_path=None, pattern=None, process_all=False):
        """
        フォルダ内のCSVファイルを処理する

        Parameters:
        folder_path (str or Path, optional): 検索対象のフォルダパス
        pattern (str, optional): 正規表現パターン
        process_all (bool): 処理済みファイルも再処理するかどうか

        Returns:
        dict: 処理結果の統計情報
        """
        # デフォルト値の設定
        folder_path = folder_path or config.get("folder")
        pattern = pattern or config.get("pattern")

        # ファイル検索
        print(f"フォルダ {folder_path} から条件に合うCSVファイルを検索中...")
        csv_files = self.find_csv_files(folder_path, pattern)
        print(f"{len(csv_files)}件のファイルが見つかりました")

        # ファイル処理
        print("CSVファイルの処理を開始します...")
        stats = self.process_csv_files(csv_files, process_all)

        return stats
