"""
ファイル処理モジュール

CSVファイルの検索、抽出、処理を統合的に行います。
"""

import concurrent.futures
import os
import re
import shutil
import tempfile
import threading
from pathlib import Path

from src.config.config import config
from src.db.db_utils import DatabaseManager
from src.file.file_utils import FileFinder, FileHasher
from src.file.zip_handler import ZipHandler
from src.processor.csv_processor import CsvProcessor


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
        self.csv_processor = CsvProcessor()

        # ファイルロックを管理するための辞書
        self.file_locks = {}
        self.lock_dict_lock = threading.Lock()

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
                    else:
                        actual_file_path = file_path

                    # ファイルハッシュを計算
                    file_hash = FileHasher.get_file_hash(actual_file_path)

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
                # 並列処理（ThreadPoolExecutorを使用）
                max_workers = min(4, len(files_to_process))  # 最大4スレッド
                print(f"並列処理を開始: {max_workers}スレッド")

                # ファイルロックを使用して処理する関数
                def process_with_lock(file_info):
                    # ファイルロックを取得
                    file_path = str(file_info["actual_file_path"])
                    lock = self.get_file_lock(file_path)

                    # 各スレッド用の独立したデータベース接続を作成
                    thread_db_manager = DatabaseManager(self.db_path)

                    try:
                        # ロックを取得してファイルを処理
                        with lock:
                            # ファイルを処理
                            result = {
                                "success": False,
                                "file_path": file_info["file_path"],
                                "source_zip": file_info["source_zip"],
                                "file_hash": file_info["file_hash"],
                            }

                            try:
                                # ファイルを処理
                                data_df = self.csv_processor.process_csv_file(
                                    file_info["actual_file_path"]
                                )
                                if data_df is not None:
                                    # メタ情報を追加
                                    data_df = self.csv_processor.add_meta_info(
                                        data_df, file_info, self.meta_info
                                    )

                                    # データベースに保存
                                    rows_inserted = (
                                        thread_db_manager.insert_sensor_data(data_df)
                                    )

                                    # 処理済みに記録
                                    thread_db_manager.mark_file_as_processed(
                                        file_info["file_path"],
                                        file_info["file_hash"],
                                        file_info["source_zip_str"],
                                    )

                                    # コミット
                                    thread_db_manager.commit()
                                    result["success"] = True
                                    result["rows_inserted"] = rows_inserted
                                else:
                                    print(
                                        f"エラー: {file_info['file_path']} の処理結果がNoneです"
                                    )
                            except Exception as e:
                                # ロールバック
                                thread_db_manager.rollback()

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

                            return result
                    finally:
                        # データベース接続を閉じる
                        thread_db_manager.close()

                # タイムアウトしたファイル情報を記録するための辞書
                timeout_files = {}

                # 並列処理の実行
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=max_workers
                ) as executor:
                    # 各ファイルを並列処理
                    futures_dict = {}
                    file_info_dict = {}  # ファイル情報を保持する辞書
                    for file_info in files_to_process:
                        future = executor.submit(process_with_lock, file_info)
                        futures_dict[future] = file_info["file_path"]
                        file_info_dict[file_info["file_path"]] = (
                            file_info  # ファイル情報を保存
                        )
                        print(f"処理開始: {file_info['file_path']}")

                    # 処理中のファイル数を追跡
                    completed = 0
                    total = len(futures_dict)

                    # タイムアウト時間（秒）
                    timeout = 60

                    # 結果を集計
                    for future in concurrent.futures.as_completed(
                        futures_dict.keys(), timeout=timeout
                    ):
                        file_path = futures_dict[future]
                        try:
                            result = future.result(timeout=10)  # 個別のタイムアウト
                            completed += 1
                            print(f"処理完了 ({completed}/{total}): {file_path}")

                            if result["success"]:
                                stats["newly_processed"] += 1
                                file_name = Path(file_path).name
                                print(f"  成功: {file_name}")
                            else:
                                stats["failed"] += 1
                                file_name = Path(file_path).name
                                print(f"  失敗: {file_name}")
                        except concurrent.futures.TimeoutError:
                            completed += 1
                            print(
                                f"処理タイムアウト ({completed}/{total}): {file_path}"
                            )
                            stats["timeout"] += 1  # タイムアウトとしてカウント

                            # タイムアウトしたファイルの情報を記録
                            if file_path in file_info_dict:
                                timeout_files[file_path] = file_info_dict[file_path]
                        except Exception as e:
                            completed += 1
                            print(f"処理エラー ({completed}/{total}): {file_path}")
                            print(f"  エラー内容: {str(e)}")
                            stats["failed"] += 1

                    # 未完了のタスクをチェック
                    remaining = [
                        path
                        for future, path in futures_dict.items()
                        if not future.done()
                    ]
                    if remaining:
                        print(
                            f"警告: {len(remaining)}件のファイルが処理完了しませんでした:"
                        )
                        for path in remaining:
                            print(f"  - {path}")
                            stats["timeout"] += 1  # タイムアウトとしてカウント

                            # タイムアウトしたファイルの情報を記録
                            if path in file_info_dict:
                                timeout_files[path] = file_info_dict[path]

                    # タイムアウトしたファイルをデータベースから削除（処理済みマークを解除）
                    if timeout_files:
                        print(
                            f"タイムアウトした {len(timeout_files)} 件のファイルを処理済みマークから解除します"
                        )
                        for file_info in timeout_files.values():
                            # ファイルが処理済みとしてマークされている場合は削除
                            self.db_manager.unmark_file_as_processed(
                                file_info["file_path"], file_info["source_zip_str"]
                            )

                    # すべてのタスクが完了したことを確認
                    print(f"すべてのファイル処理が完了しました: {completed}/{total}")

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
