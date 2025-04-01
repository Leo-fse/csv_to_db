"""
ファイル処理モジュール
CSV/ZIPファイルの検索と処理を担当
"""

import os
from typing import List, Dict, Any, Tuple, Set
from pathlib import Path
import logging

from utils import (
    find_csv_files, 
    is_target_csv_file, 
    extract_zip_file,
    clean_temp_dir,
    get_file_metadata,
    logger
)
from db_manager import DatabaseManager

class FileProcessor:
    """CSVファイルとZIPファイルの処理を行うクラス"""
    
    def __init__(self, data_dir: str, db_manager: DatabaseManager):
        """初期化

        Args:
            data_dir: データディレクトリのパス
            db_manager: DatabaseManagerインスタンス
        """
        self.data_dir = data_dir
        self.db_manager = db_manager
        self.temp_dirs = []
    
    def get_csv_files(self) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """処理対象のCSVファイルを取得する
        
        Returns:
            (通常のCSVファイル情報リスト, ZIP内のCSVファイル情報リスト)
        """
        # CSVファイルとZIPファイルを検索
        csv_file_paths, zip_files = find_csv_files(self.data_dir)
        
        # 通常のCSVファイル情報リスト
        csv_files_info = []
        for csv_path in csv_file_paths:
            file_info = get_file_metadata(csv_path)
            file_info["from_zip"] = False
            file_info["zip_path"] = None
            csv_files_info.append(file_info)
        
        # ZIP内のCSVファイル情報リスト
        zip_csv_files_info = []
        for zip_path, zip_info in zip_files.items():
            for csv_path in zip_info["extracted_csvs"]:
                file_info = get_file_metadata(csv_path)
                file_info["from_zip"] = True
                file_info["zip_path"] = zip_path
                file_info["temp_dir"] = zip_info["temp_dir"]
                zip_csv_files_info.append(file_info)
            
            # 一時ディレクトリを記録
            self.temp_dirs.append(zip_info["temp_dir"])
        
        return csv_files_info, zip_csv_files_info
    
    def filter_new_files(self, csv_files_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """未処理のCSVファイルをフィルタリングする
        
        Args:
            csv_files_info: CSVファイル情報リスト
            
        Returns:
            未処理のCSVファイル情報リスト
        """
        # 処理済みファイルのパスを取得
        processed_files = self.db_manager.get_processed_files()
        processed_paths = {record["file_path"] for record in processed_files}
        
        # 未処理のファイルのみ抽出
        new_files = []
        for file_info in csv_files_info:
            if file_info["file_path"] not in processed_paths:
                new_files.append(file_info)
            else:
                # 処理済みファイルの場合でも、サイズや更新日時が変わっていれば再処理
                for processed in processed_files:
                    if processed["file_path"] == file_info["file_path"]:
                        if (processed["file_size"] != file_info["file_size"] or
                            processed["modified_time"] != file_info["modified_time"]):
                            logger.info(f"ファイル '{file_info['file_name']}' は更新されています。再処理します。")
                            new_files.append(file_info)
                        break
        
        return new_files
    
    def cleanup(self) -> None:
        """一時ディレクトリなどの後処理を行う"""
        for temp_dir in self.temp_dirs:
            clean_temp_dir(temp_dir)
        
        self.temp_dirs = []
    
    def process_files(self) -> Tuple[int, int]:
        """ファイルの処理を実行する
        
        Returns:
            (処理されたファイル数, エラーが発生したファイル数)
        """
        processed_count = 0
        error_count = 0
        
        try:
            # CSVファイルを検索
            csv_files_info, zip_csv_files_info = self.get_csv_files()
            logger.info(f"{len(csv_files_info)}個の通常CSVファイル、{len(zip_csv_files_info)}個のZIP内CSVファイルを検出しました")
            
            # すべてのファイル情報を結合
            all_files_info = csv_files_info + zip_csv_files_info
            
            # 未処理のファイルのみ抽出
            new_files = self.filter_new_files(all_files_info)
            logger.info(f"{len(new_files)}個の未処理ファイルがあります")
            
            # 未処理ファイルを処理
            from data_transformer import DataTransformer
            transformer = DataTransformer(self.db_manager)
            
            for file_info in new_files:
                try:
                    logger.info(f"ファイル '{file_info['file_name']}' を処理します")
                    transformer.process_csv_file(file_info)
                    processed_count += 1
                except Exception as e:
                    logger.error(f"ファイル '{file_info['file_name']}' の処理中にエラーが発生しました: {str(e)}")
                    error_count += 1
            
            return processed_count, error_count
            
        finally:
            # 一時ディレクトリを削除
            self.cleanup()
