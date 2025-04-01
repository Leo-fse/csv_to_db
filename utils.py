"""
ユーティリティ関数モジュール
CSV→DuckDB変換処理で使用する汎用関数を提供
"""

import os
import logging
import tempfile
import shutil
from pathlib import Path
import zipfile
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

# ロギング設定
def setup_logger(name: str = 'csv_to_db', level: int = logging.INFO) -> logging.Logger:
    """ロガーの設定を行う

    Args:
        name: ロガー名
        level: ログレベル

    Returns:
        設定済みのロガーインスタンス
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

# グローバルロガーインスタンス
logger = setup_logger()

def is_target_csv_file(filename: str) -> bool:
    """処理対象のCSVファイルかどうかを判定する
    
    Args:
        filename: 検証するファイル名
        
    Returns:
        ファイル名に「Cond」または「User」が含まれるかつ拡張子が.csvの場合True
    """
    base_name = os.path.basename(filename).lower()
    return (base_name.endswith('.csv') and 
            ('cond' in base_name.lower() or 'user' in base_name.lower()))

def extract_zip_file(zip_path: str, extract_dir: Optional[str] = None) -> str:
    """ZIPファイルを解凍する
    
    Args:
        zip_path: ZIPファイルのパス
        extract_dir: 解凍先ディレクトリ（指定されていない場合は一時ディレクトリを作成）
        
    Returns:
        解凍されたディレクトリのパス
    """
    if extract_dir is None:
        extract_dir = tempfile.mkdtemp(prefix="csv_to_db_")
    
    logger.info(f"ZIPファイル '{zip_path}' を '{extract_dir}' に解凍します")
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    return extract_dir

def clean_temp_dir(dir_path: str) -> None:
    """一時ディレクトリを削除する
    
    Args:
        dir_path: 削除する一時ディレクトリのパス
    """
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        logger.info(f"一時ディレクトリ '{dir_path}' を削除します")
        shutil.rmtree(dir_path)

def get_file_metadata(file_path: str) -> Dict[str, Any]:
    """ファイルのメタデータを取得する
    
    Args:
        file_path: ファイルパス
        
    Returns:
        ファイルメタデータを含む辞書
    """
    file_stat = os.stat(file_path)
    file_name = os.path.basename(file_path)
    
    return {
        "file_path": file_path,
        "file_name": file_name,
        "file_size": file_stat.st_size,
        "modified_time": datetime.fromtimestamp(file_stat.st_mtime),
        "load_timestamp": datetime.now()
    }

def find_csv_files(directory: str) -> Tuple[List[str], Dict[str, List[str]]]:
    """指定ディレクトリ内のCSVファイルとZIPファイルを検索する
    
    Args:
        directory: 検索対象ディレクトリ
        
    Returns:
        (CSVファイルパスのリスト, {ZIPファイルパス: 抽出されたCSVファイルパスのリスト}の辞書)
    """
    csv_files = []
    zip_files = {}
    temp_dirs = []
    
    # ディレクトリ内のファイルをスキャン
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            
            if file.lower().endswith('.csv'):
                if is_target_csv_file(file):
                    csv_files.append(file_path)
            elif file.lower().endswith('.zip'):
                # ZIPファイルを一時ディレクトリに解凍
                temp_dir = extract_zip_file(file_path)
                temp_dirs.append(temp_dir)
                
                # 解凍したディレクトリ内のCSVファイルを検索
                extracted_csvs = []
                for ext_root, _, ext_files in os.walk(temp_dir):
                    for ext_file in ext_files:
                        ext_file_path = os.path.join(ext_root, ext_file)
                        if ext_file.lower().endswith('.csv') and is_target_csv_file(ext_file):
                            extracted_csvs.append(ext_file_path)
                
                if extracted_csvs:
                    zip_files[file_path] = {
                        "extracted_csvs": extracted_csvs,
                        "temp_dir": temp_dir
                    }
    
    return csv_files, zip_files
