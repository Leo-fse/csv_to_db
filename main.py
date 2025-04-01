#!/usr/bin/env python3
"""
CSV→DuckDB変換処理メインスクリプト
センサーデータを含むCSVファイルを読み込み、DuckDBに変換して格納する
"""

import os
import sys
import argparse
from typing import Dict, Any, List
import time

from utils import logger, setup_logger
from db_manager import DatabaseManager
from file_processor import FileProcessor

# デフォルトのデータディレクトリ
data_dir = "./data"

def parse_args():
    """コマンドライン引数を解析する"""
    parser = argparse.ArgumentParser(description='CSVファイルからセンサーデータをDuckDBに変換するツール')
    parser.add_argument('-d', '--data-dir', default=data_dir,
                        help=f'データディレクトリのパス (デフォルト: {data_dir})')
    parser.add_argument('-o', '--output', default='sensor_data.duckdb',
                        help='出力DuckDBファイルのパス (デフォルト: sensor_data.duckdb)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='詳細なログ出力を有効にする')
    return parser.parse_args()

def main():
    """メイン処理"""
    # コマンドライン引数の解析
    args = parse_args()
    
    # ログレベルの設定
    log_level = 'DEBUG' if args.verbose else 'INFO'
    setup_logger(level=getattr(logging, log_level))
    
    logger.info("CSV→DuckDB変換処理を開始します")
    
    # データディレクトリの確認
    if not os.path.exists(args.data_dir):
        logger.error(f"データディレクトリ '{args.data_dir}' が存在しません")
        return 1
    
    # 処理時間計測開始
    start_time = time.time()
    
    try:
        # データベース接続
        db_manager = DatabaseManager(args.output)
        
        # ファイル処理
        processor = FileProcessor(args.data_dir, db_manager)
        processed_count, error_count = processor.process_files()
        
        # 処理結果の表示
        total_records = db_manager.get_sensor_data_count()
        
        logger.info(f"処理が完了しました:")
        logger.info(f"- 処理ファイル数: {processed_count}")
        logger.info(f"- エラーファイル数: {error_count}")
        logger.info(f"- 総レコード数: {total_records}")
        
        # データベース接続を閉じる
        db_manager.close()
        
        # 処理時間の表示
        elapsed_time = time.time() - start_time
        logger.info(f"処理時間: {elapsed_time:.2f}秒")
        
        return 0
        
    except Exception as e:
        logger.exception(f"処理中にエラーが発生しました: {str(e)}")
        return 1

if __name__ == "__main__":
    import logging
    sys.exit(main())
