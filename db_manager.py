"""
データベース管理モジュール
DuckDBの操作と管理を担当
"""

import duckdb
import polars as pl
from typing import Dict, Any, List, Union
from datetime import datetime
import os

from utils import logger

class DatabaseManager:
    """DuckDBの管理を行うクラス"""
    
    def __init__(self, db_path: str = 'sensor_data.duckdb'):
        """初期化
        
        Args:
            db_path: DuckDBファイルのパス
        """
        self.db_path = db_path
        self.conn = None
        self.initialize_db()
    
    def initialize_db(self) -> None:
        """データベースを初期化する"""
        create_db = not os.path.exists(self.db_path)
        
        # データベースに接続
        self.conn = duckdb.connect(self.db_path)
        
        # 初回接続時にテーブル作成
        if create_db:
            logger.info(f"データベース '{self.db_path}' を新規作成します")
            
            # センサーデータテーブル
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data (
                    file_path TEXT,
                    file_name TEXT,
                    zip_path TEXT,
                    load_timestamp TIMESTAMP,
                    timestamp TIMESTAMP,
                    sensor_id TEXT,
                    sensor_name TEXT,
                    unit TEXT,
                    value DOUBLE,
                    PRIMARY KEY (timestamp, sensor_id, file_path)
                )
            """)
            
            # 処理済みファイルテーブル
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    file_path TEXT PRIMARY KEY,
                    file_name TEXT,
                    file_size INTEGER,
                    modified_time TIMESTAMP,
                    load_timestamp TIMESTAMP,
                    zip_path TEXT,
                    status TEXT
                )
            """)
            
            self.conn.commit()
    
    def close(self) -> None:
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def get_processed_files(self) -> List[Dict[str, Any]]:
        """処理済みファイルの一覧を取得する
        
        Returns:
            処理済みファイル情報のリスト
        """
        result = self.conn.execute("SELECT * FROM processed_files").fetchall()
        columns = ["file_path", "file_name", "file_size", "modified_time", 
                  "load_timestamp", "zip_path", "status"]
        
        records = []
        for row in result:
            record = {columns[i]: row[i] for i in range(len(columns))}
            records.append(record)
        
        return records
    
    def insert_processed_file(self, file_info: Dict[str, Any]) -> None:
        """処理済みファイル情報をデータベースに格納する
        
        Args:
            file_info: ファイル情報
        """
        # 既存レコードがあれば削除
        self.conn.execute("""
            DELETE FROM processed_files
            WHERE file_path = ?
        """, [file_info["file_path"]])
        
        # 新たにレコードを挿入
        self.conn.execute("""
            INSERT INTO processed_files (
                file_path, file_name, file_size, modified_time, 
                load_timestamp, zip_path, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            file_info["file_path"],
            file_info["file_name"],
            file_info["file_size"],
            file_info["modified_time"],
            datetime.now(),
            file_info.get("zip_path"),
            "completed"
        ])
        
        self.conn.commit()
    
    def insert_sensor_data(self, lazy_df: pl.LazyFrame, file_info: Dict[str, Any]) -> int:
        """センサーデータをDuckDBに格納する（PyArrowを使用）
        
        Args:
            lazy_df: 変換済みのLazyFrame
            file_info: ファイル情報
            
        Returns:
            挿入されたレコード数
        """
        # collect_schemaでスキーマを取得（warning回避のため）
        schema = lazy_df.collect_schema()
        if schema:  # スキーマが存在する場合（空でない場合）
            # 既存の重複データを削除
            self.conn.execute("""
                DELETE FROM sensor_data
                WHERE file_path = ?
            """, [file_info["file_path"]])
            
            # データを収集
            df = lazy_df.collect()
            if df.shape[0] > 0:
                # PyArrowを使用してデータを転送
                # Arrow形式に変換
                arrow_table = df.to_arrow()
                
                # Arrowテーブルを一時テーブルとしてDuckDBに登録
                self.conn.execute("CREATE TEMPORARY TABLE temp_sensor_data AS SELECT * FROM arrow_table")
                
                # メインテーブルに挿入
                self.conn.execute("""
                    INSERT INTO sensor_data
                    SELECT * FROM temp_sensor_data
                """)
                
                # 一時テーブル削除
                self.conn.execute("DROP TABLE temp_sensor_data")
                
                self.conn.commit()
                return df.shape[0]
        
        return 0
    
    def get_sensor_data_count(self) -> int:
        """センサーデータの総レコード数を取得する
        
        Returns:
            レコード数
        """
        result = self.conn.execute("SELECT COUNT(*) FROM sensor_data").fetchone()
        return result[0] if result else 0
    
    def get_sensor_summary(self) -> List[Dict[str, Any]]:
        """センサーデータの概要を取得する
        
        Returns:
            センサーごとの概要情報
        """
        result = self.conn.execute("""
            SELECT 
                sensor_id, 
                sensor_name, 
                unit, 
                COUNT(*) as record_count,
                MIN(timestamp) as first_timestamp,
                MAX(timestamp) as last_timestamp
            FROM sensor_data
            GROUP BY sensor_id, sensor_name, unit
            ORDER BY sensor_id
        """).fetchall()
        
        columns = ["sensor_id", "sensor_name", "unit", "record_count", 
                  "first_timestamp", "last_timestamp"]
        
        summary = []
        for row in result:
            record = {columns[i]: row[i] for i in range(len(columns))}
            summary.append(record)
        
        return summary
