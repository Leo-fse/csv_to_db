"""
データ変換モジュール
CSVデータの読み込みと変換処理を担当
"""

import polars as pl
from typing import Dict, Any, List, Tuple
from datetime import datetime
import logging
import os

from utils import logger

class DataTransformer:
    """CSVデータの変換を行うクラス"""
    
    def __init__(self, db_manager):
        """初期化
        
        Args:
            db_manager: DatabaseManagerインスタンス
        """
        self.db_manager = db_manager
    
    def _detect_encoding(self, file_path: str) -> str:
        """CSVファイルのエンコーディングを検出する
        
        Args:
            file_path: CSVファイルパス
            
        Returns:
            検出されたエンコーディング（'shift-jis'または'utf-8'）
        """
        try:
            # まずShift-JISとして読み込みを試みる
            with open(file_path, 'r', encoding='shift-jis') as f:
                f.read(100)
            return 'shift-jis'
        except UnicodeDecodeError:
            # Shift-JISでエラーの場合はUTF-8を試す
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    f.read(100)
                return 'utf-8'
            except UnicodeDecodeError:
                # デフォルトとしてShift-JISを返す
                logger.warning(f"ファイル '{file_path}' のエンコーディングを検出できませんでした。Shift-JISとして処理します。")
                return 'shift-jis'
    
    def _read_csv_header(self, file_path: str) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
        """CSVファイルのヘッダー行（3行）を読み込む
        
        Args:
            file_path: CSVファイルパス
            
        Returns:
            (カラム名リスト, センサー情報辞書)
        """
        # ファイルのエンコーディングを検出
        encoding = self._detect_encoding(file_path)
        logger.info(f"ファイル '{file_path}' のエンコーディングを '{encoding}' として処理します。")
        
        # ヘッダー3行を読み込む（区切り文字や不整合に対応）
        header_df = pl.read_csv(file_path, n_rows=3, truncate_ragged_lines=True, separator=',', encoding=encoding)
        
        # カラム名（0列目は日時のため空欄になっている）
        columns = header_df.columns
        
        # センサー情報を格納する辞書
        sensor_info = {}
        
        # 各カラムの情報を取得
        for col in columns:
            if col == '':  # 0列目（日時列）
                continue
                
            # センサーID, センサー名, 単位を取得
            sensor_id = col
            sensor_name = header_df[0, col] if header_df.shape[0] > 0 else '-'
            unit = header_df[1, col] if header_df.shape[0] > 1 else '-'
            
            # センサー名と単位が「-」ではないカラムのみ保持
            if not (sensor_name == '-' and unit == '-'):
                sensor_info[col] = {
                    'sensor_id': sensor_id,
                    'sensor_name': sensor_name,
                    'unit': unit
                }
        
        return columns, sensor_info
    
    def _transform_csv_data(self, file_path: str, file_info: Dict[str, Any]) -> pl.LazyFrame:
        """CSVデータを読み込んで変換する（縦持ち形式に変換）
        
        Args:
            file_path: CSVファイルパス
            file_info: ファイル情報
            
        Returns:
            変換後のLazyFrame
        """
        # ヘッダー情報を取得
        columns, sensor_info = self._read_csv_header(file_path)
        
        # 有効なセンサーIDのみ抽出（センサー名と単位が両方「-」のカラムは除外）
        valid_columns = [''] + list(sensor_info.keys())  # 0列目（日時列）も含める
        
        # エンコーディングを検出
        encoding = self._detect_encoding(file_path)
        
        # LazyFrameでCSVを読み込む（ヘッダー行をスキップ）
        lazy_df = pl.scan_csv(
            file_path,
            skip_rows=3,  # ヘッダー3行をスキップ
            has_header=False,
            new_columns=columns,
            separator=',',
            truncate_ragged_lines=True,
            ignore_errors=True,
            encoding=encoding
        )
        
        # 有効なカラムのみ選択
        lazy_df = lazy_df.select(valid_columns)
        
        # 日時列の処理
        lazy_df = lazy_df.with_columns(
            pl.col('').alias('timestamp')
        ).drop('')
        
        # 空白除去
        lazy_df = lazy_df.with_columns(
            pl.col('timestamp').cast(pl.Utf8).str.strip_chars().alias('timestamp')
        )
        
        # 日時をタイムスタンプに変換
        lazy_df = lazy_df.with_columns(
            pl.col('timestamp').str.to_datetime('%Y/%m/%d %H:%M:%S')
        )
        
        # メタ情報を追加
        meta_cols = {
            'file_path': file_info['file_path'],
            'file_name': file_info['file_name'],
            'load_timestamp': datetime.now(),
            'zip_path': file_info.get('zip_path', None)
        }
        
        # 縦持ち（long/tall形式）に変換
        result_dfs = []
        
        for col, info in sensor_info.items():
            # 一列ずつ縦持ちデータに変換
            col_df = lazy_df.select(['timestamp', col]).filter(
                ~pl.col(col).is_null()  # NULL値を除外
            ).with_columns([
                pl.lit(info['sensor_id']).alias('sensor_id'),
                pl.lit(info['sensor_name']).alias('sensor_name'),
                pl.lit(info['unit']).alias('unit'),
                # 数値型に統一する（変換できない場合はnull）
                pl.col(col).cast(pl.Float64, strict=False).alias('value'),
                *[pl.lit(v).alias(k) for k, v in meta_cols.items()]
            ]).drop(col)
            
            result_dfs.append(col_df)
        
        # 結合前にスキーマが一致していることを確認
        if result_dfs:
            # すべてのデータフレームのスキーマを最初のデータフレームに合わせる
            first_schema = result_dfs[0].collect_schema()
            aligned_dfs = []
            
            for df in result_dfs:
                # スキーマを調整
                aligned_df = df
                for name, dtype in first_schema.items():
                    if name in df.collect_schema():
                        aligned_df = aligned_df.with_columns(
                            pl.col(name).cast(dtype, strict=False)
                        )
                aligned_dfs.append(aligned_df)
            
            return pl.concat(aligned_dfs, how='diagonal')
        else:
            # 有効なセンサーがなかった場合は空のLazyFrameを返す
            logger.warning(f"ファイル '{file_info['file_name']}' に有効なセンサーデータがありませんでした")
            schema = {
                'timestamp': pl.Datetime,
                'sensor_id': pl.Utf8,
                'sensor_name': pl.Utf8,
                'unit': pl.Utf8,
                'value': pl.Float64,
                'file_path': pl.Utf8,
                'file_name': pl.Utf8,
                'load_timestamp': pl.Datetime,
                'zip_path': pl.Utf8
            }
            return pl.LazyFrame(schema=schema)
    
    def process_csv_file(self, file_info: Dict[str, Any]) -> int:
        """CSVファイルを処理してDBに格納する
        
        Args:
            file_info: ファイル情報
            
        Returns:
            処理されたレコード数
        """
        file_path = file_info['file_path']
        logger.info(f"ファイル '{file_info['file_name']}' を処理しています...")
        
        # CSVデータを変換
        transformed_lf = self._transform_csv_data(file_path, file_info)
        
        # 変換結果をDuckDBに格納
        record_count = self.db_manager.insert_sensor_data(transformed_lf, file_info)
        
        # 処理済みファイル情報を登録
        self.db_manager.insert_processed_file(file_info)
        
        logger.info(f"ファイル '{file_info['file_name']}' の処理が完了しました。{record_count}件のレコードを登録しました。")
        return record_count
