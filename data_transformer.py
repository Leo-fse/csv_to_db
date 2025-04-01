"""
データ変換モジュール
CSVデータの読み込みと変換処理を担当
"""

from typing import Dict, Any, List, Tuple
from datetime import datetime
import logging
import os
import tempfile
import shutil
import re

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
    
    def _convert_to_utf8(self, file_path: str) -> Tuple[str, bool]:
        """CSVファイルをUTF-8に変換する
        
        Args:
            file_path: 元のCSVファイルパス
            
        Returns:
            (変換後のファイルパス, 一時ファイルが作成されたかどうか)
        """
        # エンコーディングを検出
        encoding = self._detect_encoding(file_path)
        logger.info(f"ファイル '{file_path}' のエンコーディングを '{encoding}' として処理します。")
        
        # すでにUTF-8の場合はそのまま返す
        if encoding.lower() in ['utf-8', 'utf8']:
            return file_path, False
        
        # 一時ファイルを作成
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        temp_file_path = temp_file.name
        temp_file.close()
        
        try:
            # 元のファイルを読み込み
            with open(file_path, 'r', encoding=encoding) as f_in:
                content = f_in.read()
            
            # UTF-8で書き込み
            with open(temp_file_path, 'w', encoding='utf-8') as f_out:
                f_out.write(content)
            
            logger.info(f"ファイル '{file_path}' を一時的にUTF-8に変換しました（{encoding} → UTF-8）")
            return temp_file_path, True
            
        except Exception as e:
            # エラーが発生した場合は一時ファイルを削除
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            
            logger.error(f"エンコーディング変換エラー: {str(e)}")
            raise
    
    def _process_csv_manually(self, file_path: str, file_info: Dict[str, Any]) -> List[Dict]:
        """CSVファイルを完全に手動で読み込んで処理する
        
        Args:
            file_path: CSVファイルパス
            file_info: ファイル情報
            
        Returns:
            変換後のレコードリスト
        """
        encoding = self._detect_encoding(file_path)
        
        # ファイルの全行を読み込む
        lines = []
        with open(file_path, 'r', encoding=encoding) as f:
            for line in f:
                line = line.strip()
                if line:  # 空行を除外
                    lines.append(line)
        
        if len(lines) < 4:  # ヘッダー3行 + データ行が少なくとも1行必要
            logger.warning(f"ファイル '{file_path}' には十分な行数がありません")
            return []
        
        # ヘッダー行を解析
        header_rows = [line.split(',') for line in lines[:3]]
        
        # 各行の列数が一致していない場合は調整
        max_cols = max(len(row) for row in header_rows)
        for i in range(len(header_rows)):
            # 不足している列を空文字で埋める
            while len(header_rows[i]) < max_cols:
                header_rows[i].append('')
            
            # 各列の空白を削除
            header_rows[i] = [col.strip() for col in header_rows[i]]
        
        # センサー情報を構築
        sensor_info = {}
        for col_idx in range(1, max_cols):  # 最初の列（日時列）をスキップ
            sensor_id = header_rows[0][col_idx] if col_idx < len(header_rows[0]) else ''
            if not sensor_id:
                continue
            
            sensor_name = header_rows[1][col_idx] if col_idx < len(header_rows[1]) else '-'
            unit = header_rows[2][col_idx] if col_idx < len(header_rows[2]) else '-'
            
            # センサー名と単位が「-」ではないカラムのみ保持
            if not (sensor_name == '-' and unit == '-'):
                sensor_info[col_idx] = {
                    'sensor_id': sensor_id,
                    'sensor_name': sensor_name,
                    'unit': unit
                }
        
        # データ行を処理
        results = []
        timestamp_pattern = re.compile(r'\d{4}/\d{1,2}/\d{1,2}\s+\d{1,2}:\d{1,2}:\d{1,2}')
        
        for line in lines[3:]:  # ヘッダー3行をスキップ
            cols = line.split(',')
            cols = [col.strip() for col in cols]
            
            # 必要に応じて列を追加
            while len(cols) < max_cols:
                cols.append('')
            
            # タイムスタンプのチェック（最初の列）
            if not cols[0] or not timestamp_pattern.match(cols[0]):
                continue
            
            # タイムスタンプを解析
            try:
                timestamp = datetime.strptime(cols[0], '%Y/%m/%d %H:%M:%S')
            except ValueError:
                logger.warning(f"無効な日時フォーマット: {cols[0]}")
                continue
            
            # 各センサーのデータを処理
            for col_idx, info in sensor_info.items():
                if col_idx < len(cols) and cols[col_idx]:
                    try:
                        # 数値に変換
                        value = float(cols[col_idx])
                        
                        # レコードを作成
                        results.append({
                            'timestamp': timestamp,
                            'sensor_id': info['sensor_id'],
                            'sensor_name': info['sensor_name'],
                            'unit': info['unit'],
                            'value': value,
                            'file_path': file_info['file_path'],
                            'file_name': file_info['file_name'],
                            'load_timestamp': datetime.now(),
                            'zip_path': file_info.get('zip_path', None)
                        })
                    except ValueError:
                        # 文字列値の場合は警告せずにスキップ
                        pass
        
        return results
    
    def _transform_csv_data(self, file_path: str, file_info: Dict[str, Any]) -> List[Dict]:
        """CSVデータを読み込んで変換する（縦持ち形式に変換）
        
        Args:
            file_path: CSVファイルパス
            file_info: ファイル情報
            
        Returns:
            変換後のレコードリスト
        """
        # 必要に応じてUTF-8に変換
        utf8_file_path, temp_file_created = self._convert_to_utf8(file_path)
        
        try:
            # ファイルを手動で処理
            result_records = self._process_csv_manually(utf8_file_path, file_info)
            
            if not result_records:
                logger.warning(f"ファイル '{file_info['file_name']}' に有効なセンサーデータがありませんでした")
            
            return result_records
            
        except Exception as e:
            logger.error(f"変換エラー: {str(e)}")
            raise
        finally:
            # エラーの有無にかかわらず一時ファイルを削除
            if temp_file_created and os.path.exists(utf8_file_path):
                os.unlink(utf8_file_path)
    
    def process_csv_file(self, file_info: Dict[str, Any]) -> int:
        """CSVファイルを処理してDBに格納する
        
        Args:
            file_info: ファイル情報
            
        Returns:
            処理されたレコード数
        """
        file_path = file_info['file_path']
        logger.info(f"ファイル '{file_info['file_name']}' を処理しています...")
        
        try:
            # CSVデータを変換
            transformed_records = self._transform_csv_data(file_path, file_info)
            
            # 既存の重複データを削除
            self.db_manager.conn.execute("""
                DELETE FROM sensor_data
                WHERE file_path = ?
            """, [file_info["file_path"]])
            
            # 変換結果をDuckDBに直接挿入（重複キーを避けるため一時テーブルを使用）
            record_count = 0
            if transformed_records:
                # 一時テーブルの作成（一意の名前を使用）
                temp_table_name = f"temp_sensor_data_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
                try:
                    # 一時テーブルの作成
                    self.db_manager.conn.execute(f"""
                        CREATE TEMPORARY TABLE {temp_table_name} (
                            timestamp TIMESTAMP,
                            sensor_id TEXT,
                            sensor_name TEXT,
                            unit TEXT,
                            value DOUBLE,
                            file_path TEXT,
                            file_name TEXT,
                            load_timestamp TIMESTAMP,
                            zip_path TEXT
                        )
                    """)
                    
                    # 一時テーブルにデータを挿入
                    for record in transformed_records:
                        self.db_manager.conn.execute(f"""
                            INSERT INTO {temp_table_name} (
                                timestamp, sensor_id, sensor_name, unit, value,
                                file_path, file_name, load_timestamp, zip_path
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, [
                            record['timestamp'],
                            record['sensor_id'],
                            record['sensor_name'],
                            record['unit'],
                            record['value'],
                            record['file_path'],
                            record['file_name'],
                            record['load_timestamp'],
                            record['zip_path']
                        ])
                    
                    # 一時テーブルから一意のレコードのみをメインテーブルに挿入
                    # GROUP BYを使用して重複を排除
                    # レコード数を事前に取得
                    record_count_result = self.db_manager.conn.execute(f"""
                        SELECT COUNT(*) FROM (
                            SELECT 
                                timestamp, 
                                sensor_id, 
                                file_path
                            FROM {temp_table_name}
                            GROUP BY timestamp, sensor_id, file_path
                        )
                    """).fetchone()
                    record_count = record_count_result[0] if record_count_result else 0
                    
                    # 一意のレコードをメインテーブルに挿入
                    self.db_manager.conn.execute(f"""
                        INSERT INTO sensor_data
                        SELECT 
                            timestamp, 
                            sensor_id, 
                            MAX(sensor_name) as sensor_name,
                            MAX(unit) as unit, 
                            MAX(value) as value,
                            file_path, 
                            MAX(file_name) as file_name, 
                            MAX(load_timestamp) as load_timestamp, 
                            MAX(zip_path) as zip_path
                        FROM {temp_table_name}
                        GROUP BY timestamp, sensor_id, file_path
                    """)
                    
                    self.db_manager.conn.commit()
                    
                finally:
                    # 一時テーブルを削除（エラーが発生しても削除を試みる）
                    try:
                        self.db_manager.conn.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
                    except Exception as e:
                        logger.warning(f"一時テーブル削除中にエラーが発生しました: {str(e)}")
            
            # 処理済みファイル情報を登録
            self.db_manager.insert_processed_file(file_info)
            
            logger.info(f"ファイル '{file_info['file_name']}' の処理が完了しました。{record_count}件のレコードを登録しました。")
            return record_count
        except Exception as e:
            pass