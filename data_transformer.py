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

import polars as pl

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
        # ただし空の要素が末尾にある場合は削除
        for i in range(len(header_rows)):
            # 末尾の空の要素を削除
            while header_rows[i] and not header_rows[i][-1].strip():
                header_rows[i].pop()
        
        # この時点でのヘッダー行の最大列数を取得
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
    
    def _read_sensor_metadata(self, file_path: str, encoding: str) -> Tuple[List[str], List[str], List[str]]:
        """CSVの先頭3行を手動で読み込み、センサーID行、センサー名行、ユニット行を返す。
        
        Args:
            file_path: CSVファイルパス
            encoding: ファイルのエンコーディング
            
        Returns:
            (センサーID行, センサー名行, ユニット行) のタプル
        """
        # 先頭3行を取得
        lines = []
        with open(file_path, 'r', encoding=encoding) as f:
            for _ in range(3):
                line = f.readline()
                if not line:
                    break
                lines.append(line.rstrip('\n'))
        
        # 行が3行以下ならエラーまたは処理打ち切り
        if len(lines) < 3:
            logger.warning(f"ファイル '{file_path}' の先頭3行が取得できません。形式不正の可能性。")
            return [], [], []
        
        # カンマ区切りで分割し、各行を処理
        meta1 = [col.strip() for col in lines[0].split(',')]
        meta2 = [col.strip() for col in lines[1].split(',')]
        meta3 = [col.strip() for col in lines[2].split(',')]
        
        # 末尾の空の要素を削除
        while meta1 and not meta1[-1]:
            meta1.pop()
        while meta2 and not meta2[-1]:
            meta2.pop()
        while meta3 and not meta3[-1]:
            meta3.pop()
        
        return meta1, meta2, meta3
    
    def _transform_csv_data_polars(self, utf8_file_path: str, file_info: Dict[str, Any]) -> List[Dict]:
        """Polarsを用いてCSVを高速に読み込み・縦持ち変換して、DuckDBに格納しやすい形式(list of dict)で返す。
        
        Args:
            utf8_file_path: UTF-8エンコードされたCSVファイルパス
            file_info: ファイル情報
            
        Returns:
            変換後のレコードリスト
        """
        # 先にセンサーメタデータを取得
        meta1, meta2, meta3 = self._read_sensor_metadata(utf8_file_path, encoding='utf-8')
        
        if len(meta1) < 2 or len(meta2) < 2 or len(meta3) < 2:
            # 十分なメタデータがない場合は空を返す
            return []
        
        # 以降の行をPolarsで読み込み
        try:
            # has_header=Falseで読んで、全列をstringとして受け取る
            df = pl.read_csv(
                utf8_file_path,
                skip_rows=3,
                has_header=False,
                infer_schema_length=0  # 大きなファイルでも一気に推定しようとしない
            )
        except Exception as e:
            logger.error(f"PolarsでのCSV読み込みに失敗しました: {e}")
            return []
        
        if df.height == 0:
            logger.warning(f"ファイル '{file_info['file_name']}' にデータ行がありません。")
            return []
        
        # カラム数チェック
        col_count = df.width
        
        # データ行の末尾の空の列を検出して除外する処理
        # 最後の列が全て空であるかチェック
        if col_count > len(meta1):
            last_col = df.select(pl.col(df.columns[-1]))
            # 最後の列が全て空または空白文字のみならば除外
            if last_col.is_empty() or (last_col.cast(pl.Utf8).str.strip().is_empty().all()):
                # 最後の列を除外
                df = df.select(df.columns[:-1])
                col_count = df.width
                logger.info(f"データ行の末尾に余分なカンマが検出されたため、末尾の空の列を除外しました。")
        
        # meta1,meta2,meta3は少なくともdfのカラム数と同じ以上の要素を含む必要がある（0列目=日付列+N列）
        if len(meta1) < col_count:
            logger.warning(f"ヘッダー列数({len(meta1)}) < 実データ列数({col_count})。一部列が対応付けできません。")
        # ここではエラーにはせずに、読み込めた範囲で処理する
        
        # カラム名を文字列に変換
        df.columns = [f"col_{i}" for i in range(col_count)]
        
        # 0列目(timestamp)を日時型に変換(失敗する場合はnull)
        # 注: str.strip()は直接使えないため、pl.lit().str_strip()またはstrptimeで直接処理
        df = df.with_columns(
            pl.col("col_0").str.strptime(pl.Datetime, "%Y/%m/%d %H:%M:%S", strict=False).alias("timestamp")
        )
        
        # meltで1列目以降を縦持ちに変換
        sensor_cols = [f"col_{i}" for i in range(1, col_count)]
        melted = df.melt(
            id_vars=["timestamp"],      # この列を主軸に
            value_vars=sensor_cols,     # melt対象の列一覧
            variable_name="col_index",  # melt後にどの列だったかを示す列
            value_name="value"          # 値を示す列
        )
        
        # valueをfloatに変換（変換に失敗する場合はnull）
        melted = melted.with_columns(
            pl.col("value").cast(pl.Float64, strict=False)
        )
        
        # col_indexからindex番号を抽出（"col_3" → 3）
        # 注: 正規表現置換の構文を最新のPolarsに合わせて修正
        melted = melted.with_columns(
            pl.col("col_index").str.replace_all("col_", "").cast(pl.UInt32).alias("col_num")
        )
        
        # メタ情報を辞書にまとめておく { col_num: (sensor_id, sensor_name, unit) }
        meta_map = {}
        for col_i in range(1, len(meta1)):
            sensor_id = meta1[col_i] if col_i < len(meta1) else ""
            sensor_name = meta2[col_i] if col_i < len(meta2) else "-"
            unit = meta3[col_i] if col_i < len(meta3) else "-"
            meta_map[col_i] = (sensor_id, sensor_name, unit)
        
        # Polars DataFrameをPython (list of dict)に変換
        final_records = []
        
        for row in melted.iter_rows(named=True):
            ts = row["timestamp"]
            col_num = row["col_num"]
            val = row["value"]
            
            # timestampがNoneの場合や、対応するメタ情報がない場合、valueがNoneの場合はスキップ
            if ts is None or col_num not in meta_map or val is None:
                continue
            
            sensor_id, sensor_name, unit = meta_map[col_num]
            
            final_records.append({
                'timestamp': ts,
                'sensor_id': sensor_id,
                'sensor_name': sensor_name,
                'unit': unit,
                'value': float(val),
                'file_path': file_info['file_path'],
                'file_name': file_info['file_name'],
                'load_timestamp': datetime.now(),
                'zip_path': file_info.get('zip_path', None)
            })
        
        return final_records
    
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
            # Polarsを使用した高速処理
            result_records = self._transform_csv_data_polars(utf8_file_path, file_info)
            
            # Polarsでの処理に失敗した場合やレコードが得られなかった場合は従来の手動処理を試みる
            if not result_records:
                logger.info(f"Polarsでの処理が失敗またはデータなし。従来の手動処理を試みます。")
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
            
            # 変換結果をDuckDBに挿入
            record_count = 0
            if transformed_records:
                try:
                    # 重複排除はクライアント側でメモリ内で行う
                    # センサーID、タイムスタンプ、ファイルパスの組み合わせごとに最新のレコードのみを保持
                    unique_records = {}
                    for record in transformed_records:
                        key = (record['timestamp'], record['sensor_id'], record['file_path'])
                        # 同じキーのレコードがすでにある場合、最新のレコードを保持
                        unique_records[key] = record
                    
                    # 一意のレコードのみをメインテーブルに挿入
                    for record in unique_records.values():
                        self.db_manager.conn.execute("""
                            INSERT INTO sensor_data (
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
                    
                    # レコード数をカウント
                    record_count = len(unique_records)
                    
                    self.db_manager.conn.commit()
                except Exception as e:
                    logger.error(f"データベース挿入エラー: {str(e)}")
                    raise
            
            # 処理済みファイル情報を登録
            self.db_manager.insert_processed_file(file_info)
            
            logger.info(f"ファイル '{file_info['file_name']}' の処理が完了しました。{record_count}件のレコードを登録しました。")
            return record_count
        except Exception as e:
            logger.error(f"ファイル '{file_info['file_name']}' の処理中にエラーが発生しました: {e}")
            raise
