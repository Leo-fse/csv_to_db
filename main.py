"""
センサーデータCSVファイルを読み込み、DuckDBデータベースに変換するプログラム

特徴:
- Shift-JISエンコードのCSVファイル対応
- 3行ヘッダー形式（センサーID、センサー名、単位）の処理
- 末尾カンマの処理
- ZIPファイル内のCSVファイル対応
- ファイル名パターンによるフィルタリング
- 処理済みファイルのスキップ
- 縦持ちデータ形式への変換

使用方法:
python main.py --csv_path data --plant_name Plant1 --machine_no Machine1 --file_pattern "Cond|User"
"""

import os
import re
import zipfile
import io
import argparse
from datetime import datetime
import polars as pl
import duckdb


def parse_args():
    """コマンドライン引数を解析する"""
    parser = argparse.ArgumentParser(description="CSVセンサーデータをDuckDBに変換")
    parser.add_argument('--csv_path', type=str, default='data',
                        help='CSVファイルのあるディレクトリ')
    parser.add_argument('--plant_name', type=str, required=True,
                        help='プラント名')
    parser.add_argument('--machine_no', type=str, required=True,
                        help='機器番号')
    parser.add_argument('--file_pattern', type=str, default='Cond|User',
                        help='ファイル名フィルタリングのための正規表現パターン（デフォルト: "Cond|User"）')
    return parser.parse_args()


def get_target_files(base_dir, pattern):
    """
    通常のCSVファイルとZIPファイル内のCSVファイルをリストアップ
    
    Args:
        base_dir: 基本ディレクトリ
        pattern: ファイル名のフィルタリングパターン（正規表現）
    
    Returns:
        処理対象ファイルのリスト（パスとZIP内パスの組み合わせ）
    """
    target_files = []
    pattern_regex = re.compile(pattern)
    
    # ディレクトリ内の全ファイルを走査
    for root, _, files in os.walk(base_dir):
        for filename in files:
            file_path = os.path.join(root, filename)
            
            # ZIPファイルの場合
            if filename.lower().endswith('.zip'):
                try:
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        for zip_info in zip_ref.infolist():
                            # ZIPファイル内のCSVファイルをチェック
                            if zip_info.filename.lower().endswith('.csv'):
                                # パターンに一致するかチェック
                                if pattern_regex.search(os.path.basename(zip_info.filename)):
                                    target_files.append({
                                        'type': 'zip',
                                        'zip_path': file_path,
                                        'inner_path': zip_info.filename,
                                        'full_path': f"{file_path}:{zip_info.filename}"
                                    })
                except Exception as e:
                    print(f"警告: ZIPファイルの読み込みエラー {file_path}: {e}")
            
            # 通常のCSVファイルの場合
            elif filename.lower().endswith('.csv'):
                # パターンに一致するかチェック
                if pattern_regex.search(filename):
                    target_files.append({
                        'type': 'normal',
                        'full_path': file_path
                    })
    
    return target_files


def read_csv_headers(content_lines):
    """
    CSVファイルの最初の3行からヘッダー情報を抽出
    
    Args:
        content_lines: CSVファイルの行のリスト
    
    Returns:
        (sensor_ids, sensor_names, sensor_units)のタプル
    """
    if len(content_lines) < 3:
        raise ValueError("CSVファイルは少なくとも3行のヘッダーが必要です")
    
    # 各行を解析（最初の列と末尾の余分な列を除外）
    sensor_ids = content_lines[0].strip().split(',')
    sensor_names = content_lines[1].strip().split(',')
    sensor_units = content_lines[2].strip().split(',')
    
    # 最初の列（時間列のヘッダー）を除外
    sensor_ids = sensor_ids[1:]
    sensor_names = sensor_names[1:]
    sensor_units = sensor_units[1:]
    
    # 末尾の余分なカンマを処理（空の要素を削除）
    if sensor_ids and sensor_ids[-1] == '':
        sensor_ids = sensor_ids[:-1]
    if sensor_names and sensor_names[-1] == '':
        sensor_names = sensor_names[:-1]
    if sensor_units and sensor_units[-1] == '':
        sensor_units = sensor_units[:-1]
    
    # 各値の前後の空白を削除
    sensor_ids = [sid.strip() for sid in sensor_ids]
    sensor_names = [name.strip() for name in sensor_names]
    sensor_units = [unit.strip() for unit in sensor_units]
    
    return sensor_ids, sensor_names, sensor_units


def read_csv_file(file_info):
    """
    通常のCSVファイルまたはZIP内のCSVファイルを読み込む
    
    Args:
        file_info: ファイル情報の辞書
    
    Returns:
        (sensor_ids, sensor_names, sensor_units, data_df)のタプル
    """
    try:
        if file_info['type'] == 'normal':
            # 通常のファイルの場合
            file_path = file_info['full_path']
            
            # ヘッダー3行を別々に読み込む
            with open(file_path, 'r', encoding='shift-jis') as f:
                content_lines = f.readlines()
                sensor_ids, sensor_names, sensor_units = read_csv_headers(content_lines)
            
            # データ部分（4行目以降）を読み込む
            data_df = pl.read_csv(
                file_path,
                encoding="shift-jis",
                skip_rows=3,
                has_header=False,
                truncate_ragged_lines=True
            )
        
        else:  # 'zip'の場合
            # ZIPファイル内のCSVを読み込む
            with zipfile.ZipFile(file_info['zip_path'], 'r') as zip_ref:
                with zip_ref.open(file_info['inner_path']) as csv_file:
                    # ZIPからバイナリとして読み込んでデコード
                    content = csv_file.read().decode('shift-jis')
                    content_lines = content.splitlines()
                    
                    # ヘッダー3行を解析
                    sensor_ids, sensor_names, sensor_units = read_csv_headers(content_lines)
                    
                    # データ部分をPolarsで読み込む
                    data_content = '\n'.join(content_lines[3:])
                    data_df = pl.read_csv(
                        io.StringIO(data_content),
                        has_header=False,
                        truncate_ragged_lines=True
                    )
        
        # 余分な列を削除（末尾カンマによる）
        if data_df.shape[1] > len(sensor_ids) + 1:
            data_df = data_df.drop(data_df.columns[-1])
        
        # 列名を設定（1列目は時間、残りはセンサー値）
        data_df.columns = ["timestamp"] + [f"value_{i}" for i in range(len(sensor_ids))]
        
        return sensor_ids, sensor_names, sensor_units, data_df
    
    except Exception as e:
        raise ValueError(f"CSVファイルの読み込みエラー: {e}")


def is_file_processed(conn, file_info):
    """
    ファイルが既に処理済みかチェック
    
    Args:
        conn: DuckDB接続
        file_info: ファイル情報の辞書
    
    Returns:
        処理済みならTrue、そうでなければFalse
    """
    # ZIPファイルの場合はZIPパスと内部パスの組み合わせをキーとする
    file_path = file_info['full_path']
    
    result = conn.execute("""
        SELECT 1 FROM processed_files
        WHERE file_path = ?
    """, [file_path]).fetchone()
    
    return result is not None


def mark_file_as_processed(conn, file_info):
    """
    ファイルを処理済みとマーク
    
    Args:
        conn: DuckDB接続
        file_info: ファイル情報の辞書
    """
    file_path = file_info['full_path']
    
    conn.execute("""
        INSERT INTO processed_files (file_path, processed_at)
        VALUES (?, CURRENT_TIMESTAMP)
    """, [file_path])


def convert_to_vertical_df(data_df, sensor_ids, sensor_names, sensor_units, plant_name, machine_no):
    """
    センサーデータを縦持ちデータフレームに変換
    
    Args:
        data_df: センサーデータのデータフレーム
        sensor_ids: センサーID配列
        sensor_names: センサー名配列
        sensor_units: センサー単位配列
        plant_name: プラント名
        machine_no: 機器番号
    
    Returns:
        縦持ちデータフレーム
    """
    vertical_data = []
    
    for i in range(len(sensor_ids)):
        sensor_id = sensor_ids[i]
        sensor_name = sensor_names[i]
        sensor_unit = sensor_units[i]
        
        # 不要センサーのスキップ
        if sensor_name == "-" and sensor_unit == "-":
            continue
        
        # 値の列インデックス
        value_col = f"value_{i}"
        
        # 縦持ちデータ作成
        try:
            # すべての値を文字列として扱うことで型の互換性問題を回避
            sensor_df = data_df.select([
                pl.lit(plant_name).alias("plant_name"),
                pl.lit(machine_no).alias("machine_no"),
                pl.col("timestamp"),
                pl.lit(sensor_id).alias("sensor_id"),
                pl.lit(sensor_name).alias("sensor_name"),
                pl.lit(sensor_unit).alias("sensor_unit"),
                pl.col(value_col).cast(pl.Utf8).alias("value")  # すべての値を文字列に変換
            ])
            vertical_data.append(sensor_df)
        except Exception as e:
            print(f"警告: センサーID:{sensor_id}, 名前:{sensor_name}の処理中にエラーが発生しました: {e}")
    
    # 全センサーデータの結合
    if not vertical_data:
        return None
    
    return pl.concat(vertical_data)


def init_database(db_path):
    """
    データベースの初期化
    
    Args:
        db_path: データベースファイルのパス
    
    Returns:
        DuckDB接続オブジェクト
    """
    conn = duckdb.connect(db_path)
    
    # テーブル作成（初回のみ）
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS sensor_id_seq;
        
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER DEFAULT(nextval('sensor_id_seq')),
            plant_name VARCHAR,
            machine_no VARCHAR,
            time TIMESTAMP,
            sensor_id VARCHAR,
            sensor_name VARCHAR,
            sensor_unit VARCHAR,
            value VARCHAR,  -- 数値か文字列かに応じて柔軟に対応する文字列型
            PRIMARY KEY(id)
        )
    """)
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path VARCHAR PRIMARY KEY,
            processed_at TIMESTAMP
        )
    """)
    
    return conn


def main():
    """メイン処理"""
    # コマンドライン引数の解析
    args = parse_args()
    
    print(f"開始: {datetime.now()}")
    print(f"ディレクトリ: {args.csv_path}")
    print(f"プラント名: {args.plant_name}")
    print(f"機器番号: {args.machine_no}")
    print(f"ファイルパターン: {args.file_pattern}")
    
    # データベース接続
    conn = init_database("sensor_data.duckdb")
    
    # 処理対象ファイルのリストを取得
    target_files = get_target_files(args.csv_path, args.file_pattern)
    
    print(f"{len(target_files)}個のファイルが処理対象として見つかりました")
    
    # 処理済みファイル数と新規処理ファイル数のカウンタ
    processed_count = 0
    skipped_count = 0
    
    # 各ファイルを処理
    for file_info in target_files:
        file_path = file_info['full_path']
        
        # 既に処理済みかチェック
        if is_file_processed(conn, file_info):
            print(f"スキップ: {file_path} (既に処理済み)")
            skipped_count += 1
            continue
        
        print(f"処理中: {file_path}")
        
        try:
            # ファイル読み込み
            sensor_ids, sensor_names, sensor_units, data_df = read_csv_file(file_info)
            
            # 縦持ちデータに変換
            result_df = convert_to_vertical_df(
                data_df, sensor_ids, sensor_names, sensor_units, 
                args.plant_name, args.machine_no
            )
            
            if result_df is not None and len(result_df) > 0:
                # タイムスタンプの変換
                try:
                    result_df = result_df.with_columns(
                        pl.col("timestamp").str.to_datetime("%Y/%m/%d %H:%M:%S").alias("time")
                    ).drop("timestamp")
                except Exception as e:
                    print(f"  警告: タイムスタンプ変換エラー: {e}")
                    result_df = result_df.rename({"timestamp": "time"})
                
                # データの挿入（IDは自動生成させる）
                conn.execute("""
                    INSERT INTO sensor_data (plant_name, machine_no, time, sensor_id, sensor_name, sensor_unit, value)
                    SELECT plant_name, machine_no, time, sensor_id, sensor_name, sensor_unit, value 
                    FROM result_df
                """)
                
                row_count = len(result_df)
                print(f"  {row_count}行のデータを挿入しました")
                
                # 処理済みマーク
                mark_file_as_processed(conn, file_info)
                processed_count += 1
            else:
                print(f"  処理可能なセンサーデータが見つかりませんでした")
            
        except Exception as e:
            print(f"エラー: {file_path} の処理中にエラーが発生しました: {e}")
    
    # 接続を閉じる
    conn.close()
    
    print("\n処理サマリ:")
    print(f"- 処理対象ファイル数: {len(target_files)}")
    print(f"- 処理済みファイル数: {processed_count}")
    print(f"- スキップされたファイル数: {skipped_count}")
    print(f"- エラーファイル数: {len(target_files) - processed_count - skipped_count}")
    print(f"\n完了: {datetime.now()}")


if __name__ == "__main__":
    main()
