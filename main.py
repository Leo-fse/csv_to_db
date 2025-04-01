from pathlib import Path
import re
import zipfile
import tempfile
import os
import duckdb
import hashlib
import datetime
import shutil

# ====== ファイル抽出部分 ======

def find_csv_files(folder_path, pattern):
    """
    フォルダ内およびZIPファイル内から正規表現パターンに一致するCSVファイルを抽出する
    
    Parameters:
    folder_path (str or Path): 検索対象のフォルダパス
    pattern (str): 正規表現パターン
    
    Returns:
    list: [{'path': ファイルパス, 'source_zip': ZIPファイルパス（ない場合はNone）}]
    """
    found_files = []
    
    # Pathオブジェクトへ変換
    folder = Path(folder_path)
    
    # コンパイル済み正規表現パターン
    regex = re.compile(pattern)
    
    # 通常のCSVファイルを検索
    for file in folder.rglob("*.csv"):
        if regex.search(file.name):
            found_files.append({
                'path': file,
                'source_zip': None
            })
    
    # ZIPファイルを検索して中身を確認
    for zip_file in folder.rglob("*.zip"):
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # ZIPファイル内のファイル一覧を取得
                zip_contents = zip_ref.namelist()
                
                # CSVファイルかつ条件に合うものを抽出
                for file_in_zip in zip_contents:
                    if file_in_zip.endswith('.csv') and regex.search(Path(file_in_zip).name):
                        found_files.append({
                            'path': file_in_zip,
                            'source_zip': zip_file
                        })
        except zipfile.BadZipFile:
            print(f"警告: {zip_file}は有効なZIPファイルではありません。")
    
    return found_files

def extract_from_zip(zip_path, file_path, output_dir):
    """
    ZIPファイルから特定のファイルを抽出する（改良版）
    
    Parameters:
    zip_path (str or Path): ZIPファイルのパス
    file_path (str): 抽出するファイルのZIP内パス
    output_dir (str or Path): 出力先ディレクトリ
    
    Returns:
    Path: 抽出されたファイルのパス
    """
    # 出力ディレクトリの確認と作成
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # ZIPファイルを開いて処理
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # ZIPファイル内のファイルパスを正規化
        normalized_path = file_path.replace('\\', '/')
        
        # ファイル名のみを取得
        file_name = Path(normalized_path).name
        
        # 出力先のフルパス
        output_path = output_dir / file_name
        
        # ファイルを抽出
        try:
            # まずそのままのパスで試す
            zip_ref.extract(normalized_path, output_dir)
            # 階層構造があればそのファイルへのフルパスを返す
            if '/' in normalized_path:
                return output_dir / normalized_path
            return output_path
        except KeyError:
            # 正確なパスでなければ、ファイル名でマッチするものを探す
            for zip_info in zip_ref.infolist():
                zip_file_path = zip_info.filename.replace('\\', '/')
                if zip_file_path.endswith('/' + file_name) or zip_file_path == file_name:
                    # 見つかったファイルを抽出
                    zip_ref.extract(zip_info, output_dir)
                    # 抽出されたファイルのパスを返す
                    if '/' in zip_info.filename:
                        return output_dir / zip_info.filename
                    return output_dir / file_name
            
            # ファイルが見つからない場合はエラー
            raise FileNotFoundError(f"ZIPファイル内に {file_path} または {file_name} が見つかりません。")

# ====== データベース管理部分 ======

def setup_database(db_path):
    """DuckDBデータベースを初期化する"""
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path VARCHAR NOT NULL,,
            file_hash VARCHAR NOT NULL,,
            source_zip VARCHAR,
            processed_date TIMESTAMP,
            PRIMARY KEY (file_path, source_zip)
        )
    """)
    return conn

def get_file_hash(file_path):
    """ファイルのSHA256ハッシュを計算する"""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # ファイルを小さなチャンクで読み込んでハッシュ計算
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def is_file_processed(conn, file_path, source_zip=None):
    """ファイルが既に処理済みかどうかを確認する"""
    source_zip_value = "" if source_zip is None else str(source_zip)
    result = conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE file_path = ? AND source_zip = ?",
            [str(file_path), source_zip]
        ).fetchone()

    
    return result[0] > 0

def mark_file_as_processed(conn, file_path, file_hash, source_zip=None):
    """ファイルを処理済みとしてデータベースに記録する"""
    now = datetime.datetime.now()
    source_zip_value = "" if source_zip is None else str(source_zip)
    conn.execute(
        "INSERT INTO processed_files (file_path, file_hash, source_zip, processed_date) VALUES (?, ?, ?, ?)",
        [str(file_path), file_hash, source_zip, now]
    )

# ====== 処理実行部分 ======

def process_csv_file(file_path):
    """
    CSVファイルを処理する関数（実際の処理はここに実装）
    
    Parameters:
    file_path (Path): 処理するCSVファイルのパス
    
    Returns:
    bool: 処理が成功したかどうか
    """
    print(f"処理中: {file_path}")
    
    # ここに実際のCSV処理ロジックを実装する
    # 例: pandas でCSVを読み込んで何らかの処理を行う
    # import pandas as pd
    # df = pd.read_csv(file_path)
    # ... 処理 ...
    
    # 処理が成功したことを示す（実際の実装に応じて変更）
    return True

def process_csv_files(csv_files, db_path, process_all=False):
    """
    CSVファイルのリストを処理する
    
    Parameters:
    csv_files (list): 処理するCSVファイルのリスト
    db_path (str or Path): DuckDBデータベースのパス
    process_all (bool): 処理済みファイルも再処理するかどうか
    
    Returns:
    dict: 処理結果の統計情報
    """
    # 結果統計
    stats = {
        "total_found": len(csv_files),
        "already_processed": 0,
        "newly_processed": 0,
        "failed": 0
    }
    
    # データベース接続
    conn = setup_database(db_path)
    
    # 一時ディレクトリを作成
    temp_dir = Path(tempfile.mkdtemp())
    
    try:
        for file_info in csv_files:
            file_path = file_info['path']
            source_zip = file_info['source_zip']
            
            # 既に処理済みかチェック
            if not process_all and is_file_processed(conn, file_path, str(source_zip) if source_zip else None):
                stats["already_processed"] += 1
                print(f"スキップ (既処理): {file_path}" + (f" (in {source_zip})" if source_zip else ""))
                continue
            
            try:
                # ZIPファイル内のファイルなら抽出
                if source_zip:
                    # 一時ファイルにZIPから抽出
                    actual_file_path = extract_from_zip(source_zip, file_path, temp_dir)
                else:
                    actual_file_path = file_path
                
                # ファイルを処理
                if process_csv_file(actual_file_path):
                    # ハッシュを計算して処理済みとマーク
                    file_hash = get_file_hash(actual_file_path)
                    mark_file_as_processed(
                        conn, 
                        file_path, 
                        file_hash, 
                        str(source_zip) if source_zip else None
                    )
                    stats["newly_processed"] += 1
                else:
                    stats["failed"] += 1
            except Exception as e:
                print(f"エラー処理中 {file_path}" + (f" (in {source_zip})" if source_zip else "") + f": {str(e)}")
                stats["failed"] += 1
    
    finally:
        # 一時ディレクトリを削除
        shutil.rmtree(temp_dir)
        
        # データベース接続をコミットして閉じる
        conn.commit()
        conn.close()
    
    return stats

# ====== メイン実行部分 ======

def main(folder_path, pattern, db_path, process_all=False):
    """
    メイン実行関数
    
    Parameters:
    folder_path (str or Path): 検索対象のフォルダパス
    pattern (str): 正規表現パターン
    db_path (str or Path): DuckDBデータベースのパス
    process_all (bool): 処理済みファイルも再処理するかどうか
    """
    # ファイル検索（抽出部分）
    print(f"フォルダ {folder_path} から条件に合うCSVファイルを検索中...")
    csv_files = find_csv_files(folder_path, pattern)
    print(f"{len(csv_files)}件のファイルが見つかりました")
    
    # ファイル処理（処理部分）
    print("CSVファイルの処理を開始します...")
    stats = process_csv_files(csv_files, db_path, process_all)
    
    # 結果の表示
    print("\n---- 処理結果 ----")
    print(f"見つかったファイル数: {stats['total_found']}")
    print(f"既に処理済み: {stats['already_processed']}")
    print(f"新たに処理: {stats['newly_processed']}")
    print(f"処理失敗: {stats['failed']}")

# 使用例
if __name__ == "__main__":
    folder_path = "検索したいフォルダのパス"  # ここに実際のフォルダパスを入力
    db_path = "processed_files.duckdb"        # DuckDBデータベースのパス
    
    # "Cond"または"User"を含むファイル名の正規表現パターン
    pattern = r"(Cond|User)"
    
    # 処理実行
    main(folder_path, pattern, db_path, process_all=False)