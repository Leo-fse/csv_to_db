from pathlib import Path
import re
import zipfile
import tempfile
import os
import duckdb
import hashlib
import datetime

def get_file_hash(file_path):
    """ファイルのSHA256ハッシュを計算する"""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        # ファイルを小さなチャンクで読み込んでハッシュ計算
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def setup_database(db_path):
    """DuckDBデータベースを初期化する"""
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path VARCHAR,
            file_hash VARCHAR,
            source_zip VARCHAR,
            processed_date TIMESTAMP,
            PRIMARY KEY (file_path, source_zip)
        )
    """)
    return conn

def is_file_processed(conn, file_path, source_zip=None):
    """ファイルが既に処理済みかどうかを確認する"""
    if source_zip:
        result = conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE file_path = ? AND source_zip = ?",
            [str(file_path), str(source_zip)]
        ).fetchone()
    else:
        result = conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE file_path = ? AND source_zip IS NULL",
            [str(file_path)]
        ).fetchone()
    
    return result[0] > 0

def mark_file_as_processed(conn, file_path, file_hash, source_zip=None):
    """ファイルを処理済みとしてデータベースに記録する"""
    now = datetime.datetime.now()
    conn.execute(
        "INSERT INTO processed_files (file_path, file_hash, source_zip, processed_date) VALUES (?, ?, ?, ?)",
        [str(file_path), file_hash, source_zip, now]
    )

def process_csv_file(file_path, source_zip=None):
    """CSVファイルを処理する関数（実際の処理はここに実装）"""
    print(f"処理中: {file_path}" + (f" (from {source_zip})" if source_zip else ""))
    
    # ここに実際のCSV処理ロジックを実装する
    # 例: pandas でCSVを読み込んで何らかの処理を行う
    # import pandas as pd
    # df = pd.read_csv(file_path)
    # ... 処理 ...
    
    # 処理が成功したことを示す（実際の実装に応じて変更）
    return True

def extract_and_process_csv_files(folder_path, pattern, db_path, process_all=False):
    """
    フォルダ内およびZIPファイル内からCSVファイルを見つけて処理する
    
    Parameters:
    folder_path (str or Path): 検索対象のフォルダパス
    pattern (str): 正規表現パターン
    db_path (str or Path): DuckDBデータベースのパス
    process_all (bool): 処理済みファイルも再処理するかどうか
    
    Returns:
    dict: 処理結果の統計情報
    """
    # 結果統計
    stats = {
        "total_found": 0,
        "already_processed": 0,
        "newly_processed": 0,
        "failed": 0
    }
    
    # フォルダとデータベースのパスをPathオブジェクトに変換
    folder = Path(folder_path)
    db_path = Path(db_path)
    
    # データベース接続
    conn = setup_database(db_path)
    
    # 正規表現パターンをコンパイル
    regex = re.compile(pattern)
    
    # 一時ディレクトリを作成
    temp_dir = Path(tempfile.mkdtemp())
    
    try:
        # 通常のCSVファイルを処理
        for file in folder.rglob("*.csv"):
            if regex.search(file.name):
                stats["total_found"] += 1
                
                if not process_all and is_file_processed(conn, file):
                    stats["already_processed"] += 1
                    print(f"スキップ (既処理): {file}")
                    continue
                
                try:
                    # ファイルを処理
                    if process_csv_file(file):
                        # ハッシュを計算して処理済みとマーク
                        file_hash = get_file_hash(file)
                        mark_file_as_processed(conn, file, file_hash)
                        stats["newly_processed"] += 1
                    else:
                        stats["failed"] += 1
                except Exception as e:
                    print(f"エラー処理中 {file}: {str(e)}")
                    stats["failed"] += 1
        
        # ZIPファイルを処理
        for zip_file in folder.rglob("*.zip"):
            try:
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    # ZIPファイル内のファイル一覧を取得
                    zip_contents = zip_ref.namelist()
                    
                    # CSVファイルかつ条件に合うものを抽出
                    for file_in_zip in zip_contents:
                        if file_in_zip.endswith('.csv') and regex.search(Path(file_in_zip).name):
                            stats["total_found"] += 1
                            
                            if not process_all and is_file_processed(conn, file_in_zip, str(zip_file)):
                                stats["already_processed"] += 1
                                print(f"スキップ (既処理): {file_in_zip} in {zip_file}")
                                continue
                            
                            try:
                                # ファイルを一時ディレクトリに抽出
                                extracted_path = temp_dir / Path(file_in_zip).name
                                zip_ref.extract(file_in_zip, temp_dir)
                                
                                # 抽出したファイルを処理
                                if process_csv_file(extracted_path, str(zip_file)):
                                    # ハッシュを計算して処理済みとマーク
                                    file_hash = get_file_hash(extracted_path)
                                    mark_file_as_processed(conn, file_in_zip, file_hash, str(zip_file))
                                    stats["newly_processed"] += 1
                                else:
                                    stats["failed"] += 1
                            except Exception as e:
                                print(f"エラー処理中 {file_in_zip} in {zip_file}: {str(e)}")
                                stats["failed"] += 1
            except zipfile.BadZipFile:
                print(f"警告: {zip_file}は有効なZIPファイルではありません。")
    
    finally:
        # 一時ディレクトリを削除
        import shutil
        shutil.rmtree(temp_dir)
        
        # データベース接続をコミットして閉じる
        conn.commit()
        conn.close()
    
    return stats

# 使用例
if __name__ == "__main__":
    folder_path = "検索したいフォルダのパス"  # ここに実際のフォルダパスを入力
    db_path = "processed_files.duckdb"        # DuckDBデータベースのパス
    
    # "Cond"または"User"を含むファイル名の正規表現パターン
    pattern = r"(Cond|User)"
    
    # 処理実行
    stats = extract_and_process_csv_files(folder_path, pattern, db_path)
    
    # 結果の表示
    print("\n---- 処理結果 ----")
    print(f"見つかったファイル数: {stats['total_found']}")
    print(f"既に処理済み: {stats['already_processed']}")
    print(f"新たに処理: {stats['newly_processed']}")
    print(f"処理失敗: {stats['failed']}")