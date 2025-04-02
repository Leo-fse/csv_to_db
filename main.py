import datetime
import hashlib
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path

import duckdb
import polars as pl

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
            found_files.append({"path": file, "source_zip": None})

    # ZIPファイルを検索して中身を確認
    for zip_file in folder.rglob("*.zip"):
        try:
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                # ZIPファイル内のファイル一覧を取得
                zip_contents = zip_ref.namelist()

                # CSVファイルかつ条件に合うものを抽出
                for file_in_zip in zip_contents:
                    if file_in_zip.endswith(".csv") and regex.search(
                        Path(file_in_zip).name
                    ):
                        found_files.append(
                            {"path": file_in_zip, "source_zip": zip_file}
                        )
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
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        # ZIPファイル内のファイルパスを正規化
        normalized_path = file_path.replace("\\", "/")

        # ファイル名のみを取得
        file_name = Path(normalized_path).name

        # 出力先のフルパス
        output_path = output_dir / file_name

        # ファイルを抽出
        try:
            # まずそのままのパスで試す
            zip_ref.extract(normalized_path, output_dir)
            # 階層構造があればそのファイルへのフルパスを返す
            if "/" in normalized_path:
                return output_dir / normalized_path
            return output_path
        except KeyError:
            # 正確なパスでなければ、ファイル名でマッチするものを探す
            for zip_info in zip_ref.infolist():
                zip_file_path = zip_info.filename.replace("\\", "/")
                if (
                    zip_file_path.endswith("/" + file_name)
                    or zip_file_path == file_name
                ):
                    # 見つかったファイルを抽出
                    zip_ref.extract(zip_info, output_dir)
                    # 抽出されたファイルのパスを返す
                    if "/" in zip_info.filename:
                        return output_dir / zip_info.filename
                    return output_dir / file_name

            # ファイルが見つからない場合はエラー
            raise FileNotFoundError(
                f"ZIPファイル内に {file_path} または {file_name} が見つかりません。"
            )


# ====== データベース管理部分 ======


def setup_database(db_path):
    """DuckDBデータベースを初期化する"""
    conn = duckdb.connect(str(db_path))

    # processed_filesテーブルを作成し、file_hashに一意性制約を追加
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            file_path VARCHAR NOT NULL,
            file_hash VARCHAR NOT NULL,
            source_zip VARCHAR,
            processed_date TIMESTAMP,
            PRIMARY KEY (file_path, source_zip)
        )
    """)

    # file_hashに一意性インデックスが存在するか確認
    result = conn.execute("""
        SELECT COUNT(*) 
        FROM duckdb_indexes() 
        WHERE table_name = 'processed_files' AND index_name = 'idx_processed_files_hash'
    """).fetchone()

    # インデックスが存在しない場合は作成
    if result[0] == 0:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_files_hash 
            ON processed_files(file_hash)
        """)

    # センサーデータ格納テーブル
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            Time TIMESTAMP,
            value VARCHAR,
            sensor_id VARCHAR,
            sensor_name VARCHAR,
            unit VARCHAR,
            source_file VARCHAR,
            source_zip VARCHAR
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


def is_file_processed_by_path(conn, file_path, source_zip=None):
    """ファイルパスに基づいて処理済みかどうかを確認する"""
    source_zip_value = "" if source_zip is None else str(source_zip)
    result = conn.execute(
        "SELECT COUNT(*) FROM processed_files WHERE file_path = ? AND source_zip = ?",
        [str(file_path), source_zip_value],
    ).fetchone()

    return result[0] > 0


def is_file_processed_by_hash(conn, file_hash):
    """ファイルハッシュに基づいて処理済みかどうかを確認する"""
    result = conn.execute(
        "SELECT COUNT(*) FROM processed_files WHERE file_hash = ?", [file_hash]
    ).fetchone()

    return result[0] > 0


def mark_file_as_processed(conn, file_path, file_hash, source_zip=None):
    """ファイルを処理済みとしてデータベースに記録する"""
    now = datetime.datetime.now()
    source_zip_value = "" if source_zip is None else str(source_zip)

    # UPSERTパターンを使用して挿入（一意制約違反を防ぐ）
    try:
        conn.execute(
            """
            INSERT INTO processed_files (file_path, file_hash, source_zip, processed_date) 
            VALUES (?, ?, ?, ?)
        """,
            [str(file_path), file_hash, source_zip_value, now],
        )
    except duckdb.ConstraintException:
        print(f"  情報: 同一ハッシュ({file_hash})のファイルが既に処理済みです")


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

    # 1~3行目はヘッダー(1行目はセンサーID、2行目はセンサー名、３行目は単位)として読み込む
    # 4行目以降はデータとして読み込む
    header_df = pl.read_csv(
        file_path, n_rows=3, has_header=False, truncate_ragged_lines=True
    )

    # 1列多く読み込まれる（最終列は不要）
    # スキーマ推論の範囲を増やす。

    data_df = pl.read_csv(
        file_path,
        skip_rows=3,
        has_header=False,
        truncate_ragged_lines=True,
        infer_schema_length=10000,  # スキーマ推論の範囲を増やす
    )[:, :-1]

    # 列名を設定する（変換前）
    column_names = ["Time"] + [f"col_{i}" for i in range(1, data_df.width)]
    data_df.columns = column_names

    # 縦持ちデータにしたい
    # １列目を日時として、残りの列を値として読み込む
    data_df = data_df.unpivot(
        index=["Time"],
        on=[f"col_{i}" for i in range(1, data_df.width)],
        variable_name="sensor_column",
        value_name="value",
    )

    # センサー情報のマッピングを作成
    sensor_ids = list(header_df.row(0)[1:])
    sensor_names = list(header_df.row(1)[1:])
    sensor_units = list(header_df.row(2)[1:])

    # 列番号からセンサー情報へのマッピング辞書を作成
    sensor_info = {}
    for i, (sensor_id, sensor_name, sensor_unit) in enumerate(
        zip(sensor_ids, sensor_names, sensor_units)
    ):
        col_name = f"col_{i + 1}"
        sensor_info[col_name] = {
            "sensor_id": sensor_id,
            "sensor_name": sensor_name,
            "unit": sensor_unit,
        }

    # センサー情報を追加する関数
    def add_sensor_info(row):
        col_name = row["sensor_column"]
        info = sensor_info.get(
            col_name, {"sensor_id": "", "sensor_name": "", "unit": ""}
        )
        return {
            "sensor_id": info["sensor_id"],
            "sensor_name": info["sensor_name"],
            "unit": info["unit"],
        }

    # センサー情報を追加
    data_df = data_df.with_columns(
        [
            pl.struct(["sensor_column"])
            .map_elements(add_sensor_info, return_dtype=pl.Struct)
            .alias("sensor_info")
        ]
    )

    # 構造体を展開
    data_df = data_df.unnest("sensor_info")

    # Filter out rows where both sensor_id and sensor_name are "-"
    data_df = data_df.filter(
        ~(
            (pl.col("sensor_name").str.strip_chars() == "-")
            & (pl.col("unit").str.strip_chars() == "-")
        )
    )

    # Time列の末尾の空白を除去し、datetime型に変換する
    data_df = data_df.with_columns(
        pl.col("Time")
        .str.strip_chars()
        .str.strptime(pl.Datetime, format="%Y/%m/%d %H:%M:%S")
    )

    # Remove the sensor_column from the results
    data_df = data_df.drop("sensor_column")
    # Remove duplicate rows based on all columns
    data_df = data_df.unique()
    # 結果を表示
    print(data_df.head(2))

    return data_df


def check_disk_space(path, required_mb=100):
    """
    指定されたパスのディスクの空き容量をチェックする

    Parameters:
    path (str or Path): チェックするパス
    required_mb (int): 必要な最小空き容量（MB）

    Returns:
    bool: 十分な空き容量があるかどうか
    """
    try:
        path = Path(path)

        # パスが存在しない場合は、親ディレクトリを再帰的にチェック
        while not path.exists():
            parent = path.parent
            if parent == path:  # ルートディレクトリに達した場合
                path = Path.cwd()  # カレントディレクトリを使用
                break
            path = parent

        # ディスク使用量を取得
        total, used, free = shutil.disk_usage(path)

        # MB単位に変換
        free_mb = free / (1024 * 1024)

        print(f"ディスク空き容量: {free_mb:.2f} MB (必要: {required_mb} MB)")

        return free_mb >= required_mb
    except Exception as e:
        print(f"ディスク容量チェックエラー: {str(e)}")
        # エラーの場合は安全のためFalseを返す
        return False


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
    # ディスク容量チェック - カレントディレクトリをチェック
    if not check_disk_space(Path.cwd(), required_mb=500):
        print("警告: ディスク容量が不足しています。少なくとも500MB必要です。")
        print("処理を続行しますが、エラーが発生する可能性があります。")

    # 結果統計
    stats = {
        "total_found": len(csv_files),
        "already_processed_by_path": 0,
        "already_processed_by_hash": 0,
        "newly_processed": 0,
        "failed": 0,
    }

    # データベース接続
    conn = setup_database(db_path)

    # 一時ディレクトリを作成
    temp_dir = None
    files_to_process = []

    try:
        temp_dir = Path(tempfile.mkdtemp())

        # 前処理：処理済みファイルのフィルタリング
        for file_info in csv_files:
            file_path = file_info["path"]
            source_zip = file_info["source_zip"]
            source_zip_str = str(source_zip) if source_zip else None

            # パスベースで既に処理済みかチェック
            if not process_all and is_file_processed_by_path(
                conn, file_path, source_zip_str
            ):
                stats["already_processed_by_path"] += 1
                print(
                    f"スキップ (既処理 - パス一致): {file_path}"
                    + (f" (in {source_zip})" if source_zip else "")
                )
                continue

            # ZIPファイル内のファイルなら抽出
            try:
                if source_zip:
                    # 一時ファイルにZIPから抽出
                    actual_file_path = extract_from_zip(source_zip, file_path, temp_dir)
                else:
                    actual_file_path = file_path

                # ファイルハッシュを計算
                file_hash = get_file_hash(actual_file_path)

                # ハッシュベースで既に処理済みかチェック
                if not process_all and is_file_processed_by_hash(conn, file_hash):
                    stats["already_processed_by_hash"] += 1
                    print(
                        f"スキップ (既処理 - 内容一致): {file_path}"
                        + (f" (in {source_zip})" if source_zip else "")
                    )
                    continue

                # 処理対象リストに追加
                files_to_process.append(
                    {
                        "file_path": file_path,
                        "actual_file_path": actual_file_path,
                        "source_zip": source_zip,
                        "source_zip_str": source_zip_str,
                        "file_hash": file_hash,
                    }
                )
            except Exception as e:
                print(
                    f"エラー前処理中 {file_path}"
                    + (f" (in {source_zip})" if source_zip else "")
                    + f": {str(e)}"
                )
                stats["failed"] += 1

        # バッチ処理の設定
        batch_size = 10  # 一度に処理するファイル数

        # バッチ処理
        for i in range(0, len(files_to_process), batch_size):
            # 定期的にディスク容量をチェック
            if i > 0 and i % 50 == 0:
                if not check_disk_space(db_path, required_mb=200):
                    raise IOError(
                        "処理中にディスク容量が不足しました。処理を中断します。"
                    )

            batch = files_to_process[i : i + batch_size]

            # 各ファイルを処理
            for file_info in batch:
                try:
                    # ファイルを処理
                    data_df = process_csv_file(file_info["actual_file_path"])
                    if data_df is not None:
                        # ソースファイル情報を列として追加
                        data_df = data_df.with_columns(
                            [
                                pl.lit(str(file_info["file_path"])).alias(
                                    "source_file"
                                ),
                                pl.lit(
                                    str(file_info["source_zip"])
                                    if file_info["source_zip"]
                                    else ""
                                ).alias("source_zip"),
                            ]
                        )

                        # DuckDBへ保存（メモリ内処理でディスク使用量を削減）
                        # データフレームをレコードのリストに変換
                        records = data_df.rows()

                        # カラム名を取得
                        columns = data_df.columns

                        # プレースホルダを作成
                        placeholders = ", ".join(["?" for _ in columns])
                        columns_str = ", ".join(columns)

                        # バッチ挿入の準備
                        insert_batch_size = 500  # 一度に挿入する行数を減らす

                        # バッチ単位でデータを挿入
                        for j in range(0, len(records), insert_batch_size):
                            batch_records = records[j : j + insert_batch_size]
                            # バッチ挿入クエリを実行
                            conn.executemany(
                                f"INSERT INTO sensor_data ({columns_str}) VALUES ({placeholders})",
                                batch_records,
                            )

                            # 小さなバッチごとにコミット
                            conn.commit()

                        # 処理済みに記録
                        mark_file_as_processed(
                            conn,
                            file_info["file_path"],
                            file_info["file_hash"],
                            file_info["source_zip_str"],
                        )
                        stats["newly_processed"] += 1
                    else:
                        stats["failed"] += 1
                except Exception as e:
                    print(
                        f"エラー処理中 {file_info['file_path']}"
                        + (
                            f" (in {file_info['source_zip']})"
                            if file_info["source_zip"]
                            else ""
                        )
                        + f": {str(e)}"
                    )
                    stats["failed"] += 1

            # バッチごとにコミット
            conn.commit()

    except Exception as e:
        print(f"エラー: {str(e)}")
        stats["failed"] += (
            len(files_to_process) - stats["newly_processed"] - stats["failed"]
        )

    finally:
        # 一時ディレクトリを削除（必ず実行）
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir)
                print(f"一時ディレクトリを削除しました: {temp_dir}")
            except Exception as e:
                print(f"一時ディレクトリの削除に失敗しました: {str(e)}")

        # データベース接続を閉じる
        try:
            conn.close()
            print("データベース接続を閉じました")
        except Exception as e:
            print(f"データベース接続を閉じる際にエラーが発生しました: {str(e)}")

    return stats


def cleanup_temp_files():
    """一時ファイルをクリーンアップする"""
    try:
        # tempディレクトリ内の古い一時ディレクトリを検索
        temp_root = Path(tempfile.gettempdir())
        current_time = datetime.datetime.now()

        # 24時間以上前の一時ディレクトリを削除
        for item in temp_root.glob("tmp*"):
            if item.is_dir():
                try:
                    # ディレクトリの作成時間を取得
                    created_time = datetime.datetime.fromtimestamp(item.stat().st_ctime)
                    age_hours = (current_time - created_time).total_seconds() / 3600

                    # 24時間以上経過していれば削除
                    if age_hours > 24:
                        shutil.rmtree(item)
                        print(
                            f"古い一時ディレクトリを削除しました: {item} (作成から {age_hours:.1f} 時間)"
                        )
                except Exception as e:
                    print(f"一時ディレクトリ {item} の削除に失敗しました: {str(e)}")
    except Exception as e:
        print(f"一時ファイルのクリーンアップ中にエラーが発生しました: {str(e)}")


def vacuum_database(db_path):
    """データベースを最適化する"""
    try:
        print(f"データベース {db_path} を最適化しています...")
        conn = duckdb.connect(str(db_path))
        conn.execute("VACUUM")
        conn.close()
        print(f"データベースの最適化が完了しました")
        return True
    except Exception as e:
        print(f"データベースの最適化中にエラーが発生しました: {str(e)}")
        return False


# ====== メイン実行部分 ======


def main(folder_path, pattern, db_path, process_all=False, cleanup=True, vacuum=True):
    """
    メイン実行関数

    Parameters:
    folder_path (str or Path): 検索対象のフォルダパス
    pattern (str): 正規表現パターン
    db_path (str or Path): DuckDBデータベースのパス
    process_all (bool): 処理済みファイルも再処理するかどうか
    cleanup (bool): 古い一時ファイルをクリーンアップするかどうか
    vacuum (bool): 処理後にデータベースを最適化するかどうか
    """
    # ディスク容量チェック
    if not check_disk_space(Path.cwd(), required_mb=500):
        print(
            "警告: ディスク容量が不足しています。処理を続行する前に空き容量を確保してください。"
        )
        return

    # 古い一時ファイルのクリーンアップ
    if cleanup:
        cleanup_temp_files()

    # ファイル検索（抽出部分）
    print(f"フォルダ {folder_path} から条件に合うCSVファイルを検索中...")
    csv_files = find_csv_files(folder_path, pattern)
    print(f"{len(csv_files)}件のファイルが見つかりました")

    # ファイル処理（処理部分）
    print("CSVファイルの処理を開始します...")
    stats = process_csv_files(csv_files, db_path, process_all)

    # データベースの最適化
    if vacuum and stats["newly_processed"] > 0:
        vacuum_database(db_path)

    # 結果の表示
    print("\n---- 処理結果 ----")
    print(f"見つかったファイル数: {stats['total_found']}")
    print(f"パスで既に処理済み: {stats['already_processed_by_path']}")
    print(f"内容が同一で処理済み: {stats['already_processed_by_hash']}")
    print(f"新たに処理: {stats['newly_processed']}")
    print(f"処理失敗: {stats['failed']}")


# 使用例
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="CSVファイル処理ツール")
    parser.add_argument(
        "--folder", type=str, default="data", help="検索対象のフォルダパス"
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=r"(Cond|User|test)",
        help="ファイル名フィルタリングのための正規表現パターン",
    )
    parser.add_argument(
        "--db",
        type=str,
        default="processed_files.duckdb",
        help="処理記録用データベースファイルのパス",
    )
    parser.add_argument(
        "--process-all",
        action="store_true",
        help="処理済みファイルも再処理する場合に指定",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="古い一時ファイルをクリーンアップしない場合に指定",
    )
    parser.add_argument(
        "--no-vacuum",
        action="store_true",
        help="データベースの最適化を行わない場合に指定",
    )
    parser.add_argument(
        "--sensor-db",
        type=str,
        default="sensor_data.duckdb",
        help="センサーデータ保存用データベースファイルのパス",
    )

    args = parser.parse_args()

    # 処理実行
    main(
        args.folder,
        args.pattern,
        args.db,
        args.process_all,
        not args.no_cleanup,
        not args.no_vacuum,
    )
