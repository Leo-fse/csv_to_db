import argparse
import datetime
import os
from pathlib import Path

import duckdb
import polars as pl
from dotenv import load_dotenv


def query_sensor_data(
    db_path,
    plant_name=None,
    machine_no=None,
    start_time=None,
    end_time=None,
    sensor_names=None,
):
    """
    条件に基づいてセンサーデータを検索し、横持ちデータ（ピボットテーブル）として返す

    Parameters:
    db_path (str or Path): DuckDBデータベースのパス
    plant_name (str, optional): 工場名
    machine_no (str, optional): 機械ID
    start_time (datetime, optional): 開始時間
    end_time (datetime, optional): 終了時間
    sensor_names (list, optional): センサー名のリスト

    Returns:
    pl.DataFrame: 横持ちデータ（ピボットテーブル）
    """
    # データベースに接続
    conn = duckdb.connect(str(db_path))

    try:
        # 条件に基づいてクエリを構築
        query = "SELECT time, sensor_name, value FROM sensor_data WHERE 1=1"
        params = []

        if plant_name:
            query += " AND plant_name = ?"
            params.append(plant_name)

        if machine_no:
            query += " AND machine_no = ?"
            params.append(machine_no)

        if start_time:
            query += " AND time >= ?"
            params.append(start_time)

        if end_time:
            query += " AND time <= ?"
            params.append(end_time)

        if sensor_names and len(sensor_names) > 0:
            placeholders = ", ".join(["?" for _ in sensor_names])
            query += f" AND sensor_name IN ({placeholders})"
            params.extend(sensor_names)

        # クエリを実行
        result = conn.execute(query, params).fetchdf()

        # 結果がない場合は空のDataFrameを返す
        if len(result) == 0:
            print("条件に一致するデータがありません。")
            return pl.DataFrame({"time": [], "sensor_name": [], "value": []})

        # Polarsに変換
        df = pl.from_pandas(result)

        # 横持ちデータに変換（ピボットテーブル）
        # 同じ時間とセンサー名の組み合わせに複数の値がある場合は最初の値を使用
        pivot_df = df.pivot(
            index="time", on="sensor_name", values="value", aggregate_function="first"
        )

        return pivot_df

    finally:
        # 接続を閉じる
        conn.close()


def get_config():
    """
    環境変数とコマンドライン引数から設定を取得する
    コマンドライン引数が指定された場合は、環境変数より優先される

    Returns:
    dict: 設定値の辞書
    """
    # .envファイルから環境変数を読み込む
    load_dotenv()

    # デフォルト値を設定
    config = {
        "query_db": os.environ.get("query_db", "sensor_data.duckdb"),
        "plant_name": os.environ.get("plant_name", None),
        "machine_no": os.environ.get("machine_no", None),
        "start_time": os.environ.get("start_time", None),
        "end_time": os.environ.get("end_time", None),
        "sensor_names": os.environ.get("sensor_names", ""),
        "output_file": os.environ.get("output_file", None),
    }

    # コマンドライン引数を解析
    parser = argparse.ArgumentParser(description="センサーデータ検索ツール")
    parser.add_argument("--db", type=str, help="検索対象のデータベースファイルのパス")
    parser.add_argument("--plant", type=str, help="工場名")
    parser.add_argument("--machine", type=str, help="機械ID")
    parser.add_argument("--start", type=str, help="開始時間（YYYY-MM-DD HH:MM:SS形式）")
    parser.add_argument("--end", type=str, help="終了時間（YYYY-MM-DD HH:MM:SS形式）")
    parser.add_argument("--sensors", type=str, help="センサー名（カンマ区切り）")
    parser.add_argument("--output", type=str, help="出力ファイル名")

    args = parser.parse_args()

    # コマンドライン引数が指定された場合は、環境変数より優先する
    if args.db:
        config["query_db"] = args.db
    if args.plant:
        config["plant_name"] = args.plant
    if args.machine:
        config["machine_no"] = args.machine
    if args.start:
        config["start_time"] = args.start
    if args.end:
        config["end_time"] = args.end
    if args.sensors:
        config["sensor_names"] = args.sensors
    if args.output:
        config["output_file"] = args.output

    # センサー名をリストに変換
    if config["sensor_names"]:
        config["sensor_names"] = [s.strip() for s in config["sensor_names"].split(",")]
    else:
        config["sensor_names"] = []

    # 時間文字列をdatetimeオブジェクトに変換
    if config["start_time"]:
        try:
            config["start_time"] = datetime.datetime.strptime(
                config["start_time"], "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            print(f"警告: 開始時間の形式が正しくありません: {config['start_time']}")
            print("正しい形式は 'YYYY-MM-DD HH:MM:SS' です。例: '2024-01-01 00:00:00'")
            config["start_time"] = None

    if config["end_time"]:
        try:
            config["end_time"] = datetime.datetime.strptime(
                config["end_time"], "%Y-%m-%d %H:%M:%S"
            )
        except ValueError:
            print(f"警告: 終了時間の形式が正しくありません: {config['end_time']}")
            print("正しい形式は 'YYYY-MM-DD HH:MM:SS' です。例: '2024-01-01 00:00:00'")
            config["end_time"] = None

    return config


def main():
    """メイン実行関数"""
    # 設定を取得
    config = get_config()

    print("=== 検索条件 ===")
    print(f"データベース: {config['query_db']}")
    print(f"工場名: {config['plant_name'] or '指定なし'}")
    print(f"機械ID: {config['machine_no'] or '指定なし'}")
    print(f"開始時間: {config['start_time'] or '指定なし'}")
    print(f"終了時間: {config['end_time'] or '指定なし'}")
    print(
        f"センサー名: {', '.join(config['sensor_names']) if config['sensor_names'] else '指定なし'}"
    )
    print(f"出力ファイル: {config['output_file'] or '指定なし（コンソール出力のみ）'}")
    print()

    # データを検索
    result_df = query_sensor_data(
        config["query_db"],
        config["plant_name"],
        config["machine_no"],
        config["start_time"],
        config["end_time"],
        config["sensor_names"],
    )

    # 結果を表示
    print("=== 検索結果 ===")
    print(result_df)

    # 出力ファイルが指定されている場合は保存
    if config["output_file"]:
        result_df.write_csv(config["output_file"])
        print(f"\n結果を {config['output_file']} に保存しました")


if __name__ == "__main__":
    main()
