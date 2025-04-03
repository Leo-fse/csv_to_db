"""
CSV処理モジュール

CSVファイルの読み込み、データ変換などの機能を提供します。
"""

import codecs
import os
import tempfile
from pathlib import Path

import polars as pl

from src.config.config import config


class CsvProcessor:
    """CSVファイル処理を行うクラス"""

    def __init__(self, encoding=None):
        """
        初期化

        Parameters:
        encoding (str, optional): CSVファイルのエンコーディング
        """
        self.encoding = encoding or config.get("encoding", "shift-jis")

    def process_csv_file(self, file_path):
        """
        CSVファイルを処理する

        Parameters:
        file_path (str or Path): 処理するCSVファイルのパス

        Returns:
        pl.DataFrame: 処理されたデータフレーム
        """
        print(f"処理中: {file_path}")

        # ファイル全体を一度に読み込む
        encoding = self.encoding

        # 一時ファイルのパス（初期値はNone）
        temp_path = None

        # Shift-JISエンコーディングの場合、一度ファイルを読み込んでUTF-8に変換
        if encoding.lower() in ["shift-jis", "shift_jis", "sjis", "cp932", "ms932"]:
            # 一時ファイルを作成
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
                temp_path = temp_file.name

            try:
                # テキストモードでの読み込みを試みる
                with open(file_path, "r", encoding=encoding) as src_file:
                    content = src_file.read()
                    with open(temp_path, "w", encoding="utf-8") as dest_file:
                        dest_file.write(content)
            except UnicodeDecodeError:
                # エンコーディングエラーが発生した場合、バイナリモードで読み込み
                with open(file_path, "rb") as src_file:
                    content = src_file.read()
                    # バイナリデータをShift-JISとしてデコードし、UTF-8にエンコード
                    try:
                        decoded = content.decode(encoding, errors="replace")
                        with open(temp_path, "w", encoding="utf-8") as dest_file:
                            dest_file.write(decoded)
                    except Exception as e:
                        print(f"エンコーディング変換エラー: {str(e)}")
                        # 最終手段：バイナリデータをそのまま書き込み、Polarsのutf8-lossyで処理
                        with open(temp_path, "wb") as dest_file:
                            dest_file.write(content)
                        # 以降の処理ではutf8-lossyを使用
                        encoding = "utf8-lossy"

            # 一時ファイルを処理対象に変更
            file_path = temp_path
            # 以降の処理ではUTF-8として扱う
            encoding = "utf-8"

        try:
            # Polarsのscan_csvは'utf8'または'utf8-lossy'のみをサポート
            polars_encoding = (
                "utf8" if encoding.lower() in ["utf-8", "utf8"] else "utf8-lossy"
            )

            # LazyFrameとDataFrameの変数
            lazy_df = None
            header_df = None
            data_df = None

            lazy_df = pl.scan_csv(
                file_path,
                has_header=False,
                truncate_ragged_lines=True,
                encoding=polars_encoding,
                infer_schema_length=10000,  # スキーマ推論の範囲を増やす
            )

            # ヘッダー部分（最初の3行）を取得
            header_df = lazy_df.slice(0, 3).collect()

            # データ部分（4行目以降）を取得し、最後の列（空白列）を除外
            data_df = lazy_df.slice(3, None).collect()[:, :-1]

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

            # センサー情報のDataFrameを作成（ベクトル化処理のため）
            sensor_df = pl.DataFrame(
                {
                    "sensor_column": [f"col_{i + 1}" for i in range(len(sensor_ids))],
                    "sensor_id": sensor_ids,
                    "sensor_name": sensor_names,
                    "unit": sensor_units,
                }
            )

            # 結合操作でセンサー情報を追加（ベクトル化された処理）
            data_df = data_df.join(sensor_df, on="sensor_column", how="left")

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

            return data_df

        finally:
            # 一時ファイルを削除（Shift-JISからの変換時のみ）
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except:
                    pass

    def add_meta_info(self, data_df, file_info, meta_info=None):
        """
        データフレームにメタ情報を追加する

        Parameters:
        data_df (pl.DataFrame): 処理されたデータフレーム
        file_info (dict): ファイル情報
        meta_info (dict, optional): メタ情報

        Returns:
        pl.DataFrame: メタ情報が追加されたデータフレーム
        """
        if meta_info is None:
            meta_info = config.get_meta_info()

        # ソースファイル情報とメタ情報を列として追加
        return data_df.with_columns(
            [
                pl.lit(str(file_info["file_path"])).alias("source_file"),
                pl.lit(
                    str(file_info["source_zip"]) if file_info["source_zip"] else ""
                ).alias("source_zip"),
                pl.lit(meta_info.get("factory", "")).alias("factory"),
                pl.lit(meta_info.get("machine_id", "")).alias("machine_id"),
                pl.lit(meta_info.get("data_label", "")).alias("data_label"),
            ]
        )
