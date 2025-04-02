#!/usr/bin/env python
"""
テスト用ダミーデータ生成スクリプト

センサーデータのCSVファイルを生成します。
"""

import argparse
import csv
import os
import random
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np


class DummyDataGenerator:
    """ダミーデータを生成するクラス"""

    # センサータイプのサンプル
    SENSOR_TYPES = [
        {"name": "温度", "unit": "℃", "min": 20, "max": 80, "pattern": "sine"},
        {"name": "圧力", "unit": "MPa", "min": 0.1, "max": 10, "pattern": "random"},
        {"name": "流量", "unit": "L/min", "min": 10, "max": 100, "pattern": "sine"},
        {"name": "振動", "unit": "mm/s", "min": 0, "max": 20, "pattern": "random"},
        {"name": "電流", "unit": "A", "min": 0.5, "max": 50, "pattern": "sine"},
        {"name": "電圧", "unit": "V", "min": 100, "max": 240, "pattern": "stable"},
        {"name": "回転数", "unit": "rpm", "min": 500, "max": 3000, "pattern": "sine"},
        {"name": "湿度", "unit": "%", "min": 30, "max": 90, "pattern": "random"},
        {"name": "pH", "unit": "", "min": 4, "max": 10, "pattern": "stable"},
        {
            "name": "導電率",
            "unit": "μS/cm",
            "min": 100,
            "max": 1000,
            "pattern": "random",
        },
    ]

    # 工場名のサンプル
    FACTORIES = ["AAA", "BBB", "CCC", "DDD", "EEE"]

    # 機器IDのサンプル
    MACHINE_IDS = ["No.1", "No.2", "No.3", "No.4", "No.5"]

    # データラベルのサンプル
    DATA_LABELS = ["２０２４年点検", "定期点検", "異常検知", "通常運転", "試験運転"]

    def __init__(
        self,
        output_dir="data",
        num_files=5,
        sensors_per_file=5,
        data_points=100,
        start_date=None,
        time_interval=60,
        file_prefix="test",
        create_zip=False,
    ):
        """
        初期化

        Parameters:
        output_dir (str): 出力ディレクトリ
        num_files (int): 生成するファイル数
        sensors_per_file (int): 各ファイルのセンサー数
        data_points (int): 各ファイルのデータポイント数
        start_date (datetime): 開始日時
        time_interval (int): 時間間隔（秒）
        file_prefix (str): ファイル名のプレフィックス
        create_zip (bool): ZIPファイルを作成するかどうか
        """
        self.output_dir = Path(output_dir)
        self.num_files = num_files
        self.sensors_per_file = sensors_per_file
        self.data_points = data_points
        self.start_date = start_date or datetime.now()
        self.time_interval = time_interval
        self.file_prefix = file_prefix
        self.create_zip = create_zip

    def generate_sensor_data(self, sensor_type, num_points):
        """
        センサーデータを生成する

        Parameters:
        sensor_type (dict): センサータイプ情報
        num_points (int): データポイント数

        Returns:
        list: 生成されたデータ値のリスト
        """
        min_val = sensor_type["min"]
        max_val = sensor_type["max"]
        pattern = sensor_type["pattern"]

        if pattern == "random":
            # ランダムな値
            return np.random.uniform(min_val, max_val, num_points).tolist()

        elif pattern == "sine":
            # サイン波パターン（ノイズ付き）
            x = np.linspace(0, 4 * np.pi, num_points)
            amplitude = (max_val - min_val) / 2
            offset = min_val + amplitude
            noise = np.random.normal(0, amplitude * 0.1, num_points)
            return (offset + amplitude * np.sin(x) + noise).tolist()

        elif pattern == "stable":
            # 安定した値（小さな変動あり）
            base_val = (min_val + max_val) / 2
            noise_level = (max_val - min_val) * 0.05
            return (base_val + np.random.normal(0, noise_level, num_points)).tolist()

        else:
            # デフォルトはランダム
            return np.random.uniform(min_val, max_val, num_points).tolist()

    def generate_csv_file(self, file_index):
        """
        CSVファイルを生成する

        Parameters:
        file_index (int): ファイルインデックス

        Returns:
        tuple: (ファイルパス, メタ情報)
        """
        # ファイル名を生成
        prefixes = ["Cond", "User", "test"]
        prefix = (
            random.choice(prefixes)
            if self.file_prefix == "random"
            else self.file_prefix
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"{prefix}_{timestamp}_{file_index}.csv"
        file_path = self.output_dir / file_name

        # メタ情報を生成
        meta_info = {
            "factory": random.choice(self.FACTORIES),
            "machine_id": random.choice(self.MACHINE_IDS),
            "data_label": random.choice(self.DATA_LABELS),
        }

        # センサー情報を生成
        sensors = random.sample(
            self.SENSOR_TYPES, min(self.sensors_per_file, len(self.SENSOR_TYPES))
        )

        # 時間データを生成
        start_time = self.start_date
        timestamps = [
            (start_time + timedelta(seconds=i * self.time_interval)).strftime(
                "%Y/%m/%d %H:%M:%S"
            )
            for i in range(self.data_points)
        ]

        # センサーデータを生成
        sensor_data = []
        for sensor in sensors:
            data = self.generate_sensor_data(sensor, self.data_points)
            sensor_data.append(data)

        # CSVファイルを作成
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # ヘッダー行1: センサーID
            sensor_ids = [f"S{i + 1:03d}" for i in range(len(sensors))]
            writer.writerow([""] + sensor_ids)

            # ヘッダー行2: センサー名
            writer.writerow([""] + [sensor["name"] for sensor in sensors])

            # ヘッダー行3: 単位
            writer.writerow([""] + [sensor["unit"] for sensor in sensors])

            # データ行
            for i in range(self.data_points):
                # 直接文字列を作成して書き込む
                line = timestamps[i]
                for j in range(len(sensors)):
                    line += f",{sensor_data[j][i]:.2f}"
                # 末尾にカンマを追加
                line += ","
                f.write(line + "\n")

        return file_path, meta_info

    def generate_all_files(self):
        """
        すべてのファイルを生成する

        Returns:
        list: 生成されたファイルのリスト
        """
        # 出力ディレクトリを作成
        self.output_dir.mkdir(parents=True, exist_ok=True)

        generated_files = []

        print(f"{self.num_files}個のダミーデータファイルを生成中...")

        for i in range(self.num_files):
            file_path, meta_info = self.generate_csv_file(i + 1)
            print(
                f"  生成: {file_path} (工場: {meta_info['factory']}, 機器: {meta_info['machine_id']})"
            )
            generated_files.append((file_path, meta_info))

        # ZIPファイルを作成する場合
        if self.create_zip and generated_files:
            zip_path = (
                self.output_dir
                / f"dummy_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            )

            with zipfile.ZipFile(zip_path, "w") as zip_file:
                for file_path, _ in generated_files:
                    zip_file.write(file_path, file_path.name)

            print(f"ZIPファイルを作成: {zip_path}")

        return generated_files


def main():
    """メイン実行関数"""
    parser = argparse.ArgumentParser(description="テスト用ダミーデータ生成スクリプト")

    parser.add_argument(
        "--output-dir",
        type=str,
        default="data",
        help="出力ディレクトリ",
    )
    parser.add_argument(
        "--num-files",
        type=int,
        default=5,
        help="生成するファイル数",
    )
    parser.add_argument(
        "--sensors",
        type=int,
        default=5,
        help="各ファイルのセンサー数",
    )
    parser.add_argument(
        "--data-points",
        type=int,
        default=100,
        help="各ファイルのデータポイント数",
    )
    parser.add_argument(
        "--time-interval",
        type=int,
        default=60,
        help="時間間隔（秒）",
    )
    parser.add_argument(
        "--file-prefix",
        type=str,
        default="random",
        choices=["Cond", "User", "test", "random"],
        help="ファイル名のプレフィックス",
    )
    parser.add_argument(
        "--create-zip",
        action="store_true",
        help="生成したファイルをZIPにまとめる",
    )

    args = parser.parse_args()

    # ダミーデータ生成器を作成
    generator = DummyDataGenerator(
        output_dir=args.output_dir,
        num_files=args.num_files,
        sensors_per_file=args.sensors,
        data_points=args.data_points,
        time_interval=args.time_interval,
        file_prefix=args.file_prefix,
        create_zip=args.create_zip,
    )

    # ファイルを生成
    generator.generate_all_files()

    print("\n生成完了！")
    print(f"出力ディレクトリ: {args.output_dir}")


if __name__ == "__main__":
    main()
