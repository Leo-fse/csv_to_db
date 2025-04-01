"""
main.pyのユニットテスト

特定の機能をテストするための単体テスト
"""

import unittest
import os
import tempfile
import shutil
from io import StringIO
import zipfile
from unittest.mock import patch, MagicMock
import polars as pl
import duckdb

# テスト対象のモジュールをインポート
import main


class TestCSVProcessing(unittest.TestCase):
    """CSVファイル処理のテスト"""

    def setUp(self):
        """テスト前の準備"""
        # テスト用の一時ディレクトリを作成
        self.test_dir = tempfile.mkdtemp()
        
        # テスト用CSVファイルの内容
        self.test_csv_content = """
, 1000, 1001, 1000, 1002
, param_A, param_B, param_A, param_C
, kg, mm, kg, cm
2024/1/1 00:00:00,1,2,"a",4,
2024/1/1 00:00:01,1,2,"a",4,
2024/1/1 00:00:02,1,2,"a",4,
""".strip()
        
        # テスト用CSVファイルを作成
        self.normal_csv_path = os.path.join(self.test_dir, "test_Cond.csv")
        with open(self.normal_csv_path, "w", encoding="shift-jis") as f:
            f.write(self.test_csv_content)
        
        # テスト用ZIPファイルを作成
        self.zip_path = os.path.join(self.test_dir, "archive.zip")
        with zipfile.ZipFile(self.zip_path, "w") as zip_file:
            zip_file.writestr("test_User.csv", self.test_csv_content)
        
        # テスト用データベースファイル
        self.db_path = os.path.join(self.test_dir, "test.duckdb")

    def tearDown(self):
        """テスト後のクリーンアップ"""
        # テスト用一時ディレクトリを削除
        shutil.rmtree(self.test_dir)

    def test_get_target_files(self):
        """ファイルリスト取得機能のテスト"""
        # パターンにマッチするファイルのみ取得されるかテスト
        target_files = main.get_target_files(self.test_dir, "Cond")
        self.assertEqual(len(target_files), 1)
        self.assertEqual(target_files[0]["type"], "normal")
        self.assertEqual(target_files[0]["full_path"], self.normal_csv_path)
        
        # ZIP内のファイルも取得されるかテスト
        target_files = main.get_target_files(self.test_dir, "User")
        self.assertEqual(len(target_files), 1)
        self.assertEqual(target_files[0]["type"], "zip")
        self.assertEqual(target_files[0]["zip_path"], self.zip_path)
        self.assertEqual(target_files[0]["inner_path"], "test_User.csv")
        
        # 複数パターンのテスト
        target_files = main.get_target_files(self.test_dir, "Cond|User")
        self.assertEqual(len(target_files), 2)

    def test_read_csv_headers(self):
        """CSVヘッダー読み込み機能のテスト"""
        # 本文の最初の3行を取得
        content_lines = self.test_csv_content.splitlines()[:3]
        
        # ヘッダー読み込み
        sensor_ids, sensor_names, sensor_units = main.read_csv_headers(content_lines)
        
        # 想定される結果を検証（空白が除去された結果を期待）
        self.assertEqual(sensor_ids, ["1000", "1001", "1000", "1002"])
        self.assertEqual(sensor_names, ["param_A", "param_B", "param_A", "param_C"])
        self.assertEqual(sensor_units, ["kg", "mm", "kg", "cm"])

    def test_convert_to_vertical_df(self):
        """縦持ちデータ変換機能のテスト"""
        # テスト用データフレーム
        test_df = pl.DataFrame({
            "timestamp": ["2024/1/1 00:00:00", "2024/1/1 00:00:01"],
            "value_0": [1, 1],
            "value_1": [2, 2],
            "value_2": ["a", "a"],
            "value_3": [4, 4]
        })
        
        # センサー情報
        sensor_ids = ["1000", "1001", "1000", "1002"]
        sensor_names = ["param_A", "param_B", "param_A", "param_C"]
        sensor_units = ["kg", "mm", "kg", "cm"]
        
        # 縦持ちデータ変換
        result_df = main.convert_to_vertical_df(
            test_df, sensor_ids, sensor_names, sensor_units, "TestPlant", "TestMachine"
        )
        
        # 結果を検証
        self.assertIsNotNone(result_df)
        self.assertEqual(len(result_df), 8)  # 2行 × 4センサー = 8行
        
        # 特定のセンサー値を確認（文字列型に変換されていることに注意）
        first_row = result_df.filter(
            (pl.col("sensor_id") == "1000") & (pl.col("sensor_name") == "param_A")
        ).row(0)
        self.assertEqual(first_row[0], "TestPlant")  # plant_name
        self.assertEqual(first_row[1], "TestMachine")  # machine_no
        self.assertEqual(first_row[2], "2024/1/1 00:00:00")  # timestamp
        self.assertEqual(first_row[6], "1")  # value - 文字列型に変換されていることを確認

    def test_database_operations(self):
        """データベース操作機能のテスト"""
        try:
            # テスト用データベース接続
            conn = main.init_database(self.db_path)
            
            # ファイル情報
            file_info = {"full_path": self.normal_csv_path}
            
            # 初期状態では処理済みでないことを確認
            self.assertFalse(main.is_file_processed(conn, file_info))
            
            # 処理済みとしてマーク
            main.mark_file_as_processed(conn, file_info)
            
            # 処理済みになったことを確認
            self.assertTrue(main.is_file_processed(conn, file_info))
            
            # データベース接続を閉じる
            conn.close()
        except Exception as e:
            self.fail(f"データベース操作テストが失敗しました: {e}")


if __name__ == "__main__":
    unittest.main()
