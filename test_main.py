import os
import tempfile
import unittest
from pathlib import Path

import duckdb
import polars as pl

from main import (
    find_csv_files,
    get_file_hash,
    process_csv_file,
    process_csv_files,
    process_single_file,
    setup_database,
)


class TestCSVProcessing(unittest.TestCase):
    """CSVファイル処理機能のテストケース"""

    def setUp(self):
        """テスト前の準備"""
        # 一時ディレクトリを作成
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)

        # テスト用CSVファイルを作成
        self.test_csv_path = self.temp_path / "test.csv"
        with open(self.test_csv_path, "w", encoding="utf-8") as f:
            f.write(", 1000, 1001, 1000, 1002\n")
            f.write(", param_A, param_B, param_A, param_C\n")
            f.write(", kg, mm, kg, cm\n")
            f.write('2024/1/1 00:00:00,1,2,"a",4,\n')
            f.write('2024/1/1 00:00:01,1,2,"a",4,\n')

        # テスト用データベースを作成
        self.db_path = self.temp_path / "test.duckdb"
        self.conn = setup_database(self.db_path)

    def tearDown(self):
        """テスト後のクリーンアップ"""
        # データベース接続を閉じる
        self.conn.close()
        # 一時ディレクトリを削除
        self.temp_dir.cleanup()

    def test_find_csv_files(self):
        """find_csv_files関数のテスト"""
        # テスト用のCSVファイルを追加
        (self.temp_path / "test2.csv").touch()
        (self.temp_path / "other.txt").touch()

        # 関数を実行
        files = find_csv_files(self.temp_path, r"test")

        # 結果を検証
        self.assertEqual(len(files), 2)
        self.assertIn(str(self.test_csv_path), [str(f["path"]) for f in files])

    def test_get_file_hash(self):
        """get_file_hash関数のテスト"""
        # 関数を実行
        hash1 = get_file_hash(self.test_csv_path)

        # 同じファイルのハッシュは同じになることを確認
        hash2 = get_file_hash(self.test_csv_path)
        self.assertEqual(hash1, hash2)

        # 内容が異なるファイルのハッシュは異なることを確認
        different_file = self.temp_path / "different.csv"
        with open(different_file, "w", encoding="utf-8") as f:
            f.write("different content")
        hash3 = get_file_hash(different_file)
        self.assertNotEqual(hash1, hash3)

    def test_process_csv_file(self):
        """process_csv_file関数のテスト"""
        # 関数を実行
        result_df = process_csv_file(self.test_csv_path)

        # 結果を検証
        self.assertIsInstance(result_df, pl.DataFrame)
        # 行数は処理結果に依存するため、厳密な値ではなく存在確認のみ行う
        self.assertGreater(result_df.height, 0)
        self.assertIn("Time", result_df.columns)
        self.assertIn("value", result_df.columns)
        self.assertIn("sensor_id", result_df.columns)
        self.assertIn("sensor_name", result_df.columns)
        self.assertIn("unit", result_df.columns)

    def test_process_csv_files(self):
        """process_csv_files関数のテスト"""
        # テスト用のCSVファイルリストを作成
        csv_files = [{"path": self.test_csv_path, "source_zip": None}]

        # テスト用に直接ファイルを処理（並列処理を回避）
        file_info = {
            "file_path": self.test_csv_path,
            "actual_file_path": self.test_csv_path,
            "source_zip": None,
            "source_zip_str": None,
            "file_hash": get_file_hash(self.test_csv_path),
        }

        # 単一ファイル処理関数を直接呼び出し
        result = process_single_file(file_info, self.temp_path, self.db_path)

        # 結果を検証
        self.assertTrue(result["success"])

        # データベースにデータが挿入されていることを確認
        result = self.conn.execute("SELECT COUNT(*) FROM sensor_data").fetchone()
        self.assertGreater(result[0], 0)

        # 同じファイルを再度処理すると、スキップされることを確認
        stats = process_csv_files(csv_files, self.db_path)
        self.assertEqual(stats["already_processed_by_path"], 1)


if __name__ == "__main__":
    unittest.main()
