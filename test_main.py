import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from main import check_disk_space, cleanup_temp_files, vacuum_database


class TestDiskSpaceManagement(unittest.TestCase):
    """ディスク容量管理機能のテスト"""

    @patch("main.shutil.disk_usage")
    def test_check_disk_space_sufficient(self, mock_disk_usage):
        """十分なディスク容量がある場合のテスト"""
        # 1GBの空き容量をシミュレート (1GB = 1024 * 1024 * 1024 bytes)
        mock_disk_usage.return_value = (2000000000, 1000000000, 1024 * 1024 * 1024)

        # 100MBの必要容量でチェック
        result = check_disk_space(Path.cwd(), 100)

        # 結果が真であることを確認
        self.assertTrue(result)
        mock_disk_usage.assert_called_once()

    @patch("main.shutil.disk_usage")
    def test_check_disk_space_insufficient(self, mock_disk_usage):
        """不十分なディスク容量の場合のテスト"""
        # 50MBの空き容量をシミュレート
        mock_disk_usage.return_value = (2000000000, 1950000000, 50 * 1024 * 1024)

        # 100MBの必要容量でチェック
        result = check_disk_space(Path.cwd(), 100)

        # 結果が偽であることを確認
        self.assertFalse(result)
        mock_disk_usage.assert_called_once()

    @patch("main.shutil.disk_usage")
    def test_check_disk_space_error(self, mock_disk_usage):
        """エラーが発生した場合のテスト"""
        # 例外をシミュレート
        mock_disk_usage.side_effect = Exception("テスト例外")

        # 結果が偽であることを確認（エラー時は安全のためFalseを返す）
        result = check_disk_space(Path.cwd(), 100)
        self.assertFalse(result)
        mock_disk_usage.assert_called_once()


class TestDatabaseOptimization(unittest.TestCase):
    """データベース最適化機能のテスト"""

    @patch("main.duckdb.connect")
    def test_vacuum_database_success(self, mock_connect):
        """データベース最適化が成功する場合のテスト"""
        # モックの設定
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # 関数を実行
        result = vacuum_database("test.duckdb")

        # 結果と呼び出しを確認
        self.assertTrue(result)
        mock_connect.assert_called_once_with("test.duckdb")
        mock_conn.execute.assert_called_once_with("VACUUM")
        mock_conn.close.assert_called_once()

    @patch("main.duckdb.connect")
    def test_vacuum_database_error(self, mock_connect):
        """データベース最適化でエラーが発生する場合のテスト"""
        # 例外をシミュレート
        mock_connect.side_effect = Exception("テスト例外")

        # 関数を実行
        result = vacuum_database("test.duckdb")

        # 結果を確認
        self.assertFalse(result)
        mock_connect.assert_called_once_with("test.duckdb")


if __name__ == "__main__":
    unittest.main()
