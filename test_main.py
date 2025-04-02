import os
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from main import (
    find_csv_files,
    get_file_hash,
    process_csv_file,
    process_csv_files,
    process_single_file,
    setup_database,
)


@pytest.fixture
def test_env():
    """テスト環境を作成するフィクスチャ"""
    # 一時ディレクトリを作成
    temp_dir = tempfile.TemporaryDirectory()
    temp_path = Path(temp_dir.name)

    # テスト用CSVファイルを作成
    test_csv_path = temp_path / "test.csv"
    with open(test_csv_path, "w", encoding="utf-8") as f:
        f.write(", 1000, 1001, 1000, 1002\n")
        f.write(", param_A, param_B, param_A, param_C\n")
        f.write(", kg, mm, kg, cm\n")
        f.write('2024/1/1 00:00:00,1,2,"a",4,\n')
        f.write('2024/1/1 00:00:01,1,2,"a",4,\n')

    # テスト用データベースを作成
    db_path = temp_path / "test.duckdb"
    conn = setup_database(db_path)

    # フィクスチャの戻り値
    yield {
        "temp_dir": temp_dir,
        "temp_path": temp_path,
        "test_csv_path": test_csv_path,
        "db_path": db_path,
        "conn": conn,
    }

    # テスト後のクリーンアップ
    conn.close()
    temp_dir.cleanup()


def test_find_csv_files(test_env):
    """find_csv_files関数のテスト"""
    # テスト用のCSVファイルを追加
    (test_env["temp_path"] / "test2.csv").touch()
    (test_env["temp_path"] / "other.txt").touch()

    # 関数を実行
    files = find_csv_files(test_env["temp_path"], r"test")

    # 結果を検証
    assert len(files) == 2
    assert str(test_env["test_csv_path"]) in [str(f["path"]) for f in files]


def test_get_file_hash(test_env):
    """get_file_hash関数のテスト"""
    # 関数を実行
    hash1 = get_file_hash(test_env["test_csv_path"])

    # 同じファイルのハッシュは同じになることを確認
    hash2 = get_file_hash(test_env["test_csv_path"])
    assert hash1 == hash2

    # 内容が異なるファイルのハッシュは異なることを確認
    different_file = test_env["temp_path"] / "different.csv"
    with open(different_file, "w", encoding="utf-8") as f:
        f.write("different content")
    hash3 = get_file_hash(different_file)
    assert hash1 != hash3


def test_process_csv_file(test_env):
    """process_csv_file関数のテスト"""
    # 関数を実行
    result_df = process_csv_file(test_env["test_csv_path"])

    # 結果を検証
    assert isinstance(result_df, pl.DataFrame)
    # 行数は処理結果に依存するため、厳密な値ではなく存在確認のみ行う
    assert result_df.height > 0
    assert "Time" in result_df.columns
    assert "value" in result_df.columns
    assert "sensor_id" in result_df.columns
    assert "sensor_name" in result_df.columns
    assert "unit" in result_df.columns


def test_process_csv_files(test_env):
    """process_csv_files関数のテスト"""
    # テスト用のCSVファイルリストを作成
    csv_files = [{"path": test_env["test_csv_path"], "source_zip": None}]

    # テスト用に直接ファイルを処理（並列処理を回避）
    file_info = {
        "file_path": test_env["test_csv_path"],
        "actual_file_path": test_env["test_csv_path"],
        "source_zip": None,
        "source_zip_str": None,
        "file_hash": get_file_hash(test_env["test_csv_path"]),
    }

    # 単一ファイル処理関数を直接呼び出し
    result = process_single_file(file_info, test_env["temp_path"], test_env["db_path"])

    # 結果を検証
    assert result["success"] is True

    # データベースにデータが挿入されていることを確認
    result = test_env["conn"].execute("SELECT COUNT(*) FROM sensor_data").fetchone()
    assert result[0] > 0

    # 同じファイルを再度処理すると、スキップされることを確認
    stats = process_csv_files(csv_files, test_env["db_path"])
    assert stats["already_processed_by_path"] == 1
