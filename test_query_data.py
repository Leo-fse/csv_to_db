import datetime
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest

from query_data import query_sensor_data


@pytest.fixture
def test_db():
    """テスト用データベースを作成するフィクスチャ"""
    # 一時ディレクトリを作成
    temp_dir = tempfile.TemporaryDirectory()
    temp_path = Path(temp_dir.name)

    # テスト用データベースを作成
    db_path = temp_path / "test_query.duckdb"
    conn = duckdb.connect(str(db_path))

    # テスト用テーブルを作成
    conn.execute("""
        CREATE TABLE sensor_data (
            id INTEGER PRIMARY KEY,
            plant_name VARCHAR,
            machine_no VARCHAR,
            time TIMESTAMP,
            sensor_id VARCHAR,
            sensor_name VARCHAR,
            sensor_unit VARCHAR,
            value VARCHAR
        )
    """)

    # テストデータを挿入
    test_data = [
        # TestPlant, Machine1のデータ
        (
            1,
            "TestPlant",
            "Machine1",
            "2024-01-01 00:00:05",
            "1000",
            "param_A",
            "kg",
            "1",
        ),
        (
            2,
            "TestPlant",
            "Machine1",
            "2024-01-01 00:00:06",
            "1000",
            "param_A",
            "kg",
            "2",
        ),
        (
            3,
            "TestPlant",
            "Machine1",
            "2024-01-01 00:00:07",
            "1000",
            "param_A",
            "kg",
            "3",
        ),
        (
            4,
            "TestPlant",
            "Machine1",
            "2024-01-01 00:00:08",
            "1001",
            "param_B",
            "mm",
            "10",
        ),
        (
            5,
            "TestPlant",
            "Machine1",
            "2024-01-01 00:00:09",
            "1001",
            "param_B",
            "mm",
            "20",
        ),
        (
            6,
            "TestPlant",
            "Machine1",
            "2024-01-01 00:00:10",
            "1002",
            "param_C",
            "cm",
            "100",
        ),
        # TestPlant, Machine2のデータ
        (
            7,
            "TestPlant",
            "Machine2",
            "2024-01-01 00:00:05",
            "1000",
            "param_A",
            "kg",
            "5",
        ),
        (
            8,
            "TestPlant",
            "Machine2",
            "2024-01-01 00:00:06",
            "1001",
            "param_B",
            "mm",
            "50",
        ),
        # OtherPlant, Machine1のデータ
        (
            9,
            "OtherPlant",
            "Machine1",
            "2024-01-01 00:00:05",
            "1000",
            "param_A",
            "kg",
            "9",
        ),
        (
            10,
            "OtherPlant",
            "Machine1",
            "2024-01-01 00:00:06",
            "1002",
            "param_C",
            "cm",
            "900",
        ),
    ]

    # データを挿入
    for row in test_data:
        conn.execute(
            """
            INSERT INTO sensor_data (id, plant_name, machine_no, time, sensor_id, sensor_name, sensor_unit, value)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            row,
        )

    # コミット
    conn.commit()

    # フィクスチャの戻り値
    yield {
        "db_path": db_path,
        "conn": conn,
        "temp_dir": temp_dir,
    }

    # テスト後のクリーンアップ
    conn.close()
    temp_dir.cleanup()


def test_query_all_data(test_db):
    """全データを検索するテスト"""
    # 関数を実行
    result_df = query_sensor_data(test_db["db_path"])

    # 結果を検証
    assert isinstance(result_df, pl.DataFrame)
    # 全データなので10行あるはず
    assert result_df.height == 6  # 時間ごとにピボットされるので6行（一意の時間の数）


def test_query_by_plant(test_db):
    """工場名で検索するテスト"""
    # 関数を実行
    result_df = query_sensor_data(test_db["db_path"], plant_name="TestPlant")

    # 結果を検証
    assert isinstance(result_df, pl.DataFrame)
    # TestPlantのデータは8行あるはず
    assert result_df.height == 6  # 時間ごとにピボットされるので6行


def test_query_by_machine(test_db):
    """機械IDで検索するテスト"""
    # 関数を実行
    result_df = query_sensor_data(test_db["db_path"], machine_no="Machine1")

    # 結果を検証
    assert isinstance(result_df, pl.DataFrame)
    # Machine1のデータは8行あるはず
    assert result_df.height == 6  # 時間ごとにピボットされるので6行


def test_query_by_time_range(test_db):
    """時間範囲で検索するテスト"""
    # 関数を実行
    start_time = datetime.datetime(2024, 1, 1, 0, 0, 5)
    end_time = datetime.datetime(2024, 1, 1, 0, 0, 7)
    result_df = query_sensor_data(
        test_db["db_path"], start_time=start_time, end_time=end_time
    )

    # 結果を検証
    assert isinstance(result_df, pl.DataFrame)
    # 指定した時間範囲のデータは6行あるはず
    assert result_df.height == 3  # 時間ごとにピボットされるので3行


def test_query_by_sensor_names(test_db):
    """センサー名で検索するテスト"""
    # 関数を実行
    result_df = query_sensor_data(
        test_db["db_path"], sensor_names=["param_A", "param_B"]
    )

    # 結果を検証
    assert isinstance(result_df, pl.DataFrame)
    # param_AとParam_Bのデータは7行あるはず
    assert result_df.height == 5  # 時間ごとにピボットされるので5行


def test_query_with_multiple_conditions(test_db):
    """複数条件で検索するテスト"""
    # 関数を実行
    start_time = datetime.datetime(2024, 1, 1, 0, 0, 5)
    end_time = datetime.datetime(2024, 1, 1, 0, 0, 10)
    result_df = query_sensor_data(
        test_db["db_path"],
        plant_name="TestPlant",
        machine_no="Machine1",
        start_time=start_time,
        end_time=end_time,
        sensor_names=["param_A", "param_B"],
    )

    # 結果を検証
    assert isinstance(result_df, pl.DataFrame)
    # 指定した条件のデータは5行あるはず
    assert result_df.height == 5  # 時間ごとにピボットされるので5行
