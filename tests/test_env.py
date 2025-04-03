"""
環境変数読み込みテスト

.envファイルから環境変数が正しく読み込まれているかをテストします。
"""

from src.config.config import config


def test_env_variables():
    """環境変数が正しく読み込まれているかをテストする関数"""
    print("===== 環境変数読み込みテスト =====")

    # .envファイルの期待値
    expected = {
        "folder": "data",
        "pattern": "(Cond|User|test)",
        "db": "sensor_data.duckdb",
        "encoding": "utf-8",
        "factory": "AAA",
        "machine_id": "No.1",
        "data_label": "２０２４年点検",
    }

    # 実際に読み込まれた値
    actual = config.get_all()

    print("\n実際に読み込まれた値:")
    for key, value in actual.items():
        print(f"{key}: {value}")

    print("\n期待値との比較:")
    all_correct = True
    for key, expected_value in expected.items():
        actual_value = actual.get(key)
        is_correct = actual_value == expected_value
        status = "✓" if is_correct else "✗"
        print(f"{status} {key}: 期待値={expected_value}, 実際値={actual_value}")
        if not is_correct:
            all_correct = False

    print("\n結果:", "すべて正常" if all_correct else "不一致あり")

    return all_correct


if __name__ == "__main__":
    test_env_variables()
