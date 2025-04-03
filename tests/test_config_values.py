"""
設定値確認スクリプト

コマンドライン引数とconfig.pyから読み込まれた設定値を表示します。
"""

import argparse

from src.config.config import config


def main():
    """メイン実行関数"""
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(description="設定値確認ツール")
    parser.add_argument(
        "--folder",
        type=str,
        default=config.get("folder"),
        help="検索対象のフォルダパス",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=config.get("pattern"),
        help="ファイル名フィルタリングのための正規表現パターン",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=config.get("db"),
        help="処理記録用データベースファイルのパス",
    )
    parser.add_argument(
        "--factory",
        type=str,
        default=config.get("factory"),
        help="工場名",
    )
    parser.add_argument(
        "--machine-id",
        type=str,
        default=config.get("machine_id"),
        help="号機ID",
    )
    parser.add_argument(
        "--data-label",
        type=str,
        default=config.get("data_label"),
        help="データラベル名",
    )

    args = parser.parse_args()

    # configから直接取得した値
    print("===== configから直接取得した値 =====")
    config_values = config.get_all()
    for key, value in config_values.items():
        print(f"{key}: {value}")

    # コマンドライン引数から取得した値（デフォルト値を含む）
    print("\n===== コマンドライン引数から取得した値 =====")
    print(f"folder: {args.folder}")
    print(f"pattern: {args.pattern}")
    print(f"db: {args.db}")
    print(f"factory: {args.factory}")
    print(f"machine_id: {args.machine_id}")
    print(f"data_label: {args.data_label}")

    # メタ情報
    print("\n===== メタ情報 =====")
    meta_info = config.get_meta_info()
    for key, value in meta_info.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
