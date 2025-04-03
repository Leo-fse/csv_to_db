"""
CSVファイル処理ツール

センサーデータCSVファイルを検索、処理し、DuckDBデータベースに保存します。
"""

import argparse

from src.config.config import config
from src.file.file_processor import FileProcessor


def main():
    """メイン実行関数"""
    # コマンドライン引数の解析
    parser = argparse.ArgumentParser(description="CSVファイル処理ツール")
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
        "--process-all",
        action="store_true",
        help="処理済みファイルも再処理する場合に指定",
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

    # メタ情報の辞書を作成
    meta_info = {
        "factory": args.factory,
        "machine_id": args.machine_id,
        "data_label": args.data_label,
    }

    # ファイル処理オブジェクトを作成
    processor = FileProcessor(args.db, meta_info)

    # フォルダ内のCSVファイルを処理
    stats = processor.process_folder(args.folder, args.pattern, args.process_all)

    # 結果の表示
    print("\n---- 処理結果 ----")
    print(f"見つかったファイル数: {stats['total_found']}")
    print(f"パスで既に処理済み: {stats['already_processed_by_path']}")
    print(f"内容が同一で処理済み: {stats['already_processed_by_hash']}")
    print(f"新たに処理: {stats['newly_processed']}")
    print(f"処理失敗: {stats['failed']}")


# 使用例
if __name__ == "__main__":
    main()
