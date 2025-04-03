"""
CSVファイル処理ツール

センサーデータCSVファイルを検索、処理し、DuckDBデータベースに保存します。
"""

import argparse
import concurrent.futures
import signal
import sys

from src.config.config import config
from src.file.file_processor import FileProcessor

# グローバル変数
processor = None


# シグナルハンドラ
def signal_handler(sig, frame):
    """
    シグナルハンドラ関数
    Ctrl+Cなどのシグナルを受け取った場合に呼び出される
    """
    print("\n中断シグナルを受信しました。クリーンアップを実行します...")

    # グローバル変数のprocessorが存在する場合
    if processor:
        # キャンセルフラグを全て設定
        for key in processor.cancel_flags.keys():
            processor.cancel_flags[key] = True
        print("すべての処理タスクにキャンセル要求を送信しました。")

    print("プログラムを終了します。")
    sys.exit(0)


def main():
    """メイン実行関数"""
    # シグナルハンドラを設定
    signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # kill
    try:
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

        # ファイル処理オブジェクトを作成（グローバル変数に設定）
        global processor
        processor = FileProcessor(args.db, meta_info)

        # フォルダ内のCSVファイルを処理
        stats = processor.process_folder(args.folder, args.pattern, args.process_all)

        # 結果の表示
        print("\n---- 処理結果 ----")
        print(f"見つかったファイル数: {stats['total_found']}")
        print(f"ファイル名で既に処理済み: {stats['already_processed_by_path']}")
        print(f"内容が同一で処理済み: {stats['already_processed_by_hash']}")
        print(f"新たに処理: {stats['newly_processed']}")
        print(f"処理失敗: {stats['failed']}")
        # タイムアウトによる処理失敗件数を表示
        if "timeout" in stats:
            print(f"タイムアウト: {stats['timeout']}")
    except concurrent.futures.TimeoutError as e:
        print(f"\nエラー: 処理がタイムアウトしました: {str(e)}")

        # キャンセルフラグを全て設定
        if processor:
            for key in processor.cancel_flags.keys():
                processor.cancel_flags[key] = True
            print("すべての処理タスクにキャンセル要求を送信しました。")

        if "stats" in locals():
            # タイムアウトが発生しても統計情報を表示
            print("\n---- 処理結果（タイムアウト発生） ----")
            print(f"見つかったファイル数: {stats['total_found']}")
            print(f"ファイル名で既に処理済み: {stats['already_processed_by_path']}")
            print(f"内容が同一で処理済み: {stats['already_processed_by_hash']}")
            print(f"新たに処理: {stats['newly_processed']}")
            print(f"処理失敗: {stats['failed']}")
            if "timeout" in stats:
                print(f"タイムアウト: {stats['timeout']}")

        print("\n再実行時には、タイムアウトしたファイルは再処理の対象となります。")
    except Exception as e:
        print(f"\nエラー: 処理中に例外が発生しました: {str(e)}")


# 使用例
if __name__ == "__main__":
    main()
