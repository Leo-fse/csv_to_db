"""
CSVファイル処理ツール

センサーデータCSVファイルを検索、処理し、DuckDBデータベースに保存します。
"""

import argparse
import concurrent.futures
import signal
import sys
from pathlib import Path
from typing import Dict, Optional, Union

from src.config.config import config
from src.file.file_processor import FileProcessor
from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("main")

# グローバル変数
processor: Optional[FileProcessor] = None


# シグナルハンドラ
def signal_handler(sig: int, frame: Optional[object]) -> None:
    """
    シグナルハンドラ関数
    Ctrl+Cなどのシグナルを受け取った場合に呼び出される

    Parameters:
        sig (int): シグナル番号
        frame (object): 現在のスタックフレーム
    """
    logger.warning("\n中断シグナルを受信しました。クリーンアップを実行します...")

    # グローバル変数のprocessorが存在する場合
    if processor:
        # キャンセルフラグを全て設定
        for key in processor.cancel_flags.keys():
            processor.cancel_flags[key] = True
        logger.info("すべての処理タスクにキャンセル要求を送信しました。")

    logger.info("プログラムを終了します。")
    sys.exit(0)


def main() -> None:
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
        parser.add_argument(
            "--log-file",
            type=str,
            help="ログファイルのパス（指定しない場合はコンソールのみに出力）",
        )
        parser.add_argument(
            "--log-level",
            type=str,
            choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            default=None,
            help="ログレベル（環境変数 LOG_LEVEL より優先）",
        )

        args = parser.parse_args()

        # ログファイルが指定されている場合は、ファイル出力を追加
        if args.log_file:
            from src.utils.logging_config import set_log_level, setup_logger

            log_file_path = Path(args.log_file)
            # グローバル変数loggerを更新するためにglobal宣言
            global logger
            logger = setup_logger("main", log_file_path, detailed_format=True)

            # ログレベルが指定されている場合は設定
            if args.log_level:
                set_log_level(args.log_level)
                logger.info(f"ログレベルを {args.log_level} に設定しました")

        logger.info("CSVファイル処理ツールを開始します")

        # メタ情報の辞書を作成
        meta_info: Dict[str, str] = {
            "factory": args.factory,
            "machine_id": args.machine_id,
            "data_label": args.data_label,
        }

        # ファイル処理オブジェクトを作成（グローバル変数に設定）
        global processor
        processor = FileProcessor(args.db, meta_info)

        # フォルダ内のCSVファイルを処理
        logger.info(
            f"フォルダ {args.folder} のCSVファイルを処理します（パターン: {args.pattern}）"
        )
        stats = processor.process_folder(args.folder, args.pattern, args.process_all)

        # 結果の表示
        logger.info("\n---- 処理結果 ----")
        logger.info(f"見つかったファイル数: {stats['total_found']}")
        logger.info(f"ファイル名で既に処理済み: {stats['already_processed_by_path']}")
        logger.info(f"内容が同一で処理済み: {stats['already_processed_by_hash']}")
        logger.info(f"新たに処理: {stats['newly_processed']}")
        logger.info(f"処理失敗: {stats['failed']}")
        # タイムアウトによる処理失敗件数を表示
        if "timeout" in stats:
            logger.info(f"タイムアウト: {stats['timeout']}")

        logger.info("処理が完了しました")
    except concurrent.futures.TimeoutError as e:
        logger.error(f"\nエラー: 処理がタイムアウトしました: {str(e)}")

        # キャンセルフラグを全て設定
        if processor:
            for key in processor.cancel_flags.keys():
                processor.cancel_flags[key] = True
            logger.warning("すべての処理タスクにキャンセル要求を送信しました。")

        if "stats" in locals():
            # タイムアウトが発生しても統計情報を表示
            logger.info("\n---- 処理結果（タイムアウト発生） ----")
            logger.info(f"見つかったファイル数: {stats['total_found']}")
            logger.info(
                f"ファイル名で既に処理済み: {stats['already_processed_by_path']}"
            )
            logger.info(f"内容が同一で処理済み: {stats['already_processed_by_hash']}")
            logger.info(f"新たに処理: {stats['newly_processed']}")
            logger.info(f"処理失敗: {stats['failed']}")
            if "timeout" in stats:
                logger.info(f"タイムアウト: {stats['timeout']}")

        logger.info(
            "\n再実行時には、タイムアウトしたファイルは再処理の対象となります。"
        )
    except Exception as e:
        logger.exception(f"\nエラー: 処理中に例外が発生しました: {str(e)}")


# 使用例
if __name__ == "__main__":
    main()
