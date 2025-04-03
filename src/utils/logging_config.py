"""
ロギング設定モジュール

アプリケーション全体のロギング設定を一元管理します。
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Dict, Optional, Union

# デフォルトのログレベル
DEFAULT_LOG_LEVEL = "INFO"

# ログレベルのマッピング
LOG_LEVELS: Dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

# ログフォーマット
DEFAULT_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
DETAILED_LOG_FORMAT = (
    "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
)

# 初期化済みロガーを保持する辞書
_loggers: Dict[str, logging.Logger] = {}


def get_log_level() -> int:
    """
    環境変数からログレベルを取得する

    Returns:
        int: ログレベル（logging.DEBUG, logging.INFO など）
    """
    log_level_str = os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL).upper()
    return LOG_LEVELS.get(log_level_str, logging.INFO)


def setup_logger(
    name: str,
    log_file: Optional[Union[str, Path]] = None,
    level: Optional[int] = None,
    detailed_format: bool = False,
) -> logging.Logger:
    """
    ロガーを設定する

    Parameters:
        name (str): ロガー名
        log_file (str or Path, optional): ログファイルのパス
        level (int, optional): ログレベル（指定しない場合は環境変数から取得）
        detailed_format (bool): 詳細なフォーマットを使用するかどうか

    Returns:
        logging.Logger: 設定されたロガー
    """
    # 既に初期化済みのロガーがあれば返す
    if name in _loggers:
        return _loggers[name]

    # ログレベルの設定
    if level is None:
        level = get_log_level()

    # ロガーの作成
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # ハンドラが既に設定されている場合は追加しない
    if logger.handlers:
        _loggers[name] = logger
        return logger

    # フォーマットの設定
    log_format = DETAILED_LOG_FORMAT if detailed_format else DEFAULT_LOG_FORMAT
    formatter = logging.Formatter(log_format)

    # コンソールハンドラの設定
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ファイルハンドラの設定（指定されている場合）
    if log_file:
        log_file_path = Path(log_file)
        # ディレクトリが存在しない場合は作成
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        # ローテーティングファイルハンドラを使用（最大10MB、バックアップ5つ）
        file_handler = RotatingFileHandler(
            log_file_path, maxBytes=10 * 1024 * 1024, backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # 初期化済みロガーとして保存
    _loggers[name] = logger
    return logger


def get_logger(
    name: str,
    log_file: Optional[Union[str, Path]] = None,
    detailed_format: bool = False,
) -> logging.Logger:
    """
    ロガーを取得する（存在しない場合は作成）

    Parameters:
        name (str): ロガー名
        log_file (str or Path, optional): ログファイルのパス
        detailed_format (bool): 詳細なフォーマットを使用するかどうか

    Returns:
        logging.Logger: ロガー
    """
    return setup_logger(name, log_file, None, detailed_format)


def set_log_level(level: Union[str, int]) -> None:
    """
    すべてのロガーのログレベルを設定する

    Parameters:
        level (str or int): ログレベル（"DEBUG", "INFO" などの文字列、または logging.DEBUG などの整数）
    """
    # 文字列の場合は整数に変換
    if isinstance(level, str):
        level = LOG_LEVELS.get(level.upper(), logging.INFO)

    # すべてのロガーのレベルを設定
    for logger in _loggers.values():
        logger.setLevel(level)
        for handler in logger.handlers:
            handler.setLevel(level)


# アプリケーション全体のロガー
app_logger = get_logger("csv_to_db")
