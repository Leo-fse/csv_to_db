"""
設定管理モジュール

環境変数の読み込みと設定の一元管理を行います。
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional, TypeVar, Union, cast

from dotenv import load_dotenv

from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("config")

# 型変数の定義
T = TypeVar("T")


class Config:
    """アプリケーション設定を管理するクラス"""

    _instance: Optional["Config"] = None
    _initialized: bool = False

    def __new__(cls) -> "Config":
        """シングルトンパターンの実装"""
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        """設定の初期化"""
        # 二重初期化を防止
        if self._initialized:
            return
        self._initialized = True

        # 環境変数を読み込む
        dotenv_path = Path(".env")
        if dotenv_path.exists():
            logger.debug(f".env ファイルを読み込みます: {dotenv_path.absolute()}")
            load_dotenv(dotenv_path=dotenv_path)
        else:
            logger.debug(".env ファイルが見つかりません。環境変数のみを使用します。")

        # 設定値を保持する辞書
        self._settings: Dict[str, Any] = {
            "folder": os.environ.get("folder", "data"),
            "pattern": os.environ.get("pattern", r"(Cond|User|test)"),
            "db": os.environ.get("db", "processed_files.duckdb"),
            "encoding": os.environ.get("encoding", "shift-jis"),
            "factory": os.environ.get("factory", "AAA"),
            "machine_id": os.environ.get("machine_id", "No.1"),
            "data_label": os.environ.get("data_label", "２０２４年点検"),
        }

        logger.debug(f"設定を初期化しました: {self._settings}")

    def get(self, key: str, default: Optional[T] = None) -> Union[Any, T]:
        """
        設定値を取得する

        Parameters:
            key (str): 設定キー
            default (T, optional): キーが存在しない場合のデフォルト値

        Returns:
            Union[Any, T]: 設定値
        """
        value = self._settings.get(key, default)
        logger.debug(f"設定値を取得: {key} = {value}")
        return value

    def set(self, key: str, value: Any) -> None:
        """
        設定値を設定する

        Parameters:
            key (str): 設定キー
            value (Any): 設定値
        """
        logger.debug(f"設定値を更新: {key} = {value}")
        self._settings[key] = value

    def get_all(self) -> Dict[str, Any]:
        """
        すべての設定値を取得する

        Returns:
            Dict[str, Any]: すべての設定値
        """
        return self._settings.copy()

    def get_meta_info(self) -> Dict[str, str]:
        """
        メタ情報を取得する

        Returns:
            Dict[str, str]: メタ情報（工場名、号機ID、データラベル名）
        """
        return {
            "factory": cast(str, self.get("factory")),
            "machine_id": cast(str, self.get("machine_id")),
            "data_label": cast(str, self.get("data_label")),
        }


# 設定のグローバルインスタンス
config = Config()
