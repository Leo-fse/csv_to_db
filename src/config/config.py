"""
設定管理モジュール

環境変数の読み込みと設定の一元管理を行います。
"""

import os
from pathlib import Path

from dotenv import load_dotenv


class Config:
    """アプリケーション設定を管理するクラス"""

    _instance = None
    _initialized = False

    def __new__(cls):
        """シングルトンパターンの実装"""
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """設定の初期化"""
        # 二重初期化を防止
        if self._initialized:
            return
        self._initialized = True

        # 環境変数を読み込む
        load_dotenv()

        # 設定値を保持する辞書
        self._settings = {
            "folder": os.environ.get("folder", "data"),
            "pattern": os.environ.get("pattern", r"(Cond|User|test)"),
            "db": os.environ.get("db", "processed_files.duckdb"),
            "encoding": os.environ.get("encoding", "shift-jis"),
            "factory": os.environ.get("factory", "AAA"),
            "machine_id": os.environ.get("machine_id", "No.1"),
            "data_label": os.environ.get("data_label", "２０２４年点検"),
        }

    def get(self, key, default=None):
        """
        設定値を取得する

        Parameters:
        key (str): 設定キー
        default: キーが存在しない場合のデフォルト値

        Returns:
        設定値
        """
        return self._settings.get(key, default)

    def set(self, key, value):
        """
        設定値を設定する

        Parameters:
        key (str): 設定キー
        value: 設定値
        """
        self._settings[key] = value

    def get_all(self):
        """
        すべての設定値を取得する

        Returns:
        dict: すべての設定値
        """
        return self._settings.copy()

    def get_meta_info(self):
        """
        メタ情報を取得する

        Returns:
        dict: メタ情報（工場名、号機ID、データラベル名）
        """
        return {
            "factory": self.get("factory"),
            "machine_id": self.get("machine_id"),
            "data_label": self.get("data_label"),
        }


# 設定のグローバルインスタンス
config = Config()
