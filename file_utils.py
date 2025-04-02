"""
ファイル操作ユーティリティモジュール

ファイル検索、ハッシュ計算などのファイル操作に関する機能を提供します。
"""

import hashlib
import mmap
import os
import re
from pathlib import Path


class FileFinder:
    """ファイル検索を行うクラス"""

    def __init__(self, pattern=None):
        """
        初期化

        Parameters:
        pattern (str, optional): 正規表現パターン
        """
        self.pattern = pattern
        self.regex = re.compile(pattern) if pattern else None

    def set_pattern(self, pattern):
        """
        検索パターンを設定する

        Parameters:
        pattern (str): 正規表現パターン
        """
        self.pattern = pattern
        self.regex = re.compile(pattern)

    def find_csv_files(self, folder_path):
        """
        フォルダ内から正規表現パターンに一致するCSVファイルを検索する

        Parameters:
        folder_path (str or Path): 検索対象のフォルダパス

        Returns:
        list: [{'path': ファイルパス, 'source_zip': None}]
        """
        found_files = []

        # Pathオブジェクトへ変換
        folder = Path(folder_path)

        # 正規表現パターンが設定されていない場合はエラー
        if not self.regex:
            raise ValueError("検索パターンが設定されていません")

        # 通常のCSVファイルを検索
        for file in folder.rglob("*.csv"):
            if self.regex.search(file.name):
                found_files.append({"path": file, "source_zip": None})

        return found_files

    def find_files_with_extension(self, folder_path, extension):
        """
        指定した拡張子のファイルを検索する

        Parameters:
        folder_path (str or Path): 検索対象のフォルダパス
        extension (str): 拡張子（先頭のドットを含む、例: '.csv'）

        Returns:
        list: 見つかったファイルのPathオブジェクトのリスト
        """
        # Pathオブジェクトへ変換
        folder = Path(folder_path)

        # 拡張子の先頭のドットを確認
        if not extension.startswith("."):
            extension = "." + extension

        # ファイルを検索
        return list(folder.rglob(f"*{extension}"))


class FileHasher:
    """ファイルハッシュ計算を行うクラス"""

    @staticmethod
    def get_file_hash(file_path):
        """
        ファイルのSHA256ハッシュを計算する

        Parameters:
        file_path (str or Path): ハッシュを計算するファイルのパス

        Returns:
        str: SHA256ハッシュ値（16進数文字列）
        """
        sha256_hash = hashlib.sha256()

        # ファイルサイズを取得
        file_size = os.path.getsize(file_path)

        with open(file_path, "rb") as f:
            # 小さなファイルは通常の方法で処理
            if file_size < 1024 * 1024:  # 1MB未満
                sha256_hash.update(f.read())
            else:
                # 大きなファイルはメモリマッピングを使用
                try:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        # メモリマップされたファイルを直接ハッシュ計算に使用
                        sha256_hash.update(mm)
                except (ValueError, OSError):
                    # mmapが使用できない場合は通常の方法にフォールバック
                    f.seek(0)
                    for byte_block in iter(lambda: f.read(4096), b""):
                        sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()
