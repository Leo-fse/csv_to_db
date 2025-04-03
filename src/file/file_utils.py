"""
ファイル操作ユーティリティモジュール

ファイル検索、ハッシュ計算などのファイル操作に関する機能を提供します。
"""

import hashlib
import mmap
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Pattern, Union, cast

from src.utils.error_handlers import FileOperationError, safe_operation
from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("file_utils")


class FileFinder:
    """ファイル検索を行うクラス"""

    def __init__(self, pattern: Optional[str] = None) -> None:
        """
        初期化

        Parameters:
            pattern (str, optional): 正規表現パターン
        """
        self.pattern = pattern
        self.regex: Optional[Pattern[str]] = re.compile(pattern) if pattern else None
        logger.debug(f"FileFinder を初期化しました: pattern={pattern}")

    def set_pattern(self, pattern: str) -> None:
        """
        検索パターンを設定する

        Parameters:
            pattern (str): 正規表現パターン
        """
        self.pattern = pattern
        self.regex = re.compile(pattern)
        logger.debug(f"検索パターンを設定しました: {pattern}")

    def find_csv_files(
        self, folder_path: Union[str, Path]
    ) -> List[Dict[str, Optional[Path]]]:
        """
        フォルダ内から正規表現パターンに一致するCSVファイルを検索する

        Parameters:
            folder_path (str or Path): 検索対象のフォルダパス

        Returns:
            List[Dict[str, Optional[Path]]]: [{'path': ファイルパス, 'source_zip': None}]

        Raises:
            ValueError: 検索パターンが設定されていない場合
        """
        found_files: List[Dict[str, Optional[Path]]] = []

        # Pathオブジェクトへ変換
        folder = Path(folder_path)
        logger.debug(f"CSVファイルの検索を開始: {folder}")

        # 正規表現パターンが設定されていない場合はエラー
        if not self.regex:
            logger.error("検索パターンが設定されていません")
            raise ValueError("検索パターンが設定されていません")

        # 通常のCSVファイルを検索
        try:
            for file in folder.rglob("*.csv"):
                if self.regex.search(file.name):
                    found_files.append({"path": file, "source_zip": None})
                    logger.debug(f"CSVファイルを見つけました: {file}")
        except Exception as e:
            logger.error(f"CSVファイル検索中にエラー: {str(e)}")
            raise FileOperationError(
                f"CSVファイル検索中にエラー: {str(e)}", folder_path
            )

        logger.info(f"{len(found_files)}個のCSVファイルが見つかりました: {folder}")
        return found_files

    def find_files_with_extension(
        self, folder_path: Union[str, Path], extension: str
    ) -> List[Path]:
        """
        指定した拡張子のファイルを検索する

        Parameters:
            folder_path (str or Path): 検索対象のフォルダパス
            extension (str): 拡張子（先頭のドットを含む、例: '.csv'）

        Returns:
            List[Path]: 見つかったファイルのPathオブジェクトのリスト
        """
        # Pathオブジェクトへ変換
        folder = Path(folder_path)
        logger.debug(f"拡張子 {extension} のファイル検索を開始: {folder}")

        # 拡張子の先頭のドットを確認
        if not extension.startswith("."):
            extension = "." + extension
            logger.debug(f"拡張子にドットを追加: {extension}")

        # ファイルを検索
        try:
            files = list(folder.rglob(f"*{extension}"))
            logger.info(
                f"{len(files)}個の{extension}ファイルが見つかりました: {folder}"
            )
            return files
        except Exception as e:
            logger.error(f"ファイル検索中にエラー: {str(e)}")
            raise FileOperationError(f"ファイル検索中にエラー: {str(e)}", folder_path)


class FileHasher:
    """ファイルハッシュ計算を行うクラス"""

    @staticmethod
    @safe_operation("ファイルハッシュ計算", reraise=True)
    def get_file_hash(file_path: Union[str, Path]) -> str:
        """
        ファイルのSHA256ハッシュを計算する

        Parameters:
            file_path (str or Path): ハッシュを計算するファイルのパス

        Returns:
            str: SHA256ハッシュ値（16進数文字列）

        Raises:
            FileOperationError: ファイル操作中にエラーが発生した場合
        """
        sha256_hash = hashlib.sha256()
        file_path_obj = Path(file_path)
        logger.debug(f"ファイルハッシュ計算を開始: {file_path_obj}")

        # ファイルサイズを取得
        try:
            file_size = file_path_obj.stat().st_size
        except Exception as e:
            logger.error(f"ファイルサイズ取得中にエラー: {str(e)}")
            raise FileOperationError(
                f"ファイルサイズ取得中にエラー: {str(e)}", file_path
            )

        try:
            with open(file_path, "rb") as f:
                # 小さなファイルは通常の方法で処理
                if file_size < 1024 * 1024:  # 1MB未満
                    logger.debug(
                        f"通常の方法でハッシュ計算: {file_path_obj} (サイズ: {file_size}バイト)"
                    )
                    sha256_hash.update(f.read())
                else:
                    # 大きなファイルはメモリマッピングを使用
                    logger.debug(
                        f"メモリマッピングでハッシュ計算: {file_path_obj} (サイズ: {file_size}バイト)"
                    )
                    try:
                        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                            # メモリマップされたファイルを直接ハッシュ計算に使用
                            sha256_hash.update(mm)
                    except (ValueError, OSError) as e:
                        # mmapが使用できない場合は通常の方法にフォールバック
                        logger.warning(
                            f"メモリマッピングに失敗、通常の方法にフォールバック: {file_path_obj} - {str(e)}"
                        )
                        f.seek(0)
                        for byte_block in iter(lambda: f.read(4096), b""):
                            sha256_hash.update(byte_block)

            hash_value = sha256_hash.hexdigest()
            logger.debug(f"ハッシュ計算完了: {file_path_obj} -> {hash_value[:8]}...")
            return hash_value
        except Exception as e:
            logger.error(f"ファイルハッシュ計算中にエラー: {str(e)}")
            raise FileOperationError(
                f"ファイルハッシュ計算中にエラー: {str(e)}", file_path
            )
