"""
ファイル検索デバッグツール

指定されたパターンに一致するCSVファイルを検索し、結果を表示します。
"""

import sys
from pathlib import Path

from config import config
from file_utils import FileFinder


def debug_find_csv_files(folder_path, pattern):
    """
    フォルダ内から正規表現パターンに一致するCSVファイルを検索する（デバッグ版）

    Parameters:
    folder_path (str or Path): 検索対象のフォルダパス
    pattern (str): 正規表現パターン

    Returns:
    list: 見つかったファイルのリスト
    """
    print(f"検索パターン: {pattern}")
    print(f"検索フォルダ: {folder_path}")

    # ファイル検索オブジェクトを作成
    file_finder = FileFinder(pattern)

    # ファイルを検索
    found_files = file_finder.find_csv_files(folder_path)

    return found_files


# テスト実行
if __name__ == "__main__":
    folder = config.get("folder")
    pattern = sys.argv[1] if len(sys.argv) > 1 else config.get("pattern")

    print(f"フォルダ {folder} から条件に合うCSVファイルを検索中...")
    csv_files = debug_find_csv_files(folder, pattern)
    print(f"{len(csv_files)}件のファイルが見つかりました")

    if csv_files:
        print("見つかったファイル:")
        for file in csv_files:
            print(f"  {file['path']}")
