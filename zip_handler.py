"""
ZIPファイル処理モジュール

ZIPファイルからのファイル抽出などの機能を提供します。
"""

import zipfile
from pathlib import Path


class ZipHandler:
    """ZIPファイル処理を行うクラス"""

    @staticmethod
    def find_csv_files_in_zip(zip_path, pattern_regex):
        """
        ZIPファイル内から正規表現パターンに一致するCSVファイルを検索する

        Parameters:
        zip_path (str or Path): ZIPファイルのパス
        pattern_regex: コンパイル済み正規表現パターン

        Returns:
        list: [{'path': ファイルパス, 'source_zip': ZIPファイルパス}]
        """
        found_files = []

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # ZIPファイル内のファイル一覧を取得
                zip_contents = zip_ref.namelist()

                # CSVファイルかつ条件に合うものを抽出
                for file_in_zip in zip_contents:
                    if file_in_zip.endswith(".csv") and pattern_regex.search(
                        Path(file_in_zip).name
                    ):
                        found_files.append(
                            {"path": file_in_zip, "source_zip": zip_path}
                        )
        except zipfile.BadZipFile:
            print(f"警告: {zip_path}は有効なZIPファイルではありません。")

        return found_files

    @staticmethod
    def extract_file(zip_path, file_path, output_dir):
        """
        ZIPファイルから特定のファイルを抽出する

        Parameters:
        zip_path (str or Path): ZIPファイルのパス
        file_path (str): 抽出するファイルのZIP内パス
        output_dir (str or Path): 出力先ディレクトリ

        Returns:
        Path: 抽出されたファイルのパス

        Raises:
        FileNotFoundError: ファイルが見つからない場合
        zipfile.BadZipFile: 無効なZIPファイルの場合
        """
        # 出力ディレクトリの確認と作成
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # ZIPファイルを開いて処理
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            # ZIPファイル内のファイルパスを正規化
            normalized_path = file_path.replace("\\", "/")

            # ファイル名のみを取得
            file_name = Path(normalized_path).name

            # 出力先のフルパス
            output_path = output_dir / file_name

            # ファイルを抽出
            try:
                # まずそのままのパスで試す
                zip_ref.extract(normalized_path, output_dir)
                # 階層構造があればそのファイルへのフルパスを返す
                if "/" in normalized_path:
                    return output_dir / normalized_path
                return output_path
            except KeyError:
                # 正確なパスでなければ、ファイル名でマッチするものを探す
                for zip_info in zip_ref.infolist():
                    zip_file_path = zip_info.filename.replace("\\", "/")
                    if (
                        zip_file_path.endswith("/" + file_name)
                        or zip_file_path == file_name
                    ):
                        # 見つかったファイルを抽出
                        zip_ref.extract(zip_info, output_dir)
                        # 抽出されたファイルのパスを返す
                        if "/" in zip_info.filename:
                            return output_dir / zip_info.filename
                        return output_dir / file_name

                # ファイルが見つからない場合はエラー
                raise FileNotFoundError(
                    f"ZIPファイル内に {file_path} または {file_name} が見つかりません。"
                )
