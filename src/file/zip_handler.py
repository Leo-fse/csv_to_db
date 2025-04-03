"""
ZIPファイル処理モジュール

ZIPファイルからのファイル抽出などの機能を提供します。
"""

import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Pattern, Union

from src.utils.error_handlers import FileOperationError, safe_operation
from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("zip_handler")


class ZipHandler:
    """ZIPファイル処理を行うクラス"""

    @staticmethod
    def find_csv_files_in_zip(
        zip_path: Union[str, Path], pattern_regex: Pattern[str]
    ) -> List[Dict[str, Union[str, Path]]]:
        """
        ZIPファイル内から正規表現パターンに一致するCSVファイルを検索する

        Parameters:
            zip_path (str or Path): ZIPファイルのパス
            pattern_regex (Pattern): コンパイル済み正規表現パターン

        Returns:
            List[Dict[str, Union[str, Path]]]: [{'path': ファイルパス, 'source_zip': ZIPファイルパス}]
        """
        found_files: List[Dict[str, Union[str, Path]]] = []
        zip_path_obj = Path(zip_path)
        logger.debug(f"ZIPファイル内のCSVファイル検索を開始: {zip_path_obj}")

        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # ZIPファイル内のファイル一覧を取得
                zip_contents = zip_ref.namelist()
                logger.debug(f"ZIPファイル内のファイル数: {len(zip_contents)}")

                # CSVファイルかつ条件に合うものを抽出
                for file_in_zip in zip_contents:
                    if file_in_zip.endswith(".csv") and pattern_regex.search(
                        Path(file_in_zip).name
                    ):
                        found_files.append(
                            {"path": file_in_zip, "source_zip": zip_path}
                        )
                        logger.debug(
                            f"ZIPファイル内のCSVファイルを見つけました: {file_in_zip}"
                        )
        except zipfile.BadZipFile:
            logger.warning(f"{zip_path}は有効なZIPファイルではありません。")
        except Exception as e:
            logger.error(f"ZIPファイル処理中にエラー: {str(e)}")
            raise FileOperationError(f"ZIPファイル処理中にエラー: {str(e)}", zip_path)

        logger.info(
            f"{len(found_files)}個のCSVファイルがZIP内で見つかりました: {zip_path_obj}"
        )
        return found_files

    @staticmethod
    @safe_operation("ZIPファイル抽出", reraise=True)
    def extract_file(
        zip_path: Union[str, Path], file_path: str, output_dir: Union[str, Path]
    ) -> Path:
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
            FileOperationError: その他のファイル操作エラー
        """
        # 出力ディレクトリの確認と作成
        output_dir_obj = Path(output_dir)
        output_dir_obj.mkdir(parents=True, exist_ok=True)
        zip_path_obj = Path(zip_path)

        logger.debug(
            f"ZIPファイルからファイルを抽出: {zip_path_obj} -> {file_path} (出力先: {output_dir_obj})"
        )

        try:
            # ZIPファイルを開いて処理
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                # ZIPファイル内のファイルパスを正規化
                normalized_path = file_path.replace("\\", "/")
                logger.debug(f"正規化されたパス: {normalized_path}")

                # ファイル名のみを取得
                file_name = Path(normalized_path).name

                # 出力先のフルパス
                output_path = output_dir_obj / file_name

                # ファイルを抽出
                try:
                    # まずそのままのパスで試す
                    logger.debug(f"パス {normalized_path} で抽出を試みます")
                    zip_ref.extract(normalized_path, output_dir_obj)
                    # 階層構造があればそのファイルへのフルパスを返す
                    if "/" in normalized_path:
                        extracted_path = output_dir_obj / normalized_path
                        logger.info(f"ファイルを抽出しました: {extracted_path}")
                        return extracted_path
                    logger.info(f"ファイルを抽出しました: {output_path}")
                    return output_path
                except KeyError:
                    # 正確なパスでなければ、ファイル名でマッチするものを探す
                    logger.debug(
                        f"パス {normalized_path} が見つかりません。ファイル名 {file_name} で検索します"
                    )
                    for zip_info in zip_ref.infolist():
                        zip_file_path = zip_info.filename.replace("\\", "/")
                        if (
                            zip_file_path.endswith("/" + file_name)
                            or zip_file_path == file_name
                        ):
                            # 見つかったファイルを抽出
                            logger.debug(
                                f"ファイル名 {file_name} に一致するファイルを見つけました: {zip_file_path}"
                            )
                            zip_ref.extract(zip_info, output_dir_obj)
                            # 抽出されたファイルのパスを返す
                            if "/" in zip_info.filename:
                                extracted_path = output_dir_obj / zip_info.filename
                                logger.info(f"ファイルを抽出しました: {extracted_path}")
                                return extracted_path
                            logger.info(f"ファイルを抽出しました: {output_path}")
                            return output_path

                    # ファイルが見つからない場合はエラー
                    error_msg = f"ZIPファイル内に {file_path} または {file_name} が見つかりません。"
                    logger.error(error_msg)
                    raise FileNotFoundError(error_msg)
        except zipfile.BadZipFile as e:
            logger.error(f"無効なZIPファイル: {zip_path_obj} - {str(e)}")
            raise FileOperationError(f"無効なZIPファイル: {str(e)}", zip_path)
        except FileNotFoundError as e:
            # FileNotFoundErrorはそのまま再送出
            raise
        except Exception as e:
            logger.error(f"ZIPファイル抽出中にエラー: {str(e)}")
            raise FileOperationError(f"ZIPファイル抽出中にエラー: {str(e)}", zip_path)
