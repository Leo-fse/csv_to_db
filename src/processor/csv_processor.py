"""
CSV処理モジュール

CSVファイルの読み込み、データ変換などの機能を提供します。
"""

import codecs
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union, cast

import polars as pl

from src.config.config import config
from src.utils.error_handlers import FileOperationError, temp_file
from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("csv_processor")


class CsvProcessor:
    """CSVファイル処理を行うクラス"""

    def __init__(
        self, encoding: Optional[str] = None, force_encoding: bool = True
    ) -> None:
        """
        初期化

        Parameters:
            encoding (str, optional): CSVファイルのエンコーディング
            force_encoding (bool): エンコーディングを強制するかどうか（Trueの場合は自動検出をスキップ）
        """
        self.encoding = encoding or config.get("encoding", "shift-jis")
        self.force_encoding = force_encoding
        logger.debug(
            f"CsvProcessor を初期化しました: encoding={self.encoding}, force_encoding={self.force_encoding}"
        )

    def process_csv_file(
        self,
        file_path: Union[str, Path],
        check_cancelled: Optional[Callable[[], bool]] = None,
    ) -> Optional[pl.DataFrame]:
        """
        CSVファイルを処理する

        Parameters:
            file_path (str or Path): 処理するCSVファイルのパス
            check_cancelled (callable, optional): キャンセルされたかどうかをチェックする関数

        Returns:
            pl.DataFrame or None: 処理されたデータフレーム、キャンセルされた場合はNone
        """
        file_path_obj = Path(file_path)
        logger.info(f"CSVファイル処理を開始: {file_path_obj}")

        # キャンセルチェック関数がない場合は、常にFalseを返す関数を使用
        if check_cancelled is None:
            check_cancelled = lambda: False

        # キャンセルされたかチェック
        if check_cancelled():
            logger.warning(f"キャンセル要求を検出: {file_path_obj}")
            return None

        # ファイル全体を一度に読み込む
        encoding = self.encoding

        # 一時ファイルのパス（初期値はNone）
        temp_path: Optional[Path] = None

        try:
            # 一時ファイルを作成
            with temp_file(suffix=".csv") as temp_file_path:
                temp_path = temp_file_path
                logger.debug(f"一時ファイルを作成: {temp_path}")

                # エンコーディングを強制するかどうかで処理を分岐
                if self.force_encoding:
                    logger.info(f"エンコーディングを強制: {self.encoding}")
                    try:
                        # バイナリモードで読み込み
                        logger.debug(f"バイナリモードで読み込み開始: {file_path_obj}")
                        with open(file_path, "rb") as src_file:
                            content = src_file.read()

                            # 指定されたエンコーディングでデコード
                            try:
                                decoded = content.decode(
                                    self.encoding, errors="replace"
                                )
                                logger.info(
                                    f"エンコーディング {self.encoding} でデコードしました（エラーは置換）"
                                )
                            except Exception as e:
                                logger.error(
                                    f"{self.encoding}でのデコード中にエラー: {str(e)}"
                                )
                                # 最終手段としてlatin-1を使用
                                decoded = content.decode("latin-1", errors="replace")
                                logger.warning(
                                    f"警告: latin-1でエラーを置換してデコードしました"
                                )

                            # デコードしたデータをUTF-8で一時ファイルに保存
                            with open(temp_path, "w", encoding="utf-8") as dest_file:
                                dest_file.write(decoded)
                                logger.debug(
                                    f"デコードしたデータを一時ファイルに保存: {temp_path}"
                                )
                    except Exception as e:
                        logger.error(f"ファイル読み込み中にエラー: {str(e)}")
                        # 最終手段：バイナリデータをそのまま書き込む
                        with open(file_path, "rb") as src_file:
                            content = src_file.read()
                        with open(temp_path, "wb") as dest_file:
                            dest_file.write(content)
                        logger.warning(
                            "最終手段: バイナリデータをそのまま書き込みました"
                        )
                        # 以降の処理ではutf8-lossyを使用
                        encoding = "utf8-lossy"
                else:
                    # 既存の自動検出ロジック
                    # まずエンコーディングを検出してみる
                    detected_encoding = None
                    try:
                        # ファイルの先頭部分を読み込んでエンコーディングを推測
                        with open(file_path, "rb") as f:
                            raw_data = f.read(
                                8192
                            )  # 先頭8KBを読み込む（より多くのデータを検査）
                            logger.debug(
                                f"ファイルの先頭8KBを読み込みました: {file_path_obj}"
                            )

                            # BOMの検出
                            if raw_data.startswith(codecs.BOM_UTF8):
                                detected_encoding = "utf-8-sig"
                                logger.debug("BOMを検出: UTF-8 with BOM")
                            elif raw_data.startswith(codecs.BOM_UTF16_LE):
                                detected_encoding = "utf-16-le"
                                logger.debug("BOMを検出: UTF-16 LE")
                            elif raw_data.startswith(codecs.BOM_UTF16_BE):
                                detected_encoding = "utf-16-be"
                                logger.debug("BOMを検出: UTF-16 BE")

                            # 日本語エンコーディングの特徴を検出（改善版）
                            if not detected_encoding:
                                # 日本語エンコーディングを検出するための優先順位付きリスト
                                encodings_to_try = [
                                    "cp932",
                                    "shift-jis",
                                    "euc-jp",
                                    "utf-8",
                                ]

                                # 各エンコーディングを試す
                                for enc in encodings_to_try:
                                    try:
                                        # サンプルデータをデコードしてみる
                                        raw_data.decode(enc)
                                        detected_encoding = enc
                                        logger.debug(f"{enc}としてデコード可能")
                                        break
                                    except UnicodeDecodeError:
                                        continue

                                # どのエンコーディングでもデコードできなかった場合
                                if not detected_encoding:
                                    # CP932（Windows版Shift-JIS）を優先的に使用
                                    detected_encoding = "cp932"
                                    logger.debug(
                                        f"エンコーディング検出失敗、CP932を使用"
                                    )
                    except Exception as e:
                        logger.error(f"エンコーディング検出中にエラー: {str(e)}")
                        detected_encoding = encoding

                    logger.info(f"検出されたエンコーディング: {detected_encoding}")

                    # 検出されたエンコーディングでファイルを読み込む
                    try:
                        # バイナリモードで読み込み、より堅牢な変換を行う
                        logger.debug(f"バイナリモードで読み込み開始: {file_path_obj}")
                        with open(file_path, "rb") as src_file:
                            content = src_file.read()

                            # 優先順位を付けた複数のエンコーディングを試す
                            # CP932（Windows版Shift-JIS）を最初に試す
                            encodings_to_try = [
                                "cp932",
                                "shift-jis",
                                "euc-jp",
                                "utf-8",
                                "iso-2022-jp",
                                "latin-1",
                            ]
                            decoded = None

                            for enc in encodings_to_try:
                                try:
                                    # まずstrictモードで試す
                                    decoded = content.decode(enc, errors="strict")
                                    logger.info(
                                        f"エンコーディング {enc} で正常にデコードできました"
                                    )
                                    break
                                except UnicodeDecodeError as e:
                                    # エラー位置を記録
                                    error_pos = e.start if hasattr(e, "start") else -1
                                    logger.debug(
                                        f"エンコーディング {enc} でデコード失敗 (位置: {error_pos})"
                                    )

                                    # 特定の位置でエラーが発生した場合、部分的なデコードを試みる
                                    if error_pos > 0:
                                        try:
                                            # エラー位置までをデコード
                                            partial_content = content[:error_pos]
                                            partial_decoded = partial_content.decode(
                                                enc, errors="strict"
                                            )
                                            logger.debug(
                                                f"位置 {error_pos} までは {enc} でデコード可能"
                                            )

                                            # 残りをreplaceモードでデコード
                                            remaining = content[error_pos:]
                                            remaining_decoded = remaining.decode(
                                                enc, errors="replace"
                                            )

                                            # 結合
                                            decoded = (
                                                partial_decoded + remaining_decoded
                                            )
                                            logger.info(
                                                f"エンコーディング {enc} で部分的にデコードし、残りは置換しました"
                                            )
                                            break
                                        except Exception as partial_e:
                                            logger.debug(
                                                f"部分デコード失敗: {str(partial_e)}"
                                            )
                                    continue

                            if decoded is None:
                                # どのエンコーディングでもデコードできない場合は、CP932でreplaceモードを使用
                                decoded = content.decode("cp932", errors="replace")
                                logger.warning(
                                    f"警告: CP932でエラーを置換してデコードしました"
                                )

                            # デコードしたデータをUTF-8で一時ファイルに保存
                            with open(temp_path, "w", encoding="utf-8") as dest_file:
                                dest_file.write(decoded)
                                logger.debug(
                                    f"デコードしたデータを一時ファイルに保存: {temp_path}"
                                )
                    except Exception as e:
                        logger.error(f"ファイル読み込み中にエラー: {str(e)}")
                        # 最終手段：バイナリデータをそのまま書き込み、Polarsのutf8-lossyで処理
                        logger.warning(
                            "最終手段: バイナリデータをそのまま書き込み、utf8-lossyで処理"
                        )
                        try:
                            # 一度latin-1でデコードしてからUTF-8にエンコードし直す
                            # （latin-1は任意のバイト列を文字にマッピングできる）
                            with open(file_path, "rb") as src_file:
                                content = src_file.read()
                                decoded = content.decode("latin-1")

                            with open(temp_path, "w", encoding="utf-8") as dest_file:
                                dest_file.write(decoded)
                                logger.debug(
                                    f"latin-1経由でUTF-8に変換して保存: {temp_path}"
                                )
                        except Exception as e2:
                            logger.error(f"latin-1変換も失敗: {str(e2)}")
                            # 本当の最終手段：バイナリデータをそのまま書き込む
                            with open(file_path, "rb") as src_file:
                                content = src_file.read()
                            with open(temp_path, "wb") as dest_file:
                                dest_file.write(content)

                        # 以降の処理ではutf8-lossyを使用
                        encoding = "utf8-lossy"

                # 一時ファイルを処理対象に変更
                file_path = temp_path
                # 以降の処理ではUTF-8として扱う
                encoding = "utf-8"
                logger.debug(f"処理対象を一時ファイルに変更: {file_path}")

            # キャンセルされたかチェック
            if check_cancelled():
                logger.warning(f"キャンセル要求を検出: {file_path}")
                return None

            # Polarsのscan_csvは'utf8'または'utf8-lossy'のみをサポート
            polars_encoding = (
                "utf8" if encoding.lower() in ["utf-8", "utf8"] else "utf8-lossy"
            )
            logger.debug(f"Polars用エンコーディング: {polars_encoding}")

            # LazyFrameとDataFrameの変数
            lazy_df = None
            header_df = None
            data_df = None

            try:
                # CSVファイルをスキャン
                logger.debug(f"CSVファイルをスキャン開始: {file_path}")
                lazy_df = pl.scan_csv(
                    file_path,
                    has_header=False,
                    truncate_ragged_lines=True,
                    encoding=polars_encoding,
                    infer_schema_length=10000,  # スキーマ推論の範囲を増やす
                )

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # ヘッダー部分（最初の3行）を取得
                logger.debug("ヘッダー部分（最初の3行）を取得")
                header_df = lazy_df.slice(0, 3).collect()

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # データ部分（4行目以降）を取得し、最後の列（空白列）を除外
                logger.debug("データ部分（4行目以降）を取得")
                data_df = lazy_df.slice(3, None).collect()[:, :-1]

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # 列名を設定する（変換前）
                column_names = ["Time"] + [f"col_{i}" for i in range(1, data_df.width)]
                data_df.columns = column_names
                logger.debug(f"列名を設定: {column_names}")

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # 縦持ちデータにしたい
                # １列目を日時として、残りの列を値として読み込む
                logger.debug("データを縦持ち形式に変換")
                data_df = data_df.unpivot(
                    index=["Time"],
                    on=[f"col_{i}" for i in range(1, data_df.width)],
                    variable_name="sensor_column",
                    value_name="value",
                )

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # センサー情報のマッピングを作成
                logger.debug("センサー情報のマッピングを作成")
                sensor_ids = list(header_df.row(0)[1:])
                sensor_names = list(header_df.row(1)[1:])
                sensor_units = list(header_df.row(2)[1:])

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # センサー情報のDataFrameを作成（ベクトル化処理のため）
                logger.debug("センサー情報のDataFrameを作成")
                sensor_df = pl.DataFrame(
                    {
                        "sensor_column": [
                            f"col_{i + 1}" for i in range(len(sensor_ids))
                        ],
                        "sensor_id": sensor_ids,
                        "sensor_name": sensor_names,
                        "unit": sensor_units,
                    }
                )

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # 結合操作でセンサー情報を追加（ベクトル化された処理）
                logger.debug("センサー情報をデータに結合")
                data_df = data_df.join(sensor_df, on="sensor_column", how="left")

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # 無効なセンサーデータを除外
                logger.debug("無効なセンサーデータを除外")
                data_df = data_df.filter(
                    ~(
                        (pl.col("sensor_name").str.strip_chars() == "-")
                        & (pl.col("unit").str.strip_chars() == "-")
                    )
                )

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # Time列の末尾の空白を除去し、datetime型に変換する
                logger.debug("Time列をdatetime型に変換")
                data_df = data_df.with_columns(
                    pl.col("Time")
                    .str.strip_chars()
                    .str.strptime(pl.Datetime, format="%Y/%m/%d %H:%M:%S")
                )

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                # センサー列を削除し、重複行を削除
                logger.debug("センサー列を削除し、重複行を削除")
                data_df = data_df.drop("sensor_column")
                data_df = data_df.unique()

                # キャンセルされたかチェック
                if check_cancelled():
                    logger.warning(f"キャンセル要求を検出: {file_path}")
                    return None

                logger.info(
                    f"CSVファイル処理完了: {file_path_obj} - {len(data_df)}行のデータ"
                )
                return data_df
            except Exception as e:
                logger.error(f"CSV処理中にエラー: {str(e)}")
                raise FileOperationError(f"CSV処理中にエラー: {str(e)}", file_path)
        except Exception as e:
            logger.error(f"エンコーディング変換処理中にエラー: {str(e)}")
            # エラーが発生した場合でも処理を続行するため、元のファイルと元のエンコーディングを使用
            return None
        finally:
            # 一時ファイルは temp_file コンテキストマネージャによって自動的に削除されるため、
            # ここでの明示的な削除は不要
            pass

    def add_meta_info(
        self,
        data_df: pl.DataFrame,
        file_info: Dict[str, Any],
        meta_info: Optional[Dict[str, str]] = None,
    ) -> pl.DataFrame:
        """
        データフレームにメタ情報を追加する

        Parameters:
            data_df (pl.DataFrame): 処理されたデータフレーム
            file_info (dict): ファイル情報
            meta_info (dict, optional): メタ情報

        Returns:
            pl.DataFrame: メタ情報が追加されたデータフレーム
        """
        if meta_info is None:
            meta_info = config.get_meta_info()

        logger.debug("データフレームにメタ情報を追加")

        # ソースファイル情報とメタ情報を列として追加
        result_df = data_df.with_columns(
            [
                pl.lit(str(file_info["file_path"])).alias("source_file"),
                pl.lit(
                    str(file_info["source_zip"]) if file_info["source_zip"] else ""
                ).alias("source_zip"),
                pl.lit(meta_info.get("factory", "")).alias("factory"),
                pl.lit(meta_info.get("machine_id", "")).alias("machine_id"),
                pl.lit(meta_info.get("data_label", "")).alias("data_label"),
            ]
        )

        logger.debug(f"メタ情報追加完了: {len(result_df)}行のデータ")
        return result_df
