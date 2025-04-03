"""
エラーハンドリングモジュール

共通のエラーハンドリング機能を提供します。
"""

import contextlib
import functools
import os
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar, Union, cast

from src.utils.logging_config import get_logger

# ロガーの取得
logger = get_logger("error_handlers")

# 型変数の定義
T = TypeVar("T")
R = TypeVar("R")


def safe_operation(
    operation_name: str,
    default_return: Optional[R] = None,
    log_exception: bool = True,
    reraise: bool = False,
) -> Callable[[Callable[..., R]], Callable[..., Optional[R]]]:
    """
    操作を安全に実行するためのデコレータ

    Parameters:
        operation_name (str): 操作の名前（ログ出力用）
        default_return (R, optional): 例外発生時のデフォルト戻り値
        log_exception (bool): 例外をログに記録するかどうか
        reraise (bool): 例外を再送出するかどうか

    Returns:
        Callable: デコレータ関数
    """

    def decorator(func: Callable[..., R]) -> Callable[..., Optional[R]]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Optional[R]:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_exception:
                    logger.error(f"{operation_name}中にエラーが発生: {str(e)}")
                if reraise:
                    raise
                return default_return

        return wrapper

    return decorator


def safe_db_operation(
    db_manager: Any,
    operation_func: Callable[..., R],
    error_message: str,
    *args: Any,
    **kwargs: Any,
) -> Optional[R]:
    """
    データベース操作を安全に実行する

    Parameters:
        db_manager: データベースマネージャ
        operation_func (Callable): 実行する操作関数
        error_message (str): エラー時のメッセージ
        *args: 操作関数に渡す位置引数
        **kwargs: 操作関数に渡すキーワード引数

    Returns:
        Optional[R]: 操作関数の戻り値、エラー時はNone
    """
    try:
        result = operation_func(*args, **kwargs)
        db_manager.commit()
        return result
    except Exception as e:
        db_manager.rollback()
        logger.error(f"{error_message}: {str(e)}")
        return None


@contextlib.contextmanager
def temp_directory():
    """
    一時ディレクトリを作成し、使用後に削除する

    Yields:
        Path: 一時ディレクトリのパス
    """
    temp_dir = Path(tempfile.mkdtemp())
    try:
        yield temp_dir
    finally:
        try:
            import shutil

            shutil.rmtree(temp_dir)
        except Exception as e:
            logger.warning(f"一時ディレクトリの削除中にエラー: {str(e)}")


@contextlib.contextmanager
def temp_file(
    suffix: str = ".tmp", content: Optional[str] = None, encoding: str = "utf-8"
):
    """
    一時ファイルを作成し、使用後に削除する

    Parameters:
        suffix (str): ファイル名の接尾辞
        content (str, optional): ファイルに書き込む内容
        encoding (str): ファイルのエンコーディング

    Yields:
        Path: 一時ファイルのパス
    """
    fd, temp_path = tempfile.mkstemp(suffix=suffix)
    temp_file_path = Path(temp_path)
    try:
        os.close(fd)  # ファイル記述子を閉じる

        # 内容が指定されている場合は書き込む
        if content is not None:
            temp_file_path.write_text(content, encoding=encoding)

        yield temp_file_path
    finally:
        try:
            if temp_file_path.exists():
                os.unlink(temp_file_path)
        except Exception as e:
            logger.warning(f"一時ファイルの削除中にエラー: {str(e)}")


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple = (Exception,),
):
    """
    操作を複数回試行するデコレータ

    Parameters:
        max_attempts (int): 最大試行回数
        delay (float): 初回リトライまでの遅延時間（秒）
        backoff (float): 遅延時間の増加倍率
        exceptions (tuple): 捕捉する例外のタプル

    Returns:
        Callable: デコレータ関数
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            import time

            attempt = 1
            current_delay = delay

            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            f"最大試行回数（{max_attempts}回）に達しました: {func.__name__}, エラー: {str(e)}"
                        )
                        raise

                    logger.warning(
                        f"試行 {attempt}/{max_attempts} が失敗しました: {func.__name__}, "
                        f"エラー: {str(e)}, {current_delay}秒後に再試行します"
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff
                    attempt += 1

            # ここには到達しないはずだが、型チェックのために必要
            raise RuntimeError("予期しないエラー: retry デコレータの終了")

        return wrapper

    return decorator


class FileOperationError(Exception):
    """ファイル操作に関するエラー"""

    def __init__(self, message: str, file_path: Optional[Union[str, Path]] = None):
        self.file_path = str(file_path) if file_path else None
        super().__init__(
            f"{message}" + (f" (ファイル: {self.file_path})" if self.file_path else "")
        )


class DatabaseOperationError(Exception):
    """データベース操作に関するエラー"""

    def __init__(self, message: str, operation: Optional[str] = None):
        self.operation = operation
        super().__init__(
            f"{message}" + (f" (操作: {self.operation})" if self.operation else "")
        )
