"""
データベース操作モジュール

DuckDBデータベースの初期化、処理済みファイル管理などの機能を提供します。
"""

import datetime
from pathlib import Path

import duckdb


class DatabaseManager:
    """データベース操作を行うクラス"""

    def __init__(self, db_path):
        """
        初期化

        Parameters:
        db_path (str or Path): データベースファイルのパス
        """
        self.db_path = Path(db_path)
        self.conn = None
        self.setup_database()

    def setup_database(self):
        """
        データベースを初期化する

        Returns:
        duckdb.DuckDBPyConnection: データベース接続
        """
        self.conn = duckdb.connect(str(self.db_path))

        # processed_filesテーブルを作成し、file_hashに一意性制約を追加
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_path VARCHAR NOT NULL,
                file_hash VARCHAR NOT NULL,
                source_zip VARCHAR,
                processed_date TIMESTAMP,
                PRIMARY KEY (file_path, source_zip)
            )
        """)

        # file_hashに一意性インデックスが存在するか確認
        result = self.conn.execute("""
            SELECT COUNT(*) 
            FROM duckdb_indexes() 
            WHERE table_name = 'processed_files' AND index_name = 'idx_processed_files_hash'
        """).fetchone()

        # インデックスが存在しない場合は作成
        if result[0] == 0:
            self.conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_processed_files_hash 
                ON processed_files(file_hash)
            """)

        # センサーデータ格納テーブル
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data (
                Time TIMESTAMP,
                value VARCHAR,
                sensor_id VARCHAR,
                sensor_name VARCHAR,
                unit VARCHAR,
                source_file VARCHAR,
                source_zip VARCHAR,
                factory VARCHAR,
                machine_id VARCHAR,
                data_label VARCHAR
            )
        """)

        return self.conn

    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def is_file_processed_by_path(self, file_path, source_zip=None):
        """
        ファイルパスに基づいて処理済みかどうかを確認する

        Parameters:
        file_path (str or Path): ファイルパス
        source_zip (str or Path, optional): ZIPファイルパス

        Returns:
        bool: 処理済みの場合はTrue
        """
        source_zip_value = "" if source_zip is None else str(source_zip)
        result = self.conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE file_path = ? AND source_zip = ?",
            [str(file_path), source_zip_value],
        ).fetchone()

        return result[0] > 0

    def is_file_processed_by_hash(self, file_hash):
        """
        ファイルハッシュに基づいて処理済みかどうかを確認する

        Parameters:
        file_hash (str): ファイルハッシュ

        Returns:
        bool: 処理済みの場合はTrue
        """
        result = self.conn.execute(
            "SELECT COUNT(*) FROM processed_files WHERE file_hash = ?", [file_hash]
        ).fetchone()

        return result[0] > 0

    def mark_file_as_processed(self, file_path, file_hash, source_zip=None):
        """
        ファイルを処理済みとしてデータベースに記録する

        Parameters:
        file_path (str or Path): ファイルパス
        file_hash (str): ファイルハッシュ
        source_zip (str or Path, optional): ZIPファイルパス

        Returns:
        bool: 成功した場合はTrue
        """
        now = datetime.datetime.now()
        source_zip_value = "" if source_zip is None else str(source_zip)

        # UPSERTパターンを使用して挿入（一意制約違反を防ぐ）
        try:
            self.conn.execute(
                """
                INSERT INTO processed_files (file_path, file_hash, source_zip, processed_date) 
                VALUES (?, ?, ?, ?)
            """,
                [str(file_path), file_hash, source_zip_value, now],
            )
            return True
        except duckdb.ConstraintException:
            print(f"  情報: 同一ハッシュ({file_hash})のファイルが既に処理済みです")
            return False

    def insert_sensor_data(self, data_df):
        """
        センサーデータをデータベースに挿入する

        Parameters:
        data_df: Polars DataFrame

        Returns:
        int: 挿入された行数
        """
        # DataFrameをArrowテーブルに変換
        arrow_table = data_df.to_arrow()

        # 一時テーブルとして登録
        self.conn.register("temp_sensor_data", arrow_table)

        # トランザクションを開始
        self.conn.execute("BEGIN TRANSACTION")

        try:
            # SQLで一括挿入（Arrow形式からの直接挿入）
            result = self.conn.execute("""
                INSERT INTO sensor_data 
                SELECT * FROM temp_sensor_data
            """)

            # 一時テーブルを削除
            self.conn.execute("DROP VIEW IF EXISTS temp_sensor_data")

            # 挿入された行数を取得（DuckDBでは直接取得できないため、データフレームの行数を使用）
            row_count = len(data_df) if data_df is not None else 0

            return row_count
        except Exception as e:
            # エラーが発生した場合はロールバック
            self.conn.execute("ROLLBACK")
            raise e

    def commit(self):
        """変更をコミットする"""
        if self.conn:
            try:
                self.conn.execute("COMMIT")
            except duckdb.duckdb.TransactionException:
                # トランザクションがアクティブでない場合は無視
                pass

    def rollback(self):
        """変更をロールバックする"""
        if self.conn:
            try:
                self.conn.execute("ROLLBACK")
            except duckdb.duckdb.TransactionException:
                # トランザクションがアクティブでない場合は無視
                pass

    def execute(self, query, params=None):
        """
        SQLクエリを実行する

        Parameters:
        query (str): SQLクエリ
        params (list, optional): クエリパラメータ

        Returns:
        duckdb.DuckDBPyResult: クエリ結果
        """
        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)
