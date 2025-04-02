# CSVセンサーデータ管理システム

このシステムは、時系列のセンサーデータを含むCSVファイルをDuckDBデータベースに変換し、効率的に管理・検索するためのツールです。

## 主要機能

### 1. データ取り込み機能 (`main.py`)

- **ファイル抽出機能**
  - 指定フォルダ内のCSVファイルを正規表現パターンで検索
  - ZIPファイル内のCSVも自動的に抽出して処理
  - `find_csv_files`関数と`extract_from_zip`関数で実装

- **データベース管理機能**
  - DuckDBを使用してファイル処理履歴を管理
  - ファイルパスとハッシュ値で重複処理を防止
  - `setup_database`、`get_file_hash`、`is_file_processed_by_path`、`is_file_processed_by_hash`、`mark_file_as_processed`関数で実装

- **処理実行機能**
  - CSVファイルを個別に処理する`process_csv_file`関数
  - ファイルリストを順次処理する`process_csv_files`関数
  - 処理結果の統計情報を収集

- **その他の特徴**
  - Shift-JISエンコーディング対応
  - 特殊ヘッダー形式の処理（センサーID、センサー名、単位の行）
  - 縦持ちデータ形式への変換処理
  - コマンドラインインターフェース

### 2. データ参照機能 (`query_data.py`)

- **条件指定検索機能**
  - 工場名（plant_name）
  - 機械ID（machine_no）
  - 時間範囲（開始時間と終了時間）
  - センサー名（複数指定可能）
  - `query_sensor_data`関数で実装

- **横持ちデータ変換機能**
  - 検索結果を時間を行、センサー名を列とするピボットテーブル形式に変換
  - Polarsのピボット機能を使用

- **設定管理機能**
  - コマンドライン引数と.envファイルの両方から設定を読み込み
  - コマンドライン引数が指定された場合は.env設定より優先
  - `get_config`関数で実装

- **出力機能**
  - 検索結果をコンソールに表示
  - CSVファイルとして保存するオプション

## 使用方法

### データ取り込み

```bash
python main.py --folder data --pattern "(Cond|User|test)" --db processed_files.duckdb --factory AAA --machine-id No.1 --data-label "２０２４年点検"
```

または.envファイルに設定を記述：

```
folder = data
pattern = (Cond|User|test)
db = processed_files.duckdb
encoding = utf-8
```

### データ参照

```bash
python query_data.py --plant TestPlant --machine Machine1 --start "2024-01-01 00:00:00" --end "2024-01-01 01:00:00" --sensors param_A,param_B --output result.csv
```

または.envファイルに設定を記述：

```
query_db = sensor_data.duckdb
plant_name = TestPlant
machine_no = Machine1
start_time = 2024-01-01 00:00:00
end_time = 2024-01-01 01:00:00
sensor_names = param_A,param_B
output_file = result.csv
```

## 技術スタック

- Python 3.11
- DuckDB: データベース管理
- Polars: データフレーム処理
- zipfile: ZIP圧縮ファイルの処理
- hashlib: ファイルハッシュ計算
- argparse: コマンドラインインターフェース実装
- dotenv: 環境変数管理

## 環境要件

- Python 3.11以上
- 依存ライブラリ: duckdb, polars, python-dotenv, pytest

## テスト実行方法

テストはpytestフレームワークを使用して実装されています。以下のコマンドでテストを実行できます：

```bash
# すべてのテストを実行
pytest

# 詳細な出力でテストを実行
pytest -v

# 特定のテストファイルを実行
pytest test_main.py
pytest test_query_data.py

# 特定のテスト関数を実行
pytest test_main.py::test_find_csv_files
```
