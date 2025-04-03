CSVセンサーデータ変換プログラムの実装内容についてまとめます。

# CSVセンサーデータ変換プログラム実装概要

このプログラムは、時系列のセンサーデータを含むCSVファイルをDuckDBデータベースに変換するツールです。

## フォルダ構成

```
csv_to_db/                      # プロジェクトルートディレクトリ
│
├── src/                        # ソースコード
│   ├── __init__.py             # Pythonパッケージとして認識させるためのファイル
│   ├── main.py                 # メインエントリーポイント
│   │
│   ├── config/                 # 設定関連
│   │   ├── __init__.py
│   │   └── config.py           # 設定管理クラス
│   │
│   ├── db/                     # データベース関連
│   │   ├── __init__.py
│   │   └── db_utils.py         # データベース操作クラス
│   │
│   ├── file/                   # ファイル操作関連
│   │   ├── __init__.py
│   │   ├── file_utils.py       # ファイル検索・ハッシュ計算
│   │   ├── zip_handler.py      # ZIPファイル処理
│   │   └── file_processor.py   # ファイル処理統合クラス
│   │
│   └── processor/              # データ処理関連
│       ├── __init__.py
│       └── csv_processor.py    # CSV処理クラス
│
├── tests/                      # テスト
│   ├── __init__.py
│   ├── test_config_values.py   # 設定値確認テスト
│   ├── test_env.py             # 環境変数読み込みテスト
│   └── test_main.py            # 機能テスト
│
├── data/                       # サンプルデータやデフォルトの入力ディレクトリ
│
├── main.py                     # プロジェクトルートからの実行用エントリーポイント
├── .env                        # 環境変数設定ファイル
├── .gitignore                  # Gitの除外設定
├── .python-version             # Pythonバージョン指定
├── pyproject.toml              # プロジェクト設定
├── uv.lock                     # 依存関係ロックファイル
└── README.md                   # プロジェクト説明
```

## 主要機能

1. **ファイル抽出機能**
   - 指定フォルダ内のCSVファイルを正規表現パターンで検索
   - ZIPファイル内のCSVも自動的に抽出して処理
   - `find_csv_files`関数と`extract_from_zip`関数で実装

2. **データベース管理機能**
   - DuckDBを使用してファイル処理履歴を管理
   - ファイルパスとハッシュ値で重複処理を防止
   - `setup_database`、`get_file_hash`、`is_file_processed_by_path`、`is_file_processed_by_hash`、`mark_file_as_processed`関数で実装

3. **処理実行機能**
   - CSVファイルを個別に処理する`process_csv_file`関数
   - ファイルリストを順次処理する`process_csv_files`関数
   - 処理結果の統計情報を収集

4. **その他の特徴**
   - Shift-JISエンコーディング対応（READMEに記載）
   - 特殊ヘッダー形式の処理（センサーID、センサー名、単位の行）
   - 縦持ちデータ形式への変換処理
   - コマンドラインインターフェース

## 技術スタック

- Python 3.11
- DuckDB: 処理履歴管理用のデータベース
- zipfile: ZIP圧縮ファイルの処理
- hashlib: ファイルハッシュ計算でコンテンツベースの重複チェック
- argparse: コマンドラインインターフェース実装

## 環境要件

- Python 3.11以上
- 依存ライブラリ: duckdb, pandas, polars, pyarrow

## 使用方法

プロジェクトのルートディレクトリから以下のコマンドで実行できます：

```bash
python main.py [オプション]
```

### 主なオプション

- `--folder`: 検索対象のフォルダパス（デフォルト: data）
- `--pattern`: ファイル名フィルタリングのための正規表現パターン（デフォルト: (Cond|User|test)）
- `--db`: 処理記録用データベースファイルのパス（デフォルト: processed_files.duckdb）
- `--process-all`: 処理済みファイルも再処理する場合に指定
- `--factory`: 工場名（デフォルト: AAA）
- `--machine-id`: 号機ID（デフォルト: No.1）
- `--data-label`: データラベル名（デフォルト: ２０２４年点検）

## 現状と今後の課題

- 実際のCSV処理ロジック（`process_csv_file`関数）は実装中
- センサーデータの縦持ち変換処理の実装完了が必要
