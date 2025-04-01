# CSVセンサーデータ変換プログラム

センサーデータを含むCSVファイルをDuckDBデータベースに変換するプログラムです。時系列のセンサーデータを効率的に管理・分析するためのツールです。

## 特徴

このプログラムは以下の特徴を持っています：

1. **Shift-JISエンコーディング対応**：
   - 日本語を含むShift-JISエンコードのCSVファイルを正しく読み込み

2. **特殊ヘッダー形式の処理**：
   - 1行目: センサーID
   - 2行目: センサー名
   - 3行目: 単位
   - 4行目以降: タイムスタンプとデータ

3. **データのクリーニング**：
   - 末尾の余分なカンマの処理
   - 不要列のフィルタリング（センサー名・単位が"-"のもの）

4. **縦持ちデータ形式への変換**：
   - 横持ちCSVデータを「PlantName, MachineNo, Time, sensor_id, sensor_name, sensor_unit, value」の縦持ちデータに変換

5. **ZIPファイルサポート**：
   - ZIP圧縮されたCSVファイルも直接読み込み可能

6. **ファイル名フィルタリング**：
   - パターンマッチによる対象ファイルの選別

7. **重複処理の防止**：
   - 処理済みファイルのスキップ機能

## 要件

- Python 3.11以上
- 以下のライブラリ：
  - polars >= 1.26.0
  - duckdb >= 1.2.1
  - pyarrow >= 19.0.1

## インストール

```bash
# 仮想環境を作成
python -m venv .venv
source .venv/bin/activate  # Windowsの場合: .venv\Scripts\activate

# 依存パッケージのインストール
pip install -e .
```

## 使用方法

```bash
# 基本的な使用方法
python main.py --plant_name [プラント名] --machine_no [機器番号] --file_pattern [パターン]

# 例：プラント名「Plant1」、機器番号「Machine1」、ファイル名に「Cond」または「User」を含むファイルを処理
python main.py --plant_name Plant1 --machine_no Machine1 --file_pattern "Cond|User"

# ディレクトリ指定（デフォルトは「data」）
python main.py --csv_path /path/to/csv/files --plant_name Plant1 --machine_no Machine1
```

## コマンドラインオプション

| オプション       | 説明                                           | デフォルト値   |
|----------------|------------------------------------------------|--------------|
| --csv_path     | CSVファイルを含むディレクトリパス                    | data         |
| --plant_name   | プラント名（必須）                               | -            |
| --machine_no   | 機器番号（必須）                                 | -            |
| --file_pattern | 処理対象ファイル名のパターン（正規表現）               | Cond\|User   |

## データベース構造

データベースには以下の2つのテーブルが作成されます：

1. **sensor_data**：センサーデータの縦持ち形式のテーブル
   - id: 自動増分主キー
   - plant_name: プラント名
   - machine_no: 機器番号
   - time: タイムスタンプ
   - sensor_id: センサーID
   - sensor_name: センサー名
   - sensor_unit: 単位
   - value: センサー値（文字列として格納）

2. **processed_files**：処理済みファイル管理テーブル
   - file_path: ファイルパス（主キー）
   - processed_at: 処理日時

## 技術的特長

- **高速データ処理**：Polarsを使用した効率的なデータ処理
- **堅牢なエラー処理**：各処理段階でのエラーハンドリング
- **柔軟なデータ型対応**：数値と文字列両方に対応する値格納
- **メモリ効率**：大規模ファイルにも対応するストリーミング処理

## ファイル構成

- `main.py`: メインプログラム
- `test_main.py`: ユニットテスト
- `pyproject.toml`: プロジェクト設定
- `data/`: サンプルデータディレクトリ

## ライセンス

このプロジェクトはMITライセンスのもとで公開されています。
