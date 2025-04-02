import duckdb

# データベースに接続
conn = duckdb.connect("sensor_data.duckdb")

# テーブル構造を表示
print("=== sensor_data テーブル構造 ===")
result = conn.execute("DESCRIBE sensor_data").fetchall()
for row in result:
    print(row)

# サンプルデータを表示
print("\n=== sensor_data サンプルデータ ===")
result = conn.execute("SELECT * FROM sensor_data LIMIT 5").fetchall()
for row in result:
    print(row)

# 一意のセンサー名を表示
print("\n=== 一意のセンサー名 ===")
result = conn.execute("SELECT DISTINCT sensor_name FROM sensor_data").fetchall()
for row in result:
    print(row[0])

# 一意の工場名を表示
print("\n=== 一意の工場名（plant_name） ===")
result = conn.execute("SELECT DISTINCT plant_name FROM sensor_data").fetchall()
for row in result:
    print(row[0])

# 一意の機械IDを表示
print("\n=== 一意の機械ID（machine_no） ===")
result = conn.execute("SELECT DISTINCT machine_no FROM sensor_data").fetchall()
for row in result:
    print(row[0])

# 接続を閉じる
conn.close()
