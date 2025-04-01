"""
CSVファイルをUTF-8からShift-JISエンコーディングに変換するスクリプト
"""

import os

def convert_encoding(input_file, output_file):
    """
    ファイルをUTF-8からShift-JISに変換する
    
    Args:
        input_file: 入力ファイルパス（UTF-8）
        output_file: 出力ファイルパス（Shift-JIS）
    """
    # UTF-8でファイルを読み込む
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Shift-JISで書き込む
    with open(output_file, 'w', encoding='shift-jis') as f:
        f.write(content)
    
    print(f"ファイルを変換しました: {input_file} → {output_file}")

if __name__ == "__main__":
    # 入出力ファイルパス
    input_file = "data/test_jp_Cond.csv"
    output_file = "data/test_jp_Cond_sjis.csv"
    
    convert_encoding(input_file, output_file)
