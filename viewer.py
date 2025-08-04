import sqlite3
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "log")

def check_results_count(db_path):
    if not os.path.exists(db_path):
        print(f"[エラー] DBファイルが存在しません: {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM 照合結果")
        count = cur.fetchone()[0]
        conn.close()

        if count == 0:
            print(f"[確認結果] 照合結果テーブルは空です ({db_path})")
        else:
            print(f"[確認結果] 照合結果テーブルに {count} 件のデータがあります ({db_path})")
    except sqlite3.Error as e:
        print(f"[エラー] SQLite 操作中に問題が発生しました: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使い方: python viewer.py <DBファイル名>")
        print("例: python viewer.py billing_payment_20250801_123456.db")
        sys.exit(1)

    db_file = sys.argv[1]
    db_path = os.path.join(DB_DIR, db_file)
    check_results_count(db_path)

