from flask import Flask, request, render_template, redirect, url_for, session
import pandas as pd
import sqlite3
import os
import re
import unicodedata
import itertools
from collections import defaultdict
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'your-secret-key'

TOLERANCE_STRICT = 0
TOLERANCE_LOOSE = 900
MAX_COMB_STRICT = 10
MAX_COMB_LOOSE = 20

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "log")
os.makedirs(DB_DIR, exist_ok=True)
DB_FILE = os.path.join(DB_DIR, f"billing_payment_{timestamp}.db")

def get_connection():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.executescript('''
        CREATE TABLE IF NOT EXISTS 顧客 (
            顧客ID INTEGER PRIMARY KEY AUTOINCREMENT,
            顧客名 TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS 支払元 (
            支払ID INTEGER PRIMARY KEY AUTOINCREMENT,
            支払者名 TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS 請求情報 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            顧客ID INTEGER,
            受注番号 TEXT,
            発注者コード TEXT,
            変換後発注者名 TEXT,
            変換後発注者名カナ TEXT,
            請求日 TEXT,
            入金予定日 TEXT,
            請求金額 REAL,
            FOREIGN KEY (顧客ID) REFERENCES 顧客(顧客ID)
        );

        CREATE TABLE IF NOT EXISTS 入金情報 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            支払ID INTEGER,
            照会口座 TEXT,
            番号 TEXT,
            勘定日 TEXT,
            起算日 TEXT,
            出金金額 REAL,
            入金金額 REAL,
            小切手区分 TEXT,
            残高 REAL,
            取引区分 TEXT,
            明細区分 TEXT,
            金融機関名 TEXT,
            支店名 TEXT,
            変換後発注者名 TEXT,
            FOREIGN KEY (支払ID) REFERENCES 支払元(支払ID)
        );

        CREATE TABLE IF NOT EXISTS 照合グループ (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        );

        CREATE TABLE IF NOT EXISTS 照合結果 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            照合グループID INTEGER,
            請求ID INTEGER,
            入金ID INTEGER,
            FOREIGN KEY (照合グループID) REFERENCES 照合グループ(id),
            FOREIGN KEY (請求ID) REFERENCES 請求情報(id),
            FOREIGN KEY (入金ID) REFERENCES 入金情報(id)
        );

        ''')

def insert_or_get_id(cur, table, column, value):
    cur.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (value,))
    cur.execute(f"SELECT rowid AS id FROM {table} WHERE {column} = ?", (value,))
    row = cur.fetchone()
    return row["id"] if row else None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    bill_file = request.files.get('bill_csv')
    pay_file = request.files.get('payment_csv')
    with get_connection() as conn:
        cur = conn.cursor()
        if bill_file:
            df_bill = pd.read_csv(bill_file)
            bill_records = []
            for _, row in df_bill.iterrows():
                顧客名 = str(row.get("変換後発注者名（ｶﾅ）", "")).strip()
                if not 顧客名:
                    continue
                顧客ID = insert_or_get_id(cur, "顧客", "顧客名", 顧客名)
                if 顧客ID is None:
                    continue
                try:
                    請求額 = float(str(row.get("請求額", row.get("請求金額", "0"))).replace(",", ""))
                except ValueError:
                    continue
                bill_records.append((顧客ID, 請求額, 顧客名))
            cur.executemany("INSERT INTO 請求情報 (顧客ID, 請求金額, 変換後発注者名カナ) VALUES (?, ?, ?)", bill_records)
        if pay_file:
            df_pay = pd.read_csv(pay_file)
            pay_records = []
            for _, row in df_pay.iterrows():
                支払者名 = str(row.get("照会口座", "")).strip()
                if not 支払者名:
                    continue
                支払ID = insert_or_get_id(cur, "支払元", "支払者名", 支払者名)
                if 支払ID is None:
                    continue
                try:
                    入金額 = float(str(row.get("入金金額（円）", "0")).replace(",", ""))
                except ValueError:
                    continue
                pay_records.append((支払ID, 入金額, row.get("変換後発注者名", "")))
            cur.executemany("INSERT INTO 入金情報 (支払ID, 入金金額, 変換後発注者名) VALUES (?, ?, ?)", pay_records)
    return redirect(url_for('index'))

def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = unicodedata.normalize('NFKC', name)  # 全角→半角
    name = re.sub(r'（.*?）|\(.*?\)', '', name)  # （ｶ）や(ｶ)削除
    name = re.sub(r'[,\.\s]', '', name)          # 記号削除
    return name.strip()

# 金額一致判定
def amount_match(amount1: float, amount2: float, tolerance: int = 0) -> bool:
    try:
        return abs(float(amount1) - float(amount2)) <= tolerance
    except (ValueError, TypeError):
        return False

# 1対1
def match_one_to_one(bills, payments, tolerance, cur, matched_bills, matched_payments):
    for b_id, b_amt in bills:
        for p_id, p_amt in payments:
            if b_id in matched_bills or p_id in matched_payments:
                continue
            if amount_match(b_amt, p_amt, tolerance):
                cur.execute("INSERT INTO 照合グループ DEFAULT VALUES")
                gid = cur.lastrowid
                cur.execute("INSERT INTO 照合結果 (照合グループID, 請求ID, 入金ID) VALUES (?, ?, ?)",
                            (gid, b_id, p_id))
                matched_bills.add(b_id)
                matched_payments.add(p_id)
    return matched_bills, matched_payments

# N対1
def match_n_to_1(bills, payments, tolerance, max_comb, cur, matched_bills, matched_payments):
    for pay_id, pay_amt in payments:
        for r in range(2, min(len(bills), max_comb) + 1):
            for comb in itertools.combinations([b for b in bills if b[0] not in matched_bills], r):
                if amount_match(sum(c[1] for c in comb), pay_amt, tolerance):
                    cur.execute("INSERT INTO 照合グループ DEFAULT VALUES")
                    gid = cur.lastrowid
                    for bid, _ in comb:
                        cur.execute("INSERT INTO 照合結果 (照合グループID, 請求ID, 入金ID) VALUES (?, ?, ?)",
                                    (gid, bid, pay_id))
                        matched_bills.add(bid)
                    matched_payments.add(pay_id)
                    break
    return matched_bills, matched_payments

# 1対N
def match_1_to_n(bills, payments, tolerance, max_comb, cur, matched_bills, matched_payments):
    for bill_id, bill_amt in bills:
        for r in range(2, min(len(payments), max_comb) + 1):
            for comb in itertools.combinations([p for p in payments if p[0] not in matched_payments], r):
                if amount_match(sum(c[1] for c in comb), bill_amt, tolerance):
                    cur.execute("INSERT INTO 照合グループ DEFAULT VALUES")
                    gid = cur.lastrowid
                    for pid, _ in comb:
                        cur.execute("INSERT INTO 照合結果 (照合グループID, 請求ID, 入金ID) VALUES (?, ?, ?)",
                                    (gid, bill_id, pid))
                        matched_payments.add(pid)
                    matched_bills.add(bill_id)
                    break
    return matched_bills, matched_payments


# 改良版 perform_matching
def perform_matching(tolerance, store_unmatched, use_only_unmatched=False, max_comb=5):
    with get_connection() as conn:
        cur = conn.cursor()

        # 既存結果をクリア
        cur.execute("DELETE FROM 照合結果")
        cur.execute("DELETE FROM 照合グループ")

        # データ取得
        cur.execute("SELECT id, 変換後発注者名カナ, 請求金額 FROM 請求情報")
        bills_all = cur.fetchall()
        cur.execute("SELECT id, 変換後発注者名, 入金金額 FROM 入金情報")
        payments_all = cur.fetchall()

        # 再照合モード
        if use_only_unmatched:
            bills_all = [b for b in bills_all if b["id"] in session.get("unmatched_bills_ids", [])]
            payments_all = [p for p in payments_all if p["id"] in session.get("unmatched_payments_ids", [])]

        # 名前正規化してグループ化
        grouped_bills = defaultdict(list)
        for b in bills_all:
            grouped_bills[normalize_name(b["変換後発注者名カナ"])].append((b["id"], float(b["請求金額"])))
        grouped_payments = defaultdict(list)
        for p in payments_all:
            grouped_payments[normalize_name(p["変換後発注者名"])].append((p["id"], float(p["入金金額"])))

        matched_bills, matched_payments = set(), set()

        # 1. 1対1
        for name in set(grouped_bills) & set(grouped_payments):
            matched_bills, matched_payments = match_one_to_one(grouped_bills[name], grouped_payments[name], tolerance, cur, matched_bills, matched_payments)

        # 2. N対1
        for name in set(grouped_bills) & set(grouped_payments):
            matched_bills, matched_payments = match_n_to_1(grouped_bills[name], grouped_payments[name], tolerance, max_comb, cur, matched_bills, matched_payments)

        # 3. 1対N
        for name in set(grouped_bills) & set(grouped_payments):
            matched_bills, matched_payments = match_1_to_n(grouped_bills[name], grouped_payments[name], tolerance, max_comb, cur, matched_bills, matched_payments)

        # 未照合データ保存
        if store_unmatched:
            session["unmatched_bills_ids"] = [bid for name, bl in grouped_bills.items() for bid, _ in bl if bid not in matched_bills]
            session["unmatched_payments_ids"] = [pid for name, pl in grouped_payments.items() for pid, _ in pl if pid not in matched_payments]

        conn.commit()

        # 照合結果取得
        cur.execute('''
            SELECT g.id AS 照合グループID, c.顧客名, s.支払者名,
                   b.請求金額, p.入金金額
            FROM 照合結果 r
            JOIN 請求情報 b ON r.請求ID = b.id
            JOIN 顧客 c ON b.顧客ID = c.顧客ID
            JOIN 入金情報 p ON r.入金ID = p.id
            JOIN 支払元 s ON p.支払ID = s.支払ID
            JOIN 照合グループ g ON r.照合グループID = g.id
            ORDER BY g.id
        ''')
        results = cur.fetchall()

    return render_template("result.html", results=results)

@app.route('/match')
def match():
    # 既に結果があるか確認
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM 照合結果")
        count = cur.fetchone()[0]

    if count == 0:  # 初回だけ計算
        return perform_matching(TOLERANCE_STRICT, store_unmatched=True, max_comb=MAX_COMB_STRICT)
    else:  # 以降は即表示
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute('''
                SELECT g.id AS 照合グループID, c.顧客名, s.支払者名,
                       b.請求金額, p.入金金額
                FROM 照合結果 r
                JOIN 請求情報 b ON r.請求ID = b.id
                JOIN 顧客 c ON b.顧客ID = c.顧客ID
                JOIN 入金情報 p ON r.入金ID = p.id
                JOIN 支払元 s ON p.支払ID = s.支払ID
                JOIN 照合グループ g ON r.照合グループID = g.id
                ORDER BY g.id
            ''')
            results = cur.fetchall()
        return render_template("result.html", results=results)


@app.route('/match_loose')
def match_loose():
    return perform_matching(TOLERANCE_LOOSE, store_unmatched=True, max_comb=MAX_COMB_LOOSE)

@app.route('/match_errors')
def match_errors():
    bill_ids = session.get("unmatched_bills_ids", [])
    pay_ids = session.get("unmatched_payments_ids", [])
    bills, pays = [], []
    with get_connection() as conn:
        cur = conn.cursor()
        if bill_ids:
            q_marks = ",".join(["?"] * len(bill_ids))
            cur.execute(f"SELECT b.id, b.請求金額 AS 金額, c.顧客名 FROM 請求情報 b JOIN 顧客 c ON b.顧客ID = c.顧客ID WHERE b.id IN ({q_marks})", bill_ids)
            bills = [dict(r) for r in cur.fetchall()]
        if pay_ids:
            q_marks = ",".join(["?"] * len(pay_ids))
            cur.execute(f"SELECT p.id, p.入金金額 AS 金額, s.支払者名 FROM 入金情報 p JOIN 支払元 s ON p.支払ID = s.支払ID WHERE p.id IN ({q_marks})", pay_ids)
            pays = [dict(r) for r in cur.fetchall()]
    return render_template("match_errors.html", bills=bills, payments=pays)



@app.route('/viewer', methods=['GET', 'POST'])
def viewer():
    db_files = [f for f in os.listdir(DB_DIR) if f.endswith('.db')]
    if request.method == 'POST':
        selected_file = request.form.get('db_file')
        if selected_file and selected_file in db_files:
            global DB_FILE
            DB_FILE = os.path.join(DB_DIR, selected_file)
            return redirect(url_for('db_list'))
    return render_template('viewer.html', db_files=db_files, selected_file=os.path.basename(DB_FILE))

@app.route('/db_list')
def db_list():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT b.id, c.顧客名, b.請求金額, b.変換後発注者名カナ FROM 請求情報 b JOIN 顧客 c ON b.顧客ID = c.顧客ID")
        bills = [dict(row) for row in cur.fetchall()]
        cur.execute("SELECT p.id, s.支払者名, p.入金金額, p.変換後発注者名 FROM 入金情報 p JOIN 支払元 s ON p.支払ID = s.支払ID")
        payments = [dict(row) for row in cur.fetchall()]
    return render_template('db_list.html', bills=bills, payments=payments)

@app.route('/results')
def results():
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT g.id AS 照合グループID, c.顧客名, s.支払者名, 
                   b.請求金額, p.入金金額
            FROM 照合結果 r
            JOIN 請求情報 b ON r.請求ID = b.id
            JOIN 顧客 c ON b.顧客ID = c.顧客ID
            JOIN 入金情報 p ON r.入金ID = p.id
            JOIN 支払元 s ON p.支払ID = s.支払ID
            JOIN 照合グループ g ON r.照合グループID = g.id
            ORDER BY g.id
        """)
        results = [dict(row) for row in cur.fetchall()]
    return render_template('match_results.html', results=results)

@app.route('/group_detail/<int:group_id>')
def group_detail(group_id):
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT '請求' AS タイプ, b.id AS ID, c.顧客名 AS 名前, b.請求金額 AS 金額, b.請求日, NULL AS 勘定日
            FROM 照合結果 r
            JOIN 請求情報 b ON r.請求ID = b.id
            JOIN 顧客 c ON b.顧客ID = c.顧客ID
            WHERE r.照合グループID = ?
            UNION ALL
            SELECT '入金' AS タイプ, p.id AS ID, s.支払者名 AS 名前, p.入金金額 AS 金額, NULL AS 請求日, p.勘定日
            FROM 照合結果 r
            JOIN 入金情報 p ON r.入金ID = p.id
            JOIN 支払元 s ON p.支払ID = s.支払ID
            WHERE r.照合グループID = ?
        """, (group_id, group_id))
        records = [dict(row) for row in cur.fetchall()]
    return render_template('group_detail.html', group_id=group_id, records=records)

if __name__ == '__main__':
    create_tables()
    app.run(debug=True, host='0.0.0.0', port=8000)

