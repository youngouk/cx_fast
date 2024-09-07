import sqlite3
import time
from datetime import datetime

# SQLite 데이터베이스 연결
conn = sqlite3.connect('naver_kin.db')
cursor = conn.cursor()

# 기존 테이블 삭제 (경고: 이 작업은 모든 데이터를 삭제합니다)
cursor.execute('DROP TABLE IF EXISTS kin_data')

# 테이블을 다시 생성
cursor.execute('''CREATE TABLE kin_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT,
                    url TEXT,
                    date TEXT,
                    author TEXT,
                    views INTEGER,
                    created_at TEXT,
                    question_detail TEXT,
                    tags TEXT,
                    scraped_at TEXT
                )''')
