import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np
import os

print("--- 1. الاتصال بـ MySQL ---")

try:
    connection = mysql.connector.connect(
    host="localhost",  # بدل mysql_db
    user="appuser",
    password="apppass",
    database="apppulse"
)

    if connection.is_connected():
        print("✅ تم الاتصال بنجاح بقاعدة البيانات.")
except Error as e:
    print(f"❌ خطأ في الاتصال بـ MySQL: {e}")
    exit()

# --- 2. قراءة ملف CSV ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(PROJECT_ROOT, "..", "data", "google_play_apps.csv")
print(f"📥 جاري قراءة ملف التطبيقات من: {csv_path}...")

df = pd.read_csv(csv_path)

# --- تنظيف البيانات كما عندك ---
rename_map = {
    "Content Rating": "Content_Rating",
    "Last Updated": "Last_Updated",
    "Current Ver": "Current_Ver",
    "Android Ver": "Android_Ver"
}
df.rename(columns=rename_map, inplace=True)
df = df.dropna(subset=["App"]).fillna("")
df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
df["Installs"] = df["Installs"].astype(str).str.replace(",", "").str.replace("+", "").str.strip()
df = df[df["Installs"].str.match(r"^\d+$", na=False)]
df["Installs"] = df["Installs"].astype(int)

# --- إنشاء الجدول وإدخال البيانات كما عندك ---
create_table_query = """
CREATE TABLE IF NOT EXISTS apps_raw (
    App VARCHAR(255),
    Category VARCHAR(100),
    Rating FLOAT NULL,
    Reviews VARCHAR(50),
    Size VARCHAR(50),
    Installs BIGINT,
    Type VARCHAR(50),
    Price VARCHAR(50),
    Content_Rating VARCHAR(100),
    Genres VARCHAR(100),
    Last_Updated VARCHAR(100),
    Current_Ver VARCHAR(100),
    Android_Ver VARCHAR(100)
);
"""
cursor = connection.cursor()
cursor.execute(create_table_query)
connection.commit()

insert_query = """
INSERT INTO apps_raw (
    App, Category, Rating, Reviews, Size, Installs, Type, Price,
    Content_Rating, Genres, Last_Updated, Current_Ver, Android_Ver
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

df = df.replace({np.nan: None})
for _, row in df.iterrows():
    try:
        cursor.execute(insert_query, tuple(row))
    except Error as e:
        print(f"❌ خطأ MySQL أثناء إدخال صف: {e}")
        break

connection.commit()
cursor.close()
connection.close()
print("✅ تم إدخال البيانات بنجاح وأُغلق اتصال MySQL.")
