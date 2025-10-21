import pandas as pd
import mysql.connector
from mysql.connector import Error
import numpy as np

print("--- 1. الاتصال بـ MySQL ---")

try:
    connection = mysql.connector.connect(
        host="localhost",
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
csv_path = r"F:\DEPI\Materials\Project_1_data_engineer\apppulse-project\data\google_play_apps.csv"
print(f"📥 جاري قراءة ملف التطبيقات من: {csv_path}...")

df = pd.read_csv(csv_path)

# --- 3. تنظيف الأعمدة ---
rename_map = {
    "Content Rating": "Content_Rating",
    "Last Updated": "Last_Updated",
    "Current Ver": "Current_Ver",
    "Android Ver": "Android_Ver"
}
df.rename(columns=rename_map, inplace=True)
print(f"🔄 تم إعادة تسمية الأعمدة: {list(rename_map.keys())}")

# --- 4. تنظيف البيانات ---
df = df.dropna(subset=["App"])
df = df.fillna("")

# تنظيف عمود Rating (تحويل إلى رقم)
df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")

# تنظيف عمود Installs
df["Installs"] = (
    df["Installs"]
    .astype(str)
    .str.replace(",", "", regex=False)
    .str.replace("+", "", regex=False)
    .str.strip()
)

# 🔥 استبعاد الصفوف التي فيها قيم غير رقمية في Installs (زي Free أو "")
df = df[df["Installs"].str.match(r"^\d+$", na=False)]

# نحول Installs إلى int بعد الفلترة
df["Installs"] = df["Installs"].astype(int)

print(f"📊 الأعمدة النهائية بعد التنظيف: {list(df.columns)}")
print(f"✅ تم قراءة وتنظيف {len(df)} صفاً.")

# --- 5. إنشاء الجدول في MySQL إن لم يكن موجود ---
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

# --- 6. إدخال البيانات في MySQL ---
insert_query = """
INSERT INTO apps_raw (
    App, Category, Rating, Reviews, Size, Installs, Type, Price,
    Content_Rating, Genres, Last_Updated, Current_Ver, Android_Ver
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

df = df.replace({np.nan: None})
print("🚀 جاري إدخال البيانات إلى قاعدة البيانات...")

for _, row in df.iterrows():
    try:
        cursor.execute(insert_query, tuple(row))
    except Error as e:
        print(f"❌ خطأ MySQL أثناء إدخال صف: {e}")
        break

connection.commit()
print("✅ تم إدخال البيانات بنجاح إلى جدول apps_raw.")

cursor.close()
connection.close()
print("--- تم إغلاق اتصال MySQL ---")
