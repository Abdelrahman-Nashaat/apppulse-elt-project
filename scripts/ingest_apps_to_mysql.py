import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import re
import numpy as np
import sys

# Load environment variables from .env file in the project root
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

# --- Database Configuration (Using Env Vars for Docker) ---
MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql_db")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
MYSQL_DB = os.getenv("MYSQL_DB", "apppulse_apps")
TABLE_NAME = "apps_raw"

# --- File Paths ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_SOURCE_PATH = os.path.join(PROJECT_ROOT, "data", "google_play_apps.csv")
DBT_SEED_PATH = os.path.join(PROJECT_ROOT, "app_dbt", "seeds", "apps_from_mysql.csv")  # Seed file path

def ingest_apps_to_mysql_and_seed():
    """Reads CSV, applies cleaning logic, ingests into MySQL, then extracts to a dbt seed file."""
    connection = None
    cursor = None
    try:
        # --- A. MySQL Connection ---
        print("--- 1. الاتصال بـ MySQL ---")
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )

        if connection.is_connected():
            print("✅ تم الاتصال بنجاح بقاعدة البيانات.")
            cursor = connection.cursor()

            # --- B. Read and Clean CSV ---
            print(f"📥 جاري قراءة ملف التطبيقات من: {CSV_SOURCE_PATH}...")
            df = pd.read_csv(CSV_SOURCE_PATH, low_memory=False)

            # تنظيف الأعمدة
            df.columns = df.columns.str.strip()
            rename_map = {
                "Content Rating": "Content_Rating",
                "Last Updated": "Last_Updated",
                "Current Ver": "Current_Ver",
                "Android Ver": "Android_Ver"
            }
            df.rename(columns=rename_map, inplace=True)
            print(f"🔄 الأعمدة بعد إعادة التسمية الأولية: {df.columns.tolist()}")

            # حذف الصفوف اللي مافيهاش اسم تطبيق
            df = df.dropna(subset=["App"])

            # تنظيف الأعمدة الرقمية
            df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
            df["Installs"] = df["Installs"].astype(str).str.replace(",", "").str.replace("+", "").str.strip()
            df = df[df["Installs"].str.match(r"^\d+$", na=False)]
            df["Installs"] = pd.to_numeric(df["Installs"], errors='coerce').fillna(0).astype(int)

            # معالجة القيم المفقودة
            df = df.fillna(value={
                'Rating': 0.0,
                'Reviews': '0',
                'Size': 'Varies with device',
                'Type': 'Free',
                'Price': '0',
                'Content_Rating': 'Everyone',
                'Genres': 'Unknown',
                'Last_Updated': 'Unknown',
                'Current_Ver': 'Varies with device',
                'Android_Ver': 'Varies with device',
                'Category': 'UNKNOWN'
            })
            df = df.fillna('').replace({pd.NA: ''})

            print(f"📊 الأعمدة النهائية بعد التنظيف: {df.columns.tolist()}")
            print(f"✅ تم قراءة وتنظيف {len(df)} صفاً.")

            # --- C. Ingest Data into MySQL ---
            print("🚀 جاري إدخال البيانات إلى قاعدة البيانات...")
            cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            create_table_query = f"""
            CREATE TABLE {TABLE_NAME} (
                App TEXT,
                Category TEXT,
                Rating FLOAT NULL,
                Reviews TEXT,
                Size TEXT,
                Installs BIGINT,
                Type TEXT,
                Price TEXT,
                Content_Rating TEXT,
                Genres TEXT,
                Last_Updated TEXT,
                Current_Ver TEXT,
                Android_Ver TEXT
            );
            """
            cursor.execute(create_table_query)
            print(f"Table {TABLE_NAME} created successfully.")

            df_insert = df[['App', 'Category', 'Rating', 'Reviews', 'Size', 'Installs',
                            'Type', 'Price', 'Content_Rating', 'Genres',
                            'Last_Updated', 'Current_Ver', 'Android_Ver']].copy()
            df_insert = df_insert.replace({np.nan: None})
            data_tuples = [tuple(row) for row in df_insert.itertuples(index=False, name=None)]

            insert_query = f"""
            INSERT INTO {TABLE_NAME} 
            (App, Category, Rating, Reviews, Size, Installs, Type, Price, 
             Content_Rating, Genres, Last_Updated, Current_Ver, Android_Ver)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(insert_query, data_tuples)
            connection.commit()
            print(f"✅ تم إدخال البيانات بنجاح إلى جدول {TABLE_NAME}.")

            # --- D. Extract Data from MySQL to Seed File ---
            print(f"--- 2. استخراج البيانات من MySQL لملف Seed ---")
            extract_query = f"SELECT * FROM {TABLE_NAME}"
            df_extract = pd.read_sql(extract_query, connection)

            # 🩵 تنظيف عمود السعر من علامة الدولار وتحويله إلى رقم
            if "Price" in df_extract.columns:
                df_extract["Price"] = (
                    df_extract["Price"]
                    .astype(str)
                    .str.replace("$", "", regex=False)
                    .str.strip()
                )
                df_extract["Price"] = pd.to_numeric(df_extract["Price"], errors="coerce").fillna(0)

            os.makedirs(os.path.dirname(DBT_SEED_PATH), exist_ok=True)
            df_extract.to_csv(DBT_SEED_PATH, index=False, na_rep='NULL')
            print(f"✅ تم استخراج وحفظ {len(df_extract)} صف كملف Seed في: {DBT_SEED_PATH}")

    except Error as e:
        print(f"❌ خطأ أثناء الاتصال أو التعامل مع MySQL: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"❌ خطأ: لم يتم العثور على ملف CSV عند المسار: {CSV_SOURCE_PATH}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ حدث خطأ غير متوقع: {e}")
        sys.exit(1)
    finally:
        if connection and connection.is_connected():
            try:
                if cursor is not None:
                    cursor.close()
            except Exception as cursor_err:
                print(f"⚠️ Error closing cursor: {cursor_err}")
            connection.close()
            print("--- تم إغلاق اتصال MySQL ---")

if __name__ == "__main__":
    ingest_apps_to_mysql_and_seed()
