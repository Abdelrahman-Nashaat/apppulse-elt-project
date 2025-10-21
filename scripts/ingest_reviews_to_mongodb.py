import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# 1️⃣ تحميل الإعدادات من ملف .env
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

# تفاصيل MongoDB
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "apppulse_reviews") 
MONGO_COLLECTION = "reviews_raw" 

# مسارات الملفات
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_SOURCE_PATH = os.path.join(PROJECT_ROOT, "data", "googleplaystore_user_reviews.csv")
DBT_SEED_PATH = os.path.join(PROJECT_ROOT, "app_dbt", "seeds", "reviews_from_mongo.csv") 

def ingest_reviews_to_mongodb():
    """يحمل البيانات من CSV إلى MongoDB ثم يستخلصها كملف Seed لـ dbt."""
    
    print("--- 1. الاتصال بـ MongoDB ---")
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # ------------------- A. مرحلة التحميل -------------------
    
    print(f"📥 جاري قراءة ملف المراجعات من: {CSV_SOURCE_PATH}...")
    try:
        df_load = pd.read_csv(CSV_SOURCE_PATH) 
        records = df_load.to_dict(orient="records")
        
        # حذف وإدخال البيانات الجديدة
        collection.delete_many({})
        collection.insert_many(records)
        print(f"✅ تم تحميل {len(records)} مراجعة إلى MongoDB (reviews_raw).")
    except FileNotFoundError:
        print(f"❌ خطأ: لم يتم العثور على ملف CSV عند المسار: {CSV_SOURCE_PATH}")
        client.close()
        return

    # ------------------- B. مرحلة الاستخلاص لـ dbt Seed -------------------
    
    print("--- 2. استخلاص البيانات من MongoDB وتحويلها لـ dbt Seed ---")
    
    mongo_cursor = collection.find({})
    df_extract = pd.DataFrame(list(mongo_cursor))
    
    if '_id' in df_extract.columns:
        df_extract = df_extract.drop(columns=['_id'])
    
    # حفظ كملف CSV في مجلد Seeds
    os.makedirs(os.path.dirname(DBT_SEED_PATH), exist_ok=True)
    df_extract.to_csv(DBT_SEED_PATH, index=False)
    
    print(f"✅ تم استخراج وحفظ {len(df_extract)} صف كملف Seed في: {DBT_SEED_PATH}")

    client.close()
    print("--- العملية اكتملت بنجاح ---")


if __name__ == "__main__":
    ingest_reviews_to_mongodb()