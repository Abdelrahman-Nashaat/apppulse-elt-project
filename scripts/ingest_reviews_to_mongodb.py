import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import sys # Import sys to allow exiting on error

# Load environment variables from .env file in the project root
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

# --- MongoDB Configuration (Using Env Vars for Docker) ---
MONGO_HOST = os.getenv("MONGO_HOST", "mongo_db")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_DB = os.getenv("MONGO_DB", "apppulse_reviews")
MONGO_COLLECTION = "reviews_raw"

# --- File Paths ---
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_SOURCE_PATH = os.path.join(PROJECT_ROOT, "data", "googleplaystore_user_reviews.csv")
DBT_SEED_PATH = os.path.join(PROJECT_ROOT, "app_dbt", "seeds", "reviews_from_mongo.csv") # Seed file path

def ingest_reviews_to_mongodb():
    """Reads reviews CSV, loads into MongoDB, then extracts to a dbt seed file."""
    client = None # Initialize client
    try:
        # --- A. MongoDB Connection ---
        print("--- 1. الاتصال بـ MongoDB ---")
        client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=5000) # Added timeout
        # Force connection check
        client.admin.command('ping')
        print("✅ تم الاتصال بنجاح بـ MongoDB.")
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]

        # --- B. Read CSV and Load to MongoDB ---
        print(f"📥 جاري قراءة ملف المراجعات من: {CSV_SOURCE_PATH}...")
        df_load = pd.read_csv(CSV_SOURCE_PATH)
        # Drop rows with NaN in essential columns like 'App' or 'Translated_Review' before inserting
        df_load.dropna(subset=['App', 'Translated_Review'], inplace=True)
         # Fill other NaNs if necessary, e.g., Sentiment with 'Neutral'
        df_load.fillna({'Sentiment': 'Neutral', 'Sentiment_Polarity': 0.0, 'Sentiment_Subjectivity': 0.0}, inplace=True)
        # Convert DataFrame to list of dictionaries for insertion
        records = df_load.to_dict(orient="records")

        # Delete existing data and insert new records
        collection.delete_many({})
        if records: # Only insert if there are records
            collection.insert_many(records)
            print(f"✅ تم تحميل {len(records)} مراجعة إلى MongoDB ({MONGO_COLLECTION}).")
        else:
            print("⚠️ No valid records found in CSV to load.")


        # --- C. Extract Data from MongoDB to Seed File ---
        print("--- 2. استخلاص البيانات من MongoDB وتحويلها لـ dbt Seed ---")

        # Extract all documents
        mongo_cursor = collection.find({})
        df_extract = pd.DataFrame(list(mongo_cursor))

        # Check if data was actually extracted
        if df_extract.empty:
            print("⚠️ No data extracted from MongoDB. Seed file will be empty.")
            # Create an empty file with headers if needed by dbt
            header_df = pd.DataFrame(columns=['App', 'Translated_Review', 'Sentiment', 'Sentiment_Polarity', 'Sentiment_Subjectivity']) # Match expected seed columns
            os.makedirs(os.path.dirname(DBT_SEED_PATH), exist_ok=True)
            header_df.to_csv(DBT_SEED_PATH, index=False)
            print(f"✅ تم إنشاء ملف Seed فارغ بالرؤوس في: {DBT_SEED_PATH}")
        else:
            # Clean columns: drop MongoDB's _id
            if '_id' in df_extract.columns:
                df_extract = df_extract.drop(columns=['_id'])

             # Ensure seeds directory exists
            os.makedirs(os.path.dirname(DBT_SEED_PATH), exist_ok=True)
            # Save as CSV
            df_extract.to_csv(DBT_SEED_PATH, index=False, na_rep='NULL') # Use 'NULL' string for missing values
            print(f"✅ تم استخراج وحفظ {len(df_extract)} صف كملف Seed في: {DBT_SEED_PATH}")

    except FileNotFoundError:
         print(f"❌ خطأ: لم يتم العثور على ملف CSV عند المسار: {CSV_SOURCE_PATH}")
         sys.exit(1) # Exit script with error code
    except Exception as e:
        print(f"❌ حدث خطأ غير متوقع: {e}")
        sys.exit(1) # Exit script with error code
    finally:
        if client:
            client.close()
            print("--- تم إغلاق اتصال MongoDB ---")

if __name__ == "__main__":
    ingest_reviews_to_mongodb()