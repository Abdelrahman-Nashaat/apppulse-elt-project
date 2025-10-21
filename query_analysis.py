import duckdb
import os
import pandas as pd # تم إضافة pandas لطباعة النتيجة

# المسار الفعلي لقاعدة بيانات DuckDB كما هو محدد في profiles.yml
# تم استخراجه من مخرجات dbt debug
DB_FILE = "warehouse/apppulse.duckdb" 

# الاستعلام التحليلي المطلوب: أعلى 5 تطبيقات تقييماً
SQL_QUERY = """
    SELECT 
        da.app_name, 
        fm.average_user_rating 
    FROM 
        main.fact_app_metrics fm 
    JOIN 
        main.dim_apps da ON fm.app_id = da.app_id 
    ORDER BY 
        fm.average_user_rating DESC 
    LIMIT 5;
"""

def run_analysis_query():
    """يتصل بقاعدة البيانات وينفذ الاستعلام التحليلي."""
    print("--- 📊 نتائج التحليل: أعلى 5 تطبيقات تقييماً ---")
    
    # 1. إعداد المسار لاستخدامه (باستخدام المتغير المحلي)
    db_path_to_use = DB_FILE

    # 2. التحقق من المسار النسبي (warehouse/apppulse.duckdb)
    if not os.path.exists(db_path_to_use):
        # 3. إذا فشل، التحقق من المسار المطلق كبديل (لتجاوز أخطاء المسار النسبية)
        full_path_fallback = "F:/DEPI/Materials/Project_1_data_engineer/apppulse-project/warehouse/apppulse.duckdb"
        if os.path.exists(full_path_fallback):
            db_path_to_use = full_path_fallback # تحديث المسار المحلي
        else:
            print(f"❌ خطأ حرج: لم يتم العثور على ملف قاعدة بيانات DuckDB في المسار: {db_path_to_use}")
            print("الرجاء التأكد من تشغيل dbt run بنجاح في مجلد app_dbt/ قبل التنفيذ.")
            return

    conn = None
    try:
        # 4. الاتصال بالمسار المؤكد
        conn = duckdb.connect(database=db_path_to_use, read_only=True)
        
        print(f"✅ تم الاتصال بقاعدة البيانات بنجاح في المسار: {db_path_to_use}")
        
        # تنفيذ الاستعلام
        result = conn.execute(SQL_QUERY).fetchdf()
        
        # طباعة النتائج في شكل جدول
        print(result.to_markdown(index=False, numalign="left", stralign="left"))
        
    except Exception as e:
        print(f"❌ حدث خطأ أثناء تنفيذ الاستعلام: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    run_analysis_query()
