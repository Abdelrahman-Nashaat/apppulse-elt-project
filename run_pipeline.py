import os
import subprocess
import sys

# ------------------------------------------------------------------- #
# نفس المسارات اللي حددناها للـ DAG
# ------------------------------------------------------------------- #
PROJECT_ROOT = "F:/DEPI/Materials/Project_1_data_engineer/apppulse-project"
VENV_PYTHON = "F:/DEPI/Materials/Project_1_data_engineer/apppulse-project/venv/Scripts/python.exe"
VENV_DBT = "F:/DEPI/Materials/Project_1_data_engineer/apppulse-project/venv/Scripts/dbt.exe"
DBT_PROJECT_DIR = "F:/DEPI/Materials/Project_1_data_engineer/apppulse-project/app_dbt"

SCRIPT_MYSQL = os.path.join(PROJECT_ROOT, "scripts", "ingest_apps_to_mysql.py")
SCRIPT_MONGO = os.path.join(PROJECT_ROOT, "scripts", "ingest_reviews_to_mongodb.py")

# ------------------------------------------------------------------- #
# دالة لتشغيل الأوامر
# ------------------------------------------------------------------- #
def run_command(command_list):
    """ينفذ أمر في الـ terminal ويعرض الناتج مباشرة."""
    try:
        # بنستخدم المسار المطلق لملف بايثون أو dbt
        print(f"\n🚀 ... [ RUNNING ] ...\n{' '.join(command_list)}\n")
        
        # بنشغل الأمر
        process = subprocess.run(
            command_list, 
            check=True, 
            text=True, 
            capture_output=False, # اعرض الناتج أول بأول
            stderr=sys.stderr,
            stdout=sys.stdout
        )
        
        print(f"\n✅ ... [ SUCCESS ] ...\n{' '.join(command_list)}\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"\n❌ ... [ FAILED ] ...\n{' '.join(command_list)}\n")
        print(f"Error: {e}")
        return False
    except FileNotFoundError:
        print(f"\n❌ ... [ FAILED ] ...\n")
        print(f"خطأ: لم يتم العثور على الملف. هل المسار صحيح؟ \n{command_list[0]}")
        return False

# ------------------------------------------------------------------- #
# تعريف البايبلاين
# ------------------------------------------------------------------- #
def main_pipeline():
    print("==============================================")
    print("🏁 بدء تشغيل بايبلاين AppPulse ELT...")
    print("==============================================")

    # --- المهمة 1: تحميل البيانات إلى MySQL ---
    if not run_command([VENV_PYTHON, SCRIPT_MYSQL]):
        print("فشلت مهمة MySQL. يتم إيقاف البايبلاين.")
        return

    # --- المهمة 2: تحميل البيانات إلى MongoDB وإنشاء الـ Seed ---
    if not run_command([VENV_PYTHON, SCRIPT_MONGO]):
        print("فشلت مهمة MongoDB. يتم إيقاف البايبلاين.")
        return

    # --- المهمة 3: تشغيل dbt run (بعد انتهاء الإدخال) ---
    # ملاحظة: أمر dbt run بيحتاج يتنفذ من جوه مجلد dbt
    # فالأمر هيكون [مسار dbt, "run"] والـ cwd هو مسار مجلد dbt
    print(f"\n🚀 ... [ RUNNING ] ...\ndbt run (in {DBT_PROJECT_DIR})\n")
    try:
        subprocess.run(
            [VENV_DBT, "run"],  # الأمر اللي هيتنفذ
            check=True,
            text=True,
            cwd=DBT_PROJECT_DIR, # أهم جزء: غيّر مسار العمل للمجلد دا
            stderr=sys.stderr,
            stdout=sys.stdout
        )
        print(f"\n✅ ... [ SUCCESS ] ...\ndbt run\n")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ ... [ FAILED ] ...\ndbt run\nError: {e}")
        return

    print("==============================================")
    print("🎉 اكتمل تشغيل البايبلاين بنجاح!")
    print("==============================================")


# ------------------------------------------------------------------- #
# نقطة البداية
# ------------------------------------------------------------------- #
if __name__ == "__main__":
    # نتأكد إننا بنستخدم بايثون من الـ venv الصحيحة
    if sys.executable.lower() != VENV_PYTHON.lower():
        print(f"⚠️ تحذير: أنت تشغل هذا السكريبت باستخدام بايثون مختلف ({sys.executable})")
        print(f"يُفضل تشغيله باستخدام البيئة الافتراضية الصحيحة:")
        print(f"{VENV_PYTHON} run_pipeline.py")
        # هنكمل comunque بس دا مجرد تحذير
        
    main_pipeline()