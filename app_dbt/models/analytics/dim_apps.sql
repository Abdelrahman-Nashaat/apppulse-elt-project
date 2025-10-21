{{ config(
    materialized='table'
) }}

-- يستخدم هذا النموذج بيانات الـ Staging النظيفة لإنشاء جدول أبعاد (Dimension) للتطبيقات.
-- الهدف الأساسي هو توليد مفتاح رئيسي (app_id) لكل تطبيق فريد.

select
    -- توليد مفتاح رئيسي (Surrogate Key) باستخدام دالة MD5 Hash
    -- يتم استخدام اسم التطبيق والتصنيف لضمان فرادة المفتاح (إذا تكرر اسم التطبيق في تصنيف مختلف)
    md5(cast(coalesce(cast(t.App as string), '_') || '_' || coalesce(cast(t.Category as string), '_') as string)) as app_id,
    
    -- بيانات الأبعاد الثابتة للتطبيق
    t.App as app_name,
    t.Category as app_category,
    t.Genres as app_genres,
    t.Content_Rating as content_rating,

    -- بيانات السعر ونوع التطبيق
    t.Type as app_type,
    t.Price as app_price, -- تم تنظيفه ليصبح رقمًا عشريًا في Staging

    -- بيانات تاريخ التحديث والإصدارات
    t.Last_Updated_Date as last_updated_date,
    t.Current_Version as current_version,
    t.Required_Android_Version as required_android_version,

    -- لربط جدول الأبعاد هذا بجدول الحقائق لاحقًا، نحتفظ بهذه الأعمدة:
    t.Size_Bytes as app_size_bytes

from
    {{ ref('stg_apps') }} as t
where
    t.App is not null