select
    -- تنظيف البيانات
    t.App,
    t.Category,
    t.Rating,
    t.Reviews,
    -- إزالة علامات '+' و ',' وتحويلها إلى رقم صحيح
    cast(replace(replace(CAST(t.Installs AS VARCHAR), '+', ''), ',', '') as integer) as installs,
    -- تحويل الحجم (Size) إلى بايتات (KB أو MB)
    case
        when t.Size like '%M' then try_cast(replace(t.Size, 'M', '') as double) * 1024 * 1024 -- Megabytes
        when t.Size like '%k' then try_cast(replace(t.Size, 'k', '') as double) * 1024       -- Kilobytes
        else null
    end as size_bytes,
    t.Type,
    -- استخدام TRY_CAST هنا لحل مشكلة 'Everyone'
    try_cast(replace(t.Price, '$', '') as double) as price,
    t.Content_Rating as content_rating,
    t.Genres,
    t.Last_Updated as last_updated_date,
    t.Current_Ver as current_version,
    t.Android_Ver as required_android_version
from
    apppulse_mysql.apppulse.apps_raw as t
where
    -- إزالة الصفوف التي تحتوي على تصنيف فارغ أو بيانات غير صالحة
    t.Rating is not null and t.App is not null
    -- أزلنا شرط t.Price NOT ILIKE لعدم الحاجة إليه بعد TRY_CAST
