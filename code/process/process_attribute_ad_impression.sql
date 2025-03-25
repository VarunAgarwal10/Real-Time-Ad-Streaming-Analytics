INSERT INTO attributed_ad_impressions
WITH ad_stats AS (
    SELECT 
    *
    FROM impressions_tbl e
    JOIN ads_tbl a ON e.ad_id = a.ad_id
    JOIN content_tbl c ON e.content_id = c.content_id
    JOIN user_tbl u ON e.user_id = u.user_id
)
SELECT 
    event_id,
    session_id,
    user_id,
    signup_date,
    location_country,
    user_type,
    ad_id,
    ad_category,
    ad_duration,
    ad_format,
    ad_revenue_per_impression,
    content_id,
    content_type,
    genre,
    content_length,
    device_type,
    ad_position,
    user_action,
    datetime_occured
FROM ad_stats;