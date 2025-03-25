CREATE TABLE attributed_ad_impressions (
    event_id STRING,
    session_id STRING,
    user_id STRING,
    signup_date DATE,
    location_country STRING,
    user_type STRING,
    ad_id STRING,
    ad_category STRING,
    ad_duration INT,
    ad_format STRING,
    ad_revenue_per_impression DECIMAL(10, 4),
    content_id STRING,
    content_type STRING,
    genre STRING,
    content_length INT,
    device_type STRING,
    ad_position STRING,
    user_action STRING,
    datetime_occured TIMESTAMP(3),
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = '{{ connector }}',
    'url' = '{{ url }}',
    'table-name' = '{{ table_name }}',
    'username' = '{{ username }}',
    'password' = '{{ password }}',
    'driver' = '{{ driver }}'
);
