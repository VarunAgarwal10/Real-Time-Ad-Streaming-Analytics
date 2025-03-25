CREATE TABLE ads_tbl (
    ad_id STRING,
    ad_category STRING,
    ad_duration INT,
    ad_format STRING,
    ad_revenue_per_impression DECIMAL(10, 4)
)
WITH (
    'connector' = '{{ connector }}',
    'url' = '{{ url }}',
    'table-name' = '{{ table_name }}',
    'username' = '{{ username }}',
    'password' = '{{ password }}',
    'driver' = '{{ driver }}'
);