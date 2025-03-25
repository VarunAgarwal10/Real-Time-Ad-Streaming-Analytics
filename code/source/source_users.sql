CREATE TABLE user_tbl (
    user_id STRING,
    signup_date DATE,
    location_country STRING,
    user_type STRING
) WITH (
    'connector' = '{{ connector }}',
    'url' = '{{ url }}',
    'table-name' = '{{ table_name }}',
    'username' = '{{ username }}',
    'password' = '{{ password }}',
    'driver' = '{{ driver }}'
);