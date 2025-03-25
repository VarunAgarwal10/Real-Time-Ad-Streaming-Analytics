CREATE TABLE content_tbl (
    content_id STRING,
    content_type STRING,
    genre STRING,
    content_length INT
)
WITH (
    'connector' = '{{ connector }}',
    'url' = '{{ url }}',
    'table-name' = '{{ table_name }}',
    'username' = '{{ username }}',
    'password' = '{{ password }}',
    'driver' = '{{ driver }}'
);