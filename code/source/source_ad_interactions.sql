CREATE TABLE impressions_tbl (
    event_id STRING,
    session_id STRING,
    user_id STRING,
    ad_id STRING,
    content_id STRING,
    device_type STRING,
    ad_position STRING,
    user_action STRING,
    datetime_occured TIMESTAMP(3)
)
WITH (
    'connector' = '{{ connector }}',
    'topic' = '{{ topic }}',
    'properties.bootstrap.servers' = '{{ bootstrap_servers }}',
    'properties.group.id' = '{{ consumer_group_id }}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = '{{ format }}'
);