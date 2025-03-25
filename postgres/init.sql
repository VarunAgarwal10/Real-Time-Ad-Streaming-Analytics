CREATE SCHEMA ad_engagement;

-- Use commerce schema
SET
    search_path TO ad_engagement;

-- Create table for content catalog
CREATE TABLE content_catalog (
    content_id VARCHAR PRIMARY KEY,
    content_type VARCHAR(50) NOT NULL,  -- Video, Music, Podcast
    genre VARCHAR(100) NOT NULL,  -- Comedy, Action, Drama, etc.
    content_length INT NOT NULL CHECK (content_length > 0)  -- Duration in seconds
);

-- create a users table
CREATE TABLE user_profiles (
    user_id VARCHAR PRIMARY KEY,
    signup_date DATE NOT NULL,
    location_country VARCHAR(50) NOT NULL,
    user_type VARCHAR(20) NOT NULL
);

-- Create table for ads catalog
CREATE TABLE ads_catalog (
    ad_id VARCHAR PRIMARY KEY,
    ad_category VARCHAR(100) NOT NULL,  -- Tech, Food, Sports, etc.
    ad_duration INT NOT NULL CHECK (ad_duration > 0),  -- Ad length in seconds
    ad_format VARCHAR(50) NOT NULL,
    ad_revenue_per_impression DECIMAL(10, 4) NOT NULL CHECK (ad_revenue_per_impression >= 0) -- Revenue per ad view
);

-- Set REPLICA IDENTITY FULL to track changes in replication for analytics
ALTER TABLE user_profiles REPLICA IDENTITY FULL;
ALTER TABLE content_catalog REPLICA IDENTITY FULL;
ALTER TABLE ads_catalog REPLICA IDENTITY FULL;

CREATE TABLE attributed_ad_impressions (
    event_id VARCHAR PRIMARY KEY,
    session_id VARCHAR,
    user_id VARCHAR,
    signup_date DATE NOT NULL,
    location_country VARCHAR,
    user_type VARCHAR,
    ad_id VARCHAR,
    ad_category VARCHAR,
    ad_duration INT,
    ad_format VARCHAR,
    ad_revenue_per_impression DECIMAL(10, 4),
    content_id VARCHAR,
    content_type VARCHAR,
    genre VARCHAR,
    content_length INT,
    device_type VARCHAR,
    ad_position VARCHAR,
    user_action VARCHAR,
    datetime_occured TIMESTAMP
);