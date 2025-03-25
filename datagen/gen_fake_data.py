import argparse
import json
import random
from time import time
from datetime import datetime
from uuid import uuid4

import psycopg2
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

COUNTRIES = ["India", "USA", "China", "Germany", "France"]
DEVICE_TYPES = ['Mobile', 'Web', 'Smart TV']

AD_CATEGORIES = ['Tech', 'Food', 'Sports', 'Health', 'Entertainment', 'Finance']
AD_POSITIONS = ['Pre-roll', 'Mid-roll', 'Post-roll']
AD_FORMAT = ["Video", "Banner"]

CONTENT_TYPES = ['Video','Podcast','Music']
CONTENT_GENRES = ['Comedy', 'Drama', 'Action', 'Sports', 'Documentary', 'Music']

DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",  
    "port": "5432"
}

# Retry wrapper for database connection
def get_db_connection(retries=5, delay=5):
    for attempt in range(retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Connection Successful")
            return conn
        except Exception as e:
            print(f"PostgreSQL connection failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(delay)
    
    raise Exception("Max retries reached: Unable to connect to PostgreSQL.")

# Helper function to fetch from database using query
def fetch_data_from_db(query):
    """Fetch data from the database and return a list of results."""
    conn = get_db_connection()
    curr = conn.cursor()
    try:
        curr.execute(query)
        results = curr.fetchall()
        return [row[0] for row in results]  # Return list of first column values
    except Exception as e:
        print(f"Error fetching data: {e}")
        return []
    finally:
        curr.close()
        conn.close()


# Function to push the events to a Kafka topic
def push_to_kafka(event, topic):
    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    producer.produce(topic, json.dumps(event).encode('utf-8'))
    producer.flush()

# Helper function to create UUIDs
def generate_uuid():
    return str(uuid4())

# Helper function to get user_type
def get_user_type(signup_date):
    today = datetime.today().date()
    delta = today - signup_date
    return 'New User' if delta.days < 30 else 'Long Term User'

def gen_user_profiles(num_records):
    conn = get_db_connection()
    curr = conn.cursor()
    try:
        for _ in range(num_records):
            user_id = str(uuid4())
            signup_date = fake.date_between(start_date='-6w', end_date='today')
            location_country = random.choice(COUNTRIES)
            user_type = get_user_type(signup_date)

            curr.execute(
                """INSERT INTO ad_engagement.user_profiles 
                (user_id, signup_date, location_country, user_type) 
                VALUES (%s, %s, %s, %s)""",
                (user_id,signup_date, location_country,user_type)
            )
            conn.commit()
    except Exception as e:
        print(f"Error while generating data for user {id}: {e}")
        conn.rollback()  # Ensure no partial data is inserted
    finally:
        curr.close()
        conn.close()

def gen_ads_catalog(num_records):
    conn = get_db_connection()
    curr = conn.cursor()
    try:
        for _ in range(num_records):
            ad_id = generate_uuid()
            ad_category = random.choice(AD_CATEGORIES)
            ad_duration = random.choice([15, 30, 60, 90])
            ad_format = random.choice(AD_FORMAT)
            ad_rev_per_impression = round(random.uniform(0.1, 5.0), 4) 
            
            curr.execute(
                """INSERT INTO ad_engagement.ads_catalog
                (
                    ad_id, 
                    ad_category, 
                    ad_duration, 
                    ad_format, 
                    ad_revenue_per_impression
                ) 
                VALUES (%s, %s, %s, %s, %s)""",
                (ad_id, ad_category, ad_duration, ad_format, ad_rev_per_impression)
            )
            conn.commit()
    except Exception as e:
        print(f"Error while generating data for user {id}: {e}")
        conn.rollback()  # Ensure no partial data is inserted
    finally:
        curr.close()
        conn.close()

def gen_content_catalog(num_records):
    conn = get_db_connection()
    curr = conn.cursor()
    try:
        for _ in range(num_records):
            content_id = generate_uuid()
            content_type = random.choice(CONTENT_TYPES)
            genre = random.choice(CONTENT_GENRES)
            content_length = random.randint(180, 7200)
            
            curr.execute(
                """INSERT INTO ad_engagement.content_catalog
                (
                    content_id, 
                    content_type, 
                    genre, 
                    content_length
                ) 
                VALUES (%s, %s, %s, %s)""",
                (content_id, content_type, genre, content_length)
            )
            conn.commit()
    except Exception as e:
        print(f"Error while generating data for user {id}: {e}")
        conn.rollback()  # Ensure no partial data is inserted
    finally:
        curr.close()
        conn.close()  



def get_static_data():
    
    # Fetch and return available static data
    user_ids = fetch_data_from_db("SELECT user_id FROM ad_engagement.user_profiles")
    ad_ids = fetch_data_from_db("SELECT ad_id FROM ad_engagement.ads_catalog")
    content_ids = fetch_data_from_db("SELECT content_id FROM ad_engagement.content_catalog")

    if not user_ids or not ad_ids or not content_ids:
        print("Error: Could not fetch required data from database.")
        return
    else:
        return user_ids, ad_ids, content_ids


def gen_ad_interactions_eventstream(num_records):
    """Generate ad interaction events using stored database values."""
    
    user_ids, ad_ids, content_ids = get_static_data()
    try:
        for _ in range(num_records):
            user_id = random.choice(user_ids)
            ad_id = random.choice(ad_ids)
            content_id = random.choice(content_ids)
            device_type = random.choice(DEVICE_TYPES)
            ad_position = random.choice(AD_POSITIONS)
            user_action = random.choices(["Skip", "View Full", "Click"], weights=[0.50, 0.30, 0.20])[0]
            event_timestamp = datetime.now()

            event = {
                    "event_id": generate_uuid(),
                    "session_id": generate_uuid(),
                    "user_id": user_id,
                    "ad_id": ad_id,
                    "content_id": content_id,
                    "device_type": device_type,
                    "ad_position": ad_position,
                    "user_action": user_action,
                    "timestamp": event_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            }
            push_to_kafka(event, 'ad_interaction')

    except Exception as e:
        print(f"Error generating ad interaction event: {e}")

# Function to push the events to a Kafka topic
def push_to_kafka(event, topic):
    producer = Producer({'bootstrap.servers': 'localhost:9093'})
    producer.produce(topic, json.dumps(event).encode('utf-8'))
    producer.flush()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-nu",
        "--num_user_records",
        type=int,
        help="Number of user records to generate",
        default=500,
    )
    parser.add_argument(
        "-nc",
        "--num_content_records",
        type=int,
        help="Number of content records to generate",
        default=300,
    )
    parser.add_argument(
        "-na",
        "--num_ad_records",
        type=int,
        help="Number of ad records to generate",
        default= 100,
    )
    parser.add_argument(
        "-ne",
        "--num_event_records",
        type=int,
        help="Number of ad interaction evenet records to generate",
        default= 5000,
    )
    

    args = parser.parse_args()
    
    gen_user_profiles(args.num_user_records)
    gen_content_catalog(args.num_content_records)
    gen_ads_catalog(args.num_ad_records)
    gen_ad_interactions_eventstream(args.num_event_records)
    