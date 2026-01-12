import os
import json
import time
import mysql.connector
from kafka import KafkaConsumer, KafkaProducer

DB_HOST = os.getenv('DB_HOST', 'db')
DB_USER = os.getenv('DB_USER', 'app_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'app_password')
DB_NAME = os.getenv('DB_NAME', 'data_db')

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_INPUT = 'to-alert-system'
TOPIC_OUTPUT = 'to-notifier'

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )

def start_alert_system():
    print("Alert System starting...")
    time.sleep(15)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    consumer = KafkaConsumer(
        TOPIC_INPUT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='alert_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Listening on topic '{TOPIC_INPUT}'...")

    for message in consumer:
        data = message.value
        airport_code = data.get('airport')
        total_flights = data.get('total')
        
        print(f"Received data for {airport_code}: {total_flights} flights")

        try:
            conn = get_db_connection()
            cursor = conn.cursor(dictionary=True)
            
            query = """
                SELECT user_email, high_value, low_value 
                FROM interests 
                WHERE airport_code = %s 
                AND (high_value IS NOT NULL OR low_value IS NOT NULL)
            """
            cursor.execute(query, (airport_code,))
            interested_users = cursor.fetchall()
            
            for user in interested_users:
                email = user['user_email']
                high = user['high_value']
                low = user['low_value']
                alert_type = None

                if high is not None and total_flights > high:
                    alert_type = "HIGH VALUE ALERT"
                    condition = f"Flights ({total_flights}) > Threshold ({high})"
                elif low is not None and total_flights < low:
                    alert_type = "LOW VALUE ALERT"
                    condition = f"Flights ({total_flights}) < Threshold ({low})"
                
                if alert_type:
                    alert_payload = {
                        "email": email,
                        "subject": f"{alert_type}: {airport_code}",
                        "body": f"Attention, threshold exceeded for {airport_code}. {condition}."
                    }
                    producer.send(TOPIC_OUTPUT, alert_payload)
                    print(f" -> Alert sent for {email} ({condition})")

            cursor.close()
            conn.close()

        except Exception as e:
            print(f"Error processing alert: {e}")

if __name__ == "__main__":
    start_alert_system()