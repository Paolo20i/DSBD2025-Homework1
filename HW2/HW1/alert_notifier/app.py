import os
import json
import time
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_INPUT = 'to-notifier'

def start_notifier():
    print("Notifier System starting... waiting for Kafka...")
    time.sleep(10)

    consumer = KafkaConsumer(
        TOPIC_INPUT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='notifier_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Ready to send emails. Listening on '{TOPIC_INPUT}'...")

    for message in consumer:
        email_data = message.value
        
        # Simulazione invio Email
        print("\n" + "#"*40)
        print(" [EMAIL SERVICE] SENDING MESSAGE...")
        print(f" To:      {email_data.get('email')}")
        print(f" Subject: {email_data.get('subject')}")
        print(f" Body:    {email_data.get('body')}")
        print("#"*40 + "\n")

if __name__ == "__main__":
    start_notifier()