import os
import json
import time
import smtplib
from email.message import EmailMessage
from kafka import KafkaConsumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')

TOPIC_INPUT = 'to-notifier'

def send_real_email(recipient, subject, body):
    """Funzione per l'invio fisico della mail tramite SMTP"""
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = SMTP_USER
        msg['To'] = recipient

        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()  
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        print(f" [SUCCESS] Email inviata a {recipient}")
    except Exception as e:
        print(f" [ERROR] Fallimento invio email: {e}")

def start_notifier():
    print("Notifier System starting... waiting for Kafka...")
    time.sleep(10)

    consumer = KafkaConsumer(
        TOPIC_INPUT,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='notifier_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Ready to send real emails via {SMTP_SERVER}. Listening on '{TOPIC_INPUT}'...")

    for message in consumer:
        email_data = message.value
        recipient = email_data.get('email')
        subject = email_data.get('subject')
        body = email_data.get('body')

        if recipient and body:
           
            send_real_email(recipient, subject, body)
        else:
            print(" [WARNING] Dati email incompleti ricevuti da Kafka")

if __name__ == "__main__":
    start_notifier()