import os
import time
import threading
import requests
import json
import mysql.connector
import pybreaker
from kafka import KafkaProducer
from flask import Flask, request, jsonify, Response

# --- PROMETHEUS IMPORT ---
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

import grpc
import user_pb2
import user_pb2_grpc

app = Flask(__name__)

# --- CONFIGURAZIONE ---
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_USER = os.getenv('DB_USER', 'app_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'app_password')
DB_NAME = os.getenv('DB_NAME', 'data_db') 

USER_MANAGER_HOST = os.getenv('USER_MANAGER_HOST', 'user-manager')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
NODE_NAME = os.getenv('NODE_NAME', 'unknown_node')

OPENSKY_API_URL = "https://opensky-network.org/api"
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_CLIENT_ID = os.getenv('OPENSKY_CLIENT_ID')
OPENSKY_CLIENT_SECRET = os.getenv('OPENSKY_CLIENT_SECRET')

COLLECTION_INTERVAL_SECONDS = 30

# --- PROMETHEUS METRICS ---
# Metrica COUNTER: Voli scaricati
FLIGHTS_FETCHED_TOTAL = Counter(
    'flights_fetched_total', 
    'Total number of flights fetched from OpenSky',
    ['service', 'node', 'airport']
)

# Metrica GAUGE: Tempo ultima fetch (Performance)
LAST_FETCH_DURATION = Gauge(
    'last_fetch_duration_seconds',
    'Time taken for the last fetch operation in seconds',
    ['service', 'node', 'airport']
)

# Circuit Breaker
open_sky_breaker = pybreaker.CircuitBreaker(fail_max=3, reset_timeout=60)

def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )

def init_db():
    retries = 5
    while retries > 0:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS interests (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_email VARCHAR(255),
                    airport_code VARCHAR(10),
                    high_value INT DEFAULT NULL,
                    low_value INT DEFAULT NULL,
                    UNIQUE(user_email, airport_code)
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS flights (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    airport_code VARCHAR(10),
                    icao24 VARCHAR(20),
                    callsign VARCHAR(20),
                    arrival_airport VARCHAR(10),
                    departure_airport VARCHAR(10),
                    time INT,
                    type VARCHAR(10), 
                    UNIQUE(icao24, time, departure_airport, arrival_airport) 
                )
            """)
            conn.commit()
            cursor.close()
            conn.close()
            print("Data DB initialized.")
            return
        except Exception as e:
            print(f"DB Error: {e}")
            retries -= 1
            time.sleep(5)

# --- PROMETHEUS ENDPOINT ---
@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

# --- gRPC ---
def check_user_exists_grpc(email):
    try:
        channel = grpc.insecure_channel(f'{USER_MANAGER_HOST}:50051')
        stub = user_pb2_grpc.UserServiceStub(channel)
        response = stub.CheckUserExists(user_pb2.UserRequest(email=email), timeout=5) 
        return response.exists
    except grpc.RpcError as e:
        print(f"gRPC Error: {e}")
        return False

# --- OPEN SKY ---
def get_opensky_token():
    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        return None
    payload = {
        'grant_type': 'client_credentials',
        'client_id': OPENSKY_CLIENT_ID,
        'client_secret': OPENSKY_CLIENT_SECRET
    }
    try:
        response = requests.post(OPENSKY_TOKEN_URL, data=payload, timeout=10)
        if response.status_code == 200:
            return response.json().get('access_token')
        return None
    except Exception:
        return None

@open_sky_breaker
def fetch_with_breaker(endpoint, params, headers):
    return requests.get(endpoint, params=params, headers=headers, timeout=10)

def fetch_flights_for_airport(icao_code, direction='arrival', hours_ago=1):
    now = int(time.time())
    begin = now - (hours_ago * 3600) 
    end = now
    endpoint = f"{OPENSKY_API_URL}/flights/{direction}" 
    params = {'airport': icao_code, 'begin': begin, 'end': end}
    token = get_opensky_token()
    if not token: return []
    headers = {"Authorization": f"Bearer {token}"}

    # Timer per la metrica GAUGE
    start_time = time.time()
    try:
        response = fetch_with_breaker(endpoint, params, headers)
        duration = time.time() - start_time
        
        # Aggiorna Gauge durata
        LAST_FETCH_DURATION.labels(service='data-collector', node=NODE_NAME, airport=icao_code).set(duration)

        if response.status_code == 200:
            flights = response.json()
            # Aggiorna Counter voli totali
            FLIGHTS_FETCHED_TOTAL.labels(service='data-collector', node=NODE_NAME, airport=icao_code).inc(len(flights))
            return flights
        elif response.status_code == 404:
            return [] 
        elif response.status_code == 429:
            print(f"RATE LIMIT HIT for {icao_code}.")
            return []
        else:
            print(f"OpenSky API Error {response.status_code}")
            return []
    except pybreaker.CircuitBreakerError:
        print(f"CIRCUIT BREAKER OPEN. Skipping {icao_code}.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Network Error: {e}")
        return []

# --- WORKER ---
def fetch_flight_data_worker():
    print("Worker started. Waiting for Kafka...")
    kafka_producer = None

    while True:
        if kafka_producer is None:
            try:
                kafka_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                print("Kafka Producer connected.")
            except Exception:
                print("Kafka not ready. Retrying next cycle.")

        print(f"\n--- Starting cycle at {time.ctime()} ---")
        airports = []
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT airport_code FROM interests")
            airports = [row[0] for row in cursor.fetchall()]
            
            if not airports:
                print("No airports to monitor.")
                cursor.close()
                conn.close()
                time.sleep(COLLECTION_INTERVAL_SECONDS)
                continue

            for airport_code in airports:
                arrivals = fetch_flights_for_airport(airport_code, direction='arrival')
                time.sleep(1) 
                departures = fetch_flights_for_airport(airport_code, direction='departure')
                time.sleep(1)

                all_flights = [(f, 'ARRIVAL') for f in arrivals] + [(f, 'DEPARTURE') for f in departures]
                total_flights_count = len(all_flights)

                if kafka_producer:
                    try:
                        message = {
                            "airport": airport_code,
                            "total": total_flights_count,
                            "timestamp": time.time()
                        }
                        kafka_producer.send('to-alert-system', message)
                        print(f" -> Sent Kafka update for {airport_code}")
                    except Exception:
                        kafka_producer = None

                for flight_data, f_type in all_flights:
                    icao24 = flight_data.get('icao24')
                    callsign = flight_data.get('callsign', 'N/A')
                    f_time = flight_data.get('firstSeen') if f_type == 'DEPARTURE' else flight_data.get('lastSeen')
                    arr_airport = flight_data.get('estArrivalAirport', '')
                    dep_airport = flight_data.get('estDepartureAirport', '')

                    try:
                        cursor.execute("""
                            INSERT IGNORE INTO flights 
                            (airport_code, icao24, callsign, time, type, departure_airport, arrival_airport)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (airport_code, icao24, callsign, f_time, f_type, dep_airport, arr_airport))
                    except Exception:
                        pass 
                conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"Worker Error: {e}")
        
        print(f"--- Cycle finished. Sleeping... ---")
        time.sleep(COLLECTION_INTERVAL_SECONDS) 

# --- REST API ---
@app.route('/interests', methods=['POST'])
def add_interest():
    data = request.json
    email = data.get('email')
    airport = data.get('airport').upper() if data.get('airport') else None
    high_value = data.get('high_value')
    low_value = data.get('low_value')
    
    if high_value is not None and low_value is not None and int(high_value) <= int(low_value):
         return jsonify({"error": "high_value must be greater than low_value"}), 400

    if not email or not airport:
        return jsonify({"error": "Email and airport are required"}), 400
    
    if not check_user_exists_grpc(email):
        return jsonify({"error": f"User {email} not found (verified via gRPC)."}), 404
        
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
            INSERT INTO interests (user_email, airport_code, high_value, low_value) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            high_value = VALUES(high_value), 
            low_value = VALUES(low_value)
        """
        cursor.execute(query, (email, airport, high_value, low_value))
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": f"Interest updated for {airport}"}), 200
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@app.route('/interests/<email>', methods=['GET'])
def get_user_interests(email):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT airport_code FROM interests WHERE user_email = %s", (email,))
        airports = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return jsonify(airports), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/interests/<email>', methods=['DELETE'])
def delete_user_interests(email):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM interests WHERE user_email = %s", (email,))
        count = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": f"Deleted {count} interests for {email}"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/flights/<airport>', methods=['GET'])
def get_flights(airport):
    airport = airport.upper()
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT icao24, callsign, time, type, departure_airport, arrival_airport 
            FROM flights WHERE airport_code = %s 
            ORDER BY time DESC LIMIT 10
        """, (airport,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        for row in rows:
            row['timestamp_readable'] = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(row['time']))
            del row['time']
        return jsonify(rows), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/analysis/last_flight/<airport>', methods=['GET'])
def get_last_flight(airport):
    airport = airport.upper()
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT * FROM flights WHERE airport_code = %s 
            ORDER BY time DESC LIMIT 1
        """, (airport,))
        flight = cursor.fetchone()
        cursor.close()
        conn.close()
        if flight:
            flight['timestamp_readable'] = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(flight['time']))
            return jsonify(flight), 200
        else:
            return jsonify({"message": "No flights found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/analysis/average_flights/<airport>', methods=['GET'])
def get_average_flights(airport):
    airport = airport.upper()
    days = request.args.get('days', 7, type=int)
    start_time = int(time.time()) - (days * 24 * 3600)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*), 
                   COUNT(CASE WHEN type='ARRIVAL' THEN 1 END),
                   COUNT(CASE WHEN type='DEPARTURE' THEN 1 END)
            FROM flights WHERE airport_code = %s AND time >= %s
        """, (airport, start_time))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        total, arr, dep = result
        divisor = max(1, days)
        return jsonify({
            "airport": airport,
            "days_analyzed": days,
            "total_flights": total,
            "average_flights_per_day": round(total / divisor, 2),
            "average_arrivals_per_day": round(arr / divisor, 2),
            "average_departures_per_day": round(dep / divisor, 2)
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    init_db()
    worker = threading.Thread(target=fetch_flight_data_worker)
    worker.daemon = True
    worker.start()
    app.run(host='0.0.0.0', port=5001)