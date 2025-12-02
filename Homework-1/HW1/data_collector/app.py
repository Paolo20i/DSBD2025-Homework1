import os
import time
import threading
import requests
import mysql.connector
from flask import Flask, request, jsonify

# Import per gRPC
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

OPENSKY_API_URL = "https://opensky-network.org/api"
OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
OPENSKY_CLIENT_ID = os.getenv('OPENSKY_CLIENT_ID')
OPENSKY_CLIENT_SECRET = os.getenv('OPENSKY_CLIENT_SECRET')

COLLECTION_INTERVAL_SECONDS = 3600 

def get_db_connection():
    # Restituisce una connessione al Data DB
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )

def init_db():
    # Inizializza le tabelle per gli interessi e i voli
    retries = 5
    while retries > 0:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Tabella Interessi
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS interests (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    user_email VARCHAR(255),
                    airport_code VARCHAR(10),
                    UNIQUE(user_email, airport_code)
                )
            """)

            # Tabella Voli
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
            print("Data DB initialized successfully.")
            return
        except Exception as e:
            print(f"DB Error during initialization: {e}, retrying...")
            retries -= 1
            time.sleep(5)

# --- CLIENT gRPC ---

def check_user_exists_grpc(email):
    # Interroga lo User Manager via gRPC per verificare l'esistenza di un utente
    try:
        channel = grpc.insecure_channel(f'{USER_MANAGER_HOST}:50051')
        stub = user_pb2_grpc.UserServiceStub(channel)
        
        response = stub.CheckUserExists(user_pb2.UserRequest(email=email), timeout=5) 
        return response.exists
    except grpc.RpcError as e:
        print(f"gRPC Error contacting User Manager: {e}")
        return False

# --- FUNZIONI OPEN SKY & TOKEN ---

def get_opensky_token():
    # Ottiene un Access Token da OpenSky usando le credenziali Client
    if not OPENSKY_CLIENT_ID or not OPENSKY_CLIENT_SECRET:
        print("Missing OpenSky Credentials via Environment Variables.")
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
        else:
            print(f"Failed to get Token. Status: {response.status_code}, Resp: {response.text}")
            return None
    except Exception as e:
        print(f"Error connecting to Auth Server: {e}")
        return None

def fetch_flights_for_airport(icao_code, direction='arrival', hours_ago=1):
    # Scarica i dati dei voli da OpenSky per un dato aeroporto e direzione
    now = int(time.time())
    begin = now - (hours_ago * 3600) 
    end = now

    endpoint = f"{OPENSKY_API_URL}/flights/{direction}" 
    params = {'airport': icao_code, 'begin': begin, 'end': end}

    token = get_opensky_token()
    if not token:
        return []

    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(endpoint, params=params, headers=headers, timeout=10)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return [] 
        elif response.status_code == 429:
            print(f"RATE LIMIT HIT for {icao_code}. Skipping to avoid ban.")
            return []
        else:
            print(f"OpenSky API Error {response.status_code} for {icao_code}: {response.text}")
            return []
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from OpenSky for {icao_code} ({direction}): {e}")
        return []

# --- WORKER CICLICO ---

def fetch_flight_data_worker():
    # Processo in background che ciclicamente scarica i dati per gli aeroporti monitorati
    while True:
        print(f"\n--- Starting new collection cycle at {time.ctime()} ---")
        airports = []
        
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Recupera aeroporti unici
            cursor.execute("SELECT DISTINCT airport_code FROM interests")
            airports = [row[0] for row in cursor.fetchall()]
            
            if not airports:
                print("No airports to monitor. Sleeping...")
                cursor.close()
                conn.close()
                time.sleep(COLLECTION_INTERVAL_SECONDS)
                continue

            print(f"Monitoring {len(airports)} airports: {', '.join(airports)}")
            
            for airport_code in airports:
                # Arrivi
                arrivals = fetch_flights_for_airport(airport_code, direction='arrival')
                time.sleep(2) 
                
                # Partenze
                departures = fetch_flights_for_airport(airport_code, direction='departure')
                time.sleep(2)

                all_flights = [(f, 'ARRIVAL') for f in arrivals] + [(f, 'DEPARTURE') for f in departures]
                
                if all_flights:
                    print(f" -> {airport_code}: Saving {len(all_flights)} flights.")

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
        
        print(f"--- Cycle finished. Sleeping for {COLLECTION_INTERVAL_SECONDS} seconds... ---")
        time.sleep(COLLECTION_INTERVAL_SECONDS) 

# --- REST API ---

@app.route('/interests', methods=['POST'])
def add_interest():
    # Aggiunge un nuovo aeroporto da monitorare per l'utente specificato
    data = request.json
    email = data.get('email')
    airport = data.get('airport').upper() if data.get('airport') else None
    
    if not email or not airport:
        return jsonify({"error": "Email and airport are required"}), 400
    
    if not check_user_exists_grpc(email):
        return jsonify({"error": f"User {email} not found (verified via gRPC)."}), 404
        
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT IGNORE INTO interests (user_email, airport_code) VALUES (%s, %s)", (email, airport))
        if cursor.rowcount > 0:
            msg, status = "Interest added", 201
        else:
            msg, status = "Interest already exists", 200
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"message": msg}), status
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500

@app.route('/interests/<email>', methods=['GET'])
def get_user_interests(email):
    # Restituisce la lista degli aeroporti seguiti da un utente
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
    # Cancella tutti gli interessi di un utente
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
    # Restituisce gli ultimi 10 voli registrati per un aeroporto
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

# --- ANALISI ---

@app.route('/analysis/last_flight/<airport>', methods=['GET'])
def get_last_flight(airport):
    # Restituisce i dettagli dell'ultimo singolo volo registrato
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
    # Calcola la media dei voli per un dato aeroporto negli ultimi X giorni
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

# --- MAIN ---
if __name__ == '__main__':
    init_db()
    
    worker = threading.Thread(target=fetch_flight_data_worker)
    worker.daemon = True
    worker.start()
    print("Background Worker started.")
    
    print("REST API (Data Collector) starting on port 5001...")
    app.run(host='0.0.0.0', port=5001)