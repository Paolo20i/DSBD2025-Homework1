import os
import time
import threading
import requests
import mysql.connector
from concurrent import futures
from flask import Flask, request, jsonify

import grpc
import user_pb2
import user_pb2_grpc

app = Flask(__name__)

# --- CONFIGURAZIONE ---
DB_HOST = os.getenv('DB_HOST', 'db')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_USER = os.getenv('DB_USER', 'app_user')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'app_password')
DB_NAME = os.getenv('DB_NAME', 'user_db')
DATA_COLLECTOR_URL = "http://data-collector:5001" 

def get_db_connection():
    # Stabilisce la connessione al database MySQL
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )

def init_db():
    # Inizializza il database creando le tabelle necessarie se non esistono
    retries = 5
    while retries > 0:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Tabella Utenti
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    email VARCHAR(255) PRIMARY KEY,
                    username VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Tabella per tracciare le richieste (At-Most-Once)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS processed_requests (
                    request_id VARCHAR(36) PRIMARY KEY,
                    operation VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            cursor.close()
            conn.close()
            print("User DB (and Request Log) initialized.")
            return
        except mysql.connector.Error as err:
            print(f"DB Connection failed: {err}")
            retries -= 1
            time.sleep(5)

# --- gRPC ---
class UserService(user_pb2_grpc.UserServiceServicer):
    def CheckUserExists(self, request, context):
        # Implementazione del servizio gRPC per verificare l'esistenza dell'utente
        email = request.email
        exists = False
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT email FROM users WHERE email = %s", (email,))
            if cursor.fetchone():
                exists = True
            cursor.close()
            conn.close()
        except Exception:
            pass
        return user_pb2.UserResponse(exists=exists)

def serve_grpc():
    # Avvia il server gRPC in un thread separato
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_pb2_grpc.add_UserServiceServicer_to_server(UserService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

# --- REST API ---

@app.route('/users', methods=['POST'])
def register_user():
    # Registra un nuovo utente garantendo l'idempotenza tramite Request-ID (At-Most-Once)
    request_id = request.headers.get('X-Request-ID')
    
    if not request_id:
        return jsonify({"error": "Missing X-Request-ID header"}), 400

    data = request.json
    email = data.get('email')
    username = data.get('username')
    
    if not email or not username:
        return jsonify({"error": "Email and username are required"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verifica se la richiesta è già stata processata
        cursor.execute("SELECT request_id FROM processed_requests WHERE request_id = %s", (request_id,))
        if cursor.fetchone():
            print(f"Request {request_id} already processed. Skipping.")
            return jsonify({"message": "User registered successfully (Idempotent replay)"}), 200

        # Esegue la registrazione
        try:
            cursor.execute("INSERT INTO users (email, username) VALUES (%s, %s)", (email, username))
            msg = "User registered successfully"
            status = 201
        except mysql.connector.Error as err:
            if err.errno == 1062:
                msg = "User already exists"
                status = 200
            else:
                raise err

        # Registra la richiesta come processata
        cursor.execute("INSERT INTO processed_requests (request_id, operation) VALUES (%s, 'REGISTER_USER')", (request_id,))
        conn.commit()

        return jsonify({"message": msg}), status

    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route('/users/<string:email>', methods=['DELETE'])
def delete_user(email):
    # Cancella l'utente e richiede la pulizia dei dati correlati al Data Collector
    try:
        try:
            requests.delete(f"{DATA_COLLECTOR_URL}/interests/{email}", timeout=2)
        except Exception:
            pass

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE email = %s", (email,))
        conn.commit()
        cursor.close()
        conn.close()
        
        return jsonify({"message": f"User {email} deleted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    init_db()
    grpc_thread = threading.Thread(target=serve_grpc)
    grpc_thread.daemon = True
    grpc_thread.start()
    app.run(host='0.0.0.0', port=5000)