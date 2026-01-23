import os
import time
import threading
import json
import yaml
import requests
from collections import deque
from flask import Flask, request, jsonify
from kafka import KafkaProducer

app = Flask(__name__)

# --- CONFIGURAZIONE ---
CONFIG_FILE = 'sla_config.yaml'
PROMETHEUS_URL = os.getenv('PROMETHEUS_URL', 'http://prometheus-service:9090')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka-service:9092')
TOPIC_NOTIFIER = 'to-notifier'
T_CHECK = 90 

# --- STATO INTERNO ---
lock = threading.Lock()
sla_config = {}
metric_history = {} 
breach_stats = {}   

# --- FUNZIONI VARIE ---

def load_initial_config():
    """Carica la configurazione dal file YAML SOLO all'avvio"""
    global sla_config
    try:
        with open(CONFIG_FILE, 'r') as f:
            new_config = yaml.safe_load(f)
            
        with lock:
            sla_config = new_config
            init_metrics_storage()
            print("Configurazione SLA iniziale caricata.")
    except Exception as e:
        print(f"Errore nel caricamento della configurazione: {e}")

def init_metrics_storage():
    """Inizializza le strutture dati per le metriche"""
    if 'metrics' in sla_config:
        for m in sla_config['metrics']:
            name = m['name']
            if name not in metric_history:
                metric_history[name] = deque(maxlen=5)
            if name not in breach_stats:
                breach_stats[name] = {'low': 0, 'high': 0}

def get_prometheus_value(query):
    try:
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query}, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success' and data['data']['result']:
                return float(data['data']['result'][0]['value'][1])
    except Exception as e:
        print(f"Errore query Prometheus ('{query}'): {e}")
    return None

def send_kafka_alert(metric_name, value, threshold, violation_count, subject, body):
    """
    Invia l'evento di Breach su Kafka con tutti i campi richiesti:
    - Timestamp
    - Metrica / Query
    - Valore osservato e Soglia violata
    - Contatore violazioni
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Payload
        payload = {
            "timestamp": time.time(),       # Timestamp
            "metric": metric_name,          # Metrica
            "value": value,                 # Valore osservato
            "threshold": threshold,         # Soglia violata
            "violations": violation_count,  # Contatore violazioni
            
            # Campi per l'Alert Notifier
            "email": "admin@gmail.com",
            "subject": subject,
            "body": body
        }
        
        producer.send(TOPIC_NOTIFIER, payload)
        producer.close()
        print(f"Alert inviato a Kafka: {subject} (TS: {payload['timestamp']})")
    except Exception as e:
        print(f"Errore invio Kafka: {e}")

# --- WORKER DI MONITORAGGIO ---

def sla_monitor_loop():
    print(f"SLA Monitor avviato. Controllo ogni {T_CHECK} secondi.")
    time.sleep(30)
    
    while True:
        print("\n--- Esecuzione Check SLA ---")
        
        with lock:
            current_metrics = sla_config.get('metrics', [])
        
        for metric_conf in current_metrics:
            name = metric_conf['name']
            query = metric_conf['query']
            min_th = metric_conf.get('min')
            max_th = metric_conf.get('max')

            val = get_prometheus_value(query)
            
            if val is not None:
                print(f"Metrica '{name}': valore rilevato {val}")
                
                with lock:
                    if name not in metric_history: metric_history[name] = deque(maxlen=5)
                    metric_history[name].append(val)
                    history = list(metric_history[name])
                
                # Check HIGH breach
                if max_th is not None:
                    over_count = sum(1 for v in history if v > max_th)
                    if over_count >= 3:
                        msg = f"SLA BREACH (HIGH): {name}. Valore {val} > Soglia {max_th}. ({over_count} campioni sopra soglia)"
                        print(f"!!! {msg}")
                        
                        with lock:
                            if name not in breach_stats: breach_stats[name] = {'low': 0, 'high': 0}
                            breach_stats[name]['high'] += 1
                            count = breach_stats[name]['high']
                            
                        send_kafka_alert(name, val, max_th, count, f"SLA Alert: {name} too high", msg)

                # Check LOW breach
                if min_th is not None:
                    under_count = sum(1 for v in history if v < min_th)
                    if under_count >= 3:
                        msg = f"SLA BREACH (LOW): {name}. Valore {val} < Soglia {min_th}. ({under_count} campioni sotto soglia)"
                        print(f"!!! {msg}")
                        
                        with lock:
                            if name not in breach_stats: breach_stats[name] = {'low': 0, 'high': 0}
                            breach_stats[name]['low'] += 1
                            count = breach_stats[name]['low']
                            
                        send_kafka_alert(name, val, min_th, count, f"SLA Alert: {name} too low", msg)
            else:
                print(f"Metrica '{name}': nessun dato da Prometheus.")

        time.sleep(T_CHECK)

# --- API REST FLASK ---

@app.route('/sla/config', methods=['GET'])
def get_config():
    with lock:
        return jsonify(sla_config)

@app.route('/sla/config', methods=['POST'])
def update_config():
    new_data = request.json
    if not new_data or 'metrics' not in new_data:
        return jsonify({"error": "Invalid configuration format"}), 400
    
    try:
        with lock:
            global sla_config
            sla_config = new_data
            init_metrics_storage()
            print("Configurazione aggiornata in memoria (runtime).")
            
        return jsonify({"message": "Configuration updated in memory"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/sla/stats', methods=['GET'])
def get_stats():
    with lock:
        return jsonify(breach_stats)

if __name__ == '__main__':
    load_initial_config()
    monitor_thread = threading.Thread(target=sla_monitor_loop, daemon=True)
    monitor_thread.start()
    app.run(host='0.0.0.0', port=5000)