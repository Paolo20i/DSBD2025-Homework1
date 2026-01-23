import requests
import time
import json

BASE_URL = "http://localhost:8080"
SLA_CONFIG_URL = f"{BASE_URL}/sla/config"
SLA_STATS_URL = f"{BASE_URL}/sla/stats"

def print_step(msg):
    print(f"\n{msg}")

def get_stats():
    try:
        res = requests.get(SLA_STATS_URL)
        return res.json()
    except Exception as e:
        print(f"Errore connessione: {e}")
        return {}

def run_test():
    print("--- INIZIO TEST SLA BREACH DETECTOR ---")

    print_step("Lettura configurazione attuale...")
    res = requests.get(SLA_CONFIG_URL)
    print(json.dumps(res.json(), indent=2))

    # 2. Forza Violazione
    print_step("Forzatura SLA Breach (Imposto max_duration a 0.0001s)...")
    strict_config = {
        "metrics": [
            {
                "name": "fetch_duration_sla",
                "query": "last_fetch_duration_seconds",
                "min": 0,
                # Soglia bassa per testare il breach
                "max": 0.0001 
            },
            {
                "name": "users_count_sla",
                "query": "users_registered_count",
                "min": 1,
                "max": 1000
            }
        ]
    }
    res = requests.post(SLA_CONFIG_URL, json=strict_config)
    if res.status_code == 200:
        print("Configurazione aggiornata con successo!")
    else:
        print(f"Errore aggiornamento: {res.text}")
        return

    # 3. Attesa e Monitoraggio
    print_step("In attesa del rilevamento violazioni...")

    for i in range(6):  # Prova per circa 6 minuti
        stats = get_stats()
        print(f"[Minuto {i}] Stats: {json.dumps(stats)}")
        
        # Controlla se fetch_duration_sla ha rilevato violazioni HIGH
        fetch_stats = stats.get('fetch_duration_sla', {})
        if fetch_stats.get('high', 0) > 0:
            print("\nSLA BREACH RILEVATO!")
            print(f"Violazioni rilevate: {fetch_stats['high']}")
            return
        
        time.sleep(60)

    print("\nTimeout: Nessuna violazione rilevata nei tempi previsti.")

if __name__ == "__main__":
    run_test()