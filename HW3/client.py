import requests
import sys
import time
import uuid

GATEWAY_URL = "http://localhost:8080"

def print_header(title):
    print("\n" + "="*50)
    print(f" {title}")
    print("="*50)

# --- REGISTRAZIONE ---
def register_user():
    print_header("REGISTRAZIONE NUOVO UTENTE")
    email = input("Inserisci Email: ").strip()
    username = input("Inserisci Nome Utente: ").strip()
    
    if not email or not username:
        print("Dati mancanti.")
        return

    req_id = str(uuid.uuid4())
    headers = {'X-Request-ID': req_id}
    payload = {"email": email, "username": username}

    try:
        res = requests.post(f"{GATEWAY_URL}/users", json=payload, headers=headers)
        if res.status_code in [200, 201]:
            print(f"Successo: {res.json().get('message')}")
        else:
            print(f"Errore {res.status_code}: {res.text}")
    except Exception as e:
        print(f"Errore di connessione: {e}")

# --- CANCELLAZIONE ---
def delete_user():
    print_header("ELIMINAZIONE UTENTE")
    email = input("Email dell'utente da eliminare: ").strip()
    try:
        res = requests.delete(f"{GATEWAY_URL}/users/{email}")
        print(f"Risposta: {res.json().get('message', res.text)}")
    except Exception as e:
        print(f"Errore: {e}")

# --- GESTIONE MONITORAGGIO ---
def add_interest():
    print_header("AGGIUNGI/AGGIORNA MONITORAGGIO")
    email = input("Email utente: ").strip()
    airport = input("Codice ICAO Aeroporto (es. EGLL, LICC): ").strip().upper()
    
    high_val = None
    low_val = None
    
    want_thresholds = input("Vuoi impostare soglie di allerta? (s/n): ").lower()
    if want_thresholds == 's':
        h_input = input("Soglia MAX voli (High Value) - Invio per saltare: ").strip()
        l_input = input("Soglia MIN voli (Low Value)  - Invio per saltare: ").strip()
        
        if h_input: high_val = int(h_input)
        if l_input: low_val = int(l_input)
        
        if high_val is not None and low_val is not None and high_val <= low_val:
            print("Errore: La soglia MAX deve essere maggiore della MIN.")
            return

    payload = {
        "email": email, 
        "airport": airport,
        "high_value": high_val,
        "low_value": low_val
    }
    
    try:
        res = requests.post(f"{GATEWAY_URL}/interests", json=payload)
        if res.status_code == 200:
            print(f"Preferenze aggiornate per {airport}.")
        elif res.status_code == 404:
            print("Utente non trovato.")
        else:
            print(f"Risposta: {res.json()}")
    except Exception as e:
        print(f"Errore: {e}")

# --- HELPER SELEZIONE ---
def _select_user_airport(email):
    try:
        res = requests.get(f"{GATEWAY_URL}/interests/{email}")
        if res.status_code != 200:
            print("Errore nel recupero preferenze.")
            return None
            
        airports = res.json()
        if not airports:
            print("Nessun aeroporto monitorato.")
            return None
            
        print(f"\nAeroporti monitorati da {email}:")
        for i, code in enumerate(airports, 1):
            print(f"{i}. {code}")
            
        choice = input("\nSeleziona il numero: ")
        selected_idx = int(choice) - 1
        if 0 <= selected_idx < len(airports):
            return airports[selected_idx]
        return None
    except Exception:
        print("Input non valido o errore server.")
        return None

# --- ULTIMO VOLO ---
def view_last_flight():
    print_header("CONSULTAZIONE ULTIMO VOLO")
    email = input("Inserisci la tua email: ").strip()
    selected_airport = _select_user_airport(email)
    
    if not selected_airport:
        return

    try:
        res = requests.get(f"{GATEWAY_URL}/analysis/last_flight/{selected_airport}")
        if res.status_code == 200:
            f = res.json()
            print(f"\nVOLO RECENTE PER {selected_airport}:")
            print(f"   Callsign: {f.get('callsign')}")
            print(f"   ICAO24:   {f.get('icao24')}")
            print(f"   Orario:   {f.get('timestamp_readable')}")
        else:
            print("Nessun volo trovato.")
    except Exception as e:
        print(f"Errore: {e}")

# --- STATISTICHE ---
def view_average_stats():
    print_header("STATISTICHE E MEDIE")
    email = input("Inserisci la tua email: ").strip()
    selected_airport = _select_user_airport(email)
    
    if not selected_airport:
        return

    days = input(f"Giorni da analizzare (Default 7): ").strip() or "7"
    
    try:
        res = requests.get(f"{GATEWAY_URL}/analysis/average_flights/{selected_airport}?days={days}")
        if res.status_code == 200:
            data = res.json()
            print(f"\nSTATISTICHE {selected_airport} ({data.get('days_analyzed')} gg):")
            print(f"   Media voli totali:    {data.get('average_flights_per_day')}")
            print(f"   Media arrivi:         {data.get('average_arrivals_per_day')}")
            print(f"   Media partenze:       {data.get('average_departures_per_day')}")
        else:
            print("Errore calcolo statistiche.")
    except Exception as e:
        print(f"Errore: {e}")

# --- MENU ---
def main_menu():
    while True:
        print("\n" + "="*40)
        print("   SKY MONITOR CLIENT")
        print("="*40)
        print("1. Registra Utente")
        print("2. Elimina Utente")
        print("3. Gestione Monitoraggio & Allarmi")
        print("4. Ultimo Volo")
        print("5. Statistiche")
        print("0. Esci")
        
        choice = input("\nScelta: ")
        
        if choice == '1': register_user()
        elif choice == '2': delete_user()
        elif choice == '3': add_interest()
        elif choice == '4': view_last_flight()
        elif choice == '5': view_average_stats()
        elif choice == '0': sys.exit()
        else: print("Opzione non valida.")

if __name__ == "__main__":
    try:
        requests.get(f"{GATEWAY_URL}/users", timeout=1) 
    except:
        print("Impossibile contattare l'API Gateway su localhost:8080.")
        time.sleep(2)
        
    main_menu()