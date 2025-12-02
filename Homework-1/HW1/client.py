import requests
import sys
import time
import uuid

# Configurazione URL
USER_SERVICE_URL = "http://localhost:5000"
DATA_SERVICE_URL = "http://localhost:5001"

def print_header(title):
    print("\n" + "="*50)
    print(f" {title}")
    print("="*50)

# --- 1. REGISTRAZIONE ---
def register_user():
    # Registra un nuovo utente implementando la logica At-Most-Once lato client
    print_header("REGISTRAZIONE NUOVO UTENTE (At-Most-Once)")
    email = input("Inserisci Email: ").strip()
    username = input("Inserisci Nome Utente: ").strip()
    
    if not email or not username:
        print("Dati mancanti.")
        return

    # Genera ID univoco per la richiesta
    req_id = str(uuid.uuid4())
    print(f"Generated Request ID: {req_id}")

    # Simulazione invio doppio per testare l'idempotenza
    headers = {'X-Request-ID': req_id}
    payload = {"email": email, "username": username}

    try:
        print("Tentativo 1 (Invio richiesta)...")
        res1 = requests.post(f"{USER_SERVICE_URL}/users", json=payload, headers=headers)
        print(f"Risposta Server 1: {res1.status_code} - {res1.json().get('message')}")

        print("\nSimulazione 'Retry' (Invio STESSO Request ID)...")
        res2 = requests.post(f"{USER_SERVICE_URL}/users", json=payload, headers=headers)
        print(f"Risposta Server 2: {res2.status_code} - {res2.json().get('message')}")
        
        if res2.status_code == 200 and "Idempotent" in res2.json().get('message', ''):
            print("\nTEST AT-MOST-ONCE SUPERATO: Il server ha riconosciuto il duplicato!")
        
    except Exception as e:
        print(f"Errore: {e}")

# --- 2. CANCELLAZIONE ---
def delete_user():
    # Elimina un utente e i relativi dati associati
    print_header("ELIMINAZIONE UTENTE")
    print("Nota: Verranno eliminati anche gli interessi salvati.")
    email = input("Email dell'utente da eliminare: ").strip()
    try:
        res = requests.delete(f"{USER_SERVICE_URL}/users/{email}")
        print(f"Risposta: {res.json().get('message', res.text)}")
    except Exception as e:
        print(f"Errore: {e}")

# --- 3. AGGIUNGI INTERESSE ---
def add_interest():
    # Aggiunge un aeroporto alla lista degli interessi di un utente
    print_header("AGGIUNGI MONITORAGGIO")
    email = input("Email utente: ").strip()
    airport = input("Codice ICAO Aeroporto (es. EGLL, LICC): ").strip().upper()
    
    try:
        res = requests.post(f"{DATA_SERVICE_URL}/interests", json={"email": email, "airport": airport})
        if res.status_code == 201:
            print(f"Aeroporto {airport} aggiunto con successo.")
        elif res.status_code == 404:
            print("Utente non trovato (Verifica gRPC fallita).")
        else:
            print(f"Risposta: {res.json()}")
    except Exception as e:
        print(f"Errore: {e}")

# --- HELPER: SELEZIONE AEROPORTO ---
def _select_user_airport(email):
    # Recupera la lista degli aeroporti seguiti dall'utente e ne fa selezionare uno
    try:
        res = requests.get(f"{DATA_SERVICE_URL}/interests/{email}")
        if res.status_code != 200:
            print("Errore nel recupero preferenze (o utente non esiste).")
            return None
            
        airports = res.json()
        if not airports:
            print("Questo utente non sta seguendo nessun aeroporto.")
            return None
            
        print(f"\nAeroporti monitorati da {email}:")
        for i, code in enumerate(airports, 1):
            print(f"{i}. {code}")
            
        choice = input("\nSeleziona il numero dell'aeroporto: ")
        selected_idx = int(choice) - 1
        if 0 <= selected_idx < len(airports):
            return airports[selected_idx]
        else:
            print("Scelta non valida.")
            return None
    except ValueError:
        print("Devi inserire un numero.")
        return None
    except Exception as e:
        print(f"Errore: {e}")
        return None

# --- 4. CONSULTA ULTIMO VOLO ---
def view_last_flight():
    # Mostra i dettagli dell'ultimo volo registrato per l'aeroporto selezionato
    print_header("CONSULTAZIONE ULTIMO VOLO")
    
    email = input("Inserisci la tua email: ").strip()
    selected_airport = _select_user_airport(email)
    
    if not selected_airport:
        return

    print(f"\n--- ULTIMO VOLO PER {selected_airport} ---")
    try:
        flight_res = requests.get(f"{DATA_SERVICE_URL}/analysis/last_flight/{selected_airport}")
        
        if flight_res.status_code == 200:
            f = flight_res.json()
            print(f"   Callsign: {f.get('callsign')}")
            print(f"   ICAO24:   {f.get('icao24')}")
            print(f"   Tipo:     {f.get('type')}")
            print(f"   Orario:   {f.get('timestamp_readable')}")
            print(f"   Tratta:   {f.get('departure_airport')} -> {f.get('arrival_airport')}")
        else:
            print("Nessun volo registrato recentemente per questo aeroporto.")
    except Exception as e:
        print(f"Errore di comunicazione: {e}")

# --- 5. CONSULTA STATISTICHE MEDIE ---
def view_average_stats():
    # Calcola e mostra le statistiche medie dei voli per un periodo specificato
    print_header("CALCOLO MEDIA GIORNALIERA")
    
    email = input("Inserisci la tua email: ").strip()
    selected_airport = _select_user_airport(email)
    
    if not selected_airport:
        return

    days = input(f"\nSu quanti giorni vuoi calcolare la media per {selected_airport}? (Default 7): ").strip()
    days = int(days) if days.isdigit() else 7

    print(f"\n--- STATISTICHE ULTIMI {days} GIORNI ---")
    try:
        avg_res = requests.get(f"{DATA_SERVICE_URL}/analysis/average_flights/{selected_airport}?days={days}")
        
        if avg_res.status_code == 200:
            data = avg_res.json()
            print(f"   Totale voli analizzati: {data.get('total_flights')}")
            print(f"   Media voli/giorno:      {data.get('average_flights_per_day')}")
            print(f"   Media arrivi/giorno:    {data.get('average_arrivals_per_day')}")
            print(f"   Media partenze/giorno:  {data.get('average_departures_per_day')}")
        else:
            print("Impossibile calcolare le statistiche.")
    except Exception as e:
        print(f"Errore di comunicazione: {e}")

# --- MENU PRINCIPALE ---
def main_menu():
    while True:
        print("\n" + "="*40)
        print("   SKY MONITOR SYSTEM - CLIENT")
        print("="*40)
        print("1. Registra Utente")
        print("2. Elimina Utente (e dati associati)")
        print("3. Aggiungi Aeroporto ai Preferiti")
        print("4. Visualizza Ultimo Volo (User Flow)")
        print("5. Visualizza Medie e Statistiche (User Flow)")
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
        requests.get(USER_SERVICE_URL, timeout=1)
    except:
        print("Attenzione: I container Docker sembrano spenti.")
        time.sleep(1)
        
    main_menu()