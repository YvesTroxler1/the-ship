"""
Aufgabe: Kommunikation zwischen Elyse Terminal und Shangris Station.

Rollen:
- Diese VM fliegt zu EINER der beiden Stationen (siehe STATION unten),
  verbindet sich per WebSocket mit deren Comm-Modul und leitet alles,
  was dort ankommt, an die Partner-VM weiter (die bei der anderen Station ist).
- Umgekehrt: alles, was von der Partner-VM kommt, wird ans eigene Comm-Modul gesendet.
"""

import json
import threading
import time
import queue

import requests
import websocket  # pip install websocket-client
from flask import Flask, request, jsonify

# ---- Konfiguration: HIER pro VM anpassen ----------------------------------

# Welche Station bedient DIESE VM?
STATION_NAME = "Elyse Terminal"          # oder "Shangris Station"
STATION_COORDS = {"x": -70565, "y": 72811}  # bzw. {"x": 4446, "y": 4340}

COMM_WS_URL = "ws://192.168.101.21:2025/ws"  # ggf. Station-spezifischer Endpunkt
TARGET_SET_URL = "http://192.168.101.21:2009/set_target"

EIGENER_PORT = 5001                       # >= 5000, wie gefordert
PARTNER_URL = "http://192.168.101.22:5001/relay"  # IP/Port der Partner-VM

HEARTBEAT_SEKUNDEN = 3   # "mindestens alle 3s eine Nachricht"

# ---- Queues -----------------------------------------------------------

vom_comm_modul = queue.Queue()
vom_partner = queue.Queue()

# ---- Zum eigenen Comm-Modul fliegen ----------------------------------------

def fliege_zur_station():
    requests.post(TARGET_SET_URL, json={"target": STATION_COORDS}).raise_for_status()
    print(f"Kurs gesetzt auf {STATION_NAME} {STATION_COORDS}")


# ---- WebSocket-Client zum Comm-Modul --------------------------------------

def on_message(ws, message):
    print("Comm-Modul -> wir:", message)
    try:
        vom_comm_modul.put(json.loads(message))
    except json.JSONDecodeError:
        print("Ungültige Nachricht vom Comm-Modul:", message)


def on_open(ws):
    print(f"Mit Comm-Modul von {STATION_NAME} verbunden.")


def on_error(ws, error):
    print("WebSocket-Fehler:", error)


def on_close(ws, code, msg):
    print("WebSocket geschlossen, versuche Reconnect in 3s...")
    time.sleep(3)
    starte_ws()


ws_app = None


def starte_ws():
    global ws_app
    ws_app = websocket.WebSocketApp(
        COMM_WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    threading.Thread(target=ws_app.run_forever, daemon=True).start()


def sende_an_comm_modul(payload):
    if ws_app is None:
        return
    try:
        ws_app.send(json.dumps(payload))
        print("wir -> Comm-Modul:", payload)
    except Exception as e:
        print("Senden ans Comm-Modul fehlgeschlagen:", e)


# ---- Eigener REST-Server (empfängt von Partner-VM) -------------------------

app = Flask(__name__)


@app.route("/relay", methods=["POST"])
def relay_empfangen():
    daten = request.get_json()
    print("Partner-VM -> wir:", daten)
    vom_partner.put(daten)
    return jsonify({"kind": "success"})


def starte_rest_server():
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=EIGENER_PORT, use_reloader=False),
        daemon=True,
    ).start()


# ---- Weiterleitungs-Threads -------------------------------------------

def weiterleiten_an_partner():
    while True:
        try:
            nachricht = vom_comm_modul.get(timeout=1)
        except queue.Empty:
            continue
        try:
            requests.post(PARTNER_URL, json=nachricht, timeout=3).raise_for_status()
            print("wir -> Partner-VM:", nachricht)
        except requests.RequestException as e:
            print("Weiterleiten an Partner fehlgeschlagen:", e)


def weiterleiten_ans_comm_modul():
    while True:
        try:
            nachricht = vom_partner.get(timeout=1)
        except queue.Empty:
            continue
        sende_an_comm_modul(nachricht)


def heartbeat():
    """Sorgt dafür, dass mind. alle 3s etwas Richtung Comm-Modul geht,
    auch wenn gerade nichts von der Partner-VM kam."""
    while True:
        sende_an_comm_modul({"source": STATION_NAME, "data": [1, 2, 3, 4]})
        time.sleep(HEARTBEAT_SEKUNDEN)


# ---- Start ------------------------------------------------------------

if __name__ == "__main__":
    fliege_zur_station()
    time.sleep(7)  # warten, bis Schiff angekommen ist

    starte_ws()
    starte_rest_server()

    threading.Thread(target=weiterleiten_an_partner, daemon=True).start()
    threading.Thread(target=weiterleiten_ans_comm_modul, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()

    print("Kommunikation läuft... (Ctrl+C zum Beenden)")
    while True:
        time.sleep(1)