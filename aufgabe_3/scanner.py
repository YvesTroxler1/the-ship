#!/usr/bin/env python3
"""
Aufgabe: Scanner via Message Queue (RabbitMQ) nutzen, um G-Station 1-2
zu finden und 60s lang in ihrer Nähe zu bleiben.
"""

import json
import threading
import time

import pika
import requests

RABBIT_HOST = "192.168.101.21"
RABBIT_PORT = 2014

BASE = "http://192.168.101.21"
STATION_NAME = "G-Station 1-2"

# Letzte bekannte Position der gesuchten Station (wird vom Scanner-Thread aktualisiert)
letzte_position = None
lock = threading.Lock()


def scanner_thread():
    """Lauscht dauerhaft auf die Scanner-Nachrichten und merkt sich die Position
    von G-Station 1-2, sobald sie erkannt wird."""
    global letzte_position

    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBIT_HOST, port=RABBIT_PORT)
            )
            channel = connection.channel()

            channel.exchange_declare(exchange="scanner/detected_objects", exchange_type="fanout")
            result = channel.queue_declare(queue="", exclusive=True)
            queue_name = result.method.queue
            channel.queue_bind(exchange="scanner/detected_objects", queue=queue_name)

            print("Mit Scanner-Queue verbunden, warte auf Objekte...")

            for method_frame, properties, body in channel.consume(queue=queue_name, auto_ack=True):
                objekte = json.loads(body.decode("utf-8"))
                for obj in objekte:
                    if obj.get("name") == STATION_NAME:
                        with lock:
                            letzte_position = obj["pos"]
                        print(f"{STATION_NAME} gesichtet bei {obj['pos']}")

        except pika.exceptions.AMQPConnectionError as e:
            print("RabbitMQ-Verbindung verloren/fehlgeschlagen, versuche erneut in 3s:", e)
            time.sleep(3)


def fliege_zu(x, y):
    requests.post(f"{BASE}:2009/set_target", json={"target": {"x": x, "y": y}}).raise_for_status()


def hole_position():
    antwort = requests.get(f"{BASE}:2011/pos")
    antwort.raise_for_status()
    return antwort.json()["pos"]


def distanz(a, b):
    return ((a["x"] - b["x"]) ** 2 + (a["y"] - b["y"]) ** 2) ** 0.5


NAEHE_RADIUS = 500  # anpassen, je nachdem wie "in der Nähe" im Spiel definiert ist


def main():
    threading.Thread(target=scanner_thread, daemon=True).start()

    letztes_ziel = None
    zeit_in_naehe_start = None

    print("Warte auf erste Sichtung von G-Station 1-2...")

    while True:
        with lock:
            ziel = letzte_position

        if ziel is not None:
            # Ziel nur neu setzen, wenn es sich merklich geändert hat
            if ziel != letztes_ziel:
                fliege_zu(ziel["x"], ziel["y"])
                letztes_ziel = ziel
                print(f"Kurs korrigiert auf {ziel}")

            eigene_pos = hole_position()
            d = distanz(eigene_pos, ziel)

            if d <= NAEHE_RADIUS:
                if zeit_in_naehe_start is None:
                    zeit_in_naehe_start = time.time()
                vergangen = time.time() - zeit_in_naehe_start
                print(f"In Nähe der Station: {vergangen:.1f}s / 60s (Distanz {d:.0f})")
                if vergangen >= 60:
                    print("Aufgabe erfüllt: 60s in der Nähe von G-Station 1-2!")
                    break
            else:
                if zeit_in_naehe_start is not None:
                    print("Aus der Nähe gefallen, Timer zurückgesetzt.")
                zeit_in_naehe_start = None

        time.sleep(2)


if __name__ == "__main__":
    main()