#!/usr/bin/env python3
"""
Aufgabe 3: Scanner via Message Queue (RabbitMQ) nutzen, um G-Station 1-2
zu finden und 60s lang in ihrer Naehe zu bleiben.

Voraussetzung: Der Broker laeuft auf dem Schiff unter 192.168.101.20:2014
(siehe rabbitmq.yaml im gleichen Ordner). Erst dann publiziert das
Scanner-Modul in den Fanout-Exchange 'scanner/detected_objects'.
"""

import json
import threading
import time

import pika
import requests

SCHIFF = "192.168.101.20"

RABBIT_HOST = SCHIFF
RABBIT_PORT = 2014
EXCHANGE = "scanner/detected_objects"

STEUERUNG_URL = f"http://{SCHIFF}:2009/set_target"   # easy steering
POSITION_URL = f"http://{SCHIFF}:2010/pos"           # navigation

STATION_NAME = "G-Station 1-2"
# Laut WhatsUpp-Auftrag kommt die Station regelmaessig hier vorbei.
TREFFPUNKT = {"x": 11382, "y": 15255}

NAEHE_RADIUS = 100      # ab dieser Distanz zaehlen wir als "in der Naehe"
ZIEL_NACHFUEHRUNG = 20  # Kurs erst neu setzen, wenn sich das Ziel so weit bewegt hat
SICHTUNG_GUELTIG = 15   # Sekunden, die eine Sichtung als aktuell gilt
VORHALTEZEIT = 4        # Sekunden, die wir der Station vorhalten (sie bewegt sich)
TAKT = 0.5              # Sekunden zwischen zwei Regelschritten

# Wird vom Scanner-Thread geschrieben, von main() gelesen.
sichtung = None         # (position, zeitstempel)
vorherige_sichtung = None
lock = threading.Lock()


def scanner_thread():
    """Lauscht dauerhaft auf die Scanner-Nachrichten und merkt sich die Position
    von G-Station 1-2, sobald sie erkannt wird."""
    global sichtung, vorherige_sichtung

    while True:
        try:
            verbindung = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBIT_HOST, port=RABBIT_PORT, heartbeat=30
                )
            )
            channel = verbindung.channel()

            channel.exchange_declare(exchange=EXCHANGE, exchange_type="fanout")
            queue_name = channel.queue_declare(queue="", exclusive=True).method.queue
            channel.queue_bind(exchange=EXCHANGE, queue=queue_name)

            print("Mit Scanner-Queue verbunden, warte auf Objekte...")

            for method_frame, properties, body in channel.consume(
                queue=queue_name, auto_ack=True
            ):
                objekte = json.loads(body.decode("utf-8"))
                for objekt in objekte:
                    if objekt.get("name") == STATION_NAME:
                        with lock:
                            vorherige_sichtung = sichtung
                            sichtung = (objekt["pos"], time.time())

        except (pika.exceptions.AMQPError, OSError) as fehler:
            print("RabbitMQ-Verbindung fehlgeschlagen, neuer Versuch in 3s:", fehler)
            time.sleep(3)


def fliege_zu(x, y):
    antwort = requests.post(
        STEUERUNG_URL, json={"target": {"x": x, "y": y}}, timeout=5
    )
    antwort.raise_for_status()


def hole_position():
    antwort = requests.get(POSITION_URL, timeout=5)
    antwort.raise_for_status()
    return antwort.json()["pos"]


def distanz(a, b):
    return ((a["x"] - b["x"]) ** 2 + (a["y"] - b["y"]) ** 2) ** 0.5


def vorhalten(neu, alt, sekunden):
    """Die Station bewegt sich. Wenn wir immer nur ihre gemeldete Position
    ansteuern, hinken wir dauerhaft hinterher. Darum aus den letzten beiden
    Sichtungen die Geschwindigkeit schaetzen und ein Stueck vorhalten."""
    if alt is None:
        return neu[0]

    (position, jetzt), (alte_position, vorher) = neu, alt
    dt = jetzt - vorher
    if dt <= 0:
        return position

    return {
        "x": position["x"] + (position["x"] - alte_position["x"]) / dt * sekunden,
        "y": position["y"] + (position["y"] - alte_position["y"]) / dt * sekunden,
    }


def main():
    threading.Thread(target=scanner_thread, daemon=True).start()

    # Ohne Sichtung erst mal zum bekannten Treffpunkt fliegen - der Scanner
    # sieht die Station nur, wenn wir nah genug dran sind.
    print(f"Kurs auf den Treffpunkt {TREFFPUNKT}, warte auf {STATION_NAME}...")
    fliege_zu(TREFFPUNKT["x"], TREFFPUNKT["y"])

    letztes_ziel = TREFFPUNKT
    naehe_seit = None
    gemeldet = False

    while True:
        with lock:
            aktuelle_sichtung = sichtung
            letzte_sichtung = vorherige_sichtung

        if aktuelle_sichtung is None:
            print("Noch keine Sichtung, fliege weiter zum Treffpunkt.")
            time.sleep(2)
            continue

        ziel, gesehen_um = aktuelle_sichtung
        alter = time.time() - gesehen_um

        if alter > SICHTUNG_GUELTIG:
            # Station ist aus der Scanner-Reichweite verschwunden: zurueck zum
            # Treffpunkt, dort taucht sie wieder auf.
            print(f"Sichtung ist {alter:.0f}s alt - zurueck zum Treffpunkt.")
            if distanz(letztes_ziel, TREFFPUNKT) > ZIEL_NACHFUEHRUNG:
                fliege_zu(TREFFPUNKT["x"], TREFFPUNKT["y"])
                letztes_ziel = TREFFPUNKT
            naehe_seit = None
            time.sleep(2)
            continue

        # Kurs nur korrigieren, wenn sich der Vorhaltepunkt merklich bewegt hat.
        anflugpunkt = vorhalten(aktuelle_sichtung, letzte_sichtung, VORHALTEZEIT)
        if distanz(anflugpunkt, letztes_ziel) > ZIEL_NACHFUEHRUNG:
            fliege_zu(anflugpunkt["x"], anflugpunkt["y"])
            letztes_ziel = anflugpunkt

        eigene_position = hole_position()
        abstand = distanz(eigene_position, ziel)

        if abstand <= NAEHE_RADIUS:
            if naehe_seit is None:
                naehe_seit = time.time()
            dauer = time.time() - naehe_seit
            print(f"In der Naehe: {dauer:.0f}s / 60s (Distanz {abstand:.0f})")
            if dauer >= 60 and not gemeldet:
                print(
                    "60s in der Naehe von G-Station 1-2 geschafft. "
                    "Fortschritt im Cockpit (WhatsUpp-Widget) pruefen - "
                    "Ctrl+C zum Beenden."
                )
                gemeldet = True
        else:
            if naehe_seit is not None:
                print(f"Abstand wieder zu gross ({abstand:.0f}), Timer zurueckgesetzt.")
            naehe_seit = None
            gemeldet = False

        time.sleep(TAKT)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBeendet.")
