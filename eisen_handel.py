"""
Einfacher Eisen-Handel: Aufgabe 1
"""

import time
import requests

BASE = "http://192.168.101.21"

AZURA = "Azura Station"
CORE = "Core Station"
VESTA = "Vesta Station"
VESTA_COORDS = {"x": 7000, "y": 7000}

BUY_PRICE = 5     
SELL_PRICE = 10   
ZIEL_EISEN = 12

credits = 20
eisen_an_bord = 0

def fliege_zu(station, koordinaten=None):
    """Kurs setzen und warten, bis das Schiff angekommen ist."""
    ziel = koordinaten if koordinaten else station  
    requests.post(f"{BASE}:2009/set_target", json={"target": ziel}).raise_for_status()
    time.sleep(7)


def handel(aktion, station, menge):
    """Kaufen oder verkaufen. Versucht es notfalls 3x, falls der Server kurz ablehnt."""
    time.sleep(5)  # kleine Pause vor dem Request
    for versuch in range(3):
        antwort = requests.post(
            f"{BASE}:2011/{aktion}",
            json={"station": station, "what": "IRON", "amount": menge},
        ).json()

        if antwort.get("kind") == "success":
            print(f"erfolgreich: {aktion} Anzahl Eisen: {menge} bei {station}")
            return True

        print(f"Versuche erneut: {aktion}")
        time.sleep(2)

    return False

while eisen_an_bord < ZIEL_EISEN:

    fliege_zu(AZURA)
    leistbare_menge = credits // BUY_PRICE
    kaufmenge = min(leistbare_menge, ZIEL_EISEN - eisen_an_bord)

    if kaufmenge > 0 and handel("buy", AZURA, kaufmenge):
        credits -= kaufmenge * BUY_PRICE
        eisen_an_bord += kaufmenge

    if eisen_an_bord >= ZIEL_EISEN:
        break

    if credits < BUY_PRICE:
        fliege_zu(CORE)
        if handel("sell", CORE, 4):
            credits += SELL_PRICE * 4
            eisen_an_bord -= 4
        else:
            print("Verkauf fehlgeschlagen, breche ab.")
            break

print(f"Eisen an Bord: {eisen_an_bord}/{ZIEL_EISEN}, Credits: {credits}")

if eisen_an_bord >= ZIEL_EISEN:
    fliege_zu(VESTA, VESTA_COORDS)
    print("Bei Vesta Station angedockt.")