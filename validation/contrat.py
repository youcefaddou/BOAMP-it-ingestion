"""Contrat de données des annonces BOAMP : validation et normalisation."""
import copy
import json
from datetime import datetime, timezone
from pathlib import Path

import jsonschema

VERSION = "1.0"

with open(Path(__file__).parent / "schema_annonce.json", encoding="utf-8") as f:
    SCHEMA = json.load(f)
VALIDATEUR = jsonschema.Draft7Validator(SCHEMA)   # construit une seule fois, pas à chaque message

CORSE = {"20A": "2A", "20B": "2B"}   # codes BOAMP -> codes INSEE


def valider(event):
    """Renvoie None si le message respecte le contrat, sinon le motif du rejet."""
    try:
        VALIDATEUR.validate(event)
    except jsonschema.ValidationError as e:
        champ = "/".join(str(p) for p in e.path)
        return f"{champ} : {e.message}"

    # Règles de cohérence entre deux champs (JSON Schema ne compare pas deux champs)
    payload = event["payload"]
    if event["event_id"] != "boamp-" + payload["idweb"]:
        return "event_id : incohérent avec payload/idweb"
    date_limite = payload["datelimitereponse"]
    if date_limite and date_limite[:10] < payload["dateparution"]:
        return "payload/datelimitereponse : antérieure à dateparution"
    return None


def nettoyer_texte(texte):
    """Supprime les espaces en début et fin de texte, et les espaces multiples."""
    return " ".join(texte.split())


def en_utc(date_heure):
    """Date-heure ISO 8601 -> ISO 8601 UTC, ex. '2026-12-01T12:00:00+01:00' -> '2026-12-01T11:00:00Z'."""
    return datetime.fromisoformat(date_heure).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normaliser(event):
    """Renvoie une copie normalisée d'un message valide (l'original n'est pas modifié)."""
    event = copy.deepcopy(event)
    payload = event["payload"]

    payload["objet"] = nettoyer_texte(payload["objet"])
    payload["nomacheteur"] = nettoyer_texte(payload["nomacheteur"])

    if payload["code_departement"]:
        codes = []
        for code in payload["code_departement"]:
            code = CORSE.get(code, code)     # "20A" -> "2A"
            codes.append(code.zfill(2))      # "1" -> "01"
        payload["code_departement"] = codes

    if payload["datelimitereponse"]:
        payload["datelimitereponse"] = en_utc(payload["datelimitereponse"])

    event["timestamp"] = event["timestamp"] + "T00:00:00Z"
    return event