"""Observabilité du pipeline : logs JSON structurés, seuils et alertes."""
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

ALERTES = Path(__file__).resolve().parents[1] / "logs" / "alertes.jsonl"

# Seuils d'alerte, fixés à partir des valeurs mesurées (voir docs/observabilite.md)
SEUIL_REJETS = 5        # % maximum de messages rejetés par la validation
SEUIL_FRAICHEUR = 2     # jours maximum depuis la dernière annonce publiée


def log_json(logger, event, niveau=logging.INFO, **champs):
    """Journalise un événement du pipeline sous forme d'une ligne JSON."""
    ligne = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "level": logging.getLevelName(niveau),
        "logger": logger.name,
        "run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),   # run Airflow en cours (None hors Airflow)
        "event": event,
    }
    ligne.update(champs)
    logger.log(niveau, json.dumps(ligne, ensure_ascii=False))
    return ligne


def alerter(logger, motif, **champs):
    """Alerte : log ERROR + ajout au journal des alertes (logs/alertes.jsonl)."""
    ligne = log_json(logger, "alerte", logging.ERROR, motif=motif, **champs)
    ALERTES.parent.mkdir(parents=True, exist_ok=True)
    with open(ALERTES, "a", encoding="utf-8") as f:
        f.write(json.dumps(ligne, ensure_ascii=False) + "\n")


def verifier_seuils(nb_api, validation, derniere_parution, aujourd_hui=None):
    """Compare les métriques d'un run aux seuils. Renvoie la liste des dépassements."""
    aujourd_hui = aujourd_hui or date.today()
    depassements = []

    if nb_api == 0:
        depassements.append("volume : aucune annonce reçue de l'API BOAMP")

    controles = validation["valides"] + validation["rejetes"]
    if controles and 100 * validation["rejetes"] / controles > SEUIL_REJETS:
        depassements.append(f"qualité : plus de {SEUIL_REJETS} % de messages rejetés")

    fraicheur = (aujourd_hui - date.fromisoformat(derniere_parution)).days
    if fraicheur > SEUIL_FRAICHEUR:
        depassements.append(f"fraîcheur : dernière annonce publiée il y a {fraicheur} jours")

    return depassements
