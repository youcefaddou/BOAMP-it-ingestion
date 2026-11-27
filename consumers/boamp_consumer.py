"""Consumer boamp-validated -> data/output/annonces.jsonl (at-least-once + dédoublonnage)"""
import json
import logging
import os
from pathlib import Path

from confluent_kafka import Consumer
from dotenv import load_dotenv

OUTPUT = Path(__file__).resolve().parents[1] / "data" / "output" / "annonces.jsonl"
TOPIC = "boamp-validated"
SILENCE_MAX = 10      # secondes sans nouveau message -> fin du lot
COMMIT_TOUS_LES = 1000


def ids_deja_ecrits(chemin):
    """Dédoublonnage contre la cible : event_id déjà présents dans le fichier de sortie."""
    if not chemin.exists():
        return set()
    with open(chemin, encoding="utf-8") as f:
        return {json.loads(ligne)["event_id"] for ligne in f}


def run(bootstrap=None, group_id="boamp-stockage"):
    consumer = Consumer({
        "bootstrap.servers": bootstrap or os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id": group_id,
        "auto.offset.reset": "earliest",   # premier démarrage : lire depuis le début
        "enable.auto.commit": False,       # commit manuel APRÈS écriture = at-least-once
    })
    consumer.subscribe([TOPIC])

    deja_vus = ids_deja_ecrits(OUTPUT)
    lus = ecrits = doublons = a_committer = silence = 0
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT, "a", encoding="utf-8") as sortie:
        while silence < SILENCE_MAX:
            msg = consumer.poll(1.0)
            if msg is None:
                silence += 1
                continue
            if msg.error():
                logging.error("Erreur Kafka : %s", msg.error())
                continue
            silence = 0
            lus += 1
            a_committer += 1

            event = json.loads(msg.value().decode("utf-8"))
            event_id = event.get("event_id")
            if event_id and event_id in deja_vus:
                doublons += 1                      # déjà écrit : on ne l'écrit pas deux fois
            else:
                sortie.write(json.dumps(event, ensure_ascii=False) + "\n")
                deja_vus.add(event_id)
                ecrits += 1

            if a_committer >= COMMIT_TOUS_LES:
                sortie.flush()                     # d'abord écrire sur disque...
                consumer.commit(asynchronous=False)  # ...puis valider l'offset
                a_committer = 0

        sortie.flush()
        if a_committer:
            consumer.commit(asynchronous=False)
    consumer.close()

    logging.info("%d lus, %d écrits, %d doublons ignorés", lus, ecrits, doublons)
    return {"lus": lus, "ecrits": ecrits, "doublons": doublons}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    run()
