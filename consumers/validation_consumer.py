"""Consumer de validation : boamp-raw -> boamp-validated (valides normalisés) / boamp-raw-dlq (rejets)."""
import json
import logging
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

from monitoring.observabilite import log_json
from validation.contrat import VERSION, normaliser, valider

TOPIC_SOURCE = "boamp-raw"
TOPIC_VALIDES = "boamp-validated"
TOPIC_DLQ = "boamp-raw-dlq"
SILENCE_MAX = 10      # secondes sans nouveau message -> fin du lot
COMMIT_TOUS_LES = 1000
logger = logging.getLogger(__name__)


def message_dlq(original, motif, msg):
    """Message rejeté + métadonnées d'erreur, pour l'analyse et le rejeu."""
    return {
        "original_message": original,
        "rejection_reason": motif,
        "rejection_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "validator_version": VERSION,
        "correlation_id": msg.key().decode("utf-8") if msg.key() else None,
    }


def run(bootstrap=None, group_id="boamp_validation"):
    serveur = bootstrap or os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    consumer = Consumer({
        "bootstrap.servers": serveur,
        "group.id": group_id,
        "auto.offset.reset": "earliest",   # premier démarrage : lire depuis le début
        "enable.auto.commit": False,       # commit manuel APRÈS publication = at-least-once
    })
    producer = Producer({"bootstrap.servers": serveur, "acks": "all", "linger.ms": 5})
    consumer.subscribe([TOPIC_SOURCE])
    debut = time.monotonic()

    erreurs_livraison = []

    def on_delivery(err, _msg):
        if err:
            erreurs_livraison.append(err)

    def committer():
        producer.flush()   # d'abord s'assurer que Kafka a reçu les messages publiés...
        if erreurs_livraison:
            # ...sinon pas de commit : les messages seront relus au prochain essai
            raise RuntimeError(f"{len(erreurs_livraison)} erreurs de livraison : offsets non validés")
        consumer.commit(asynchronous=False)   # ...puis valider les offsets lus

    deja_vus = set()   # dédoublonnage court terme : clés (event_id) déjà traitées pendant ce lot
    lus = valides = rejetes = doublons = a_committer = silence = 0

    while silence < SILENCE_MAX:
        msg = consumer.poll(1.0)
        if msg is None:
            silence += 1
            continue
        if msg.error():
            log_json(logger, "erreur_kafka", logging.ERROR, erreur=str(msg.error()))
            continue
        silence = 0
        lus += 1
        a_committer += 1

        # 1. Désérialiser et valider
        try:
            event = json.loads(msg.value())
            motif = valider(event)
        except ValueError:
            event = msg.value().decode("utf-8", errors="replace")
            motif = "racine : JSON illisible"

        # 2. Router : doublon -> ignoré, invalide -> DLQ, valide -> normalisé
        cle = msg.key()   # clé Kafka = event_id
        if cle in deja_vus:
            doublons += 1
        elif motif:
            rejetes += 1
            dlq = message_dlq(event, motif, msg)
            log_json(logger, "message_rejete", logging.WARNING, correlation_id=dlq["correlation_id"],
                     motif=motif, source_partition=msg.partition(), source_offset=msg.offset())
            producer.produce(TOPIC_DLQ, key=cle, value=json.dumps(dlq, ensure_ascii=False),
                             headers=msg.headers(), on_delivery=on_delivery)
        else:
            valides += 1
            producer.produce(TOPIC_VALIDES, key=cle,
                             value=json.dumps(normaliser(event), ensure_ascii=False),
                             headers=msg.headers(), on_delivery=on_delivery)
        if cle:
            deja_vus.add(cle)
        producer.poll(0)   # déclenche les callbacks de livraison

        if a_committer >= COMMIT_TOUS_LES:
            committer()
            a_committer = 0

    if a_committer:
        committer()
    consumer.close()

    controles = valides + rejetes
    taux_validation = 100 * valides / controles if controles else 100
    log_json(logger, "validation_terminee", lus=lus, valides=valides, rejetes=rejetes, doublons=doublons,
             taux_validation=round(taux_validation, 2), duration_ms=int((time.monotonic() - debut) * 1000))
    return {"lus": lus, "valides": valides, "rejetes": rejetes, "doublons": doublons}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    run()
