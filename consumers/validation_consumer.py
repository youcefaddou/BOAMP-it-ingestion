"""Consumer de validation : boamp-raw -> boamp-validated (valides normalisés) / boamp-raw-dlq (rejets)"""
import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

from validation.contrat import VERSION, normaliser, valider

TOPIC_SOURCE = "boamp-raw"
TOPIC_VALIDES = "boamp-validated"
TOPIC_DLQ = "boamp-raw-dlq"
SILENCE_MAX = 10 # secondes sans nouveau message -> fin du lot
COMMIT_TOUS_LES = 1000
SEUIL_REJETS = 5 # alerte au dela de 5 % de messages rejetés

def message_dlq(original, motif, msg):
    """Message rejeté + metadata d'erreur pour l'analyse et le rejeu"""
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
        "auto.offset.reset": "earliest",  # 1er démarrage : lire depuis le debut
        "enable.auto.commit": False       # commit manuel APRES publication = at-least-once
    })
    producer = Producer({"bootstrap.servers": serveur, "acks": "all", "linger.ms": 5})
    consumer.subscribe([TOPIC_SOURCE])
    
    erreurs_livraison = []
    
    def on_delivery(err, _msg):
        if err:
            erreurs_livraison.append(err)
            
    def committer():
        producer.flush() # d'abord s'assurer que kafka a reçu les messages publiés
        if erreurs_livraison:
            #sinon pas de commit: msg relus au prochain essai
            raise RuntimeError(f"{len(erreurs_livraison)} erreurs de livraison : offsets non validés")
        consumer.commit(asynchronous=False) # on valide les offsets lus
    deja_vus = set()   # dédoublonnage court terme : clés (event_id) déjà traitées pendant ce lot
    lus = valides = rejetes = doublons = a_committer = silence = 0

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
            logging.warning("Message rejeté %s : %s", dlq["correlation_id"], motif)
            producer.produce(TOPIC_DLQ, key=cle, value=json.dumps(dlq, ensure_ascii=False),
                             on_delivery=on_delivery)
        else:
            valides += 1
            producer.produce(TOPIC_VALIDES, key=cle,
                             value=json.dumps(normaliser(event), ensure_ascii=False),
                             on_delivery=on_delivery)
        if cle:
            deja_vus.add(cle)