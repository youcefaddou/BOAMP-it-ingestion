"""Interface commune des connecteurs : extract() -> to_event() -> publish()."""
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

from monitoring.observabilite import log_json

# Champs gardés dans le message. On retire `donnees` et `gestion` :
# ils contiennent des contacts (téléphones, e-mails)
CHAMPS = [
    "idweb", "objet", "nomacheteur", "code_departement", "dateparution",
    "datelimitereponse", "nature_libelle", "famille_libelle", "type_marche",
    "procedure_libelle", "titulaire", "descripteur_libelle", "url_avis",
]

STATE_DIR = Path(__file__).resolve().parents[1] / "data" / "state"
logger = logging.getLogger(__name__)


def lire_etat(nom):
    """Extraction incrémentale : ce qui a déjà été publié (mois ou fichiers)."""
    chemin = STATE_DIR / f"{nom}.json"
    if not chemin.exists():
        return set()
    return set(json.loads(chemin.read_text(encoding="utf-8")))


def sauver_etat(nom, elements):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    chemin = STATE_DIR / f"{nom}.json"
    chemin.write_text(json.dumps(sorted(elements), indent=2), encoding="utf-8")


class Connector:
    source = "inconnue"
    topic = "boamp-raw"
    nom_etat = "inconnu"

    def __init__(self, bootstrap=None):
        self.producer = Producer({
            "bootstrap.servers": bootstrap or os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            "acks": "all",       # durabilité maximale : attendre l'acquittement de toutes les répliques
            "linger.ms": 5,
            "batch.size": 16384,
        })
        self.termines = set()
        self.nb_erreurs = 0

    # 1. extract : propre à chaque source
    def extract(self):
        raise NotImplementedError

    # 2. to_event : enveloppe d'événement standard
    def to_event(self, annonce):
        idweb = annonce.get("idweb")
        return {
            "source": self.source,
            "event_id": f"boamp-{idweb}" if idweb else None,
            "timestamp": annonce.get("dateparution"),   # date de production (source)
            "ingested_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "schema_version": "1.0",
            "payload": {champ: annonce.get(champ) for champ in CHAMPS},
        }

    # 3. publish : clé = event_id -> une même annonce va toujours dans la même partition
    def publish(self, event):
        self.producer.produce(
            self.topic,
            key=event["event_id"],
            value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
            headers={"correlation_id": event["event_id"] or ""},   # suivi du message de bout en bout
            on_delivery=self._accuse_reception,
        )
        self.producer.poll(0)

    def _accuse_reception(self, erreur, message):
        if erreur:
            self.nb_erreurs += 1
            log_json(logger, "echec_livraison", logging.ERROR,
                     correlation_id=message.key().decode("utf-8"), erreur=str(erreur))

    def run(self):
        """Lance extract -> to_event -> publish, puis sauve l'état si tout est livré."""
        debut = time.monotonic()
        self.termines = lire_etat(self.nom_etat)
        nb = 0
        for annonce in self.extract():
            self.publish(self.to_event(annonce))
            nb += 1
        self.producer.flush()

        if self.nb_erreurs:
            # Exception = la tâche Airflow échoue et sera relancée (retries).
            # L'état n'est pas sauvé : le retry republiera, le consumer dédoublonnera.
            raise RuntimeError(f"{self.nb_erreurs} erreurs de livraison : état non sauvé")
        sauver_etat(self.nom_etat, self.termines)
        log_json(logger, "publication_terminee", source=self.source, topic=self.topic,
                 records_count=nb, duration_ms=int((time.monotonic() - debut) * 1000))
        return nb