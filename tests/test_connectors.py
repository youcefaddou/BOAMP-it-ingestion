from connectors.base import Connector
from connectors.boamp_api import liste_mois, mois_suivant


def test_to_event_enveloppe():
    annonce = {"idweb": "26-1", "objet": "Achat PC", "dateparution": "2026-11-02",
               "donnees": "contient des contacts"}
    event = Connector().to_event(annonce)
    assert event["event_id"] == "boamp-26-1"
    assert event["timestamp"] == "2026-11-02"
    assert event["payload"]["objet"] == "Achat PC"
    assert "donnees" not in event["payload"]


def test_mois():
    assert mois_suivant("2026-12") == "2027-01"
    assert liste_mois("2026-11", "2027-01") == ["2026-11", "2026-12", "2027-01"]
