"""Tests du contrat de données : validation, cohérence et normalisation."""
import copy

from validation.contrat import normaliser, valider

ANNONCE = {
    "source": "api-boamp",
    "event_id": "boamp-26-12345",
    "timestamp": "2026-11-20",
    "ingested_at": "2026-11-27T09:00:00+00:00",
    "schema_version": "1.0",
    "payload": {
        "idweb": "26-12345",
        "objet": "Maintenance  du parc informatique ",
        "nomacheteur": "Ville de Lyon",
        "dateparution": "2026-11-20",
        "datelimitereponse": "2026-12-15T12:00:00+01:00",
        "code_departement": ["1", "20A"],
        "nature_libelle": "Avis de marché",
        "famille_libelle": "Marchés <90 k€ (MAPA)",
        "type_marche": ["SERVICES"],
        "procedure_libelle": "Procédure adaptée",
        "titulaire": None,
        "descripteur_libelle": ["Informatique (prestations de services)"],
        "url_avis": "https://www.boamp.fr/pages/avis/?q=idweb:26-12345",
    },
}


def test_annonce_conforme():
    assert valider(ANNONCE) is None


def test_champ_obligatoire_absent():
    annonce = copy.deepcopy(ANNONCE)
    del annonce["payload"]["objet"]
    assert "objet" in valider(annonce)


def test_valeur_interdite():
    annonce = copy.deepcopy(ANNONCE)
    annonce["payload"]["type_marche"] = ["AUTRE"]
    assert "type_marche" in valider(annonce)


def test_date_limite_anterieure_a_la_parution():
    annonce = copy.deepcopy(ANNONCE)
    annonce["payload"]["datelimitereponse"] = "2026-11-10T12:00:00+00:00"
    assert valider(annonce) == "payload/datelimitereponse : antérieure à dateparution"


def test_event_id_incoherent():
    annonce = copy.deepcopy(ANNONCE)
    annonce["event_id"] = "boamp-26-99999"
    assert valider(annonce) == "event_id : incohérent avec payload/idweb"


def test_normalisation():
    event = normaliser(ANNONCE)
    assert event["payload"]["objet"] == "Maintenance du parc informatique"
    assert event["payload"]["code_departement"] == ["01", "2A"]
    assert event["payload"]["datelimitereponse"] == "2026-12-15T11:00:00Z"
    assert event["timestamp"] == "2026-11-20T00:00:00Z"
    assert ANNONCE["payload"]["code_departement"] == ["1", "20A"]   # l'original n'est pas modifié