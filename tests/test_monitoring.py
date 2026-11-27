"""Tests de l'observabilité : format des logs et seuils d'alerte."""
import json
import logging
from datetime import date

from monitoring import observabilite
from monitoring.observabilite import alerter, log_json, verifier_seuils

RUN_NORMAL = {"valides": 339, "rejetes": 0}


def test_log_json(caplog):
    caplog.set_level(logging.INFO)
    log_json(logging.getLogger("test"), "publication_terminee", records_count=339)
    ligne = json.loads(caplog.records[0].getMessage())
    assert ligne["event"] == "publication_terminee"
    assert ligne["records_count"] == 339
    assert ligne["level"] == "INFO"


def test_run_normal_sans_alerte():
    assert verifier_seuils(339, RUN_NORMAL, "2026-11-27", date(2026, 11, 27)) == []


def test_seuils_depasses():
    validation = {"valides": 90, "rejetes": 10}
    alertes = verifier_seuils(0, validation, "2026-11-20", date(2026, 11, 27))
    assert len(alertes) == 3   # volume nul, 10 % de rejets, 7 jours sans annonce


def test_alerte_ecrite_dans_le_journal(tmp_path, monkeypatch):
    monkeypatch.setattr(observabilite, "ALERTES", tmp_path / "alertes.jsonl")
    alerter(logging.getLogger("test"), "fraîcheur : test", task_id="controler_volumes")
    alerte = json.loads((tmp_path / "alertes.jsonl").read_text(encoding="utf-8"))
    assert alerte["event"] == "alerte"
    assert alerte["task_id"] == "controler_volumes"