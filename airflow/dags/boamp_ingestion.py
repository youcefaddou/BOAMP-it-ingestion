"""DAG BOAMP : publier (API + fichiers) -> valider -> consommer -> contrôler (volumes, seuils d'alerte)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task


def alerte_echec(context):
    """Alerte sur échec définitif d'une tâche (appelée après le dernier retry, pas à chaque essai)."""
    import logging
    from monitoring.observabilite import alerter

    ti = context["task_instance"]
    alerter(logging.getLogger("pipeline.alertes"), "échec définitif d'une tâche",
            run_id=context["run_id"], task_id=ti.task_id, essai=ti.try_number,
            erreur=str(context.get("exception")))


# Réglages appliqués à TOUTES les tâches
RESILIENCE = {
    "retries": 3,                                # erreur transitoire -> 3 nouveaux essais
    "retry_delay": timedelta(minutes=1),         # 1er nouvel essai après 1 min...
    "retry_exponential_backoff": True,           # ...puis 2 min, puis 4 min
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),  # une tâche bloquée finit par échouer
    "on_failure_callback": alerte_echec,         # alerte uniquement sur échec définitif
}

with DAG(
    dag_id="boamp_ingestion",
    start_date=datetime(2026, 11, 1),
    schedule="0 7,19 * * *",   # 7 h et 19 h : le BOAMP publie 2 fois par jour
    catchup=False,             # ne pas rattraper tous les runs depuis le 1er novembre
    max_active_runs=1,         # runs séquentiels : un consumer group partagé ne tolère pas deux runs en parallèle
    default_args=RESILIENCE,
    tags=["boamp", "kafka"],
) as dag:

    # 1. Ingestion : les connecteurs publient dans boamp-raw
    @task
    def publier_api():
        import os
        import requests
        from airflow.exceptions import AirflowFailException
        from connectors.boamp_api import BoampApiConnector

        try:
            return BoampApiConnector(os.environ.get("BOAMP_MOIS_DEBUT", "2026-11")).run()
        except requests.HTTPError as erreur:
            if erreur.response.status_code < 500:
                # 4xx = erreur définitive : réessayer ne sert à rien -> échec sans retry
                raise AirflowFailException(f"Erreur définitive : {erreur}") from erreur
            raise

    @task
    def publier_fichiers():
        from connectors.file_connector import FileConnector
        return FileConnector().run()

    # 2. Validation : boamp-raw -> boamp-validated (valides) / boamp-raw-dlq (rejets)
    @task
    def valider():
        from consumers import validation_consumer
        return validation_consumer.run()

    # 3. Consommation : boamp-validated -> data/output/annonces.jsonl (dédoublonné)
    @task
    def consommer():
        from consumers import boamp_consumer
        return boamp_consumer.run()

    # 4. Contrôle : cohérence des volumes, doublons, puis seuils d'alerte
    @task
    def controler_volumes(nb_api, nb_fichiers, validation, bilan):
        import json
        import logging
        from consumers.boamp_consumer import OUTPUT
        from monitoring.observabilite import alerter, log_json, verifier_seuils

        logger = logging.getLogger("pipeline.controle")

        # Erreurs bloquantes : la tâche échoue (retries, puis alerte si l'échec persiste)
        publies = nb_api + nb_fichiers
        if validation["lus"] < publies:
            raise ValueError(f"{publies} messages publiés mais seulement {validation['lus']} validés")
        if bilan["lus"] < validation["valides"]:
            raise ValueError(f"{validation['valides']} messages valides mais seulement {bilan['lus']} lus")

        ids = []
        derniere_parution = ""
        with open(OUTPUT, encoding="utf-8") as f:
            for ligne in f:
                event = json.loads(ligne)
                ids.append(event["event_id"])
                derniere_parution = max(derniere_parution, event["payload"]["dateparution"])
        if len(ids) != len(set(ids)):
            raise ValueError(f"{len(ids) - len(set(ids))} doublons dans {OUTPUT.name}")

        # Métriques du run
        log_json(logger, "controle_run", publies=publies, valides=validation["valides"],
                 rejetes=validation["rejetes"], ecrits=bilan["ecrits"],
                 doublons_ignores=validation["doublons"] + bilan["doublons"],
                 total_sortie=len(ids), derniere_parution=derniere_parution)

        # Seuils : alerte sans faire échouer le run (les données sont correctement traitées)
        for motif in verifier_seuils(nb_api, validation, derniere_parution):
            alerter(logger, motif)

    # Dépendances
    api = publier_api()
    fichiers = publier_fichiers()
    validation = valider()
    bilan = consommer()
    [api, fichiers] >> validation >> bilan               # valider attend les DEUX sources
    controler_volumes(api, fichiers, validation, bilan)  # utiliser leurs résultats crée aussi le lien
