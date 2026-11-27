"""DAG BOAMP : publier (API + fichiers) -> consommer -> contrôler les volumes."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

# Réglages appliqués à TOUTES les tâches
RESILIENCE = {
    "retries": 3,                                # erreur transitoire -> 3 nouveaux essais
    "retry_delay": timedelta(minutes=1),         # 1er nouvel essai après 1 min...
    "retry_exponential_backoff": True,           # ...puis 2 min, puis 4 min
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),  # une tache bloquée finit par échouer
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

    # 2. Consommation : boamp-raw -> data/output/annonces.jsonl (dédoublonné) 
    @task
    def consommer():
        from consumers import boamp_consumer
        return boamp_consumer.run()

    # 3. Contrôle : comparer au volume publié + vérifier l'absence de doublons 
    @task
    def controler_volumes(nb_api, nb_fichiers, bilan):
        import json
        import logging
        from consumers.boamp_consumer import OUTPUT

        publies = nb_api + nb_fichiers
        if bilan["lus"] < publies:
            raise ValueError(f"{publies} messages publiés mais seulement {bilan['lus']} lus")

        with open(OUTPUT, encoding="utf-8") as f:
            ids = [json.loads(ligne)["event_id"] for ligne in f]
        if len(ids) != len(set(ids)):
            raise ValueError(f"{len(ids) - len(set(ids))} doublons dans {OUTPUT.name}")

        logging.info("Publiés %d | lus %d | écrits %d | doublons ignorés %d | total sortie %d",
                     publies, bilan["lus"], bilan["ecrits"], bilan["doublons"], len(ids))

    # Dépendances
    api = publier_api()
    fichiers = publier_fichiers()
    bilan = consommer()
    [api, fichiers] >> bilan                 # consommer attend les DEUX sources
    controler_volumes(api, fichiers, bilan)  # utiliser leurs résultats crée aussi le lien