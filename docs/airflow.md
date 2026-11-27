# Étape 4 — Orchestration avec Airflow

## DAG `boamp_ingestion` (`airflow/dags/boamp_ingestion.py`)

```
publier_api ──────┐
                  ├──► consommer ──► controler_volumes
publier_fichiers ─┘
```

| Tâche | Rôle | Valeur renvoyée (XCom) |
|---|---|---|
| `publier_api` | Connecteur API BOAMP → `boamp-raw` | nombre de messages publiés |
| `publier_fichiers` | Connecteur fichiers historiques → `boamp-raw` | nombre de messages publiés |
| `consommer` | `boamp-raw` → `data/output/annonces.jsonl`, dédoublonnage par `event_id` (depuis l'étape 5 : lit `boamp-validated`, voir [qualite.md](qualite.md)) | `{lus, ecrits, doublons}` |
| `controler_volumes` | Vérifie `lus >= publiés` et l'absence de doublons dans la sortie | — (échoue sinon) |

Le DAG ne contient pas de logique métier : il orchestre les connecteurs (étape 2) et le consumer (étape 3).

## Dépendances

- Fan-in : `[publier_api, publier_fichiers] >> consommer`, la consommation attend les deux sources.
- `controler_volumes` reçoit les résultats des trois tâches par XCom, ce qui crée aussi les dépendances.

## Planification

| Paramètre | Valeur | Raison |
|---|---|---|
| `schedule` | `0 7,19 * * *` | Le BOAMP publie 2 fois par jour |
| `catchup` | `False` | À l'activation, un seul run (le dernier créneau), pas de rattrapage depuis `start_date` |
| `max_active_runs` | `1` | Les runs partagent le consumer group `boamp-stockage` : deux runs simultanés se « volent » les messages |
| `start_date` | `2026-11-01` | Date fixe (jamais `datetime.now()`) |

Le run créé à l'activation s'appelle `scheduled__2026-11-26T19:00` : Airflow nomme un run d'après le **début**
de l'intervalle de données qu'il couvre (`data_interval_start`), pas d'après l'heure où il s'exécute.

## Résilience

Paramètres appliqués à toutes les tâches (`default_args`) :

| Paramètre | Valeur |
|---|---|
| `retries` | 3 |
| `retry_delay` | 1 min, avec `retry_exponential_backoff` : 1, 2, 4 min |
| `max_retry_delay` | 10 min |
| `execution_timeout` | 30 min |

| Type d'erreur | Exemple | Traitement |
|---|---|---|
| Transitoire, requête HTTP | 429, 5xx, timeout réseau | Backoff exponentiel + jitter dans le connecteur (`get_avec_backoff`) |
| Transitoire, tâche | Kafka indisponible | `Connector.run()` lève une exception si des messages ne sont pas livrés → retry Airflow |
| Définitive | HTTP 4xx (requête invalide, clé refusée) | `AirflowFailException` : échec immédiat, sans retry |

## Idempotence

Un retry, un *Clear* ou une relance republie des annonces déjà présentes dans Kafka.
Pas de doublon dans la sortie : clé Kafka = `event_id`, et le consumer ignore les `event_id` déjà écrits.
`controler_volumes` le vérifie à chaque run (nombre de lignes = nombre d'`event_id` distincts).

## Vérifications (27/11/2026)

| Run | Scénario | Publiés | Lus | Écrits | Doublons | Résultat |
|---|---|---|---|---|---|---|
| `scheduled__2026-11-26T19:00` | Activation du DAG | 339 | 678 | 60 | 618 | ✅ |
| `manual__…09:15:36` | Déclenché en même temps que le run précédent | 339 | 0 | 0 | 0 | ❌ après 3 retries |
| idem, après *Clear* | Rejeu après ajout de `max_active_runs=1` | 339 | 339 | 0 | 339 | ✅ |
| `manual__…09:33:40` | Kafka arrêté pendant le run | 339 | 339 | 0 | 339 | ✅ au 2e essai |

### Runs en parallèle : défaut détecté par le contrôle des volumes

Le consumer du 1er run a lu ses 339 messages **et** ceux du 2e run (678), car les deux runs
utilisent le même consumer group. Le consumer du 2e run n'avait plus rien à lire, et `controler_volumes`
a signalé l'écart : `339 messages publiés mais seulement 0 lus`.
Correction : `max_active_runs=1`, puis *Clear* du run en échec. Le rejeu n'a créé aucun doublon.

### Panne de Kafka : reprise automatique

`docker compose stop kafka`, puis déclenchement du DAG.

1. Tentative 1 de `publier_api` (09:33:44 → 09:38:59) : 339 × `Local: Message timed out` au bout de 5 min,
   puis `RuntimeError: 339 erreurs de livraison : état non sauvé` → `UP_FOR_RETRY`.

   ![publier_api en up_for_retry pendant l'arrêt de Kafka](../../../captures/kafka_stopped_2.png)

2. `docker compose start kafka`. Tentative 2 : `api-boamp : 339 messages publiés`, puis
   `consommer` lit 339 messages, en écrit 0 (339 doublons). Les 3 runs sont en succès.

   ![Reprise après redémarrage de Kafka : tous les runs en succès](../../../captures/kafka_restart.png)

Pendant l'arrêt, Kafka UI ne répond plus et le run est en cours (`publier_api` en *running*) :

![Kafka arrêté, run en cours](../../../captures/kafka_stopped_1.png)

### Bilan

- Sortie après les 4 runs : **85 122 lignes**, 0 doublon (85 062 avant l'étape + 60 nouvelles annonces).
- `boamp-raw` : **86 723 messages** (85 367 + 4 publications de 339). Les republications restent dans Kafka,
  le dédoublonnage se fait côté consumer.

## Corrections apportées pendant l'étape

- `connectors/base.py` : un échec de livraison lève une exception au lieu d'être seulement journalisé
  (sinon la tâche finit en succès et Airflow ne la relance pas).
- `connectors/boamp_api.py` : clé d'API facultative `BOAMP_API_KEY`, lue dans l'environnement.
- `docker-compose.yml` : `env_file: .env` pour transmettre la configuration à Airflow,
  et suppression du fichier PID résiduel du webserver au démarrage (sinon le webserver refuse de démarrer
  après un arrêt : `No response from gunicorn master within 120 seconds`).

## Choix assumés

- `retry_delay` de 1 min au lieu de 5 min, pour une démonstration rapide (5 min en production).
- `SequentialExecutor` (mode standalone, SQLite) : les deux tâches de publication s'exécutent l'une après l'autre.
  En production : `LocalExecutor` ou `CeleryExecutor` avec Postgres.
- `data_interval` non utilisé : l'incrémental repose sur l'état des connecteurs (`data/state/`), pas sur la date du run.
- Pas de branchement ni de trigger rule : aucun cas métier ne le demande. Les alertes en cas d'échec sont prévues à l'étape 6.
- `_PIP_ADDITIONAL_REQUIREMENTS` : réservé au développement. En production, on construit une image dédiée.
- Le contrôle `lus >= publiés` suppose qu'on rejoue un run **complet**. Un *Clear* de `consommer` seul
  le ferait échouer (évolution possible : mesurer le lag du consumer group).
