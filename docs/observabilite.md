# Étape 6 - Observabilité

## Logs structurés JSON

Toutes les briques journalisent leurs événements via `log_json()` (`monitoring/observabilite.py`) : une ligne JSON
par événement, filtrable et agrégeable automatiquement.

```json
{"timestamp": "2026-11-27T14:47:33Z", "level": "INFO", "logger": "connectors.base",
 "run_id": "manual__2026-11-27T14:47:25.037526+00:00", "event": "publication_terminee",
 "source": "api-boamp", "topic": "boamp-raw", "records_count": 339, "duration_ms": 4146}
```

| Champ                    | Rôle                                                                                                 |
| ------------------------ | ----------------------------------------------------------------------------------------------------- |
| `timestamp`, `level` | quand, quelle gravité (INFO : jalon normal, WARNING : anormal non bloquant, ERROR : action requise)  |
| `logger`               | quelle brique parle (`connectors.base`, `consumers.validation_consumer`, `pipeline.controle`…) |
| `run_id`               | run Airflow, lu dans `AIRFLOW_CTX_DAG_RUN_ID` : relie tous les logs d'un même run                  |
| `event`                | nom de l'événement                                                                                  |
| autres                   | volumes, durée, `correlation_id`… selon l'événement                                             |

### Événements journalisés

| Brique              | `event`                | Niveau  | Champs                                                                                |
| ------------------- | ------------------------ | ------- | ------------------------------------------------------------------------------------- |
| Connecteur API      | `mois_extrait`         | INFO    | `mois`, `records_count`                                                           |
| Connecteur API      | `nouvel_essai_http`    | WARNING | `statut` ou `erreur`, `attente_s`                                               |
| Connecteur fichiers | `ligne_illisible`      | WARNING | `fichier`, `ligne`                                                                |
| Connecteurs         | `publication_terminee` | INFO    | `source`, `topic`, `records_count`, `duration_ms`                             |
| Connecteurs         | `echec_livraison`      | ERROR   | `correlation_id`, `erreur`                                                        |
| Validation          | `message_rejete`       | WARNING | `correlation_id`, `motif`, `source_partition`, `source_offset`                |
| Validation          | `validation_terminee`  | INFO    | `lus`, `valides`, `rejetes`, `doublons`, `taux_validation`, `duration_ms` |
| Stockage            | `stockage_termine`     | INFO    | `lus`, `ecrits`, `doublons`, `duration_ms`                                    |
| Consumers           | `erreur_kafka`         | ERROR   | `erreur`                                                                            |
| Contrôle           | `controle_run`         | INFO    | volumes du run, `total_sortie`, `derniere_parution`                               |
| Contrôle / DAG     | `alerte`               | ERROR   | `motif` (+ `task_id`, `essai`, `erreur` pour un échec de tâche)             |

Les annonces valides ne sont pas journalisées une par une (85 000 lignes par rejeu) : seuls les jalons
et les anomalies le sont.

## Métriques suivies

| Métrique    | Mesure                                                                              | Où                                                                                                                                       |
| ------------ | ----------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| Volume       | messages publiés, validés, rejetés, écrits, doublons                            | `publication_terminee`, `validation_terminee`, `stockage_termine`, `controle_run`                                                 |
| Erreurs      | rejets (taux de validation), échecs de livraison, erreurs Kafka, échecs de tâche | logs WARNING / ERROR, DLQ                                                                                                                 |
| Fraîcheur   | date de la dernière annonce présente dans la sortie                               | `controle_run.derniere_parution`                                                                                                        |
| Durée       | durée de chaque brique                                                             | `duration_ms` (et durée des runs dans Airflow)                                                                                         |
| Lag consumer | —                                                                                  | 0 en fin de tâche par construction : les consumers tournent jusqu'à vider le topic ; `controler_volumes` vérifie `lus >= publiés` |

## Seuils d'alerte

Fixés à partir des valeurs nominales mesurées sur novembre 2026 (pas de seuil arbitraire) :

| Métrique          | Valeur nominale mesurée                                                 | SLO        | Alerte                                                                      |
| ------------------ | ------------------------------------------------------------------------ | ---------- | --------------------------------------------------------------------------- |
| Fraîcheur         | 0 jour : au moins 4 annonces IT publiées chaque jour, week-ends compris | ≤ 2 jours | > 2 jours sans nouvelle annonce                                             |
| Taux de rejet      | 0,015 % sur l'historique (13 / 85 122) ; 0,6 % sur un run API (2 / 339)  | < 5 %      | > 5 %                                                                       |
| Volume API par run | ~340 annonces (mois en cours republié à chaque run)                    | > 0        | 0 = absence de données                                                     |
| Échec de tâche   | —                                                                       | aucun      | échec **définitif** (après les 3 retries)                          |
| Durée d'un run    | ~1 min (12 min pour un rejeu complet)                                    | < 10 min   | tâche arrêtée après 30 min (`execution_timeout`) puis alerte d'échec |

### Fatigue d'alerte : n'alerter que ce qui compte

- **Pas d'alerte à chaque retry** : `on_failure_callback` n'est appelé qu'après le dernier essai.
- **Dépassement de seuil ≠ échec** : les seuils (`verifier_seuils`) alertent sans faire échouer le run, les données
  étant correctement traitées. Les incohérences (volumes, doublons dans la sortie) restent des erreurs bloquantes.
- Toutes les valeurs de seuil sont regroupées dans `monitoring/observabilite.py`.

### Canal d'alerte

Une alerte = un log `ERROR` (`"event": "alerte"`) + une ligne dans le journal `logs/alertes.jsonl`
(hors dépôt). Un envoi Slack ou e-mail se brancherait dans la seule fonction `alerter()`.

### Test de l'alerte (27/11/2026)

Seuil de fraîcheur abaissé temporairement à `-1`, puis DAG déclenché :

```json
{"timestamp": "2026-11-27T14:57:06Z", "level": "ERROR", "logger": "pipeline.controle",
 "run_id": "manual__2026-11-27T14:56:27.926141+00:00", "event": "alerte",
 "motif": "fraîcheur : dernière annonce publiée il y a 0 jours"}
```

L'alerte apparaît dans les logs de `controler_volumes` et dans `logs/alertes.jsonl` ; le run reste en succès.
Seuil remis à 2 jours ensuite. Le callback d'échec a été testé séparément dans le conteneur Airflow
(échec simulé de `consommer`, alerte `échec définitif d'une tâche` écrite avec `task_id`, `essai`, `erreur`).

## Identifiant de corrélation

`correlation_id` = `event_id` (`boamp-<idweb>`), créé à la création du message dans `to_event()`, puis propagé :

| Étape     | Où trouver l'identifiant                                                                                             |
| ---------- | --------------------------------------------------------------------------------------------------------------------- |
| Création  | `to_event()` : champ `event_id`                                                                                   |
| Kafka      | clé du message **et** en-tête `correlation_id` (`boamp-raw`, propagé vers `boamp-validated` et la DLQ) |
| Validation | logs `message_rejete`, champ `correlation_id` du message DLQ                                                      |
| Stockage   | champ `event_id` de chaque ligne de `annonces.jsonl`                                                              |

Le `run_id` complète ce suivi à l'échelle du lot : tous les logs d'un run portent le même `run_id`.

### Démo : retrouver le parcours d'une annonce

Annonce rejetée `boamp-26-114763`, retrouvée en quelques secondes :

```bash
docker exec airflow grep -rh "boamp-26-114763" /opt/airflow/logs/dag_id=boamp_ingestion/ | grep -o '{"timestamp.*'
grep -c "boamp-26-114763" data/output/annonces.jsonl     # 0 : absente de la sortie
```

```json
{"level": "WARNING", "logger": "consumers.validation_consumer", "run_id": "manual__2026-11-27T14:47:25.037526+00:00",
 "event": "message_rejete", "correlation_id": "boamp-26-114763",
 "motif": "payload/datelimitereponse : antérieure à dateparution", "source_partition": 1, "source_offset": 29246}
```

Dans Kafka UI, la recherche de `boamp-26-114763` dans `boamp-raw-dlq` donne le message en quarantaine,
avec `rejection_reason` et l'offset d'origine.

## Vérification sur un run réel (27/11/2026, `manual__2026-11-27T14:47:25`)

| Brique     | Événement                                 | Résultat                                                                                     |
| ---------- | ------------------------------------------- | --------------------------------------------------------------------------------------------- |
| API        | `mois_extrait` / `publication_terminee` | 339 annonces, publiées en 4,1 s                                                              |
| Validation | `validation_terminee`                     | 337 valides, 2 rejetés, taux 99,41 %, 10,2 s                                                 |
| Contrôle  | `controle_run`                            | 0 écrit (déjà présentes), 85 109 au total, dernière parution 2026-11-27 → aucune alerte |

## Consigne → réalisation

| Consigne (étape 6)                 | Où                                                                                    |
| ----------------------------------- | -------------------------------------------------------------------------------------- |
| Logs structurés                    | `log_json()`, utilisée par toutes les briques                                       |
| Suivre volumes, erreurs, fraîcheur | événements `*_terminee`, `controle_run`, WARNING / ERROR                         |
| Seuils d'alerte pertinents          | `verifier_seuils()` + `on_failure_callback`, seuils tirés des mesures             |
| Identifiant de corrélation         | `event_id` en clé et en-tête Kafka, logs, DLQ, sortie ; `run_id` dans chaque log |
| Tests                               | `tests/test_monitoring.py` (4 tests)                                                 |

## Limites et choix assumés

- **Pas de centralisation Loki + Grafana** : les logs restent dans Airflow (`/opt/airflow/logs`), déjà centralisés
  pour ce pipeline mono-machine, et interrogeables par `grep`. Évolution : Loki + Grafana pour un tableau de bord
  et une recherche par `correlation_id` dans une interface.
- **Logs JSON dans le message** : Airflow préfixe chaque ligne (`[date] {fichier} INFO -`) ; le JSON se trouve après
  ce préfixe. Évolution : configurer un formateur JSON au niveau d'Airflow.
- **Alerte = log + fichier**, pas de Slack ni d'e-mail (pas de compte dans ce projet).
- **`correlation_id` = `event_id`** : identifiant métier, stable d'un run à l'autre ; le `run_id` distingue les passages.
- **Annonces invalides du mois en cours** : republiées par l'API à chaque run, elles retournent en DLQ à chaque run
  (+2 messages par run). Évolution : mémoriser les `event_id` déjà rejetés entre deux runs.
- **Latence de bout en bout** non mesurée message par message : la fraîcheur et les durées par brique la couvrent
  pour un traitement par lot.
