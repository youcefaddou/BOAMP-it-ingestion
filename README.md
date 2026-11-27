# BOAMP IT ingestion — Kafka + Airflow

Pipeline d'ingestion des annonces de marchés publics **informatiques et télécoms** publiées au
[BOAMP](https://www.boamp.fr/) (Bulletin officiel des annonces des marchés publics).

Les annonces sont collectées depuis l'API BOAMP et depuis des fichiers historiques, transitent par Kafka,
sont validées et normalisées, puis stockées sans doublon. Airflow orchestre le tout deux fois par jour.

**Question analytique :** qui achète de l'informatique dans le secteur public, quoi, avec quel délai pour répondre,
et quelles entreprises remportent les marchés ?

## Architecture

```
 SOURCES               CONNECTEURS                KAFKA (KRaft)               VALIDATION                  STOCKAGE
 API BOAMP ─────► BoampApiConnector ──┐
 (REST, paginée)                      ├──► boamp-raw ──► validation_consumer ──┬─► boamp-validated ──► boamp_consumer ──► data/output/annonces.jsonl
 Fichiers JSONL ► FileConnector ──────┘   (3 partitions,  (JSON Schema,         │   (3 partitions)      (dédoublonnage)
 (historique)                              clé = event_id)  normalisation)      └─► boamp-raw-dlq (1 partition, + motif du rejet)

 Airflow (DAG boamp_ingestion, 7 h et 19 h) : publier_api + publier_fichiers ─► valider ─► consommer ─► controler_volumes
 Observabilité : logs JSON (run_id, correlation_id), seuils d'alerte, journal logs/alertes.jsonl
```

| Choix | Justification | Détail |
|---|---|---|
| Pattern **pull batch** | Le BOAMP publie 2 fois par jour, aucun besoin de temps réel | [docs/cadrage.md](docs/cadrage.md) |
| **Kafka** entre collecte et traitement | Découplage, conservation du brut (rejeu), DLQ | [docs/kafka.md](docs/kafka.md) |
| Clé = `event_id` (`boamp-<idweb>`) | Même annonce → même partition ; clé de dédoublonnage | [docs/kafka.md](docs/kafka.md) |
| **At-least-once** + idempotence | Commit après écriture, dédoublonnage par `event_id` : ni perte ni doublon | [docs/kafka.md](docs/kafka.md) |
| Validation **après** Kafka | `boamp-raw` reste brut et rejouable ; les invalides vont en quarantaine | [docs/qualite.md](docs/qualite.md) |
| Airflow : retries + backoff, `catchup=False`, `max_active_runs=1` | Reprise automatique des erreurs transitoires, runs séquentiels | [docs/airflow.md](docs/airflow.md) |
| Seuils tirés des mesures | Alerte sur ce qui compte uniquement | [docs/observabilite.md](docs/observabilite.md) |

### Connecteurs

Une interface commune `Connector` (`extract` → `to_event` → `publish`), réutilisée par les deux sources :

| Propriété | Mise en œuvre |
|---|---|
| Réutilisable | Classe `Connector` (`connectors/base.py`), 2 sources la spécialisent |
| Authentifiée | Clé d'API facultative `BOAMP_API_KEY`, lue dans l'environnement |
| Paginée | offset / limit, 100 annonces par page (`connectors/boamp_api.py`) |
| Incrémentale | État des mois et fichiers déjà publiés dans `data/state/` |
| Robuste | Backoff exponentiel + jitter sur 429 / 5xx, échec immédiat sur 4xx, exception si une livraison Kafka échoue |

## Stack

| Composant | Version | Accès |
|---|---|---|
| Kafka (mode KRaft, sans ZooKeeper) | Confluent 7.7.1 | `localhost:9092` (hôte), `kafka:29092` (conteneurs) |
| Kafka UI | provectuslabs/kafka-ui | http://localhost:8081 |
| Airflow (standalone) | 2.10.5 | http://localhost:8080 |
| Python | 3.12 | `confluent-kafka`, `requests`, `jsonschema`, `python-dotenv`, `pytest` |

## Installation

Prérequis : Docker (ou Podman) avec Docker Compose, Python 3.12, git.

```bash
git clone https://github.com/youcefaddou/BOAMP-it-ingestion.git
cd BOAMP-it-ingestion

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
```

### Démarrer la stack

```bash
docker compose up -d
docker compose ps                  # kafka, kafka-ui, airflow : Up
bash kafka/scripts/create_topics.sh
```

Le script crée les 3 topics (sans erreur s'ils existent déjà) : `boamp-raw` (3 partitions), `boamp-validated` (3),
`boamp-raw-dlq` (1).

Airflow met 1 à 2 minutes à démarrer (installation de `confluent-kafka` au lancement). Identifiant `admin`,
mot de passe généré au premier démarrage :

```bash
docker exec airflow cat /opt/airflow/standalone_admin_password.txt
```

### Données historiques (facultatif)

Les fichiers historiques ne sont pas dans le dépôt (~739 Mo). Pour les ingérer, déposer les fichiers
`boamp_AAAA-MM_pNNN.jsonl` (une annonce JSON par ligne) dans `data/input/`.
Sans eux, le pipeline fonctionne avec l'API seule.

## Configuration

Fichier `.env` (jamais commité ; modèle : `.env.example`) :

| Variable | Rôle | Valeur par défaut |
|---|---|---|
| `KAFKA_BOOTSTRAP` | Adresse de Kafka depuis l'hôte (Airflow utilise `kafka:29092`, fixé dans `docker-compose.yml`) | `localhost:9092` |
| `BOAMP_MOIS_DEBUT` | Premier mois collecté par l'API (`AAAA-MM`) | `2026-11` |
| `BOAMP_API_KEY` | Clé d'API BOAMP, facultative (l'API est publique) | vide |

Réglages dans le code :

| Réglage | Fichier |
|---|---|
| Planification, retries, timeout, alerte d'échec | `airflow/dags/boamp_ingestion.py` |
| Contrat de données | `validation/schema_annonce.json`, `validation/contrat.py` |
| Seuils d'alerte (`SEUIL_REJETS`, `SEUIL_FRAICHEUR`) | `monitoring/observabilite.py` |

## Utilisation

### Avec Airflow (mode normal)

1. Ouvrir http://localhost:8080, activer le DAG `boamp_ingestion` (interrupteur).
2. Il tourne à 7 h et 19 h. Pour un lancement immédiat : **▶ Trigger DAG**.
3. Suivre l'exécution dans la vue **Grid** ; les logs de chaque tâche sont dans **Logs**.

### Sans Airflow (chaque brique à la main, depuis l'hôte)

```bash
python -m connectors.boamp_api              # API -> boamp-raw
python -m connectors.file_connector         # fichiers data/input/ -> boamp-raw
python -m consumers.validation_consumer     # boamp-raw -> boamp-validated / boamp-raw-dlq
python -m consumers.boamp_consumer          # boamp-validated -> data/output/annonces.jsonl
```

Les consumers s'arrêtent seuls après 10 s sans nouveau message.

### Résultats

| Où | Contenu |
|---|---|
| `data/output/annonces.jsonl` | Annonces validées, normalisées, sans doublon (une par ligne) |
| Kafka UI → `boamp-raw-dlq` | Messages rejetés avec `rejection_reason`, topic / partition / offset d'origine |
| Airflow → Logs | Logs JSON de chaque tâche (`publication_terminee`, `validation_terminee`, `controle_run`…) |
| `logs/alertes.jsonl` | Alertes (seuil dépassé, échec définitif d'une tâche) |

### Vérifier

```bash
pytest -q                                   # 12 tests
wc -l data/output/annonces.jsonl
python -c "import json; ids=[json.loads(l)['event_id'] for l in open('data/output/annonces.jsonl')]; print(len(ids), 'lignes', len(set(ids)), 'distinctes')"
```

Suivre une annonce par son identifiant (`correlation_id` = `event_id`) :

```bash
docker exec airflow grep -rh "boamp-26-114763" /opt/airflow/logs/dag_id=boamp_ingestion/ | grep -o '{"timestamp.*'
```

### Arrêter

```bash
docker compose stop      # les données, l'état et les offsets sont conservés
```

### Dépannage

| Problème | Solution |
|---|---|
| http://localhost:8080 ne répond pas | Attendre 2 min, puis `docker compose logs airflow --tail 30` |
| Tâche en erreur `Permission denied` sur `data/` ou `logs/` | Airflow écrit avec l'utilisateur 50000 : `chmod -R a+rwX data logs` (avec Podman rootless : `podman unshare chown -R 0:0 data/`) |
| Un consumer ne lit rien | Vérifier que les topics existent (`bash kafka/scripts/create_topics.sh`) et que Kafka est `Up` |

## Résultats (27/11/2026)

| Mesure | Valeur |
|---|---|
| Annonces historiques ingérées | 84 809 (923 fichiers, mars 2015 → novembre 2026) |
| Sortie | **85 109** annonces valides, 0 doublon |
| Rejets (DLQ) | 13 annonces : date limite de réponse antérieure à la parution |
| Taux de validation | 99,98 % sur l'historique |
| Run normal | ~1 min (339 annonces du mois en cours) |
| Rejeu complet de `boamp-raw` | ~12 min pour 87 062 messages |

Scénarios d'erreur testés (panne de Kafka, plantage en plein lot, runs simultanés, alerte) :
[docs/tests.md](docs/tests.md).

## Structure du projet

```
.
├── airflow/dags/boamp_ingestion.py     DAG d'orchestration
├── connectors/                         base.py (interface), boamp_api.py, file_connector.py
├── consumers/                          validation_consumer.py, boamp_consumer.py
├── validation/                         schema_annonce.json (contrat), contrat.py
├── monitoring/observabilite.py         logs JSON, seuils, alertes
├── kafka/scripts/create_topics.sh      création des topics
├── tests/                              tests pytest
├── docs/                               documentation par étape
├── data/                               input/ (historique), output/ (résultat), state/ (incrémental) — hors dépôt
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Documentation

| Document | Contenu |
|---|---|
| [docs/cadrage.md](docs/cadrage.md) | Besoin, sources, caractérisation V3, choix du pattern |
| [docs/kafka.md](docs/kafka.md) | Topics, partitions, clés, producer, consumer, rejeu |
| [docs/airflow.md](docs/airflow.md) | DAG, planification, résilience, idempotence |
| [docs/qualite.md](docs/qualite.md) | Profil des données, contrat, normalisation, DLQ |
| [docs/observabilite.md](docs/observabilite.md) | Logs JSON, métriques, seuils, corrélation |
| [docs/tests.md](docs/tests.md) | Tests, scénarios d'erreur, vérifications |

## Limites actuelles

- **Configuration de développement** : 1 seul broker Kafka (pas de réplication), Airflow en `SequentialExecutor`
  avec SQLite, `confluent-kafka` installé au démarrage du conteneur.
- **Stockage en JSON Lines** : simple et lisible, mais peu adapté à l'analyse sur de gros volumes.
- **Dédoublonnage en sortie** : le consumer charge tous les `event_id` déjà écrits en mémoire (suffisant à ~85 000 annonces).
- **Logs non centralisés** : ils restent dans Airflow ; les alertes vont dans un fichier, pas sur Slack ni par e-mail.
- **DLQ** : les annonces invalides du mois en cours, republiées par l'API à chaque run, y reviennent à chaque run.
- **API** : pagination limitée à 10 000 résultats par requête (suffisant : ~600 annonces IT par mois).
- **Contrôle des volumes** : il suppose qu'on rejoue un run complet ; relancer `consommer` seul le ferait échouer.

## Améliorations possibles

- Kafka à 3 brokers avec réplication 3 ; Airflow en `LocalExecutor` ou `CeleryExecutor` avec Postgres, image dédiée.
- Sortie **Parquet** partitionnée par mois, ou base de données avec contrainte d'unicité sur `event_id` (upsert).
- Centralisation des logs avec **Loki + Grafana** (recherche par `correlation_id`, tableaux de bord).
- Alertes Slack ou e-mail branchées dans `alerter()`.
- Mémoriser les `event_id` déjà rejetés pour ne pas les renvoyer en DLQ à chaque run.
- Mesurer le lag des consumer groups dans `controler_volumes`.
