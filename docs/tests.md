# Étape 7 — Tests et vérifications

Synthèse des tests du pipeline : tests automatisés, exécutions de bout en bout, scénarios d'erreur avec reprise,
vérification des données produites et des messages rejetés. Les détails sont dans les docs de chaque étape.

## Tests automatisés

```bash
pytest -q        # 12 passed
```

| Fichier | Tests | Ce qui est vérifié |
|---|---|---|
| `tests/test_connectors.py` | 2 | Enveloppe d'événement (`event_id`, `timestamp`, champs de contact retirés), calcul des mois de l'incrémental |
| `tests/test_validation.py` | 6 | Annonce conforme, champ obligatoire absent, valeur interdite, les 2 règles de cohérence, normalisation (sans modifier l'original) |
| `tests/test_monitoring.py` | 4 | Format des logs JSON, run normal sans alerte, 3 seuils dépassés, écriture dans le journal des alertes |

Ces tests ne demandent ni Kafka ni Airflow : la logique (contrat, seuils, enveloppe) est séparée du transport.
Le reste est vérifié par des exécutions réelles du DAG (ci-dessous).

## Exécutions de bout en bout

DAG `boamp_ingestion` : `[publier_api, publier_fichiers] >> valider >> consommer >> controler_volumes`.

| Date | Run | Scénario | Résultat |
|---|---|---|---|
| 27/11 | `scheduled__2026-11-26T19:00` | Activation du DAG | ✅ 339 publiés, 60 nouvelles annonces écrites |
| 27/11 | `manual__…13:19:18` | Rejeu complet de `boamp-raw` (87 062 messages) avec la validation | ✅ 85 109 annonces écrites, 21 messages en DLQ (13 annonces) |
| 27/11 | `manual__…14:47:25` | Run normal avec logs JSON | ✅ 339 publiés, 337 valides, 2 rejetés, 0 écrit (déjà présentes) |
| 27/11 | `manual__…14:56:27` | Test d'alerte (seuil de fraîcheur abaissé) | ✅ alerte écrite, run en succès |

## Scénarios d'erreur et reprise

### 1. Kafka indisponible pendant un run (étape 4)

| Étape | Observation |
|---|---|
| `docker compose stop kafka`, puis déclenchement du DAG | `publier_api` en cours, le producer garde les messages |
| Après 5 min (`message.timeout.ms`) | 339 `Local: Message timed out`, puis `RuntimeError: 339 erreurs de livraison : état non sauvé` → `up_for_retry` |
| `docker compose start kafka` | Essai 2 de `publier_api` : 339 messages publiés, run en succès |
| Contrôle | `consommer` : 339 lus, 0 écrit, 339 doublons ignorés. Aucune perte, aucun doublon |

Mécanismes en jeu : `acks=all`, exception si une livraison échoue (`connectors/base.py`), état incrémental non
sauvé, retries avec backoff exponentiel (`airflow/dags/boamp_ingestion.py`), dédoublonnage par `event_id`.

![Reprise après redémarrage de Kafka](../../../captures/kafka_restart.png)

### 2. Plantage en plein traitement d'un gros lot (étape 5)

Pendant le rejeu de 87 062 messages, deux erreurs de code ont fait échouer la tâche `valider` :

| Essai | Observation |
|---|---|
| 1-2 | Échec avant la connexion à Kafka : rien n'est lu |
| 3-4 | ~74 000 messages validés (commit tous les 1000), puis échec au premier message invalide |
| 5 (corrigé) | Reprise au dernier offset committé : **13 062** messages relus seulement |

1 066 messages publiés deux fois dans `boamp-validated` (entre le dernier commit et le plantage) ont été écartés
par le consumer de stockage. Résultat : 85 109 annonces, aucune perdue, aucun doublon (at-least-once + idempotence).

![Retries de la tâche valider](../../../captures/airflow_valider.png)

### 3. Deux runs simultanés (étape 4)

Deux runs lancés en même temps partageaient le consumer group : le 1er a lu les messages des deux (678), le 2e n'a
rien lu. `controler_volumes` a détecté l'écart (`339 messages publiés mais seulement 0 lus`) et fait échouer le run.
Correction : `max_active_runs=1`, puis *Clear* du run : 339 lus, 0 doublon.

### 4. Déclenchement d'une alerte (étape 6)

Seuil de fraîcheur abaissé à `-1` : alerte `fraîcheur : dernière annonce publiée il y a 0 jours` écrite en ERROR et
dans `logs/alertes.jsonl`, run en succès. Seuil remis à 2 jours. Le callback d'échec définitif
(`on_failure_callback`) a été testé dans le conteneur Airflow avec un échec simulé.

### 5. Idempotence : rejouer un run (étapes 3 et 4)

- Rewind du consumer group (`--reset-offsets --to-earliest`) : 85 367 messages relus, 0 écrit.
- *Clear* d'un run terminé : 339 lus, 0 écrit.

## Vérification des données produites (27/11/2026)

```bash
wc -l data/output/annonces.jsonl
python -c "import json; ids=[json.loads(l)['event_id'] for l in open('data/output/annonces.jsonl')]; print(len(ids), 'lignes', len(set(ids)), 'distinctes')"
grep -c '"20A"' data/output/annonces.jsonl
grep -c '"2A"' data/output/annonces.jsonl
```

| Vérification | Résultat | Attendu |
|---|---|---|
| Lignes dans la sortie | 85 109 | = annonces valides du profilage |
| `event_id` distincts | 85 109 | 0 doublon |
| Codes Corse `20A` / `2A` | 0 / 547 | normalisation appliquée |
| Contrat sur l'historique | 85 109 valides, 13 rejetées | 13 incohérences de dates connues |

## Vérification des messages rejetés

| Vérification | Résultat |
|---|---|
| `boamp-raw-dlq` après le rejeu | 21 messages, 13 annonces distinctes (republications, corrigé ensuite) |
| Motif | toujours `payload/datelimitereponse : antérieure à dateparution` |
| Métadonnées | les 8 champs présents (message original, motif, date, topic / partition / offset d'origine, version du contrat, `correlation_id`) |
| Absence en sortie | `grep -c "boamp-26-114763" data/output/annonces.jsonl` → 0 |
| Traçabilité | `correlation_id` retrouvé dans les logs `message_rejete` (avec `run_id`, partition, offset) et dans Kafka UI |

![Message rejeté dans la DLQ](../../../captures/dlq_message.png)

## Couverture de la consigne

| Consigne (étape 7) | Où |
|---|---|
| Exécuter le pipeline de bout en bout | Exécutions ci-dessus |
| Tester au moins un scénario d'erreur et sa reprise | Scénarios 1 et 2 (+ 3, 4, 5) |
| Vérifier les données produites et les messages rejetés | Sections « Vérification » |
| Documenter installation, configuration, utilisation | `README.md` |
