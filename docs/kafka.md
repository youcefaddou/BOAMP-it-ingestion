# Étape 3 — Transport avec Kafka

## Topics

| Topic | Partitions | Clé | Rôle |
|---|---|---|---|
| `boamp-raw` | 3 | `event_id` (`boamp-<idweb>`) | Annonces brutes des 2 sources |
| `boamp-validated` | 3 | `event_id` | Annonces validées et normalisées (étape 5) |
| `boamp-raw-dlq` | 1 | `event_id` | Annonces rejetées + métadonnées d'erreur (étape 5) |

Nommage du cours : topic principal `<source>-raw`, DLQ `<source>-raw-dlq`.
Création : `kafka/scripts/create_topics.sh` (relançable grâce à `--if-not-exists`).

## Nombre de partitions

- **3 partitions** : jusqu'à 3 consumers en parallèle dans un même groupe. ~600 annonces/mois :
  1 consumer suffit, 3 laissent de la marge (plus de consumers que de partitions = consumers inactifs).
- **DLQ à 1 partition** : très faible volume, ordre total des rejets conservé.
- Réplication 1 : un seul broker en local (en production : 3 brokers minimum, réplication 3).

## Clé de message

`partition = hash(event_id) % 3`, avec `event_id` construit depuis `idweb` (identifiant unique BOAMP) :
- une même annonce va toujours dans la même partition, donc ses versions restent ordonnées ;
- toutes les clés sont différentes, donc **pas de partition chaude**
  (contre-exemple : une clé `nomacheteur` surchargerait la partition des gros acheteurs).

Répartition mesurée sur 85 088 messages : **28 300 / 28 470 / 28 318** (écart < 1 %).

## Producer (`connectors/base.py`)

| Paramètre | Valeur | Raison |
|---|---|---|
| `acks` | `all` | Durabilité maximale |
| `linger.ms` / `batch.size` | `5` / `16384` | Envoi groupé (valeurs du cours) |
| Sérialisation | JSON UTF-8 | Lisible, simple à déboguer |
| `on_delivery` | callback | Compte les échecs ; si échec, l'état incrémental n'est pas sauvé et une exception est levée (la tâche Airflow est relancée et republie) |

## Consumer (`consumers/boamp_consumer.py`)

- Groupe `boamp-stockage`, `auto.offset.reset=earliest`, sortie `data/output/annonces.jsonl`.
- `enable.auto.commit=False` : **écriture puis commit** = **at-least-once** (aucune perte, doublons possibles).
- Commit tous les 1000 messages (la sémantique reste : commit après écriture).
- Arrêt après 10 s sans message : le consumer est un lot lancé par Airflow (écart assumé au `while True` du cours).

## Vérifications

| Vérification | Résultat |
|---|---|
| Publication (`kafka-get-offsets`) | 85 088 messages (84 809 fichiers + 279 API) |
| Répartition | 28 300 / 28 470 / 28 318 |
| Consommation (`kafka-consumer-groups --describe`) | LAG = 0 sur les 3 partitions |
| 1er passage du consumer | 85 088 lus, 85 062 écrits, 26 doublons |

## Stratégie de reprise et risques de doublons

| Origine du doublon | Constaté | Parade |
|---|---|---|
| Chevauchement des sources : l'historique (collecté le 03/11/2026) contient 26 annonces de novembre aussi renvoyées par l'API | Oui : 26 doublons | Dédoublonnage par `event_id` contre la cible |
| Relance de l'API sur le mois en cours (jamais marqué terminé) | Oui : 279 lus, 0 écrits | Idem |
| Crash du consumer entre écriture et commit (at-least-once) | Théorique | Idem : messages relus mais pas réécrits |
| Échec de livraison côté producer | Théorique | État non sauvé, relance, puis dédoublonnage |
| Relance des fichiers historiques | Oui : 0 message republié | État `data/state/fichiers_publies.json` |

**Rejeu (rewind)** : `kafka-consumer-groups --reset-offsets --to-earliest --execute` puis relance du consumer :
tout est relu, **0 écrit**, la sortie reste à 85 062 lignes. Le traitement est **idempotent**.

## Écarts au cours (assumés)

| Cours | Projet | Justification |
|---|---|---|
| Boucle `while True` | Arrêt après 10 s sans message | Consumer lancé en lot par Airflow |
| Commit après chaque message | Commit tous les 1000, après écriture | Même sémantique at-least-once, plus rapide |
| `event_id` du type `evt-20241123-00042` | `boamp-<idweb>` | Identifiant stable, indispensable au dédoublonnage |
| 3+ brokers | 1 broker | Environnement local du TP |
