# Étape 1 — Cadrage du besoin

## Contexte métier et question analytique

L'État et les collectivités publient leurs appels d'offres et leurs résultats de marché
au BOAMP (Bulletin officiel des annonces des marchés publics).
On s'intéresse aux marchés **informatiques et télécoms**.

**Question analytique :** qui achète de l'informatique dans le secteur public, quoi,
avec quel délai pour répondre, et quelles entreprises remportent les marchés ?

## Sources

| | Source 1 : API BOAMP | Source 2 : fichiers historiques |
|---|---|---|
| Format | JSON (API REST Opendatasoft, endpoint `records`) | JSON Lines (1 annonce par ligne) |
| Accès | HTTP GET public, **sans authentification** | Fichiers locaux `data/input/` |
| Fréquence | Publication 2 fois par jour, 7 j/7 | Fixe (collecte faite du 2 mars 2015 au 3 nov. 2026) |
| Volume | ~600 annonces IT par mois | 84 809 annonces, 923 fichiers, ~739 Mo |
| Limites | 100 résultats max par page, offset + limit <= 10 000, risque de 429 | Champs en listes, 2 formats du champ `donnees` (BOAMP puis eForms en 2024) |
| Licence | Licence Ouverte Etalab | idem |

Données personnelles : le champ `donnees` contient des téléphones et des e-mails de contact.
Il n'est **pas** gardé dans les messages (minimisation dès le connecteur).

## Caractérisation V3

| Dimension | Niveau | Justification |
|---|---|---|
| Volume | Faible à moyen | ~85 000 annonces au total, ~600 par mois |
| Vélocité | Faible | Publication 2 fois par jour, pas de besoin temps réel |
| Variété | Moyenne | JSON semi-structuré, champs en listes, 2 schémas selon l'année |

## Choix du pattern d'ingestion

Arbre de décision du cours : la latence tolérée est de plusieurs heures (une analyse des
marchés n'a pas besoin de la seconde), donc **Pull (batch)**.
Airflow lance la collecte 2 fois par jour, au rythme de publication du BOAMP.

On place quand même **Kafka** entre la collecte et le traitement :
- **découplage** : les connecteurs publient sans connaître les traitements ;
- **rejeu** : si la validation a un bug, on la corrige et on relit le topic (offsets) ;
- **DLQ** : les annonces invalides sont isolées dans un topic dédié ;
- **deux sources dans un même topic** `boamp-raw`, avec la même enveloppe d'événement.

Limite assumée : pour ce volume, Kafka n'est pas indispensable du point de vue de la latence
(règle pratique du cours : commencer par le plus simple). Il est choisi pour le rejeu et la DLQ.

## Schéma d'architecture

```
 API BOAMP ──► connectors/boamp_api.py ──┐
 (REST, Pull)                            │  enveloppe d'événement
                                         ├──► topic boamp-raw (3 partitions, clé = event_id)
 data/input/*.jsonl ─► connectors/       │
 (historique)          file_connector.py ┘
                                                 │
                                                 ▼
                                   consumer de validation (JSON Schema)
                                      │                        │
                                 valides                   invalides
                                      ▼                        ▼
                          topic boamp-validated        topic boamp-raw-dlq
                                      │
                                      ▼
                          data/output/ (JSON Lines)

 Airflow (DAG, 2 fois par jour) orchestre : collecte ─► validation ─► contrôle des volumes
 Logs JSON + correlation_id (= event_id) à chaque étape
```
