# Étape 3 — Transport avec Kafka

## Topics

| Topic | Partitions | Clé | Rôle |
|---|---|---|---|
| `boamp-raw` | 3 | `event_id` (`boamp-<idweb>`) | Annonces brutes des 2 sources |
| `boamp-validated` | 3 | `event_id` | Annonces validées et normalisées (étape 5) |
| `boamp-raw-dlq` | 1 | `event_id` | Annonces rejetées + métadonnées d'erreur (étape 5) |

Nommage du cours : topic principal `<source>-raw`, DLQ `<source>-raw-dlq`.

## Nombre de partitions

- **3 partitions** : permet jusqu'à 3 consumers en parallèle dans un même groupe.
  Avec ~600 annonces par mois, 1 consumer suffit aujourd'hui, 3 laissent de la marge
  (plus de consumers que de partitions = consumers inactifs, cf. cours).
- **DLQ à 1 partition** : très faible volume attendu, et on garde l'ordre des rejets.
- Facteur de réplication 1 : un seul broker en local (en production : 3 brokers, réplication 3).

## Clé de message

`partition = hash(event_id) % 3`. `event_id` est construit à partir de `idweb`,
l'identifiant unique BOAMP :
- une même annonce va **toujours dans la même partition**, donc ses versions restent ordonnées ;
- les clés sont toutes différentes, donc **pas de partition chaude**.

Répartition mesurée (85 088 messages) : 28 300 / 28 470 / 28 318.

## Producer

`acks=all` (durabilité maximale), `linger.ms=5` et `batch.size=16384` (envoi groupé).
Un callback `on_delivery` compte les échecs de livraison : s'il y en a, l'état
incrémental n'est pas sauvé et la relance republie le lot.

## Consumer et stratégie de reprise

- Groupe `boamp-stockage`, `auto.offset.reset=earliest`.
- **Commit manuel après écriture** (`enable.auto.commit=False`) : **at-least-once**.
  Si le consumer plante après avoir écrit mais avant de committer, il relira ces messages
  au redémarrage : aucune perte, mais des doublons possibles.
- Le consumer s'arrête après 10 s sans message (lot piloté par Airflow).

## Risques de doublons et parades

| Origine du doublon | Parade |
|---|---|
| Relance de l'API sur le mois en cours (jamais marqué terminé) | Dédoublonnage par `event_id` au consumer |
| Crash du consumer entre l'écriture et le commit (at-least-once) | Idem : `event_id` déjà présent dans la sortie, pas réécrit |
| Échec de livraison côté producer puis relance | État non sauvé, relance, puis dédoublonnage au consumer |
| Relance des fichiers historiques | État `fichiers_publies.json` : fichiers déjà publiés sautés |

Le traitement est donc **idempotent** : le relancer sur les mêmes données donne la même sortie.
Démonstration : relancer l'API puis le consumer donne 279 lus, 0 écrits, 279 doublons ignorés.
