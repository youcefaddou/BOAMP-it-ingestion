# Étape 5 — Qualité des données

## Profil des données (85 122 annonces, 27/11/2026)

Avant d'écrire le contrat, les annonces déjà collectées ont été profilées (types, valeurs manquantes, formats) :

| Constat | Volume | Traitement |
|---|---|---|
| Champs obligatoires toujours présents (`idweb`, `objet`, `nomacheteur`, `dateparution`, `url_avis`…) | 100 % | Contrôlés par le schéma |
| `datelimitereponse` absente | 52 943 (62 %) | Autorisé (`null`) : les résultats de marché n'ont pas de date limite |
| `titulaire` absent | 62 499 (73 %) | Autorisé : seuls les résultats de marché ont un titulaire |
| `type_marche` absent | 10 545 (12 %) | Autorisé |
| `code_departement` absent | 23 | Autorisé |
| Code département sur 1 chiffre (`"1"` au lieu de `"01"`) | 5 453 codes | **Normalisé** sur 2 chiffres |
| Corse codée `20A` / `20B` par le BOAMP (code INSEE : `2A` / `2B`) | 940 codes | Accepté par le contrat, **normalisé** en `2A` / `2B` |
| Espaces doubles dans `objet` / `nomacheteur` | 322 / 47 | **Normalisés** |
| `datelimitereponse` au format `+00:00` | 32 179 | **Normalisée** en ISO 8601 UTC (`Z`) |
| Avis de marché avec date limite **antérieure** à la parution | 13 | **Rejetés** en DLQ (incohérence) |

## Contrat de données (version 1.0)

Fichier : `validation/schema_annonce.json` (JSON Schema draft-07) + règles de cohérence dans `validation/contrat.py`.

### Enveloppe

| Champ | Type | Obligatoire | Contrainte |
|---|---|---|---|
| `source` | string | oui | `api-boamp` ou `boamp-fichiers` |
| `event_id` | string | oui | `boamp-<idweb>`, cohérent avec `payload.idweb` |
| `timestamp` | string | oui | date `AAAA-MM-JJ` |
| `ingested_at` | string | oui | date-heure ISO 8601 |
| `schema_version` | string | oui | `1.0` |
| `payload` | object | oui | voir ci-dessous |

### Payload

| Champ | Type | Obligatoire | Nullable | Contrainte |
|---|---|---|---|---|
| `idweb` | string | oui | non | `AA-NNNNN` (`^[0-9]{2}-[0-9]+$`) |
| `objet` | string | oui | non | non vide |
| `nomacheteur` | string | oui | non | non vide |
| `dateparution` | string | oui | non | date `AAAA-MM-JJ` |
| `datelimitereponse` | string | oui | oui | date-heure ISO 8601 ; si présente, pas antérieure à `dateparution` |
| `code_departement` | liste de strings | oui | oui | 1 à 3 chiffres, `2A`, `2B`, `20A`, `20B` |
| `nature_libelle` | string | oui | non | non vide |
| `famille_libelle` | string | oui | non | — |
| `type_marche` | liste | oui | oui | `SERVICES`, `FOURNITURES`, `TRAVAUX` |
| `procedure_libelle` | string | oui | non | — |
| `titulaire` | liste de strings | oui | oui | — |
| `descripteur_libelle` | liste de strings | oui | non | au moins 1 élément |
| `url_avis` | string | oui | non | commence par `https://www.boamp.fr/` |

« Obligatoire » = la clé doit exister ; « nullable » = sa valeur peut être `null`.

### Règles de cohérence (hors JSON Schema, qui ne compare pas deux champs)

1. `event_id` = `boamp-` + `payload.idweb` (clé de dédoublonnage fiable).
2. `datelimitereponse` (jour) >= `dateparution`.

### Normalisation (appliquée aux messages valides uniquement)

| Champ | Avant | Après |
|---|---|---|
| `objet`, `nomacheteur` | `"Ville de  Lyon "` | `"Ville de Lyon"` |
| `code_departement` | `["1", "20A"]` | `["01", "2A"]` |
| `datelimitereponse` | `"2026-12-01T12:00:00+00:00"` | `"2026-12-01T12:00:00Z"` |
| `timestamp` (enveloppe) | `"2026-11-27"` | `"2026-11-27T00:00:00Z"` |

### Validation du contrat sur l'historique

Le contrat a été testé sur les 85 122 annonces déjà collectées avant d'être branché sur Kafka :
**85 109 valides, 13 rejetées** (les 13 incohérences de dates ci-dessus).

Une première version n'acceptait que `2A` / `2B` pour la Corse et rejetait à tort **796 annonces**.
Le contrat doit décrire ce que la source envoie réellement : on accepte le format source, puis on normalise.

## Consumer de validation (`consumers/validation_consumer.py`)

```
boamp-raw ──► valider (contrat) ──┬─► boamp-validated ──► consommer ──► annonces.jsonl
                                  └─► boamp-raw-dlq (+ métadonnées d'erreur)
```

Pour chaque message de `boamp-raw` (consumer group `boamp_validation`) :

1. **Désérialiser et valider** : `valider()` (JSON Schema + règles de cohérence). Un message qui n'est pas du JSON
   est rejeté (`racine : JSON illisible`) au lieu de bloquer le consumer.
2. **Router** :
   - clé (`event_id`) déjà traitée pendant le lot → ignorée (dédoublonnage court terme, en mémoire) ;
   - invalide → `boamp-raw-dlq` ;
   - valide → normalisé (`normaliser()`) puis publié dans `boamp-validated`, avec la même clé.
3. **Commit** des offsets lus tous les 1000 messages, **après** confirmation des publications par Kafka (`flush()`).
   En cas d'erreur de livraison : pas de commit, exception, retry Airflow → aucun message perdu (at-least-once).
4. **Fin de lot** : taux de validation journalisé en INFO. L'alerte au-delà de 5 % de rejets est gérée avec les autres seuils (voir [observabilite.md](observabilite.md)).

Le dédoublonnage long terme reste côté stockage (`boamp_consumer`, vérification contre `annonces.jsonl`).

### Message en DLQ

Chaque rejet est publié avec ses métadonnées, pour pouvoir l'analyser, corriger, puis le rejouer :

| Champ | Exemple |
|---|---|
| `original_message` | le message brut, tel que lu dans `boamp-raw` |
| `rejection_reason` | `payload/datelimitereponse : antérieure à dateparution` |
| `rejection_timestamp` | `2026-11-27T13:28:33Z` |
| `source_topic` / `source_partition` / `source_offset` | `boamp-raw` / `1` / `25423` |
| `validator_version` | `1.0` (version du contrat) |
| `correlation_id` | `boamp-24-143435` (= `event_id` = clé Kafka) |

![Message rejeté dans boamp-raw-dlq (Kafka UI)](../../../captures/dlq_message.png)

## Intégration dans le DAG

```
publier_api ──────┐
                  ├──► valider ──► consommer ──► controler_volumes
publier_fichiers ─┘
```

- `consommer` lit désormais `boamp-validated` (seule la constante `TOPIC` change).
- `controler_volumes` vérifie chaque étape : `lus (validation) >= publiés`, puis `lus (stockage) >= valides`,
  et l'absence de doublons dans la sortie.

## Rejeu complet de `boamp-raw` (27/11/2026)

La sortie des étapes 3 et 4 n'était pas normalisée. Elle a été reconstruite à partir du journal brut :

1. `annonces.jsonl` mis de côté (`annonces_avant_etape5.jsonl`, hors dépôt).
2. Déclenchement du DAG. Le groupe `boamp_validation` est nouveau : il relit `boamp-raw` depuis le début
   (`auto.offset.reset = earliest`). `boamp-stockage` lit `boamp-validated` depuis le début.

Aucune manipulation d'offset n'a été nécessaire.

### Résultats

| Mesure | Valeur |
|---|---|
| Messages dans `boamp-raw` | 87 062 (86 723 + 339 publiés par le run) |
| Lignes dans `annonces.jsonl` | **85 109** (= annonces valides attendues), 0 doublon |
| `boamp-validated` | 86 175 messages, 85 109 `event_id` distincts |
| `boamp-raw-dlq` | 21 messages, **13** annonces distinctes (toutes : date limite antérieure à la parution) |
| Corse | 547 annonces en `2A`, 0 en `20A` |

### Reprise après plantage (incident réel)

Deux erreurs de saisie dans le code ont fait échouer la tâche `valider` pendant le rejeu :

| Essai | Résultat |
|---|---|
| 1-2 | `NameError` avant la connexion à Kafka : rien lu |
| 3-4 | Validation de ~74 000 messages (commits tous les 1000), puis `NameError` au premier message invalide |
| 5 (corrigé) | Reprise au dernier offset committé : **13 062** messages lus seulement, 11 109 valides, 21 rejetés |

![Retries de la tâche valider](../../../captures/airflow_valider.png)

Les messages republiés entre le dernier commit et le plantage ont produit **1 066 doublons** dans
`boamp-validated`, tous écartés par `consommer` (86 175 lus, 85 109 écrits). Résultat : **aucune annonce
perdue, aucun doublon dans la sortie**. C'est la garantie at-least-once + idempotence.

### Défaut détecté et corrigé : doublons dans la DLQ

21 rejets pour 13 annonces : certaines annonces invalides sont présentes plusieurs fois dans `boamp-raw`
(republications), et le dédoublonnage en mémoire ne s'appliquait qu'aux messages valides. Effet : DLQ gonflée
et taux de rejet surestimé (fausse l'alerte à 5 %).

Correction : dédoublonnage sur la clé Kafka **avant** le routage, pour les valides comme pour les rejets.
Les 21 messages déjà en DLQ sont conservés (un topic Kafka est un journal, on ne le réécrit pas).

## Consigne → réalisation

| Consigne (étape 5) | Où |
|---|---|
| Contrat de données (champs, types, contraintes) | `validation/schema_annonce.json`, tableaux ci-dessus |
| Validation JSON Schema | `validation/contrat.py` (`valider`) |
| Normaliser | `normaliser()` : dates ISO 8601 UTC, espaces, codes département |
| Dédupliquer | en mémoire (validation) + contre la cible (stockage) |
| Séparer valides / invalides | `boamp-validated` / `boamp-raw-dlq` |
| Invalides en DLQ avec métadonnées d'erreur | `message_dlq()`, 8 champs |
| Tests | `tests/test_validation.py` (6 tests) |

## Choix assumés

- **Normalisation dans le consumer de validation, pas dans `to_event()`** : `boamp-raw` garde le message tel
  que la source l'a envoyé. Un bug de normalisation se corrige puis se rejoue depuis `boamp-raw`.
- **`Draft7Validator` construit une seule fois** au lieu de `jsonschema.validate()` à chaque message
  (même `ValidationError`) : 10 s au lieu de 109 s pour 85 000 annonces.
- **`pattern` au lieu de `"format": "date-time"`** : sans librairie supplémentaire, `format` n'est pas vérifié.
- **Première erreur seulement** dans `rejection_reason` (comportement de `validate`), suffisant pour l'analyse.
- **Alerte à 5 %** : d'abord un simple `WARNING` dans le consumer, déplacée à l'étape 6 dans les seuils d'alerte (`verifier_seuils`, voir [observabilite.md](observabilite.md)).
- Nom du consumer group `boamp_validation` (underscore) : conservé pour ne pas relancer un rejeu complet.
