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
