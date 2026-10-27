---
name: mover-pg-type
description: Checklist pour faire supporter un type Postgres (scalaire ou tableau) à mover de bout en bout - extract (convertValue), load (valuesToPairs / arrayToArg), clés BFS (keyToText) et tests. À utiliser quand un run échoue sur un type ("unsupported array data type", panic loukoum "cannot use ... as loukoum Expression") ou sort un JSON inattendu.
---
# Supporter un type Postgres

Un type traverse trois points, tous dans `dialect/postgres/util.go`. Les oublier un par un a déjà causé des pannes en prod, par exemple la panique au load sur `text[]` corrigée par e19a8ac.

| Étape | Fonction | Rôle |
|---|---|---|
| Extract | `convertValue(oid, value)` | Valeur pgx v5 vers une valeur sérialisable en JSON. Elle doit garder la représentation historique (pgx v4) si le type était déjà sorti. |
| Load | `valuesToPairs` puis `arrayToArg` / `buildArray` | Valeur JSON décodée (`float64`, `string`, `[]interface{}`, `map`) vers un argument accepté par loukoum et pgx, selon `normalizeDataType(column.DataType)`. |
| Clé BFS | `keyToText` | Seulement si le type peut être une PK ou une FK. Sans lui, `SelectIn` passe au repli `col = $1`, une requête par valeur. |

## Étapes
1. **Reproduire.** Regarde ce que pgx renvoie (type Go) et ce que contient le JSON produit. Un test dans `util_test.go` suffit souvent : `TestConvertValue*`, `TestValuesToPairs*`.
2. **Extract.** Ajoute la branche dans `convertValue`. Vérifie qu'un dossier extrait par l'ancienne version se recharge toujours : le format de sortie est un invariant.
3. **Load.** Pour un tableau, ajoute le type normalisé dans `arrayToArg`, avec la fonction de conversion par élément (`toString`, `toInt[T]`, `toFloat64`…). Un élément `null` doit devenir un élément NULL. Pour un scalaire non accepté par loukoum, ajoute une branche dans `valuesToPairs`.
4. **Clé.** Si c'est pertinent, ajoute le cas dans `keyToText` et dans `TestKeyToText`.
5. **Tests.**
   - Unitaires : `TestConvertValue*`, `TestValuesToPairsArrays`, `TestInsertQueryUnsupportedValue`.
   - Intégration : ajoute la colonne au fixture de `postgres_integration_test.go`, puis étends `TestBulkInsertArrays` ou `TestSelectIn` si le type sert de clé.
6. **Vérifier** avec le skill `mover-verify`, puis faire un aller-retour réel (`mover-roundtrip`) sur une table qui contient le type.

## Pièges
- Les nombres JSON arrivent en `float64`. Convertis-les explicitement vers le type entier de la colonne, et attention aux `bigint` au-delà de 2^53.
- `jsonb` : un tableau JSON doit être ré-encodé en JSON, pas converti en tableau Postgres.
- `[16]byte` (uuid) sans conversion donne un tableau de nombres en JSON.
