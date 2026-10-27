# mover

ETL Postgres : `extract` part d'une requête racine, suit les FK en largeur (BFS), anonymise et écrit un JSON par table. `load` recharge ce dossier dans une autre base. Il est utilisé par `ulule-fixtures` pour produire les fixtures à partir de la prod.

Fonctionnement détaillé : [docs/how-it-works.md](docs/how-it-works.md). Historique perf et décisions : [docs/perf-spec.md](docs/perf-spec.md).

## Carte du code

| Chemin | Rôle |
|---|---|
| `cmd/mover/main.go` | Flags, `MOVER_CONF`, dispatch `extract` / `load` / `describe`. |
| `config/config.go` | Format de la config JSON (`schema`, `extra`, `columns`…). |
| `etl/engine.go` | Orchestration : introspection, racine, `extra`, écriture, médias. |
| `etl/extractor.go` | Parcours BFS par niveaux (`visit`, `request`, `fetchLevel`). |
| `etl/sanitizer.go` | Anonymisation (`replace` > `fake` > `sanitize`, `unique`). |
| `etl/loader.go` | Lecture des `*.json` et appel de `BulkInsert`. |
| `etl/util.go` | Table de la requête racine (regex), médias (`downloadFiles`). |
| `dialect/dialect.go` | Interface `Dialect` et types neutres. |
| `dialect/postgres/postgres.go` | `Tables()` (4 requêtes catalogue), `SelectIn` (`= ANY`), `BulkInsert` (`pgx.Batch`). |
| `dialect/postgres/util.go` | Conversions de types : `convertValue` (Postgres vers JSON), `valuesToPairs` / `arrayToArg` (JSON vers Postgres), `keyToText`. |

## Commandes

```bash
go build ./... && go vet ./...
make test                      # tests unitaires, sans base
make lint                      # golangci-lint v2 (.golangci.yml)
MOVER_TEST_DSN=postgres://user@localhost:5432/db MOVER_TEST_FIXTURE=1 \
  go test -tags integration ./dialect/postgres/ -v   # tests d'intégration, base requise
```

La CI (`.github/workflows`) lance build, `go test ./...` et golangci-lint. Les tests d'intégration ne tournent pas en CI : il faut les lancer à la main dès qu'on touche `dialect/postgres`.

## Invariants à ne pas casser

- **Le format de sortie est stable.** Il prend la forme `{"table_name", "count", "data"}`, avec un fichier `<table>.json` par table. `convertValue` conserve la représentation JSON de pgx v4. Un dossier extrait par une ancienne version doit se recharger, et inversement.
- **Une seule `pgx.Conn`, pas de concurrence SQL.** Seuls les téléchargements de médias sont parallèles.
- **Le parcours est déterministe.** Les clés sont triées dans `fetchLevel`, et les tests d'extracteur en dépendent.
- **Les règles de racine s'appliquent à toutes les lignes racines.** Le sur-ensemble par rapport à l'ancien DFS est voulu.
- **Les clés sont envoyées en `= ANY($1::text[]::<type>[])` par paquets de 1000.** Tout nouveau type de clé passe par `keyToText`, sinon c'est le repli `col = $1`, une requête par valeur.
- **Le load est idempotent** (`ON CONFLICT (pk) DO NOTHING`) et indépendant de l'ordre des tables (`DISABLE TRIGGER ALL`).

## Pièges connus

- La CLI renvoie toujours le code de sortie 0. Une config illisible est seulement loguée. Il faut lire les logs.
- Seul le schéma `public` est lu. La PK est obligatoire et seule sa première colonne est utilisée. Pour une FK multi-colonnes, seule la première colonne compte.
- Les reference keys (relations vers les enfants) ne sont suivies automatiquement que depuis la racine. Ailleurs, il faut les déclarer par nom de contrainte dans `schema[].reference_keys`.
- Au load, le JSON est décodé sans `UseNumber` : un `bigint` au-delà de 2^53 perd sa précision.
- Une valeur Go non gérée par loukoum provoque une panique dans `insertQuery`, rattrapée en erreur. Un nouveau type Postgres se gère donc dans `convertValue` **et** dans `valuesToPairs` (voir le skill `mover-pg-type`).

## Consommateur : ulule-fixtures

`ulule-fixtures` épingle un sha de mover à **deux** endroits, qu'il faut toujours bumper ensemble :
- `Dockerfile` (`ARG APP_REVISION`) ;
- `.github/workflows/fixtures.yml` (`go install …/cmd/mover@<sha>`).

La config de prod est `ulule.mover.json`, et le lancement se fait via `bin/extract`. Voir le skill `mover-bump-fixtures`.

## Agents et skills du repo

- Agents (Sonnet) dans `.claude/agents/` :
  - `mover-implementer` : implémente une spec ;
  - `mover-config-auditor` : audite une config par rapport au schéma ;
  - `mover-run-analyzer` : analyse les logs d'un run.
- Skills dans `.claude/skills/` :
  - `mover-verify` : vérifications avant commit ;
  - `mover-pg-type` : ajouter un type Postgres ;
  - `mover-roundtrip` : extract puis load en local ;
  - `mover-bump-fixtures` : bumper le sha dans ulule-fixtures.

## Conventions

- Les commits suivent Conventional Commits avec un scope de paquet : `perf(etl): …`, `fix(postgres): …`, `docs: …`. Un commit par changement logique.
- Le code et les commentaires sont en anglais, la doc de `docs/` est en français.
- Écrire du code qui ressemble au code autour.
- Les tests unitaires de l'extracteur utilisent un faux dialecte en mémoire (`etl/extractor_test.go`) : y ajouter un cas plutôt que de passer par une base.
