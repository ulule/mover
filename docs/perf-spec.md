# Spec — correctifs de performance prioritaires de mover

Statut : à implémenter. Aucun code n'a été modifié.
Contexte : audit du 2026-10-26, mesures prises sur ulule-prod en lisant uniquement les catalogues et les statistiques.
- 119 tables et 226 FK dans `public`.
- 80 FK sans index côté enfant, dont 10 sur des tables de plus de 1M lignes (`shop_order` : 9,9M lignes, 11 GB).

Contraintes communes :
- Une seule `pgx.Conn` (pgx v4), pas de pool. Rien ici n'introduit de concurrence SQL.
- Go 1.21 dans `go.mod`, donc `errors.Join` est disponible.
- Le format de sortie (`jsonPayload`, un fichier par table) **ne change pas**. Un dossier extrait avant les correctifs doit toujours pouvoir être chargé, et inversement.
- Un commit, et idéalement une PR, par correctif, dans l'ordre ci-dessous (du plus simple au plus risqué).
- Vérifications à chaque étape : `go build ./...`, `go vet ./...`, `make test`, `make lint`.

---

## 1. Téléchargements de médias : vrai parallélisme et timeout

**Fichiers :** `etl/util.go` (`downloadFiles`, `downloadFile`, `chunkStrings`) et l'appel dans `etl/engine.go:172`.

**Problème :**
- `g.Wait()` est appelé dans la boucle interne, juste après chaque `g.Go`, donc un seul téléchargement tourne à la fois.
- `http.Get` utilise le client par défaut, sans timeout et sans contexte.
- La première erreur, un simple 404 par exemple, arrête tous les téléchargements restants.

**Changement :**
1. Monter `golang.org/x/sync` à une version qui fournit `errgroup.Group.SetLimit` (≥ v0.1.0, par exemple v0.7.0), puis lancer `go mod tidy`.
2. Réécrire `downloadFiles(ctx, filenames, outputPath, concurrency int) error` :
   - utiliser un `errgroup.Group` **sans** `WithContext`, pour qu'un échec n'annule pas les autres, avec `g.SetLimit(concurrency)`. Si `concurrency <= 0`, prendre 10 par défaut ;
   - lancer **un** `g.Go` par fichier, sans découpage en paquets. Supprimer `chunkStrings` s'il n'a plus d'appelant ;
   - dans chaque goroutine, en cas d'erreur, l'ajouter à un `[]error` protégé par un `sync.Mutex` et renvoyer `nil` ;
   - après `g.Wait()`, renvoyer `errors.Join(errs...)`.
3. `downloadFile(ctx, client *http.Client, absoluteURL, outputDir string) error` :
   - construire la requête avec `http.NewRequestWithContext(ctx, http.MethodGet, …)` ;
   - utiliser un client défini au niveau du package, `var httpClient = &http.Client{Timeout: 60 * time.Second}`, passé en paramètre ;
   - garder tel quel le reste du corps (vérification du code 200, `MkdirAll`, `Create`, `io.Copy`).
4. `engine.go:172` reste un log d'erreur non bloquant : on ne change pas ce comportement.
5. Dédupliquer `filenames` avant de télécharger. Deux lignes peuvent pointer vers le même média, et deux goroutines qui écrivent le même fichier se marchent dessus.

**Tests :** `etl/util_test.go`, avec un `httptest.Server` qui sert 20 fichiers dont un en 404 et ajoute un délai de 100 ms par requête.
- Les 19 autres fichiers doivent être écrits.
- L'erreur renvoyée doit mentionner le 404.
- Avec une concurrence de 10, la durée totale doit rester bien en dessous de 20 × 100 ms ; comme seuil large, < 1 s.

---

## 2. Introspection : trois requêtes globales au lieu de six par table

**Fichier :** `dialect/postgres/postgres.go` (`Tables`, `Columns`).

**Problème :** `Tables()` appelle `ReferenceKeys`, `ForeignKeys` et `PrimaryKeys` pour chaque table, et chacune commence par un `getTableOID`. Ça fait environ 714 allers-retours séquentiels pour 119 tables, à chaque `extract` **et** à chaque `load`. Par ailleurs, `Columns(ctx, "")` ne filtre ni le schéma ni le type de relation : il lit 5 128 lignes de `pg_attribute` au lieu de 1 371.

**Changement :** réécrire seulement `Tables()`. On garde **sans les modifier** les méthodes `Table`, `ReferenceKeys`, `ForeignKeys` et `PrimaryKeys`, qui sont dans l'interface `Dialect`. Elles serviront de référence (d'oracle) dans le test d'équivalence.

1. **Colonnes.** Dans `Columns`, quand `tableName == ""`, ajouter une jointure sur `pg_namespace` avec les conditions `n.nspname = 'public'` et `c.relkind = 'r'`. Supprimer dans tous les cas le `LEFT JOIN pg_description`, dont le résultat n'est jamais lu.
2. **Une seule requête sur les contraintes FK**, qui alimente à la fois les FK et les reference keys :
   ```sql
   SELECT r.conname,
          child.relname  AS child_table,
          parent.relname AS parent_table,
          pg_catalog.pg_get_constraintdef(r.oid, true) AS condef,
          (SELECT a.attname FROM pg_attribute a
            WHERE a.attrelid = r.conrelid AND a.attnum = r.conkey[1]) AS child_column
   FROM pg_catalog.pg_constraint r
   JOIN pg_catalog.pg_class child      ON child.oid = r.conrelid
   JOIN pg_catalog.pg_namespace cn     ON cn.oid = child.relnamespace
   JOIN pg_catalog.pg_class parent     ON parent.oid = r.confrelid
   JOIN pg_catalog.pg_namespace pn     ON pn.oid = parent.relnamespace
   WHERE r.contype = 'f' AND cn.nspname = 'public' AND pn.nspname = 'public'
   ORDER BY child.relname, r.conname
   ```
   - **ForeignKey** de `child_table` : `Name = conname`, `Definition = condef`. Les colonnes viennent toujours de `fkRegexp` sur `condef`, comme aujourd'hui, pour rester strictement identique.
   - **ReferenceKey** de `parent_table` : `Name = conname`, `TableName = child_table`, `ColumnName = child_column`.
   - Garder l'ordre par `conname` à l'intérieur d'une table, comme le fait le `ORDER BY 1` actuel de `ForeignKeys`.
   - Changement de comportement à documenter dans le commit : les FK dont l'une des deux tables est hors de `public` sont ignorées. Elles produisaient déjà des `Table{}` vides via `tablesMap`.
   - Les FK multi-colonnes faisaient échouer l'ancienne requête des reference keys, dont la sous-requête renvoyait plusieurs lignes. Désormais, seule la première colonne est prise : le comportement est le même, sans le crash.
3. **Une seule requête pour les PK :**
   ```sql
   SELECT c.relname AS table_name, a.attname AS name,
          format_type(a.atttypid, a.atttypmod) AS data_type
   FROM pg_index i
   JOIN pg_class c      ON c.oid = i.indrelid
   JOIN pg_namespace n  ON n.oid = c.relnamespace
   JOIN pg_attribute a  ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
   WHERE i.indisprimary AND n.nspname = 'public'
   ORDER BY c.relname, array_position(i.indkey::int2[], a.attnum)
   ```
   L'ordre suit celui de l'index, pour que `PrimaryKeys[0]` soit déterministe sur les PK composites.
4. Assembler le résultat avec des `map[string]…` indexées par nom de table, puis faire la résolution `ReferenceKeys[j].Table` / `ForeignKeys[j].ReferencedTable` via `tablesMap`, comme aujourd'hui. Les requêtes passent par `execQuery` (pgxscan), comme le reste du fichier. On obtient au total 4 requêtes au lieu d'environ 714.

**Test d'équivalence (obligatoire) :** fichier `dialect/postgres/postgres_integration_test.go`, avec le build tag `integration`, qui ne s'exécute que si `MOVER_TEST_DSN` est défini.
- Pour chaque table renvoyée par le nouveau `Tables()`, comparer avec `Table(ctx, name)`, qui suit l'ancien chemin table par table : mêmes colonnes, mêmes PK, mêmes FK (`Name`, `ColumnName`, `ReferencedTableName`, `ReferencedColumnName`) et mêmes reference keys (`Name`, `TableName`, `ColumnName`).
- Comparer sans tenir compte de l'ordre pour les reference keys, et en ignorant les champs `Table` / `ReferencedTable` imbriqués.
- Le lancer sur la base locale ulule : `MOVER_TEST_DSN=… go test -tags integration ./dialect/postgres/ -run Equivalence -v`.
- Ajouter dans le test un compteur de requêtes (un wrapper ou le tracer pgx n'est pas nécessaire : un log suffit) et vérifier qu'il vaut 4.

---

## 3. Load : insertion par lots avec `pgx.Batch`

**Fichier :** `dialect/postgres/postgres.go` (`BulkInsert`, `insert`, `exec`).

**Problème :** un `INSERT … ON CONFLICT DO NOTHING` par ligne, donc un aller-retour par ligne.

**Choix :** utiliser `pgx.Batch` avec **exactement** les mêmes requêtes qu'aujourd'hui, générées par loukoum et `valuesToPairs`. On écarte `COPY` : il ne gère pas `ON CONFLICT`, et il faudrait une table temporaire plus une conversion de types différente de `valuesToPairs`. Le gain serait marginal par rapport à `pgx.Batch` pour les volumes de mover (des milliers de lignes, pas des millions), et le risque bien plus élevé.

**Changement :**
1. Extraire de `insert` une fonction `insertQuery(table, data) (string, []interface{}, error)`, qui construit la requête sans l'exécuter.
2. Dans `BulkInsert`, à l'intérieur de `disableTriggers` :
   - parcourir `data` par paquets de `insertBatchSize = 500` (constante du package) ;
   - pour chaque paquet, créer `b := &pgx.Batch{}`, ajouter chaque requête avec `b.Queue(query, args...)`, puis `br := tx.SendBatch(ctx, b)` ;
   - appeler `br.Exec()` autant de fois qu'il y a de requêtes dans le paquet, en enveloppant chaque erreur avec la table et l'indice de la ligne ;
   - appeler ensuite `br.Close()` dans tous les cas, et vérifier son erreur.
3. Exécuter `ALTER TABLE … DISABLE/ENABLE TRIGGER` via `tx.Exec` plutôt que `d.conn.Exec`. Le résultat est le même aujourd'hui, puisque c'est la même connexion, mais le code devient explicite. `disableTriggers` reçoit donc le `pgx.Tx`.
4. Corriger le `defer` du rollback : `defer func() { _ = tx.Rollback(ctx) }()`. Aujourd'hui, il réaffecte une variable `err` locale sans aucun effet.
5. Ne pas toucher à `resetSequence` ni à la logique de `valuesToPairs`.

**Vérification :**
- Charger un même dossier de sortie avec l'ancien binaire dans une base A et avec le nouveau dans une base B, puis comparer le nombre de lignes et les PK par table.
- Tester un second `load` sur la même base : il doit être idempotent grâce à `ON CONFLICT DO NOTHING`.
- Mesurer le temps total avant et après, et l'indiquer dans la PR.

---

## 4. Extraction : parcours par niveaux (BFS) et requêtes `= ANY($1)`

**Fichiers :** `etl/extractor.go` (réécriture), `etl/engine.go` (`Extract`), `dialect/dialect.go` et `dialect/postgres/postgres.go` (nouvelle méthode).

**Problème :** pour chaque ligne, le code lance un `SELECT * FROM t WHERE col = $1` par FK et par reference key, de façon récursive, en profondeur d'abord et en séquentiel. Sur les 80 FK sans index côté enfant, chacune de ces requêtes est un seq scan, répété pour chaque ligne racine.

### 4.1 Comportement actuel à préserver (référence)
- **Requête racine** (et chaque table `Extra`) : `root = true`.
- **Pour chaque ligne :**
  - si la clé `"<table>(<pk>) = <valeur>"` est déjà dans `processedRelations`, on l'ignore ; sinon on la marque.
  - **FK** : pour chaque FK dont la valeur `v` n'est pas nil et dont la clé `"<refTable>(<refCol>) = v"` n'est pas déjà traitée, on récupère les lignes de `refTable` où `refCol = v`, avec `root = false`.
  - **Reference keys** :
    - si `root && !schema.OmitReferenceKeys`, on prend **toutes** les `table.ReferenceKeys` ;
    - on ajoute **à chaque niveau** celles dont le `Name` (le `conname`) figure dans `schema.ReferenceKeys` (config) ;
    - pour chacune, on récupère les lignes de `rk.Table` où `rk.ColumnName = row[pk]`, avec `root = false`.
  - **Queries (config)** : pour chaque `schema.Queries[k]`, on exécute `replaceVar(query, row)` telle quelle, avec `root = false`.
- Le `depth` actuel ne sert qu'à décider de `root` (`depth == 0`) et à l'indentation des logs. Les appels récursifs se font avec `depth+1` ou `depth+2`, donc seule la racine a `depth == 0`.

### 4.2 Nouvelle méthode de dialecte
```go
// dialect.Dialect
SelectIn(ctx context.Context, table Table, column string, values []interface{}) ([]map[string]interface{}, error)
```
Implémentation Postgres :
- Récupérer `dataType := table.Columns.Get(column).DataType`, c'est-à-dire la sortie de `format_type`, par exemple `integer`, `bigint`, `uuid` ou `character varying(255)`.
- Convertir chaque valeur en texte avec un helper `keyToText(v) (string, bool)` :
  - entiers signés ou non : `strconv` ;
  - `string` : telle quelle ;
  - `[16]byte` (uuid renvoyé par pgx v4) : format canonique 8-4-4-4-12 ;
  - `pgtype.UUID` s'il apparaît ;
  - tout autre type renvoie `false`.
- Si toutes les valeurs sont convertibles, exécuter `SELECT * FROM <table> WHERE <column> = ANY($1::text[]::<dataType>[])` avec un argument `[]string`.
  - Le double cast est évalué une seule fois comme une constante, donc il **n'empêche pas** l'utilisation d'un index sur `<column>`. Le vérifier avec `EXPLAIN` sur la base locale et noter le résultat dans la PR.
  - Les identifiants de table et de colonne viennent du catalogue. Les entourer quand même de guillemets avec `pgx.Identifier{…}.Sanitize()`.
- Sinon, revenir à une requête `col = $1` par valeur, via `ResultSet`, pour garder le comportement actuel sur les types exotiques.
- Découper `values` en paquets de `selectInBatchSize = 1000`.
- Transformer les lignes avec `marshalRows`, comme `ResultSet`.

### 4.3 Nouvel algorithme de l'extracteur
État de l'extracteur :
```go
type fetchKey struct{ table, column string }
type extractor struct {
    extract   extract                         // inchangé : table -> entry -> resultSet
    processed map[string]struct{}             // remplace processedRelations, même format de clé
    fetched   map[fetchKey]map[string]struct{} // valeurs déjà demandées par (table, colonne), indexées par keyToText/fmt %v
    // dialect, schema, logger inchangés
}
type pending struct {
    root bool
    in   map[fetchKey][]interface{} // valeurs à demander au prochain niveau
    raw  []rawQuery                 // requêtes Queries (config) à exécuter telles quelles
}
```
Déroulement de `Handle(ctx, schema, query)` et de `HandleRoot` :
1. Exécuter la requête racine via `ResultSet`, ranger les lignes dans `extract[table][cacheKey(query)]`, puis appeler `visit(rows, table, root=true)`.
2. `visit` applique les règles de 4.1 à chaque ligne. Au lieu de lancer des requêtes, il **accumule** dans le `pending` du niveau suivant :
   - **FK** : ajouter `v` dans `in[{refTable, refCol}]`, sauf si `v` est déjà dans `fetched` pour cette clé ou si la relation est déjà dans `processed` ;
   - **Reference key** : ajouter `row[pk]` dans `in[{rk.Table.Name, rk.ColumnName}]`, avec la même déduplication ;
   - **Queries** : ajouter la chaîne produite par `replaceVar` dans `raw`, en dédupliquant par chaîne.
3. Tant que le niveau suivant n'est pas vide :
   - pour chaque `fetchKey`, dans l'ordre trié des clés pour rester déterministe : marquer les valeurs dans `fetched`, appeler `SelectIn`, ranger les lignes dans `extract[table]["<table>.<col> IN batch#n"]` avec une clé unique par appel, puis `visit(rows, root=false)` ;
   - pour chaque requête de `raw` : `ResultSet`, rangement sous `cacheKey(sql)` si la clé est absente, puis `visit(..., root=false)`.
4. La terminaison est garantie, comme aujourd'hui, par `processed` (indexé par PK) et par `fetched`.

`engine.Extract` :
- Garder le même extracteur pour la racine puis pour chaque `Extra` : chaque `Extra` est une nouvelle racine avec `root = true`, et ils partagent `processed` et `fetched`.
- **Lire `extractor.extract` directement** au lieu de la valeur renvoyée par `Handle`. Ça corrige le bug où un cache déjà rempli renvoie `nil`, écrase `cache` et fait perdre toute la sortie.

Logs :
- Garder un log Debug par niveau : nombre de valeurs par `fetchKey` et nombre de lignes renvoyées.
- Supprimer les logs Debug ligne par ligne qui font des `fmt.Sprintf`, ou les protéger avec `if ce := logger.Check(zap.DebugLevel, …); ce != nil`.
- Ajouter un log Info final : nombre de requêtes SQL et nombre de lignes par table.

### 4.4 Tests
- **Unitaires** (`etl/extractor_test.go`), avec un faux `dialect.Dialect` en mémoire : des tables avec PK, FK et reference keys, où `SelectIn` filtre les lignes et compte ses appels. Cas à couvrir :
  1. une racine avec FK vers un parent et une reference key vers des enfants, en vérifiant l'ensemble des (table, PK) extraits ;
  2. une FK vers la même table (comme `oauth2_refresh_token.rotated_from_id`), qui doit se terminer sans boucle infinie ;
  3. `OmitReferenceKeys`, et des `ReferenceKeys` de config appliquées à un niveau plus profond que la racine ;
  4. une `Queries` de config avec `{attr}` ;
  5. N lignes racines qui partagent le même parent, avec **un seul** appel à `SelectIn` pour ce parent ;
  6. une table `Extra`, avec une sortie non vide même si la racine a déjà tout mis en cache (régression du bug d'`Extract`).
- **Équivalence** sur la base locale :
  - extraire la même requête racine avec l'ancien et le nouveau binaire, avec une config sans `fake` ni `replace` pour que la sortie soit déterministe ;
  - comparer, pour chaque fichier, l'ensemble trié des PK, par exemple avec `jq -S '[.data[].id] | sort'` ;
  - les ensembles doivent être identiques. Noter dans la PR le nombre de requêtes et le temps avant et après.

---

## Hors périmètre (à traiter à part)
- `loader.go:67` : `json.Unmarshal` sans `UseNumber`, donc les bigint au-delà de 2^53 perdent leur précision. Le corriger demande d'adapter `valuesToPairs`, dont les cas de tableaux font `v[i].(float64)`.
- `ResultSet` ne fait ni `rows.Close()` sur le chemin d'erreur ni de vérification de `rows.Err()`.
- `PrimaryKeyColumnName()` panique sur les tables sans PK.
- `resetSequence` suppose une séquence `<table>_id_seq` et une colonne `id`.
- Index sur les FK côté enfant en prod (80 FK concernées) : c'est une décision qui concerne l'API ulule, pas mover.
