---
name: mover-roundtrip
description: Faire un aller-retour réel extract puis load de mover en local, d'une base source vers une base jetable, et comparer les volumes. À utiliser pour valider un changement du parcours, des conversions de types ou du load, ou pour reproduire un échec de ulule-fixtures.
---
# Aller-retour extract → load en local

**Garde-fous**
- La base source doit être locale ou une copie. Ne lance jamais `load` sur une base partagée ou de prod.
- La base cible est créée pour l'occasion, puis supprimée.

1. **Binaire** à tester :
   ```bash
   go build -o /tmp/mover-rt ./cmd/mover
   ```
2. **Base cible jetable**, avec le schéma de la source mais sans données :
   ```bash
   createdb mover_rt
   pg_dump --schema-only --no-owner "$SRC_DSN" | psql -X -q mover_rt
   ```
3. **Extract.** Le dossier de sortie doit exister, mover ne le crée pas. Pour reproduire ulule-fixtures, prends sa config `ulule-fixtures/ulule.mover.json` et une requête racine de `bin/extract`, par exemple `SELECT * FROM ulule_presales_presale WHERE id = <id>`.
   ```bash
   rm -rf /tmp/mover-rt-out && mkdir -p /tmp/mover-rt-out
   MOVER_CONF=<config.json> /tmp/mover-rt -dsn "$SRC_DSN" -path /tmp/mover-rt-out \
     -action extract -query "SELECT * FROM <table> WHERE id = <id>" -verbose 2>&1 | tee /tmp/mover-rt-extract.log
   ```
4. **Load**, deux fois. Le second passage doit réussir sans rien insérer (`ON CONFLICT DO NOTHING`).
   ```bash
   /tmp/mover-rt -dsn "postgres://localhost/mover_rt" -path /tmp/mover-rt-out -action load 2>&1 | tee /tmp/mover-rt-load.log
   ```
5. **Contrôles.** mover renvoie toujours 0 : ce sont les logs et les comptes qui font foi.
   - `grep -iE '"level":"(error|panic)"|unable to|panic' /tmp/mover-rt-*.log` doit être vide.
   - Pour chaque `<table>.json`, `count` doit être égal à `SELECT count(*) FROM <table>` dans `mover_rt`.
   - Les colonnes anonymisées ne contiennent plus les valeurs d'origine.
   - Un `INSERT` sans `id` explicite fonctionne sur une table à séquence (vérifie `resetSequence`).
6. **Comparaison avec une autre version** (avant/après) : refais l'étape 3 avec l'autre binaire dans un autre dossier, puis compare les ensembles de PK par fichier, par exemple avec `jq -S '[.data[].id] | sort'`. Un sur-ensemble côté nouveau code est attendu quand une racine est atteinte par une autre racine (voir `docs/how-it-works.md` § 3.3).
7. **Nettoyage** :
   ```bash
   dropdb mover_rt
   rm -rf /tmp/mover-rt*
   ```
