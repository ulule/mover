---
name: mover-verify
description: Vérifications à lancer avant de commiter un changement dans mover - build, vet, tests unitaires, lint, et tests d'intégration Postgres quand dialect/postgres est touché. À utiliser à la fin de toute modification de code du repo.
---
# Vérifier un changement mover

1. Regarde ce qui a changé : `git diff --stat` (avec `main...HEAD` si tu es sur une branche).
2. Lance, dans l'ordre, en t'arrêtant à la première erreur :
   ```bash
   go build ./... && go vet ./...
   make test
   make lint
   ```
3. Si `dialect/postgres/` a changé, lance aussi les tests d'intégration. Ils ne tournent pas en CI.
   ```bash
   MOVER_TEST_DSN=postgres://<user>@localhost:5432/<db_locale_jetable> MOVER_TEST_FIXTURE=1 \
     go test -tags integration ./dialect/postgres/ -v
   ```
   - `MOVER_TEST_FIXTURE=1` crée puis supprime les tables `mvt_*`, la vue `mvt_view` et le schéma `mvt_other`. N'utilise jamais une base partagée ou de prod.
   - Sans DSN local disponible, demande-le à l'utilisateur, ou dis explicitement que ces tests n'ont pas été lancés.
4. Si le changement touche le format de sortie, les conversions de types ou le load, fais aussi un aller-retour réel avec le skill `mover-roundtrip`.
5. Rapporte chaque commande avec son résultat. Un test rouge se montre avec sa sortie.
