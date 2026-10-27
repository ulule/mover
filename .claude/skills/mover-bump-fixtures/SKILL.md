---
name: mover-bump-fixtures
description: Publier une nouvelle version de mover dans ulule-fixtures - bumper le sha aux deux endroits où il est épinglé (Dockerfile et workflow fixtures.yml), installer le binaire en local, puis suivre le premier run. À utiliser après qu'un commit mover est poussé et doit être utilisé pour générer les fixtures.
---
# Bumper mover dans ulule-fixtures

ulule-fixtures épingle mover par **sha complet**, à deux endroits qui doivent rester identiques :

| Fichier | Ligne |
|---|---|
| `Dockerfile` | `ARG APP_REVISION=<sha>` (l'image est construite via `ADD https://github.com/ulule/mover.git#${APP_REVISION}`) |
| `.github/workflows/fixtures.yml` | `go install -v github.com/ulule/mover/cmd/mover@<sha>` |

## Étapes
1. **Le sha doit être poussé** sur le remote de mover (`git branch -r --contains <sha>`), sinon `go install` et `ADD` échouent. Prends le sha complet : `git rev-parse <ref>`.
2. **Vérifier le changement mover** avec le skill `mover-verify`. Si le changement touche le load ou les types, fais aussi un aller-retour avec `mover-roundtrip`.
3. **Dans ulule-fixtures**, remplace l'ancien sha aux deux endroits, puis vérifie qu'il ne reste qu'une seule valeur :
   ```bash
   grep -rn "APP_REVISION=\|cmd/mover@" Dockerfile .github/workflows/
   ```
   Commit : `chore: bump mover to <sha court> (<résumé>)`. Ne pousse pas et n'ouvre pas de PR sans l'accord de l'utilisateur.
4. **Installation locale** (le module est privé) :
   ```bash
   GOPRIVATE=github.com/ulule/* go install github.com/ulule/mover/cmd/mover@<sha>
   ```
   Puis vérifie que le binaire installé est le bon avec `go version -m "$(command -v mover)" | grep -m1 mod`.
5. **Premier run** : le workflow `fixtures` de ulule-fixtures se déclenche manuellement. Ne le lance pas toi-même sans accord. Une fois le run fini, confie l'analyse des logs au sous-agent `mover-run-analyzer`. Rappel : un run vert ne garantit rien, puisque mover sort toujours avec le code 0.
