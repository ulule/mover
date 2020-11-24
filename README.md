# mover

`mover` is a simple utility to extract data from a backup server and load them in your local database.

It uses the underlying introspection API from your RDMS to retrieve automatically relationships from the extracted rows.

## Usage

Export configuration to a environment variable:

```console
export MOVER_CONF=`pwd`/config.json
```

Create the output directory:

```console
mkdir -p output
```

Extract data from backup database:

```console
go run cmd/mover/main.go -dsn "postgresql://user:password@localhost:5433/dbname" -path output -action extract -query "SELECT * FROM user WHERE id = 1" -table "user"
```

Load data to your local database:

```console
go run cmd/mover/main.go -dsn "postgresql://user:password@localhost:5433/dbname" -path output -action load
```
