# mover

`mover` is a simple utility to extract data from a remote database server and load them in another environment.

It uses the underlying introspection API from the RDMS to retrieve relationships from the extracted results.

## How do we use it internally?

Instead of harcoding fixtures for each workflows, we export data from our production database by sanitizing
sensible data (password, personal user information, etc.).

Thanks to this tool, we don't have to maintain anymore fixtures and we can
quickly reply production bugs in our local environment.

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
go run cmd/mover/main.go -dsn "postgresql://user:password@remote.server:5432/dbname" -path output -action extract -query "SELECT * FROM user WHERE id = 1" -table "user"
```

Load data to your local database:

```console
go run cmd/mover/main.go -dsn "postgresql://user:password@localhost:5432/dbname" -path output -action load
```
