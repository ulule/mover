# mover

`mover` is used to extract data from a remote database server and load them in another environment.

It uses the underlying introspection API from the RDMS to retrieve relationships
(foreign keys, reference keys, etc.) based on results from your database schema.

## How do we use it internally?

Instead of harcoding fixtures for each workflows, we export data from our production database by sanitizing
sensible data (password, personal user information, etc.).

Thanks to this tool, we don't have to maintain fixtures anymore and we can
quickly replicate a production bug in our local environment.

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
export REMOTE_DSN="postgresql://user:password@remote.server:5432/dbname"
go run cmd/mover/main.go -dsn $REMOTE_DSN -path output -action extract -query "SELECT * FROM user WHERE id = 1" -table "user"
```

Load data to your local database:

```console
export LOCAL_DSN="postgresql://user:password@localhost:5432/dbname"
go run cmd/mover/main.go -dsn $LOCAL_DSN -path output -action load
```
