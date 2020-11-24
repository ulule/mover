# mover

`mover` is a simple utility to extract data from a backup server and load them in your local database.

It uses the underlying introspection API from your RDMS to retrieve automatically relationships from the extracted rows.

## Usage

Tunnel backup database:

```console
ssh -L 5432:localhost:5433 root@b1.db1.prod.infra.ulule.com
```

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
run main.go -dsn "postgresql://ulule:ulule@localhost:5433/ulule" -path output -action load
```

Load data to your local database:

```console
go run main.go -dsn "postgresql://ulule:ulule@localhost/ulule" -path output -action load
```
