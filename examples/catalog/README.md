# Catalog commits against a real REST catalog

`verify_catalog.py` checks shapeshift's commits through an Iceberg REST catalog against
Apache Iceberg's own REST fixture, with DuckDB reading every table back through the catalog.
It is shapeshift 0.5's reference check (`model`) and failure-injected race (`race`); see the
script's docstring.

```sh
cargo build -p shapeshift-cli --features object_store
WH=$(mktemp -d /tmp/shapeshift-wh.XXXXXX)   # a fresh warehouse, yours
docker run -d --name catalog -p 127.0.0.1:8181:8181 --user "$(id -u):$(id -g)" \
  -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro -v "$WH:$WH" \
  -e CATALOG_WAREHOUSE="file:$WH" -e CATALOG_CLIENTS=1 apache/iceberg-rest-fixture:1.10.0
pip install -r examples/catalog/requirements.txt
python examples/catalog/verify_catalog.py model --bin target/debug/shapeshift
python examples/catalog/verify_catalog.py race  --bin target/debug/shapeshift
python examples/catalog/verify_catalog.py race  --bin target/debug/shapeshift --negative-control --load-fault-rate 0.5
```

The warehouse must be at the same path for the catalog and for this machine (DuckDB and the
binary read the paths the catalog writes), and the catalog must run as your user so both can
write into its table directories (the passwd mount gives that uid a name; without one the
fixture's Hadoop file IO answers 500 "Failed to get file system"). Keep `CATALOG_CLIENTS=1`: with the fixture's default of two
JDBC connections, its SQLite locks itself under concurrent commits.

For S3: run moto with `examples/merge/s3_proxy.py` in front of it (`--host 0.0.0.0` when the
catalog runs in a container that is not on the host's network), make the bucket, run the
catalog with `S3FileIO` pointed at the proxy, and set the `AWS_*` env for this script.
`.github/workflows/shapeshift-catalog-verify.yml` does exactly that.
