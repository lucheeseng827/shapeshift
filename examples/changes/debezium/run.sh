#!/usr/bin/env bash
# A real Debezium change feed into shapeshift, checked against the source table.
#
#   examples/changes/debezium/run.sh postgres|mysql [shapeshift-binary]
#
# Starts a database, Kafka and Kafka Connect with Debezium in containers, runs a workload of
# inserts, updates and deletes (a key deleted and inserted again among them), drains the
# connector's topic to JSON Lines, applies it with shapeshift in several writes **delivered
# out of order** (with `changes.preset` ordering by the log position, the order must not
# matter), and has DuckDB compare the table with the database's final state. A second pass
# replays the whole feed again: it must change nothing.
#
# Needs docker or podman (DOCKER=podman), python3 with duckdb, and a Linux shapeshift binary
# (default target/debug/shapeshift). Everything it starts is removed at the end. With
# FEED_OUT=<dir> it stops after draining and leaves feed.jsonl and expected.csv there, for
# verify.py to run where the binary is (a Linux container on another OS).
set -euo pipefail

DB=${1:?usage: run.sh postgres|mysql [shapeshift-binary]}
BIN=${2:-target/debug/shapeshift}
DOCKER=${DOCKER:-docker}
HERE=$(cd "$(dirname "$0")" && pwd)
NET=ss-cdc-$$
WORK=$(mktemp -d)
RUNS=${RUNS:-4}

cleanup() {
  for c in ss-cdc-connect-$$ ss-cdc-kafka-$$ ss-cdc-db-$$; do $DOCKER rm -f "$c" >/dev/null 2>&1 || true; done
  $DOCKER network rm "$NET" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# Run "$@" until it succeeds, up to $2 seconds; say what never came up.
wait_for() {
  local what=$1 secs=$2
  shift 2
  for _ in $(seq "$secs"); do "$@" >/dev/null 2>&1 && return 0; sleep 1; done
  echo "$what did not come up within ${secs}s" >&2
  exit 1
}

$DOCKER network create "$NET" >/dev/null

$DOCKER run -d --name ss-cdc-kafka-$$ --network "$NET" --network-alias kafka \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  docker.io/apache/kafka:3.8.0 >/dev/null

case "$DB" in
  postgres)
    $DOCKER run -d --name ss-cdc-db-$$ --network "$NET" --network-alias db \
      -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres quay.io/debezium/postgres:16 >/dev/null
    # Over TCP: the image's first-start server listens only on its socket, then restarts.
    sql() { $DOCKER exec -i -e PGPASSWORD=postgres ss-cdc-db-$$ psql -h 127.0.0.1 -q -U postgres -d postgres -v ON_ERROR_STOP=1 "$@"; }
    wait_for "postgres" 90 sql -c 'select 1'
    sql -c 'create table orders (id int primary key, status text not null, amount int not null)'
    CONNECTOR='{"name":"orders","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname":"db","database.port":"5432","database.user":"postgres","database.password":"postgres",
      "database.dbname":"postgres","topic.prefix":"pg","table.include.list":"public.orders","plugin.name":"pgoutput",
      "tombstones.on.delete":"false"}}'
    TOPIC=pg.public.orders
    SPEC=$HERE/postgres.yaml
    final() { sql -At -F, -c 'select id, status, amount from orders order by id'; }
    ;;
  mysql)
    $DOCKER run -d --name ss-cdc-db-$$ --network "$NET" --network-alias db \
      -e MYSQL_ROOT_PASSWORD=debezium -e MYSQL_USER=mysqluser -e MYSQL_PASSWORD=mysqlpw \
      quay.io/debezium/example-mysql:2.7 >/dev/null
    sql() { $DOCKER exec -i ss-cdc-db-$$ mysql -uroot -pdebezium inventory "$@" 2>/dev/null; }
    # Ready means the final server, over TCP: the image's first-start server skips networking,
    # then restarts. (Its root may log in only from localhost; the connector's user may not.)
    tcp() { $DOCKER exec ss-cdc-db-$$ mysql --protocol=tcp -h127.0.0.1 -udebezium -pdbz -e 'select 1'; }
    wait_for "mysql" 180 tcp
    sql -e 'create table orders2 (id int primary key, status varchar(32) not null, amount int not null)'
    CONNECTOR='{"name":"orders","config":{"connector.class":"io.debezium.connector.mysql.MySqlConnector",
      "database.hostname":"db","database.port":"3306","database.user":"debezium","database.password":"dbz",
      "database.server.id":"184054","topic.prefix":"my","database.include.list":"inventory",
      "table.include.list":"inventory.orders2","tombstones.on.delete":"false",
      "schema.history.internal.kafka.bootstrap.servers":"kafka:9092",
      "schema.history.internal.kafka.topic":"schema-changes.inventory"}}'
    TOPIC=my.inventory.orders2
    SPEC=$HERE/mysql.yaml
    final() { sql -N -B -e 'select id, status, amount from orders2 order by id' | tr '\t' ','; }
    ;;
  *) echo "postgres or mysql" >&2; exit 2 ;;
esac

# HANDOFF=1 (MySQL): a transaction open across the connector's snapshot. The table holds rows before the connector
# starts, and a transaction updates id 1, deletes id 2 and inserts id 1000, held open while
# the connector takes its snapshot; it commits after, and id 3 is updated last. The feed's
# snapshot rows (`r`) and those changes must end as the database did, in any order. (On
# Postgres the race needs a slot made consistent around the open transaction, which this
# script does not arrange: handoff/postgres-feed.jsonl was taken with Debezium Server.)
if [ -n "${HANDOFF:-}" ]; then
  [ "$DB" = mysql ] || { echo "HANDOFF=1 is for MySQL: the Postgres feed needs a slot timed around the transaction (see handoff/)" >&2; exit 2; }
  sql -e "insert into orders2 values (1,'seed',10),(2,'seed',20),(3,'seed',30),(4,'seed',40),(5,'seed',50)"
  # The session that holds the transaction runs in the database's container, fed through a
  # fifo there, and commits once /tmp/go appears.
  $DOCKER exec -d ss-cdc-db-$$ sh -c '
    mkfifo /tmp/tx
    mysql -uroot -pdebezium inventory < /tmp/tx > /tmp/tx.out 2>&1 &
    exec 3>/tmp/tx
    echo "begin; update orders2 set status = '"'"'inflight'"'"', amount = 999 where id = 1; delete from orders2 where id = 2; insert into orders2 values (1000, '"'"'inflight-new'"'"', 1);" >&3
    while [ ! -f /tmp/go ]; do sleep 1; done
    echo "commit;" >&3
    exec 3>&-
    wait
    touch /tmp/done'
  held() { [ "$(sql -N -B -e 'select coalesce(sum(trx_rows_modified), 0) from information_schema.innodb_trx')" -ge 3 ]; }
  wait_for "the open transaction" 30 held
fi

$DOCKER run -d --name ss-cdc-connect-$$ --network "$NET" -p 127.0.0.1::8083 \
  -e BOOTSTRAP_SERVERS=kafka:9092 -e GROUP_ID=1 \
  -e CONFIG_STORAGE_TOPIC=cfg -e OFFSET_STORAGE_TOPIC=off -e STATUS_STORAGE_TOPIC=st \
  -e KEY_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e VALUE_CONVERTER=org.apache.kafka.connect.json.JsonConverter \
  -e CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE=false -e CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE=false \
  quay.io/debezium/connect:2.7 >/dev/null
PORT=$($DOCKER port ss-cdc-connect-$$ 8083/tcp | head -1 | sed 's/.*://')
wait_for "kafka connect" 180 curl -sf "http://127.0.0.1:$PORT/connectors"
curl -sf -X POST -H 'Content-Type: application/json' --data "$CONNECTOR" "http://127.0.0.1:$PORT/connectors" >/dev/null
running() { curl -sf "http://127.0.0.1:$PORT/connectors/orders/status" | grep -q '"state":"RUNNING".*"state":"RUNNING"'; }
wait_for "the connector" 120 running

if [ -n "${HANDOFF:-}" ]; then
  # The snapshot's five rows are in the topic: the transaction commits only now.
  snapshotted() {
    [ "$($DOCKER exec ss-cdc-kafka-$$ /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
      --topic "$TOPIC" --from-beginning --timeout-ms 15000 2>/dev/null | grep -c '"op":"r"')" -ge 5 ]
  }
  for _ in $(seq 12); do snapshotted && break; sleep 2; done
  if ! snapshotted; then
    echo "the connector's snapshot did not show in the topic with the transaction open. What runs:" >&2
    sql -e 'select id, state, left(info, 80) from information_schema.processlist where command <> "Sleep"' >&2 || true
    curl -s "http://127.0.0.1:$PORT/connectors/orders/status" >&2 || true
    $DOCKER exec ss-cdc-kafka-$$ /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
      --topic "$TOPIC" --from-beginning --timeout-ms 15000 2>&1 | head -3 >&2 || true
    exit 1
  fi
  $DOCKER exec ss-cdc-db-$$ touch /tmp/go
  wait_for "the transaction's commit" 30 $DOCKER exec ss-cdc-db-$$ test -f /tmp/done
  sql -e "update orders2 set status = 'later', amount = 31 where id = 3"
  sleep 8 # let the connector catch up
else
sleep 5 # the connector's snapshot of the empty table

# The workload: inserts, updates, deletes, and keys deleted then inserted again.
${PYTHON:-python3} - "$DB" > "$WORK/workload.sql" <<'PY'
import random, sys
rng = random.Random(606)
table = "orders" if sys.argv[1] == "postgres" else "orders2"
live, out = set(), []
for i in range(400):
    k = rng.randrange(60)
    if k not in live:
        out.append(f"insert into {table} values ({k}, 'new', {i});"); live.add(k)
    elif rng.random() < 0.35:
        out.append(f"delete from {table} where id = {k};"); live.discard(k)
    else:
        out.append(f"update {table} set status = 's{i}', amount = {i} where id = {k};")
print("\n".join(out))
PY
sql < "$WORK/workload.sql"
sleep 8 # let the connector catch up
fi

# Drain the topic: one change per line.
$DOCKER exec ss-cdc-kafka-$$ /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic "$TOPIC" --from-beginning --timeout-ms 10000 2>/dev/null | grep '^{' > "$WORK/feed.jsonl" || true
if [ ! -s "$WORK/feed.jsonl" ]; then
  echo "the topic $TOPIC held no changes: is the connector running? (curl http://127.0.0.1:$PORT/connectors/orders/status)" >&2
  exit 1
fi
echo "feed: $(wc -l < "$WORK/feed.jsonl") changes from $DB"

if [ -n "${FEED_OUT:-}" ]; then
  mkdir -p "$FEED_OUT" && cp "$WORK/feed.jsonl" "$FEED_OUT/" && final > "$FEED_OUT/expected.csv"
  echo "feed and expected rows in $FEED_OUT"
  exit 0
fi
${PYTHON:-python3} "$HERE/verify.py" --bin "$BIN" --spec "$SPEC" --feed "$WORK/feed.jsonl" \
  --work "$WORK" --runs "$RUNS" --expected <(final)
