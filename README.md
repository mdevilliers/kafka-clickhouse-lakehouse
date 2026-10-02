# kafka-clickhouse-lakehouse

Local docker-compose dev stack: Kafka (KRaft) → ClickHouse → Iceberg (via Apache
Polaris REST catalog) → RustFS (S3-compatible). Keycloak is wired in as an OIDC
provider for human/UI auth; ClickHouse talks to Polaris with internal
client-credentials.

## Architecture

```mermaid
flowchart LR
    subgraph host["host"]
        user(["user / producer"])
        browser(["browser"])
    end

    subgraph stack["docker-compose network"]
        kafka[["kafka<br/>KRaft, :9092"]]
        ch[["clickhouse 26.9<br/>:8123 / :9000"]]
        polaris[["polaris<br/>Iceberg REST, :8181"]]
        console[["polaris-console<br/>:4000"]]
        keycloak[["keycloak<br/>OIDC, :8080"]]
        rustfs[("rustfs<br/>S3, :9900<br/>bucket: warehouse")]
    end

    schemas[/"schemas/*.json<br/>(source of truth)"/]
    schemas -- "polaris-setup<br/>loop → create tables" --> polaris

    user -- "JSON msgs<br/>topics: events, books, authors" --> kafka
    kafka -- "Kafka Engine<br/>{events,books,authors}_kafka" --> ch
    ch -- "MVs<br/>INSERT via DataLakeCatalog" --> polaris
    ch -- "write Parquet + Iceberg metadata" --> rustfs
    polaris -- "register snapshot<br/>s3://warehouse/default/{events,books,authors}/" --> rustfs

    browser -- "/" --> console
    console -. "Login with OIDC" .-> keycloak
    keycloak -. "id_token<br/>principal_name=polaris" .-> console
    console -- "catalog + mgmt APIs<br/>Bearer id_token" --> polaris

    ch -. "OAuth2 client_credentials<br/>quickstart_user" .-> polaris

    classDef store fill:#eef,stroke:#558,stroke-width:1px
    classDef auth fill:#fee,stroke:#855,stroke-width:1px
    class rustfs store
    class keycloak auth
```

**Two auth paths into Polaris** — intentional:
- **Machine (ClickHouse)**: Polaris's own OAuth2 client-credentials. Fast,
  stateless, no browser.
- **Human (Polaris Console)**: OIDC via Keycloak, PKCE flow. Token's
  `principal_name=polaris` claim maps to a Polaris principal with the same
  catalog grants.

## Services

| Service     | Image                            | Host port(s)   |
|-------------|----------------------------------|----------------|
| kafka       | `apache/kafka:4.3.1`             | `9092`         |
| rustfs      | `rustfs/rustfs:1.0.0-alpha.81`   | `9900`, `9001` |
| keycloak    | `quay.io/keycloak/keycloak:26.6.4` | `8080`       |
| polaris     | `apache/polaris:latest`          | `8181`, `8182` |
| clickhouse  | `clickhouse/clickhouse-server:26.9` | `8123`, `9000` |
| polaris-console | built from `apache/polaris-tools` (main) | `4000`     |

One-shot init containers:
- `bucket-setup` — creates the `warehouse` bucket in RustFS
- `kafka-topic-setup` — creates topics `events`, `books`, `authors`
- `polaris-setup` — bootstraps the catalog, principal, role, privileges, then
  **loops over `schemas/*.json`** to create each Iceberg namespace + table (via
  Polaris's Iceberg REST API — ClickHouse 26.9 can `INSERT` and `SELECT`
  through a `DataLakeCatalog` database but can't yet `CREATE TABLE` through
  it, so we pre-create tables catalog-side). Polaris is the single source of
  truth for Iceberg schemas.

## Quick start

```sh
docker compose up -d
docker compose ps        # wait for everything to be healthy
```

On first boot, ClickHouse automatically runs
`clickhouse/docker-entrypoint-initdb.d/01-polaris-catalog.sql`, which creates:

- Database `polaris_catalog` (DataLakeCatalog engine → Polaris REST)
- Kafka Engine source tables `default.{events,books,authors}_kafka`
- Materialized views `default.{events,books,authors}_mv` wiring each Kafka topic
  to `polaris_catalog.\`default.{events,books,authors}\``

The Iceberg target tables themselves are pre-created by `polaris-setup` from
the JSON files in [schemas/](schemas/).

## Adding an Iceberg table

Three mechanical edits, no inline JSON:

1. **Add a Kafka topic** — a line in `kafka-topic-setup` in
   [docker-compose.yml](docker-compose.yml).
2. **Drop a schema file** — new `schemas/<name>.json` with Iceberg field
   types (`long`, `string`, `int`, `timestamp`, …). The `namespace` and `name`
   fields at the top become the Polaris namespace + table name.
3. **Add a Kafka Engine + MV block** to
   [clickhouse/docker-entrypoint-initdb.d/01-polaris-catalog.sql](clickhouse/docker-entrypoint-initdb.d/01-polaris-catalog.sql).
   Column names and types must match the Iceberg schema from step 2.

### Applying to a running stack

The CH init SQL only runs on first boot (empty data dir). To apply changes
to a running stack:

```sh
# 1. Re-run the Polaris schema loop (idempotent — unchanged schemas are no-ops)
docker compose rm -sf polaris-setup && docker compose up -d polaris-setup

# 2. Re-run the ClickHouse DDL by hand
docker compose exec -T clickhouse clickhouse-client --multiquery \
    < clickhouse/docker-entrypoint-initdb.d/01-polaris-catalog.sql
```

Both are `IF NOT EXISTS` / idempotent, so re-running is safe.

## End-to-end verification

```sh
# Produce to all three topics (-T disables TTY so heredoc works)
docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:19092 --topic events <<EOF
{"id": 1, "msg": "hello", "ts": "2026-10-02 12:00:00.000000"}
{"id": 2, "msg": "world", "ts": "2026-10-02 12:00:01.000000"}
EOF

docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:19092 --topic books <<EOF
{"id": 1, "title": "Dune", "author_id": 10, "published_year": 1965, "ts": "2026-10-02 12:00:00.000000"}
EOF

docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:19092 --topic authors <<EOF
{"id": 10, "name": "Frank Herbert", "country": "US", "ts": "2026-10-02 12:00:00.000000"}
EOF

# Give the materialized views a moment to flush
sleep 10

# Read all three Iceberg tables through the Polaris catalog
docker compose exec clickhouse clickhouse-client -q "
SELECT 'events'  AS t, count() FROM polaris_catalog.\`default.events\`  UNION ALL
SELECT 'books'   AS t, count() FROM polaris_catalog.\`default.books\`   UNION ALL
SELECT 'authors' AS t, count() FROM polaris_catalog.\`default.authors\`
"
```

Then open the RustFS console at <http://localhost:9001> (login
`polaris_root` / `polaris_pass`) and browse the `warehouse` bucket. You should
see Parquet data files under `default/events/data/` and Iceberg metadata under
`default/events/metadata/`.

## UIs & credentials

| Service              | URL                                | Login                          |
|----------------------|------------------------------------|--------------------------------|
| ClickHouse HTTP      | <http://localhost:8123/play>       | `default` / _(empty)_          |
| Keycloak Admin       | <http://localhost:8080>            | `admin` / `admin`              |
| RustFS Console       | <http://localhost:9001>            | `polaris_root` / `polaris_pass`|
| Polaris Console      | <http://localhost:4000>            | Click **Login with OIDC** → `polaris` / `polaris` (see below) |
| Polaris Health       | <http://localhost:8182/q/health>   | —                              |

### Logging into the Polaris Console

1. Open <http://localhost:4000>.
2. Click **Login with OIDC**. You're redirected to Keycloak.
3. Sign in with `polaris` / `polaris`.
4. Keycloak redirects you back to the console, authenticated.

The `polaris` Keycloak user is defined in [keycloak/iceberg-realm.json](keycloak/iceberg-realm.json)
and auto-imported on first boot. Its OIDC token carries `principal_name=polaris`,
which maps to a Polaris principal of the same name — `polaris-setup` grants
that principal `quickstart_user_role`, which has read/write on
`quickstart_catalog`.

## All credentials (cheat sheet)

| Context                                             | Who                   | Credentials                       |
|-----------------------------------------------------|-----------------------|-----------------------------------|
| Polaris Console (UI login via Keycloak)             | human                 | `polaris` / `polaris`             |
| Keycloak Admin Console                              | admin                 | `admin` / `admin`                 |
| Polaris machine-to-machine (ClickHouse, curl, etc.) | `quickstart_user`     | `quickstart_user` / `quickstart_pass` |
| Polaris bootstrap/root (admin API)                  | root client           | `root` / `s3cr3t`                 |
| RustFS Console (S3 UI)                              | S3 user               | `polaris_root` / `polaris_pass`   |
| ClickHouse HTTP + native                            | `default`             | _(no password)_                   |

## Machine credentials (ClickHouse → Polaris)

| Field          | Value                                                      |
|----------------|------------------------------------------------------------|
| Client ID      | `quickstart_user`                                          |
| Client Secret  | `quickstart_pass`                                          |
| Scope          | `PRINCIPAL_ROLE:ALL`                                       |
| Token endpoint | `http://polaris:8181/api/catalog/v1/oauth/tokens`          |
| Realm          | `POLARIS`                                                  |
| Catalog        | `quickstart_catalog`                                       |

Override by copying `.env.example` to `.env` and editing.

## Connect from DuckDB

```
INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE SECRET polaris_secret (
    TYPE iceberg,
    CLIENT_ID 'quickstart_user',
    CLIENT_SECRET 'quickstart_pass',
    OAUTH2_SERVER_URI 'http://localhost:8181/api/catalog/v1/oauth/tokens',
    OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
);

ATTACH 'quickstart_catalog' AS polaris (
    TYPE iceberg,
    ENDPOINT 'http://localhost:8181/api/catalog',
    SECRET polaris_secret
);
SHOW ALL TABLES;
```

## Operational notes

- Polaris uses in-memory persistence — restart `polaris` and the catalog is
  gone. To recreate it, run `docker compose rm -sf polaris-setup && docker
  compose up -d polaris-setup`.
- Kafka's two listeners: in-compose consumers use `kafka:19092`, host-based
  tools use `localhost:9092`.
- When using `kafka-console-producer.sh` with a heredoc, pass `-T` (disable
  TTY) to `docker compose exec` — `-it` fails with "the input device is not
  a TTY" because stdin is a pipe.
- **Polaris Console build on macOS / Podman**: `docker compose up --build
  polaris-console` can fail with `error getting credentials ... User canceled
  the operation` even for public images — this is podman invoking
  `docker-credential-osxkeychain` and getting a dismissed Keychain prompt
  (not an AWS/ECR issue). Workaround: build with podman directly, then start
  the service without `--build`:
  ```sh
  podman build -t polaris-console:local \
      https://github.com/apache/polaris-tools.git#main:console \
      -f docker/Dockerfile
  docker compose up -d polaris-console
  ```
- Dev stack only. Credentials are checked-in defaults; do not reuse elsewhere.
