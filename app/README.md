# 🧾 Version History
---

## **v0.1.2-amd64** - *2025-11-11*

**Producer_realtime**
- added logger

**Producer_backfill**
- arg choices of postgres and kafka with server: `dest`, `dest-server` 
- new args for postgres: `pg-host`, `pg-port`, `pg-db`, `pg-user`, `pg-pass`, `pg-table`
- added logger

```bash
# To run Kafka 
kubectl run smd-producer-backfill-v0-1-2 \
    --restart='Never' \
    --image-pull-policy='Always' \
    --image dwnusa/smd-producer-backfill:v0.1.2-amd64 \
    --  --topic backfill-kafka \
        --dest kafka

# To run PostgreSQL
kubectl run smd-producer-backfill-v0-1-2 \
    --restart='Never' \
    --image-pull-policy='Always' \
    --image dwnusa/smd-producer-backfill:v0.1.2-amd64 \
    --  --dest postgresql \
        --pg-pass postgres
# other args for pg are loaded from .env
```

**Consumer_realtime**
- added logger


---

## **v0.1.1-amd64** - *2025-11-10*
**Producer**
- added create_topic logic
- added producer key of machine_id and key-serializer 
```bash
kubectl run smd-producer-v0-1-1-machine-1-1  \
    --restart='Never' \
    --image-pull-policy='Always' \
    --image dwnusa/smd-producer:v0.1.1-amd64 \
    --  --topic realtime-test-topic \
        --machine machine-1-1
```

**Consumer**
- 
```bash
kubectl run smd-consumer-v0-1-1 \
    --restart='Never' \
    --image-pull-policy='Always' \
    --image dwnusa/smd-consumer:v0.1.1-amd64 \
    --  --topic realtime-test-topic \
        --pg-pass dwnusa \
        --batch-size 10
```

---

## **v0.1.0-amd64** - *2025-11-10*
**Producer**
- args added `topic`, `bootstrap-servers`, `interval`, `machine`

**Consumer**
- args for kafka: `topic`, `bootstrap-servers`, `group-id`, `timeout`
- args for postgresql: `pg-host`, `pg-port`, `pg-db`, `pg-user`, `pg-pass`, `pg-table`
- args for system: `batch-size`
- `.env` required inside data_consumers folder
```bash
PG_HOST=airflow-postgresql.airflow.svc.cluster.local
PG_PORT=5432
PG_DB=postgres
PG_USER=postgres
PG_TABLE=smd_table_realtime
```

---

## **v0.0.3-amd64** — *2025-11-09*
**Producer**
- ⏱ Generates messages every **0.5 sec** for testing.

**Consumer**
- 🚫 Table **not truncated** (data is appended)
- ⏳ Waits **120 000 ms** before timeout

**Notes**
- Focused on high-frequency test scenario.
- Verified message delivery and batch behavior.

---

## **v0.0.2-amd64** — *2025-11-08*
**Producer**
- ⏱ Generates messages every **1 sec** (real-world simulation).

**Consumer**
- ⚠️ Table **TRUNCATED** on each batch (data replaced)
- 📦 `batch_size = 100`

**Notes**
- Used for initial integration test.
- Confirmed DB connectivity and schema auto-creation.
- dockerfile install psycopg2-binary