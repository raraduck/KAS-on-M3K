# 🧾 Version History

---

## **v0.1.0-amd64** - *2025-11-10*
**Producer**
- args added `topic`, `bootstrap-servers`, `interval`, `machine`

**Consumer**
- args for kafka: `topic`, `bootstrap-servers`, `group-id`, `timeout`
- args for postgresql: `pg-host`, `pg-port`, `pg-db`, `pg-user`, `pg-pass`, `pg-table`
- args for system: `batch-size`

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