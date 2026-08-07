# Pipeline de Detección de Fraude y Scoring de Crédito

Pipeline de datos con streaming, CDC (Change Data Capture), lakehouse y
transformaciones analíticas para detectar fraude en tiempo real y mantener un
scoring crediticio actualizado.

- **Data Engineering**: ingesta en tiempo real con streaming + CDC, landing de
  batch, y anonimización de datos sensibles (PII / PCI-DSS).
- **Analytics Engineering**: transformaciones en capas Bronze / Silver / Gold con
  dbt sobre el Data Warehouse.

Estructura interna del proyecto (fases y detalle) en
[`roadmap_proyecto_fintech.txt`](roadmap_proyecto_fintech.txt).

---

## Para qué sirve

El proyecto emula un sistema de detección de fraude y evaluación de crédito en
fintech, con tres usos principales:

1. **Detección de fraude en tiempo real**
   - Simulador de transacciones (`scripts/generator.py`) que inserta operaciones
     normales y con patrones de fraude simulados (múltiples montos altos en poco
     tiempo, ubicaciones lejanas, etc.).
   - Cada cambio en la tabla de transacciones se captura por CDC (Debezium) y
     viaja por streaming a un broker y de ahí a un lakehouse como Parquet.
   - Base de datos OLAP lista para alertas agregadas de fraude.

2. **Ingesta realista de datos de tarjetas (PII)**
   - Una **Mock API** (puerto 8001) simula una fuente externa de tarjetas con
     datos **sensibles** (PAN, CVV, cardholder).
   - Un **ingestor** aplica masking obligatorio (PCI-DSS): conserva solo los
     últimos 4 dígitos del PAN, descarta el CVV y enmascara el titular.
   - El dato anonimizado cae como Parquet al lakehouse; el dato crudo **nunca**
     se persiste.
   - Un proceso batch diario simula un sistema legado que deja un CSV en la
     landing zone de MinIO.
   - Detalle completo en `plan_ingresos_realistas.txt`.

3. **Scoring y data warehouse**
   - Base OLTP con usuarios (scoring 300–850, límite, riesgo) y transacciones.
   - Base OLAP pensada para que dbt construya dimensiones, hechos y alertas.

---

## Stack tecnológico

| Componente                        | Tecnología                                   | Puerto |
|-----------------------------------|---------------------------------------------|--------|
| Base OLTP (fuente)                | PostgreSQL 15                               | 5432   |
| Broker de streaming               | Redpanda (compatible Kafka)                 | 9092 / 9644 / 8081 |
| Consola de tópicos                | Redpanda Console                            | 8080 |
| CDC / Kafka Connect               | Debezium                                    | 8083 |
| Lakehouse (S3 compatible)         | MinIO                                       | 9000 / 9001 |
| Data Warehouse analítico          | PostgreSQL (OLAP) (dbt / Metabase)          | 5433 |
| BI / Dashboards                   | Metabase                                    | 3000 |
| Mock API de tarjetas (PII)        | FastAPI / uvicorn (local)                   | 8001 |

---

## Arquitectura (resumen)

```
   FLUJO 1 - TRANSACCIONES (streaming / CDC)
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                               +------- CDC (Debezium) --------+
   Simulador (OLTP) ---------->| postgres_oltp.public.transactions |--> Redpanda
   Postgres (Docker, 5432)     +-------------------------------+     tópico
                                  postgres_oltp.public.transactions / .users
                                          |
                                          v
                                Worker (scripts/worker.py)
                                          |
                                          v
                                MinIO lakehouse  (bucket fintech-lakehouse)
                                /transactions/  (Parquet CDC particionado)

   FLUJO 2 - TARJETAS / PII (streaming con anonimización)
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                                       Mock Cards API (FastAPI, 8001)
                                       GET /cards -> PAN/CVV/cardholder (crudo)
                                          |
                                          v
                               Ingestor (scripts/ingest_cards.py)
                               masking PII: PAN->últ.4, CVV DESCARTADO, titular***
                                          |
                                          v
                               Redpanda tópico  cards.api --> Worker (worker.py)
                                          |
                                          v
                               MinIO lakehouse  /cards/  (Parquet anonimizado, SIN PAN/CVV)

   FLUJO 3 - BATCH DIARIO (CSV legacy)
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   daily_batch_csv.py (scripts/daily_batch_csv.py)  
   ----------> MinIO lakehouse  /landing/cards_daily/YYYY-MM-DD/movimientos.csv

   TODOS LOS FLUJOS CONVERGEN (Fase 3 principal)
   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
       dbt (Fase 3) -> PostgreSQL OLAP (5433) -> Metabase (3000)
```

---

## Cómo bajarlo y levantarlo

### Requisitos
- Docker + Docker Compose
- Python 3.10+ (para los scripts que corren en el venv)
- WSL / Linux (los scripts asumen rutas y `venv/`)

### 1. Clonar
```bash
git clone <url-del-repo>
cd fraude_scoring/python
```

### 2. Configurar variables de entorno
```bash
cp .env.example .env
```
Editar `.env` si se quieren cambiar usuarios/puertos. **Nunca commitear `.env`**
(ya está en `.gitignore`).

### 3. Levantar la infraestructura
```bash
docker compose up -d
```
Esto levanta: `postgres_oltp`, `postgres_olap`, `redpanda`, `redpanda-console`,
`debezium`, `minio`, `minio-init`, y `metabase`.

Comprobar estado:
```bash
docker compose ps
```

### 4. Preparar el entorno Python (venv) para los scripts
```bash
python -m venv venv
./venv/bin/pip install -r scripts/requirements.txt
```

### 5. Inicializar el conector de CDC (Debezium)
El conector se publica vía REST API de Kafka Connect:
```bash
curl -s -H "Content-Type: application/json" -X POST \
  http://localhost:8083/connectors \
  -d @config/debezium/postgres-connector.json
```
Verificar:
```bash
curl -s http://localhost:8083/connectors   # -> ["fintech-postgres-oltp"]
```

### 6. Generar datos reales y movimiento
```bash
# Simular transacciones (OLTP)
./venv/bin/python scripts/generator.py --limit 150

# Levantar el Mock API de tarjetas (PII) en segundo plano
./venv/bin/uvicorn api.mock_cards_api:app --host 0.0.0.0 --port 8001 &

# Ingestar tarjetas (anonimiza y publica a Redpanda)
./venv/bin/python scripts/ingest_cards.py --pages 2

# Batch diario -> landing CSV en MinIO
./venv/bin/python scripts/daily_batch_csv.py --rows 200
```

### 7. Correr el worker (escribe Parquet al lakehouse)
```bash
# Solo transacciones CDC
./venv/bin/python scripts/worker.py --topic transactions

# Solo tarjetas (cards.api)
./venv/bin/python scripts/worker.py --topic cards

# Todos los tópicos
./venv/bin/python scripts/worker.py
```

### 8. Verificar
- **Redpanda Console:** http://localhost:8080 (ver tópicos y eventos)
- **MinIO:** http://localhost:9001 (bucket `fintech-lakehouse`, carpetas
  `transactions/`, `cards/`, `landing/cards_daily/`)
- **Metabase:** http://localhost:3000
- **Mock API cards:** http://localhost:8001/cards?page=1&page_size=2
- **Debezium status:** http://localhost:8083/connectors

> Los micro-batches del worker se escriben cuando se acumulan (`BATCH_MAX`,
> default 200) o pasa el `BATCH_TIMEOUT_S` (5 s), configurable vía `.env`.

---

## Qué partes están hechas hasta ahora

### Fase 1 — Infraestructura y entorno (COMPLETA)
- [x] Estructura de carpetas
- [x] Docker Compose (OLTP + streaming)
- [x] Docker Compose (CDC, lakehouse y tooling)
- [x] Conectividad y healthchecks

### Fase 2 — Ingesta y streaming (COMPLETA)
- [x] Diseño de la base OLTP (tablas `users`, `transactions`)
- [x] Simulador de transacciones en tiempo real (`generator.py`)
- [x] Configuración Debezium CDC (tópico `postgres_oltp.public.transactions`)
- [x] Sink worker a Parquet particionado en MinIO (`worker.py`)
- **Validada E2E** en vivo (OLTP -> CDC -> Redpanda -> MinIO).

### Fase 3 — Data Warehouse y dbt (EN PROGRESO)
- [ ] dbt (staging, snapshots, marts, tests) — aún no inicializado
- [x] **Anexo: Ingresos realistas de datos** (plan_ingresos_realistas.txt):
  - Mock API de tarjetas con PII (puerto 8001)
  - Ingestor con masking PII -> tópico `cards.api`
  - Worker multi-tópico -> Parquet en `cards/`
  - Batch diario CSV -> landing en MinIO
  - ítems de verificación 1-13 marcados como hechos

### Fase 4 — Visualización y API (PENDIENTE)
- [ ] Dashboard Metabase / Superset
- [ ] API FastAPI de scoring y fraud alerts
- [ ] CI/CD GitHub Actions

### Fase 5 — Documentación y presentación (EN PROGRESO)
- [ ] Diagrama de arquitectura (placeholder en este README)
- [x] README.md (este archivo)
- [ ] Capturas de pantalla

---

## Estructura de archivos

```
.
├── docker-compose.yml
├── .env.example                 # copiar a .env (nunca commitear .env)
├── .gitignore
├── README.md
├── roadmap_proyecto_fintech.txt  # roadmap con checkboxes de fases
├── plan_ingresos_realistas.txt   # plan de ingresos realistas (PII + batch)
├── config/
│   ├── debezium/postgres-connector.json
│   └── init_oltp/01_init.sql    # DDL OLTP (users, transactions)
├── scripts/
│   ├── generator.py             # simulador de transacciones
│   ├── worker.py                # consumidor multi-tópico -> MinIO
│   ├── ingest_cards.py          # ingesta tarjetas + masking PII
│   ├── daily_batch_csv.py       # batch diario -> landing CSV
│   └── requirements.txt
├── api/
│   └── mock_cards_api.py        # Mock API de tarjetas (FastAPI, 8001)
├── venv/                        # (no commiteado)
└── dbt_fintech/                 # proyecto dbt (a crear en Fase 3)
```

---

*Para seguimiento de pendientes, revisar los checkboxes del
`roadmap_proyecto_fintech.txt` y de `plan_ingresos_realistas.txt`.*