[ 🇺🇸 [Read in English](README.md) ] | [ 🇨🇱 Español ]

# Modern Lakehouse Analytics Pipeline

![Python](https://img.shields.io/badge/python-3.12-blue?logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/polars-⚡_rust--backed-CD792C)
![DuckDB](https://img.shields.io/badge/duckdb-embedded_OLAP-FFF000?logo=duckdb&logoColor=black)
![PyArrow](https://img.shields.io/badge/pyarrow-columnar-2C5BB4)
![Pydantic](https://img.shields.io/badge/pydantic-v2-E92063?logo=pydantic&logoColor=white)
![Tests](https://github.com/Rxyxs/ecommerce-lakehouse-duckdb/actions/workflows/tests.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-green)

Pipeline de datos moderno para ingesta y analítica de clickstream/e-commerce a escala, construido sobre un **lakehouse** de archivos Parquet particionados: Polars para ETL multihilo, PyArrow para el particionamiento columnar, y DuckDB como motor SQL analítico embebido que consulta el lake directamente sin cargarlo completo a RAM.

## Caso de uso de negocio

Un sitio de e-commerce genera eventos de clickstream (vistas de página, vistas de producto, carritos, checkouts, compras) a un volumen que ya no cabe cómodamente en un `pandas.DataFrame` en memoria. Este proyecto simula ese escenario end-to-end:

1. **Genera** 100,000+ eventos sintéticos con un embudo de conversión realista.
2. **Ingiere** esos eventos: limpia, valida el contrato de esquema, y los persiste como un Data Lake Parquet particionado por fecha.
3. **Analiza** el lake con SQL directo sobre Parquet (DuckDB) para responder preguntas de negocio: ¿dónde se cae la gente en el embudo? ¿qué cohortes retienen mejor? ¿quiénes son los clientes de mayor valor?
4. **Compara** el enfoque contra un pipeline Pandas tradicional, midiendo tiempo y memoria.

## Arquitectura

```
┌───────────────────────────┐
│ synthetic_clickstream.py  │   genera eventos (page_view -> ... -> purchase)
└─────────────┬──────────────┘
              │
              ▼
   data/raw/clickstream_events.parquet   (sin particionar, ~100k+ filas)
              │
              ▼
┌───────────────────────────┐
│      ingestion.py          │   Polars: dedup, sanidad numérica, columnas de partición
│  (contrato Pydantic v2)    │   PyArrow: escritura particionada Hive-style
└─────────────┬──────────────┘
              │
              ▼
   data/lakehouse/events/
   ├── year=2025/month=01/day=01/part-0.parquet
   ├── year=2025/month=01/day=02/part-0.parquet
   └── ...                                  (1 archivo Parquet por día)
              │
              ▼
┌───────────────────────────┐
│       analytics.py         │   DuckDB: SQL directo sobre Parquet, sin cargar todo a RAM
└─────────────┬──────────────┘
              │
   ┌──────────┼───────────┬───────────────┐
   ▼          ▼            ▼               ▼
 Embudo   Cohortes/     LTV top          Ingresos
 conv.    retención     clientes         por categoría
```

## Estructura del proyecto

```
ecommerce-lakehouse-duckdb/
├── data/
│   ├── raw/                          # clickstream_events.parquet (generado, no versionado)
│   └── lakehouse/                    # events/year=/month=/day=/*.parquet (generado, no versionado)
├── src/
│   ├── generators/
│   │   └── synthetic_clickstream.py  # Genera 100,000+ eventos de clickstream sintéticos
│   ├── lakehouse/
│   │   ├── ingestion.py              # Polars + PyArrow: limpieza, contrato de esquema, particionado
│   │   └── analytics.py              # DuckDB: embudo, cohortes, LTV, ingresos por categoría
│   └── benchmark.py                  # Pandas vs. Polars vs. DuckDB: tiempo y RAM
├── tests/
│   └── test_lakehouse.py             # Pruebas de esquema y métricas (pytest)
├── .github/workflows/tests.yml       # CI: corre pytest en cada push/PR
├── requirements.txt
└── README.md
```

## Dataset sintético

`src/generators/synthetic_clickstream.py` simula 8,000 usuarios repartidos en tres segmentos de compromiso (`one_time`, `casual`, `loyal`, con distinto número de sesiones cada uno) a lo largo de ~90 días. Cada sesión progresa por un embudo con caída de conversión realista en cada paso:

```
page_view -> product_view -> add_to_cart -> checkout_start -> purchase
  100%          ~60%             ~38%            ~55%            ~65%
                                                            (condicional al paso anterior)
```

Con la semilla por defecto esto produce **~112,000 eventos** (por encima del mínimo de 100,000 pedido), con columnas: `event_id`, `user_id`, `session_id`, `event_type`, `event_timestamp`, `product_id`, `category`, `price`, `quantity`, `revenue`, `device_type`, `country`, `referrer_source`.

## Módulos

### `src/lakehouse/ingestion.py`
- **Limpieza (Polars)**: deduplica por `event_id`, descarta filas sin `user_id`/`event_timestamp`, anula (no inventa) `price`/`quantity`/`revenue` negativos.
- **Contrato de esquema (Pydantic v2)**: `ClickstreamEventContract` valida que las columnas requeridas existan, y valida por tipo una muestra de filas (no todas — a este volumen, validar fila por fila contradice el propósito de un pipeline de alto rendimiento). Una violación de contrato lanza una excepción: indica un bug de ingestión, no un dato de negocio inválido a aislar.
- **Particionado (PyArrow)**: escribe el resultado como Parquet Hive-partitioned (`year=/month=/day=`), con `existing_data_behavior="delete_matching"` para que reprocesar sea idempotente.

### `src/lakehouse/analytics.py`
Cuatro consultas de negocio, todas SQL puro ejecutado por DuckDB directamente sobre los archivos Parquet (`read_parquet(..., hive_partitioning=true)`):
- `conversion_funnel` — sesiones que alcanzan cada paso del embudo.
- `cohort_retention` — usuarios activos por mes, agrupados por el mes de su primer evento.
- `customer_ltv` — top clientes por ingresos totales de compra.
- `revenue_by_category` — ingresos y ticket promedio por categoría.

### `src/benchmark.py`
Corre la misma agregación (ingresos por categoría, solo compras) con Pandas, Polars y DuckDB sobre el mismo archivo Parquet, y mide tiempo (`time.perf_counter`) y delta de memoria RSS del proceso (`psutil`).

## Resultados del benchmark

Corrida real sobre ~112,000 eventos (ver metodología abajo):

| Motor   | Tiempo (s) | Δ RAM (MB) |
|---------|-----------:|-----------:|
| DuckDB  |      ~0.04 |        ~6  |
| Polars  |      ~0.05 |       ~37  |
| Pandas  |      ~0.12 |       ~60  |

DuckDB gana en ambas dimensiones porque nunca materializa el archivo completo como objeto Python — agrega directamente sobre el escaneo de Parquet. Polars es ~2.5x más rápido que Pandas incluso materializando el DataFrame completo, gracias a su motor multihilo en Rust.

**Metodología y limitaciones honestas**: el tiempo es una sola corrida por motor (`time.perf_counter`); para un benchmark riguroso correspondería promediar varias corridas. La memoria es un delta de RSS del proceso *actual* entre antes/después de cada corrida (con `gc.collect()` de por medio) — no un pico aislado por subproceso, así que es una aproximación de orden de magnitud, no un profiling de memoria estricto. Reproducí los números tres veces (`python -m src.benchmark`) antes de documentarlos y el ordenamiento relativo fue estable entre corridas.

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate      # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Uso

```bash
# 1. Generar el dataset sintético (data/raw/clickstream_events.parquet)
python -m src.generators.synthetic_clickstream

# 2. Ingerir: limpiar, validar contrato, escribir el lake particionado
python -m src.lakehouse.ingestion

# 3. Correr las consultas analíticas sobre el lake
python -m src.lakehouse.analytics

# 4. Comparar Pandas vs. Polars vs. DuckDB
python -m src.benchmark

# Pruebas unitarias
pytest tests/
```

## Stack técnico

| Herramienta | Rol |
|---|---|
| **Polars** | ETL — limpieza y transformación multihilo respaldada en Rust |
| **DuckDB** | Motor SQL analítico embebido, consulta Parquet sin cargarlo a RAM |
| **PyArrow** | Escritura de Parquet particionado (Hive-style) |
| **Pydantic v2** | Contrato de esquema del evento de clickstream |
| **pytest** | Pruebas unitarias de limpieza, contrato de esquema y métricas |
| **pandas / psutil** | Solo en `benchmark.py`, como baseline de comparación y medición de RAM |
