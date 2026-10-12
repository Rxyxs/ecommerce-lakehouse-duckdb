[ 🇺🇸 [Read in English](README.md) ] | [ 🇨🇱 Español ]

# Lakehouse de e-commerce sobre 110 millones de eventos reales

![Python](https://img.shields.io/badge/python-3.12-blue?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/duckdb-embedded_OLAP-FFF000?logo=duckdb&logoColor=black)
![Polars](https://img.shields.io/badge/polars-lazy-CD792C)
![PySpark](https://img.shields.io/badge/pyspark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Tests](https://github.com/Rxyxs/ecommerce-lakehouse-duckdb/actions/workflows/tests.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-green)

Dos meses de clickstream de una tienda online multicategoría real, 110 millones de eventos en 14,7 GB de CSV, convertidos en un lake Parquet particionado por día con DuckDB y consultados en el lugar. Antes de reportar un solo embudo o cifra de ingresos, el pipeline revisa si el registro de eventos es estable, y en este dataset no lo es: **las compras dejan de registrarse durante 48 horas de alto tráfico a mediados de noviembre, y los eventos de carrito se registran bajo seis regímenes distintos**. Un embudo calculado sobre los dos meses juntos estaría midiendo el tracking, no a los clientes.

## Datos

[eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) (Kaggle, datos del proyecto REES46 / Open CDP), archivos `2019-Oct.csv` y `2019-Nov.csv`, descargados el 2026-10-08. Cada fila es un evento `view`, `cart` o `purchase` con producto, categoría, marca, precio, usuario y sesión.

**Licencia: "copyright-authors".** Los datos crudos no están en este repositorio ni se redistribuyen; `src/sources.py` los descarga con tu propio token de Kaggle. Aquí solo se publican agregados calculados a partir de ellos (`reports/*.json` y las figuras).

## Pipeline

```
2019-Oct.csv + 2019-Nov.csv  (14,7 GB)
        │  src/sources.py — descarga por la API de Kaggle
        ▼
src/lakehouse/ingestion.py — DuckDB transmite CSV → Parquet, limpieza en SQL
        │  duplicados exactos eliminados, precios <= 0 anulados, eventos sin sesión
        │  eliminados, top_category derivada, contrato pydantic sobre una muestra
        ▼
data/lakehouse/events/year=2019/month=10/day=01/…   (61 particiones, ZSTD)
        │
        ▼
src/lakehouse/analytics.py — huecos de captura → regímenes de tracking → embudos,
        │                     ingresos, concentración, retención (todo SQL sobre el lake)
        ▼
reports/results.json → src/make_figures.py → outputs/figures/
src/benchmark.py — la misma consulta en pandas, Polars, DuckDB y PySpark
```

Construir el lake desde los dos CSV toma unos 3,5 minutos en un PC de 8 núcleos sin cargar los datos en memoria; cada consulta analítica de abajo corre en menos de un minuto sobre los 110 millones de filas.

## Qué contienen los datos crudos

| Revisión | Eventos |
|---|---:|
| Eventos crudos | 109.950.743 |
| Filas duplicadas exactas (eliminadas) | 130.739 |
| Precio cero o negativo (precio anulado, evento conservado) | 256.761 |
| Sin id de sesión (eliminados) | 12 |
| Sin `category_code` (conservados como `uncoded`) | 35.413.780 |
| **Eventos en el lake** | **109.819.992** |

Un tercio de los eventos no tiene código de categoría. La descripción del dataset dice que la tienda solo codifica las categorías "relevantes"; se conservan como grupo propio `uncoded` en vez de descartarlos, porque llevan el 10,2% de los ingresos.

## Hallazgo 1: faltan compras durante cuatro días

![Vistas y compras diarias, con los huecos de captura sombreados](outputs/figures/daily_capture.png)

Una hora de esta tienda con al menos 5.000 eventos y cero compras no es una hora tranquila; las horas normales más bajas igual registran compras. Con esa regla, **48 horas entre el 14 y el 17 de noviembre son huecos de captura**: el 15 de noviembre tiene 468 mil eventos de carrito y ninguna compra, el 16 registra compras solo entre las 08:00 y las 15:00 UTC, y el 17 solo desde las 03:00. Esos días además tienen 3,5 veces las vistas diarias normales: hubo un evento de ventas real y el registro de compras falló durante él.

La consecuencia es que el "día récord" de estos datos, el 17 de noviembre con 57,8 millones en ingresos (8 veces la mediana diaria de 7,16 millones), no se puede leer como tal: una parte es demanda, otra pueden ser compras de las horas faltantes registradas tarde, y los datos no permiten separarlas. Los cuatro días quedan marcados y fuera de los embudos, de la participación por categoría y de la concentración.

## Hallazgo 2: el tracking de carritos cambia seis veces

![Eventos de carrito por compra con los regímenes detectados](outputs/figures/cart_regimes.png)

Las compras diarias son planas en los dos meses, pero los eventos de carrito por compra saltan de un día para otro: 0,89, luego 1,49, 0,80, 1,49, 0,80 y finalmente 2,87 desde el 8 de noviembre. Una segmentación binaria sobre el logaritmo del ratio diario encuentra exactamente esos seis regímenes (un corte se acepta solo si separa niveles que difieren al menos 30%). Los clientes no cambian sus hábitos de carrito de un día para otro, y la alternancia entre dos niveles parece una segunda fuente de eventos de carrito que se enciende y se apaga.

![Embudo de sesiones por régimen de tracking](outputs/figures/funnel_by_regime.png)

El embudo por sesión muestra el efecto. Hasta el 7 de noviembre, **entre el 44% y el 67% de las sesiones con compra no tienen ningún evento de carrito**, según el régimen; desde el 8 de noviembre es el 1,2%. El paso de carrito recién se volvió un paso confiable del embudo el 8 de noviembre. Antes de eso, una tasa de conversión "vista → carrito → compra" habría dividido por un número que el tracking no estaba registrando.

## Hallazgo 3: quién compra

![Ingresos por categoría y concentración de compradores](outputs/figures/revenue_concentration.png)

- **Electrónica es el 76,9% de los ingresos**, luego `uncoded` (10,2%), electrodomésticos (5,6%) y computadores (4,8%).
- **El gasto está concentrado**: el 1% superior de 589.820 compradores explica el 21,5% de los ingresos y el 10% superior el 57,7% (Gini 0,70).
- **El 46,4% de los visitantes de octubre vuelve en noviembre**; de los compradores de octubre, el 26,3% vuelve a comprar en noviembre. Esa segunda cifra está sesgada a la baja por los huecos de noviembre y se reporta como cota inferior.
- El 35,7% de los compradores compra en más de una sesión.

## Benchmark de motores

`src/benchmark.py` corre una consulta, ingresos, compras y compradores distintos por categoría sobre las compras, en cada motor y cada uno en un subproceso nuevo. La memoria es el pico residente de ese proceso más sus hijos, muestreado cada 50 ms, así que incluye la JVM que levanta PySpark. Una corrida en frío y luego la mejor de tres en caliente; el arranque de la sesión de Spark (unos 2,7 s) se mide aparte. Los cuatro motores dan la misma respuesta al centavo.

![Benchmark de motores](outputs/figures/engine_benchmark.png)

| Alcance | Motor | Frío (s) | Caliente (s) | Memoria pico (GB) |
|---|---|---:|---:|---:|
| octubre | pandas | 4,12 | 3,84 | 3,65 |
| octubre | polars | 0,43 | 0,15 | 0,44 |
| octubre | duckdb | 0,68 | 0,23 | 0,19 |
| octubre | pyspark | 7,84 | 1,44 | 3,87 |
| completo | pandas | 10,67 | 10,30 | 8,35 |
| completo | polars | 0,50 | 0,30 | 0,66 |
| completo | duckdb | 0,94 | 0,55 | 0,30 |
| completo | pyspark | 9,54 | 2,38 | 6,77 |

- **Polars y DuckDB responden en bastante menos de un segundo sobre 110 millones de filas**, porque ambos leen solo las cuatro columnas necesarias y empujan el filtro de compras al escaneo del Parquet. Polars es más rápido en caliente (0,30 s contra 0,55 s en el lake completo); DuckDB usa la mitad de memoria (0,30 GB contra 0,66 GB).
- **pandas es 35 veces más lento que Polars en el lake completo** y llega a 8,35 GB: tiene que materializar cada fila de las cuatro columnas como objetos de Python antes de poder filtrar. Su memoria crece con las filas que lee: 3,65 GB para octubre y 8,35 GB para los dos meses.
- **PySpark es el más lento de los tres motores columnares aquí** y llega a 6,77 GB incluida su JVM. Es lo esperable en una sola máquina: el costo de Spark es coordinación y serialización, que solo se paga cuando los datos ya no caben en un nodo. Este benchmark muestra el piso en un nodo, no el caso a favor de Spark.
- La consulta es selectiva (las compras son el 1,5% de los eventos), lo que favorece a los motores con filter pushdown. Una consulta que deba leer todas las filas, o un join grande, es otra prueba y podría ordenar distinto a Polars, DuckDB y Spark; no se corrió aquí.

## Reproducir

```bash
python -m venv .venv
.venv\Scripts\activate           # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

python -m src.sources              # requiere un token de la API de Kaggle en ~/.kaggle
python -m src.lakehouse.ingestion  # construye el lake, escribe reports/ingestion.json
python -m src.main                 # todas las métricas -> reports/results.json -> figuras
python -m src.benchmark            # opcional, ~15 min; PySpark requiere JAVA_HOME
pytest
```

Los tests corren sobre un fixture de 12 filas en el formato de REES46, así que el CI no necesita el dataset ni Java. `tests/test_readme.py` verifica las cifras citadas en este README contra `reports/results.json`.

## Limitaciones

- La regla de huecos es un umbral: un hueco de menos de una hora, o en una hora de poco tráfico, pasaría inadvertido. Está ajustada al volumen de esta tienda.
- La detección de regímenes usa ratios diarios; un cambio de tracking a mitad de día queda fechado en el borde de día más cercano.
- Los precios están en la moneda de la tienda tal como se publicaron, sin conversión ni deflactación.
- Las sesiones son los ids que asignó la tienda; no se re-sesionizó por inactividad.
