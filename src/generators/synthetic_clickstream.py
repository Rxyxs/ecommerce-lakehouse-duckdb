"""Generador de eventos sintéticos de clickstream/e-commerce a escala (100,000+ eventos).

Simula sesiones de usuario con un embudo de conversión realista (page_view ->
product_view -> add_to_cart -> checkout_start -> purchase), con caída de conversión
en cada paso, para poder calcular métricas de negocio significativas (embudo,
cohortes, LTV) en `src/lakehouse/analytics.py`.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "clickstream_events.parquet"

RANDOM_SEED = 42
N_USERS = 8_000
N_PRODUCTS = 250
START_DATE = "2025-01-01"
END_DATE = "2025-03-31"  # ~90 dias de historia

CATEGORIES = ["Electronics", "Apparel", "Home & Kitchen", "Beauty", "Sports & Outdoors", "Books"]
CATEGORY_PRICE_RANGE = {
    "Electronics": (30, 1200),
    "Apparel": (10, 150),
    "Home & Kitchen": (15, 400),
    "Beauty": (5, 90),
    "Sports & Outdoors": (10, 300),
    "Books": (8, 45),
}
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.55, 0.35, 0.10]
COUNTRIES = ["US", "MX", "BR", "ES", "AR", "CL", "CO"]
REFERRERS = ["organic_search", "paid_search", "social", "email", "direct"]

# Segmento de compromiso del usuario -> (peso de muestreo, rango de sesiones totales)
ENGAGEMENT_SEGMENTS: dict[str, tuple[float, tuple[int, int]]] = {
    "one_time": (0.40, (1, 1)),
    "casual": (0.35, (2, 5)),
    "loyal": (0.25, (6, 18)),
}

# Probabilidad de avanzar al siguiente paso del embudo dentro de una sesión con
# intención de compra (cada paso es condicional al anterior, como en un funnel real).
FUNNEL_STEP_CONTINUE_PROB = {
    "product_view": 0.60,
    "add_to_cart": 0.38,
    "checkout_start": 0.55,
    "purchase": 0.65,
}


def _build_product_catalog(rng: random.Random, n_products: int = N_PRODUCTS) -> list[dict]:
    catalog = []
    for i in range(n_products):
        category = rng.choice(CATEGORIES)
        low, high = CATEGORY_PRICE_RANGE[category]
        catalog.append({
            "product_id": f"P-{i:04d}",
            "category": category,
            "price": round(rng.uniform(low, high), 2),
        })
    return catalog


def _pick_engagement_segment(rng: random.Random) -> str:
    names = list(ENGAGEMENT_SEGMENTS.keys())
    weights = [ENGAGEMENT_SEGMENTS[n][0] for n in names]
    return rng.choices(names, weights=weights, k=1)[0]


def _random_timestamp(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta_seconds = max(int((end - start).total_seconds()), 1)
    return start + timedelta(seconds=rng.randint(0, delta_seconds))


def _simulate_session(
    rng: random.Random, user_id: str, session_start: datetime, country: str, catalog: list[dict]
) -> list[dict]:
    """Genera los eventos de una sesión: entrada, navegación, y avance por el embudo."""
    events: list[dict] = []
    session_id = str(uuid.uuid4())
    device = rng.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0]
    referrer = rng.choice(REFERRERS)
    clock = {"ts": session_start}

    def emit(event_type: str, product: dict | None = None, quantity: int | None = None) -> None:
        price = product["price"] if product else None
        revenue = round(price * quantity, 2) if (price is not None and quantity) else None
        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "session_id": session_id,
            "event_type": event_type,
            "event_timestamp": clock["ts"],
            "product_id": product["product_id"] if product else None,
            "category": product["category"] if product else None,
            "price": price,
            "quantity": quantity,
            "revenue": revenue,
            "device_type": device,
            "country": country,
            "referrer_source": referrer,
        })
        clock["ts"] = clock["ts"] + timedelta(seconds=rng.randint(15, 240))

    emit("page_view")

    # Navegación sin intención de compra necesaria (0-2 vistas de producto adicionales)
    for _ in range(rng.randint(0, 2)):
        emit("product_view", product=rng.choice(catalog))

    # Embudo de conversión: cada paso depende de haber alcanzado el anterior
    if rng.random() < FUNNEL_STEP_CONTINUE_PROB["product_view"]:
        product = rng.choice(catalog)
        emit("product_view", product=product)
        if rng.random() < FUNNEL_STEP_CONTINUE_PROB["add_to_cart"]:
            emit("add_to_cart", product=product)
            if rng.random() < FUNNEL_STEP_CONTINUE_PROB["checkout_start"]:
                emit("checkout_start", product=product)
                if rng.random() < FUNNEL_STEP_CONTINUE_PROB["purchase"]:
                    emit("purchase", product=product, quantity=rng.randint(1, 3))

    return events


def generate_clickstream(n_users: int = N_USERS, seed: int = RANDOM_SEED) -> pl.DataFrame:
    """Genera el DataFrame sintético completo de eventos de clickstream, ordenado por fecha."""
    rng = random.Random(seed)
    start = datetime.fromisoformat(START_DATE)
    end = datetime.fromisoformat(END_DATE)
    catalog = _build_product_catalog(rng)

    all_events: list[dict] = []
    for u in range(n_users):
        user_id = f"U-{u:06d}"
        country = rng.choice(COUNTRIES)
        segment = _pick_engagement_segment(rng)
        _, (min_sessions, max_sessions) = ENGAGEMENT_SEGMENTS[segment]
        n_sessions = rng.randint(min_sessions, max_sessions)

        first_session_time = _random_timestamp(rng, start, end - timedelta(days=1))
        for s in range(n_sessions):
            session_time = first_session_time if s == 0 else _random_timestamp(rng, first_session_time, end)
            all_events.extend(_simulate_session(rng, user_id, session_time, country, catalog))

    return pl.DataFrame(all_events).sort("event_timestamp")


if __name__ == "__main__":
    df = generate_clickstream()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUTPUT_PATH)

    print(f"Eventos generados: {df.height:,}")
    print(f"Usuarios únicos: {df['user_id'].n_unique():,}")
    print(f"Rango de fechas: {df['event_timestamp'].min()} -> {df['event_timestamp'].max()}")
    print(f"Guardado en {OUTPUT_PATH}")
