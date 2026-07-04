"""Reference specifications for the synthetic perishables dataset.

Everything about *what the world looks like* — the perishable categories and
their shelf-life / demand characteristics, store locations, ordering behaviour —
lives here as data. The generator logic in ``generate.py`` stays about *process*,
not magic numbers, so a reviewer can retune the world in one place.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CategorySpec:
    """Characteristics of one perishable category.

    Ranges are (min, max) inclusive and are sampled per-SKU so that no two
    products behave identically.
    """

    category: str
    shelf_life_days: tuple[int, int]      # how long a unit stays sellable
    base_daily_demand: tuple[float, float]  # avg units/day at a mid-size store
    unit_price: tuple[float, float]         # retail price, USD
    gross_margin: float                     # unit_cost = price * (1 - margin)


# Shorter shelf life + higher demand = more interesting risk dynamics.
# Seafood and prepared foods are the classic spoilage-vs-stockout tightropes.
CATEGORY_SPECS: list[CategorySpec] = [
    CategorySpec("Produce",        (3, 10), (8, 40),  (0.99, 6.99),  0.35),
    CategorySpec("Dairy",          (7, 21), (6, 30),  (1.49, 8.99),  0.30),
    CategorySpec("Meat",           (3, 7),  (4, 20),  (4.99, 19.99), 0.28),
    CategorySpec("Seafood",        (1, 3),  (2, 12),  (6.99, 24.99), 0.30),
    CategorySpec("Bakery",         (2, 5),  (5, 25),  (1.99, 9.99),  0.45),
    CategorySpec("Deli",           (3, 7),  (4, 18),  (2.99, 12.99), 0.38),
    CategorySpec("Prepared Foods", (1, 3),  (6, 28),  (3.99, 14.99), 0.40),
]

# A small pool of realistic-sounding SKU name parts per category.
PRODUCT_NAME_PARTS: dict[str, tuple[list[str], list[str]]] = {
    "Produce":        (["Organic", "Fresh", "Local"], ["Strawberries", "Baby Spinach", "Avocados", "Blueberries", "Kale"]),
    "Dairy":          (["Grass-Fed", "Organic", "Whole"], ["Milk", "Greek Yogurt", "Butter", "Cheddar", "Cottage Cheese"]),
    "Meat":           (["Grass-Fed", "Air-Chilled", "Prime"], ["Ground Beef", "Chicken Breast", "Pork Chops", "Ribeye"]),
    "Seafood":        (["Wild-Caught", "Fresh", "Atlantic"], ["Salmon Fillet", "Shrimp", "Cod", "Sea Scallops"]),
    "Bakery":         (["Artisan", "Sourdough", "Whole-Grain"], ["Baguette", "Croissants", "Ciabatta", "Muffins"]),
    "Deli":           (["Sliced", "House", "Smoked"], ["Turkey", "Roast Beef", "Prosciutto", "Hummus"]),
    "Prepared Foods": (["Chef's", "House-Made", "Fresh"], ["Caesar Salad", "Poke Bowl", "Soup", "Sushi Roll"]),
}

# (city, state, region) — a spread across US regions.
STORE_LOCATIONS: list[tuple[str, str, str]] = [
    ("Austin", "TX", "South"),
    ("Dallas", "TX", "South"),
    ("Atlanta", "GA", "South"),
    ("New York", "NY", "Northeast"),
    ("Jersey City", "NJ", "Northeast"),
    ("Boston", "MA", "Northeast"),
    ("Chicago", "IL", "Midwest"),
    ("Minneapolis", "MN", "Midwest"),
    ("Seattle", "WA", "West"),
    ("San Francisco", "CA", "West"),
    ("Denver", "CO", "West"),
    ("Los Angeles", "CA", "West"),
]

STORE_FORMATS: list[str] = ["Whole Foods Market", "Amazon Fresh"]

# Grocery demand skews to the weekend. Keyed by weekday() (Mon=0 ... Sun=6).
DOW_DEMAND_MULTIPLIER: dict[int, float] = {
    0: 0.90, 1: 0.85, 2: 0.90, 3: 0.95, 4: 1.10, 5: 1.35, 6: 1.25,
}

# Each store x SKU gets an ordering personality. This is the knob that
# *guarantees* the dataset contains both failure modes the platform detects.
#   overstock  -> orders too much / too infrequently -> spoilage risk
#   understock -> orders too little -> stockout risk
RISK_PROFILE_WEIGHTS: dict[str, float] = {
    "balanced": 0.70,
    "overstock": 0.15,
    "understock": 0.15,
}

# Multiplier applied to the target stock level per profile.
ORDER_BIAS: dict[str, float] = {
    "balanced": 1.00,
    "overstock": 1.45,
    "understock": 0.70,
}

# Share of SKUs whose shelf-life spec was revised at some point -> gives the
# shelf-life dimension a genuine SCD Type-2 history to model downstream.
SCD2_REVISION_SHARE: float = 0.10