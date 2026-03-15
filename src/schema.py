"""
Схема валидации датасета ЦИАН через Pandera.
Запуск: python src/schema.py
"""

import pandera as pa
import pandas as pd
from pandera import Column, Check, DataFrameSchema
from pathlib import Path


schema = DataFrameSchema(
    {
        "flat_id":      Column(str,   nullable=False, unique=True),
        "price_rub":    Column(float, Check.between(500_000, 2_000_000_000), nullable=True),
        "price_per_m2": Column(float, Check.between(30_000,  5_000_000),     nullable=True),
        "total_area":   Column(float, Check.between(5,       1000),          nullable=True),
        "living_area":  Column(float, Check.between(3,       800),           nullable=True),
        "kitchen_area": Column(float, Check.between(1,       200),           nullable=True),
        "rooms":        Column(float, Check.between(0,       20),            nullable=True),
        "floor":        Column(float, Check.between(0,       200),           nullable=True),
        "floors_total": Column(float, Check.between(1,       200),           nullable=True),
        "build_year":   Column(float, Check.between(1850,    2030),          nullable=True),
        "metro_minutes":Column(float, Check.between(0,       180),           nullable=True),
    },
    strict=False,
)


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Валидирует df, возвращает (чистые, аномалии)."""
    print(f"Входных строк: {len(df)}")

    error_mask = pd.Series(False, index=df.index)

    # Кросс-колоночные проверки
    if "living_area" in df and "total_area" in df:
        m = df["living_area"].notna() & df["total_area"].notna()
        bad = m & (df["living_area"] > df["total_area"])
        if bad.sum():
            print(f"  ⚠️  living_area > total_area: {bad.sum()} строк")
        error_mask |= bad

    if "floor" in df and "floors_total" in df:
        m = df["floor"].notna() & df["floors_total"].notna()
        bad = m & (df["floor"] > df["floors_total"])
        if bad.sum():
            print(f"  ⚠️  floor > floors_total: {bad.sum()} строк")
        error_mask |= bad

    # Pandera проверки
    try:
        schema.validate(df, lazy=True)
        print("  ✅ Все проверки пройдены")
    except pa.errors.SchemaErrors as e:
        failed = e.failure_cases["index"].dropna().unique()
        error_mask.loc[df.index.isin(failed)] = True
        for col, g in e.failure_cases.groupby("schema_context"):
            print(f"  ⚠️  {col}: {len(g)} аномалий")

    df_clean  = df[~error_mask].copy()
    df_errors = df[error_mask].copy()

    print(f"Чистых: {len(df_clean)} | Отсеяно: {len(df_errors)} ({len(df_errors)/len(df)*100:.1f}%)")
    return df_clean, df_errors


if __name__ == "__main__":
    df = pd.read_csv("data/dataset.csv", encoding="utf-8-sig")

    for col in ["price_rub", "price_per_m2", "total_area", "living_area",
                "kitchen_area", "rooms", "floor", "floors_total",
                "build_year", "metro_minutes"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["flat_id"] = df["flat_id"].astype(str)

    df_clean, df_errors = validate(df)

    if len(df_errors):
        df_errors.to_csv("data/anomalies.csv", index=False, encoding="utf-8-sig")
        print(f"Аномалии → data/anomalies.csv")