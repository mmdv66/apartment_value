"""
Анализ признаков собранного CSV.
Запуск: python analyze_features.py
"""

import pandas as pd
import os

CSV_PATH = "data/raw/flats_data.csv"

if not os.path.exists(CSV_PATH):
    print(f"[!] Файл не найден: {CSV_PATH}")
    exit()

df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

print("=" * 65)
print(f"Объявлений : {len(df)}")
print(f"Признаков  : {len(df.columns)}")
print("=" * 65)

# Группируем колонки по смыслу
groups = {
    "💰 Цена": ["price_rub", "price_per_m2", "price_raw"],
    "📐 Площадь": ["total_area", "living_area", "kitchen_area",
                   "living_ratio", "kitchen_ratio", "non_living_area"],
    "🚪 Комнаты": ["rooms", "is_studio"],
    "🏢 Этаж": ["floor", "floors_total", "is_first_floor",
                "is_last_floor", "floor_ratio", "floors_above"],
    "🏗  Дом": ["build_year", "building_age", "building_type",
                "building_type_cat", "ceiling_height"],
    "🔧 Удобства": ["renovation", "renovation_cat", "has_elevator_bin",
                    "has_parking_obj", "has_balcony_obj", "bathroom_separate",
                    "balcony_type", "elevator", "parking_type", "heating", "windows"],
    "📝 NLP описания": ["desc_len", "desc_word_count", "has_balcony",
                        "has_loggia", "has_parking", "has_furniture",
                        "has_renovated", "has_mortgage", "has_new_building",
                        "has_view", "has_alarm"],
    "🚇 Метро": ["metro_station", "metro_minutes", "metro_by_foot"],
    "📍 Гео": ["district", "address"],
    "📸 Фото": ["image_count", "image_urls"],
}

for group_name, cols in groups.items():
    present = [c for c in cols if c in df.columns]
    if not present:
        continue
    print(f"\n{group_name}")
    print(f"{'Признак':<22} {'Заполнен%':>9}  {'Пример значения'}")
    print("-" * 65)
    for col in present:
        filled = df[col].notna().sum()
        pct = filled / len(df) * 100
        # Пример — первое непустое значение
        sample = df[col].dropna().iloc[0] if filled > 0 else "—"
        if isinstance(sample, float):
            sample = f"{sample:.2f}"
        sample_str = str(sample)[:35]
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        print(f"  {col:<20} {pct:>5.1f}%  {bar}  {sample_str}")

# Числовые признаки — быстрая статистика
num_cols = [c for c in [
    "price_rub", "price_per_m2", "total_area", "living_area",
    "kitchen_area", "rooms", "floor", "floors_total",
    "build_year", "building_age", "ceiling_height",
    "metro_minutes", "desc_len", "image_count",
] if c in df.columns]

if num_cols:
    print("\n\n📊 Статистика числовых признаков:")
    print("=" * 65)
    stats = df[num_cols].describe().T[["count", "mean", "min", "50%", "max"]]
    stats.columns = ["count", "mean", "min", "median", "max"]
    stats["count"] = stats["count"].astype(int)
    print(stats.to_string(float_format=lambda x: f"{x:.1f}"))

# Пропуски — топ проблемных колонок
print("\n\n⚠️  Колонки с наибольшим % пропусков:")
print("=" * 65)
missing = (df.isnull().sum() / len(df) * 100).sort_values(ascending=False)
missing = missing[missing > 0]
for col, pct in missing.head(15).items():
    bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
    print(f"  {col:<25} {pct:>5.1f}%  {bar}")

print("\n✅ Готово к использованию в ML:")
ml_ready = [c for c in [
    "price_rub", "price_per_m2", "total_area", "living_area",
    "kitchen_area", "living_ratio", "kitchen_ratio", "non_living_area",
    "rooms", "is_studio", "floor", "floors_total",
    "is_first_floor", "is_last_floor", "floor_ratio", "floors_above",
    "build_year", "building_age", "building_type_cat", "ceiling_height",
    "renovation_cat", "has_elevator_bin", "has_parking_obj", "has_balcony_obj",
    "bathroom_separate", "desc_len", "desc_word_count",
    "has_balcony", "has_loggia", "has_parking", "has_furniture",
    "has_renovated", "has_mortgage", "has_new_building", "has_view", "has_alarm",
    "metro_minutes", "metro_by_foot", "image_count",
] if c in df.columns]
print(f"  {len(ml_ready)} числовых/бинарных признаков готовы без доп. обработки:")
print(f"  {ml_ready}")