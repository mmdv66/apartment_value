

import re
import pandas as pd
from pathlib import Path

ROOT        = Path(__file__).parent.parent
RAW_CSV     = ROOT / "data" / "raw" / "flats_data.csv"
IMAGES_DIR  = ROOT / "data" / "raw" / "images"
OUT_CSV     = ROOT / "data" / "dataset.csv"
OUT_IMG_CSV = ROOT / "data" / "dataset_images.csv"

# ══════════════════════════════════════════════════════════════════
# Загрузка
# ══════════════════════════════════════════════════════════════════

df = pd.read_csv(RAW_CSV, encoding="utf-8-sig")
print(f"Загружено: {len(df)} объявлений, {len(df.columns)} колонок")
print(f"Колонки: {list(df.columns)}\n")
n0 = len(df)


# ══════════════════════════════════════════════════════════════════
# 1. Дубликаты по flat_id
# ══════════════════════════════════════════════════════════════════

df = df.drop_duplicates(subset=["flat_id"]).copy()
print(f"[1] Дедупликация:         {n0} → {len(df)}  (удалено {n0 - len(df)})")


# ══════════════════════════════════════════════════════════════════
# 2. Цена — только явный мусор
#    Удаляем только: 0, отрицательные, > 10 млрд
#    Остальное (пропуски) — оставляем
# ══════════════════════════════════════════════════════════════════

# Парсер уже пишет числа, но страхуемся от строк
price_col = "price_rub" if "price_rub" in df.columns else "price"

if price_col in df.columns:
    df[price_col] = (
        df[price_col]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .pipe(pd.to_numeric, errors="coerce")
    )
    bad_price = df[price_col].notna() & ~df[price_col].between(500_000, 10_000_000_000)
    df.loc[bad_price, price_col] = pd.NA
    print(f"[2] Цена: {bad_price.sum()} аномальных значений → NaN (строки сохранены)")

if "price_per_m2" in df.columns:
    df["price_per_m2"] = pd.to_numeric(df["price_per_m2"], errors="coerce")
    bad = df["price_per_m2"].notna() & ~df["price_per_m2"].between(10_000, 5_000_000)
    df.loc[bad, "price_per_m2"] = pd.NA


# ══════════════════════════════════════════════════════════════════
# 3. Площади → float, аномалии → NaN (строки не удаляем)
# ══════════════════════════════════════════════════════════════════

def clean_float(val):
    if pd.isna(val):
        return pd.NA
    cleaned = re.sub(r"[^\d,\.]", "", str(val)).replace(",", ".")
    v = pd.to_numeric(cleaned, errors="coerce")
    return v if pd.notna(v) else pd.NA

for col in ["total_area", "living_area", "kitchen_area"]:
    if col not in df.columns:
        continue
    df[col] = df[col].apply(clean_float).astype("Float64")

if "total_area" in df.columns:
    bad = df["total_area"].notna() & ~df["total_area"].between(5, 2000)
    df.loc[bad, "total_area"] = pd.NA
    print(f"[3] Площадь: {bad.sum()} аномалий → NaN")

if "living_area" in df.columns:
    bad = df["living_area"].notna() & ~df["living_area"].between(3, 1500)
    df.loc[bad, "living_area"] = pd.NA

if "kitchen_area" in df.columns:
    bad = df["kitchen_area"].notna() & ~df["kitchen_area"].between(1, 200)
    df.loc[bad, "kitchen_area"] = pd.NA


# ══════════════════════════════════════════════════════════════════
# 4. Этаж → int, пересчитываем производные если нужно
# ══════════════════════════════════════════════════════════════════

for col in ["floor", "floors_total"]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int16")
        bad = df[col].notna() & ~df[col].between(0, 200)
        df.loc[bad, col] = pd.NA

# Пересчитываем производные — парсер уже пишет их, но пересчитаем для надёжности
if "floor" in df.columns and "floors_total" in df.columns:
    valid = df["floor"].notna() & df["floors_total"].notna() & (df["floors_total"] > 0)
    df["floor_ratio"]    = pd.NA
    df["is_first_floor"] = pd.NA
    df["is_last_floor"]  = pd.NA
    df["floors_above"]   = pd.NA
    df.loc[valid, "floor_ratio"]    = (df.loc[valid, "floor"] / df.loc[valid, "floors_total"]).round(3)
    df.loc[valid, "is_first_floor"] = (df.loc[valid, "floor"] == 1).astype(int)
    df.loc[valid, "is_last_floor"]  = (df.loc[valid, "floor"] == df.loc[valid, "floors_total"]).astype(int)
    df.loc[valid, "floors_above"]   = df.loc[valid, "floors_total"] - df.loc[valid, "floor"]

print(f"[4] Этаж: обработан, строк: {len(df)}")


# ══════════════════════════════════════════════════════════════════
# 5. Год постройки
# ══════════════════════════════════════════════════════════════════

if "build_year" in df.columns:
    df["build_year"] = pd.to_numeric(
        df["build_year"].astype(str).str.extract(r"((?:19|20)\d{2})")[0],
        errors="coerce",
    ).astype("Int16")
    bad = df["build_year"].notna() & ~df["build_year"].between(1850, 2030)
    df.loc[bad, "build_year"] = pd.NA
    print(f"[5] Год постройки: {bad.sum()} аномалий → NaN")

if "building_age" in df.columns:
    df["building_age"] = pd.to_numeric(df["building_age"], errors="coerce").astype("Int16")


# ══════════════════════════════════════════════════════════════════
# 6. Комнаты
# ══════════════════════════════════════════════════════════════════

if "rooms" in df.columns:
    df["rooms"] = pd.to_numeric(df["rooms"], errors="coerce").astype("Int8")
    bad = df["rooms"].notna() & ~df["rooms"].between(0, 20)
    df.loc[bad, "rooms"] = pd.NA


# ══════════════════════════════════════════════════════════════════
# 7. Категориальные признаки → коды (оригинал сохраняем)
# ══════════════════════════════════════════════════════════════════

RENOVATION_MAP = {
    "без ремонта": 0, "требует ремонта": 0,
    "косметический": 1,
    "евроремонт": 2, "хорошее": 2,
    "дизайнерский": 3, "под ключ": 2,
}

BUILDING_TYPE_MAP = {
    "панельный": 0, "блочный": 1, "кирпичный": 2,
    "монолитный": 3, "монолитно-кирпичный": 4, "деревянный": 5,
}

if "renovation" in df.columns:
    df["renovation"] = df["renovation"].astype(str).str.strip().replace("nan", pd.NA)
    df["renovation_cat"] = df["renovation"].str.lower().map(RENOVATION_MAP)

if "building_type" in df.columns:
    df["building_type"] = df["building_type"].astype(str).str.strip().replace("nan", pd.NA)
    df["building_type_cat"] = df["building_type"].str.lower().map(
        {k: v for bt in [BUILDING_TYPE_MAP] for k, v in bt.items()}
    )
    # Ещё добавим через pd.Categorical для всех значений
    df["building_type_code"] = pd.Categorical(
        df["building_type"].fillna("unknown")
    ).codes


# ══════════════════════════════════════════════════════════════════
# 8. Метро — числовые поля
# ══════════════════════════════════════════════════════════════════

if "metro_minutes" in df.columns:
    df["metro_minutes"] = pd.to_numeric(df["metro_minutes"], errors="coerce").astype("Int16")
    bad = df["metro_minutes"].notna() & ~df["metro_minutes"].between(0, 240)
    df.loc[bad, "metro_minutes"] = pd.NA

if "metro_by_foot" in df.columns:
    df["metro_by_foot"] = pd.to_numeric(df["metro_by_foot"], errors="coerce").astype("Int8")


# ══════════════════════════════════════════════════════════════════
# 9. Текстовые поля — убираем "nan" строки
# ══════════════════════════════════════════════════════════════════

text_cols = ["address", "metro_station", "metro_raw", "district",
             "description", "building_type", "renovation",
             "windows", "bathroom", "balcony_type", "elevator",
             "parking_type", "heating"]

for col in text_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})


# ══════════════════════════════════════════════════════════════════
# 10. Потолки и ceiling_height
# ══════════════════════════════════════════════════════════════════

if "ceiling_height" in df.columns:
    df["ceiling_height"] = pd.to_numeric(df["ceiling_height"], errors="coerce").astype("Float32")
    bad = df["ceiling_height"].notna() & ~df["ceiling_height"].between(1.5, 10)
    df.loc[bad, "ceiling_height"] = pd.NA


# ══════════════════════════════════════════════════════════════════
# 11. Фото — НЕ удаляем строки без фото, просто фиксируем
# ══════════════════════════════════════════════════════════════════

def get_existing_paths(flat_id) -> str:
    ad_dir = IMAGES_DIR / str(flat_id)
    if not ad_dir.exists():
        return ""
    files = (
        sorted(ad_dir.glob("*.jpg")) +
        sorted(ad_dir.glob("*.png")) +
        sorted(ad_dir.glob("*.webp"))
    )
    valid = [str(f) for f in files if f.stat().st_size > 2_000]
    return "; ".join(valid)

if IMAGES_DIR.exists():
    print("[11] Проверяем скачанные фото...")
    df["image_paths_local"] = df["flat_id"].apply(get_existing_paths)
    df["image_count_local"]  = df["image_paths_local"].apply(
        lambda x: len(x.split("; ")) if x else 0
    )
    has_photos = (df["image_count_local"] > 0).sum()
    print(f"     Объявлений с фото на диске: {has_photos} / {len(df)}")
else:
    df["image_paths_local"] = ""
    df["image_count_local"]  = 0
    print("[11] Папка images/ не найдена — image_paths_local пустые (строки сохранены)")


# ══════════════════════════════════════════════════════════════════
# 12. Итоговый порядок колонок
# ══════════════════════════════════════════════════════════════════

PRIORITY_COLS = [
    # Идентификация
    "flat_id", "url", "scraped_at",
    # Таргет
    "price_rub", "price_per_m2",
    # Планировка
    "rooms", "is_studio", "total_area", "living_area", "kitchen_area",
    "living_ratio", "kitchen_ratio", "non_living_area",
    # Этаж
    "floor", "floors_total", "is_first_floor", "is_last_floor",
    "floor_ratio", "floors_above",
    # Дом
    "build_year", "building_age", "building_type", "building_type_cat",
    "building_type_code", "ceiling_height",
    # Ремонт / удобства
    "renovation", "renovation_cat",
    "has_elevator_bin", "has_parking_obj", "has_balcony_obj", "bathroom_separate",
    "elevator", "parking_type", "balcony_type", "bathroom", "heating", "windows",
    # NLP из описания
    "desc_len", "desc_word_count",
    "has_balcony", "has_loggia", "has_parking", "has_furniture",
    "has_renovated", "has_mortgage", "has_new_building", "has_view", "has_alarm",
    # Метро / гео
    "metro_station", "metro_minutes", "metro_by_foot", "district", "address",
    # Фото
    "image_count", "image_count_local", "image_paths_local", "image_urls",
    # Текст
    "description",
    # Сырые поля (для отладки)
    "price_raw", "floor_raw", "rooms_raw", "build_year_raw",
    "metro_raw", "factoids_raw",
]

existing    = [c for c in PRIORITY_COLS if c in df.columns]
rest        = [c for c in df.columns if c not in existing]
df          = df[existing + rest]


# ══════════════════════════════════════════════════════════════════
# 13. Сохранение
# ══════════════════════════════════════════════════════════════════

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

print(f"\n{'═'*55}")
print(f"Итого объявлений : {len(df)}")
print(f"Колонок          : {len(df.columns)}")
print(f"Сохранено        : {OUT_CSV}")

# Статистика заполненности
print(f"\n{'─'*55}")
print("Заполненность ключевых признаков:")
key_cols = [
    "price_rub", "price_per_m2", "total_area", "living_area",
    "kitchen_area", "rooms", "floor", "floors_total",
    "build_year", "building_type", "renovation", "ceiling_height",
    "metro_minutes", "district", "image_count",
]
for col in key_cols:
    if col not in df.columns:
        continue
    filled = df[col].notna().sum()
    pct    = filled / len(df) * 100
    bar    = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
    print(f"  {col:<22} {pct:>5.1f}%  {bar}")

# Числовая статистика
num_cols = [c for c in [
    "price_rub", "price_per_m2", "total_area", "rooms",
    "floor", "floors_total", "build_year",
] if c in df.columns]
if num_cols:
    print(f"\n{'─'*55}")
    print("Числовая статистика:")
    stats = df[num_cols].describe().T[["count", "mean", "min", "50%", "max"]]
    stats.columns = ["count", "mean", "min", "median", "max"]
    print(stats.to_string(float_format=lambda x: f"{x:.1f}"))


# ── dataset_images.csv (только если есть фото) ───────────────────
if IMAGES_DIR.exists() and (df["image_count_local"] > 0).any():
    df_img = df[df["image_paths_local"] != ""].copy()
    df_img["image_path"] = df_img["image_paths_local"].str.split("; ")
    df_img = df_img.explode("image_path")
    df_img = df_img[df_img["image_path"].notna() & (df_img["image_path"] != "")]

    drop_cols = ["description", "factoids_raw", "image_urls", "image_paths_local"]
    df_img = df_img.drop(columns=[c for c in drop_cols if c in df_img.columns])

    df_img.to_csv(OUT_IMG_CSV, index=False, encoding="utf-8-sig")
    print(f"\n>>> dataset_images.csv → {OUT_IMG_CSV}  ({len(df_img)} строк = фото)")
else:
    print("\n>>> dataset_images.csv не создан (фото не скачаны)")


df.to_parquet("data/dataset.parquet", index=False)

df_img.to_parquet("data/dataset_images.parquet", index = False)