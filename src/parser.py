"""
ЦИАН — парсер объявлений о продаже квартир.
Максимум ML-признаков, настраиваемое количество страниц.

Запуск: python cian_parser.py
"""

import asyncio
import os
import re
import json
import time
import pandas as pd
from playwright.async_api import async_playwright
from tqdm import tqdm

# ══════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ — меняй здесь
# ══════════════════════════════════════════════════════════════════

SEARCH_URL = (
    "https://www.cian.ru/cat.php"
    "?deal_type=sale&engine_version=2&offer_type=flat&region=1"
)

MAX_SEARCH_PAGES = 1     # страниц поиска (~28 объявлений на странице)
                            #  5 стр  ≈  140 объявлений  (~10 мин)
                            # 10 стр  ≈  280 объявлений  (~20 мин)
                            # 30 стр  ≈  840 объявлений  (~60 мин)
                            # 100 стр ≈ 2800 объявлений  (~3 ч)

MAX_FLATS = None            # жёсткий лимит на кол-во объявлений
                            # None = без лимита
                            # например: MAX_FLATS = 200

CONCURRENCY = 2             # параллельных вкладок браузера
                            # 2 — безопасно, 3-4 — быстрее, выше — риск бана

DELAY_FLAT  = 2             # пауза между объявлениями (сек)
DELAY_PAGE  = 4             # пауза между страницами поиска (сек)

DATA_DIR    = "data/raw"
OUT_CSV     = os.path.join(DATA_DIR, "flats_data.csv")

# ══════════════════════════════════════════════════════════════════

os.makedirs(DATA_DIR, exist_ok=True)


# ─── Вспомогательные функции ──────────────────────────────────────

def parse_price(text: str) -> dict:
    result = {"price_raw": text, "price_rub": None, "price_per_m2": None}
    if not text:
        return result
    digits = re.sub(r"\s", "", text)
    m = re.search(r"(\d+)", digits)
    if m:
        result["price_rub"] = int(m.group(1))
    return result


def parse_area(text: str):
    if not text:
        return None
    m = re.search(r"([\d.,]+)", str(text).replace(",", "."))
    return float(m.group(1)) if m else None


def parse_floor(text: str) -> dict:
    result = {"floor_raw": text, "floor": None, "floors_total": None}
    if not text:
        return result
    # Форматы: "6 из 8", "31 / 52", "31/52"
    m = re.search(r"(\d+)\s*(?:из|/)\s*(\d+)", text)
    if m:
        result["floor"] = int(m.group(1))
        result["floors_total"] = int(m.group(2))
    else:
        m2 = re.search(r"(\d+)", text)
        if m2:
            result["floor"] = int(m2.group(1))
    return result


def floor_features(floor, floors_total) -> dict:
    f = {"is_first_floor": None, "is_last_floor": None,
         "floor_ratio": None, "floors_above": None}
    if floor and floors_total and floors_total > 0:
        f["is_first_floor"] = int(floor == 1)
        f["is_last_floor"]  = int(floor == floors_total)
        f["floor_ratio"]    = round(floor / floors_total, 3)
        f["floors_above"]   = floors_total - floor
    return f


def extract_metro(text: str) -> dict:
    result = {"metro_station": None, "metro_minutes": None, "metro_by_foot": None}
    if not text:
        return result
    m = re.search(r"(\d+)\s*мин", text)
    if m:
        result["metro_minutes"] = int(m.group(1))
    result["metro_by_foot"] = int("пешком" in text.lower())
    name = re.split(r"\d", text)[0].strip(" ,•·–-\n")
    if name:
        result["metro_station"] = name[:60]
    return result


def desc_features(text: str) -> dict:
    if not text:
        return {
            "desc_len": 0, "desc_word_count": 0,
            "has_balcony": 0, "has_loggia": 0, "has_parking": 0,
            "has_furniture": 0, "has_renovated": 0, "has_mortgage": 0,
            "has_new_building": 0, "has_view": 0, "has_alarm": 0,
        }
    t = text.lower()
    return {
        "desc_len":         len(text),
        "desc_word_count":  len(text.split()),
        "has_balcony":      int("балкон" in t),
        "has_loggia":       int("лоджи" in t),
        "has_parking":      int(any(w in t for w in ["парковк", "машиноместо", "гараж"])),
        "has_furniture":    int(any(w in t for w in ["мебел", "меблирован", "гарнитур"])),
        "has_renovated":    int(any(w in t for w in ["ремонт", "отремонтирован", "евроремонт"])),
        "has_mortgage":     int(any(w in t for w in ["ипотек", "материнский капитал", "субсидир"])),
        "has_new_building": int(any(w in t for w in ["новостро", "новый дом", "сдан", "дду"])),
        "has_view":         int(any(w in t for w in ["вид", "панорам", "река", "парк"])),
        "has_alarm":        int(any(w in t for w in ["охрана", "консьерж", "сигнализац"])),
    }


RENOVATION_MAP = {
    # Вторичка
    "без ремонта": 0, "требует ремонта": 0,
    "косметический": 1,
    "евроремонт": 2, "хорошее": 2,
    "дизайнерский": 3, "под ключ": 2,
    # Новостройки (отделка)
    "без отделки": 0, "предчистовая": 1, "чистовая": 2,
    "white box": 1, "whitebox": 1,
    "с отделкой": 2, "с мебелью": 3,
}

BUILDING_TYPE_MAP = {
    "панельный": 0, "блочный": 1, "кирпичный": 2,
    "монолитный": 3, "монолитно-кирпичный": 4, "деревянный": 5,
}


# ─── Парсинг одного объявления ─────────────────────────────────────

async def scrape_flat(page, url: str) -> dict | None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)

        data = {"url": url, "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S")}
        m = re.search(r"/(\d+)/?$", url)
        data["flat_id"] = m.group(1) if m else url.split("/")[-2]

        # ── Цена ──────────────────────────────────────────────────
        el = await page.query_selector("[data-name='PriceInfo']")
        price_text = (await el.inner_text()).strip() if el else ""
        data.update(parse_price(price_text))

        # ── Описание ──────────────────────────────────────────────
        el = await page.query_selector("[data-name='Description']")
        desc = (await el.inner_text()).strip() if el else ""
        data["description"] = desc
        data.update(desc_features(desc))

        # ── Factoids ──────────────────────────────────────────────
        factoids = {}
        items = await page.query_selector_all("[data-name='ObjectFactoids'] [class*='item']")
        for item in items:
            text = (await item.inner_text()).strip()
            parts = [p.strip() for p in text.split("\n") if p.strip()]
            if len(parts) >= 2:
                factoids[parts[0].lower()] = parts[1]

        data["factoids_raw"] = json.dumps(factoids, ensure_ascii=False)

        def fact(*keys):
            """Ищет первое совпадение по любому из ключей (подстрока, case-insensitive)."""
            for key in keys:
                for k, v in factoids.items():
                    if key.lower() in k.lower():
                        return str(v).replace("\xa0", " ").strip()
            return None

        # ── Площади ───────────────────────────────────────────────
        # Реальные ключи ЦИАН: "общая площадь", "жилая площадь", "площадь кухни"
        data["total_area"]   = parse_area(fact("общая площадь", "общая"))
        data["living_area"]  = parse_area(fact("жилая площадь", "жилая"))
        data["kitchen_area"] = parse_area(fact("площадь кухни", "кухня"))

        ta, la, ka = data["total_area"], data["living_area"], data["kitchen_area"]
        data["living_ratio"]    = round(la / ta, 3) if ta and la else None
        data["kitchen_ratio"]   = round(ka / ta, 3) if ta and ka else None
        data["non_living_area"] = round(ta - la, 2)  if ta and la else None

        # ── Этаж ──────────────────────────────────────────────────
        fd = parse_floor(fact("этаж"))
        data.update(fd)
        data.update(floor_features(fd["floor"], fd["floors_total"]))

        # ── Комнаты ───────────────────────────────────────────────
        # 1. Пробуем factoids
        rooms_raw = fact("количество комнат", "комнат")
        # 2. Парсим h1/title страницы — там всегда есть "2-комн." или "Студия"
        if not rooms_raw:
            for sel in ["h1[class*='title']", "h1", "title"]:
                el = await page.query_selector(sel)
                if el:
                    t = (await el.inner_text()).strip()
                    m_t = re.search(r"(\d+)-комн", t)
                    if m_t:
                        rooms_raw = m_t.group(1)
                        break
                    if "студия" in t.lower() or "studio" in t.lower():
                        rooms_raw = "студия"
                        break
        data["rooms_raw"] = rooms_raw
        if rooms_raw:
            rm = re.search(r"(\d+)", str(rooms_raw))
            data["rooms"]     = int(rm.group(1)) if rm else None
            data["is_studio"] = int("студия" in str(rooms_raw).lower())
        else:
            data["rooms"]     = None
            data["is_studio"] = 0

        # Цена за кв.м.
        if data.get("price_rub") and ta:
            data["price_per_m2"] = round(data["price_rub"] / ta)

        # ── Параметры дома ────────────────────────────────────────
        # "год сдачи" — новостройки, "год постройки" — вторичка
        data["build_year_raw"]  = fact("год постройки", "год сдачи", "год")
        # "тип дома" или "материал стен"
        # Тип дома, ремонт, лифт и т.д. убраны — ЦИАН не отдаёт их в factoids
        # оставляем только отделку (новостройки) и год
        data["renovation"]      = fact("отделка", "ремонт", "состояние")

        # Числовой год и возраст
        if data["build_year_raw"]:
            my = re.search(r"(19|20)\d{2}", data["build_year_raw"])
            data["build_year"] = int(my.group()) if my else None
        else:
            data["build_year"] = None
        data["building_age"] = (2025 - data["build_year"]) if data["build_year"] else None

        # Ремонт — кодировка (актуально для новостроек)
        data["renovation_cat"] = RENOVATION_MAP.get(
            str(data["renovation"]).lower() if data["renovation"] else "", None)

        # ── Адрес ─────────────────────────────────────────────────
        for sel in ["[data-name='Geo']", "[class*='geo']", "address"]:
            el = await page.query_selector(sel)
            if el:
                data["address"] = (await el.inner_text()).strip().replace("\n", ", ")
                break
        else:
            data["address"] = None

        # ── Метро ─────────────────────────────────────────────────
        metro_el = await page.query_selector("[data-name='Undergrounds']")
        if not metro_el:
            metro_el = await page.query_selector("[class*='underground']")
        metro_text = (await metro_el.inner_text()).strip() if metro_el else ""
        data.update(extract_metro(metro_text))
        data["metro_raw"] = metro_text[:120] if metro_text else None

        # Округ из адреса
        addr = data.get("address") or ""
        data["district"] = None
        for part in addr.split(","):
            if "округ" in part.lower() or "район" in part.lower():
                data["district"] = part.strip()
                break

        # ── Ссылки на фото ────────────────────────────────────────
        img_urls = []
        try:
            await page.wait_for_selector("[data-name='OfferGallery']", timeout=5000)
        except:
            pass
        await page.evaluate("window.scrollBy(0, 300)")
        await asyncio.sleep(1)

        imgs = await page.query_selector_all("[data-name='OfferGallery'] img")
        for img in imgs:
            src = await img.get_attribute("src") or await img.get_attribute("data-src")
            if src and src.startswith("http") and src not in img_urls:
                img_urls.append(re.sub(r"/\d+x\d+/", "/", src))

        if len(img_urls) <= 1:
            try:
                gallery = await page.query_selector("[data-name='OfferGallery']")
                if gallery:
                    await gallery.click()
                    await asyncio.sleep(2)
                    for sel in ["img[class*='slide']", "img[class*='fullscreen']", "[data-name='OfferGallery'] img"]:
                        for img in await page.query_selector_all(sel):
                            src = await img.get_attribute("src") or await img.get_attribute("data-src")
                            if src and src.startswith("http") and src not in img_urls:
                                img_urls.append(re.sub(r"/\d+x\d+/", "/", src))
                        if len(img_urls) > 1:
                            break
                    for cs in ["button[class*='close']", "[data-name='CloseButton']"]:
                        c = await page.query_selector(cs)
                        if c:
                            await c.click()
                            break
            except:
                pass

        data["image_urls"]  = "|".join(img_urls)
        data["image_count"] = len(img_urls)

        print(
            f"  [+] {str(data.get('address',''))[:42]:42s}"
            f" | {str(data.get('price_rub','?')):>12} ₽"
            f" | {data.get('rooms','?')}к"
            f" | {data.get('total_area','?')} м²"
            f" | эт {data.get('floor','?')}/{data.get('floors_total','?')}"
            f" | фото {len(img_urls)}"
        )
        return data

    except Exception as e:
        print(f"  [!] Ошибка {url}: {e}")
        return None


# ─── Сбор ссылок со страниц поиска ────────────────────────────────

async def get_listing_urls(page, search_url: str, max_pages: int) -> list:
    all_urls = []
    for page_num in range(1, max_pages + 1):
        paginated = f"{search_url}&p={page_num}"
        print(f"  Страница поиска {page_num}/{max_pages}...")
        try:
            await page.goto(paginated, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(DELAY_PAGE)
            await page.wait_for_selector("a[href*='/sale/flat/']", timeout=20000)
            for link in await page.query_selector_all("a[href*='/sale/flat/']"):
                href = await link.get_attribute("href")
                if href and re.search(r"/sale/flat/\d{7,}/", href):
                    full = href if href.startswith("http") else "https://www.cian.ru" + href
                    all_urls.append(full)
            print(f"    Уникальных ссылок: {len(set(all_urls))}")
        except Exception as e:
            print(f"  [!] Ошибка стр.{page_num}: {e}")
            break
    return list(set(all_urls))


# ─── Сохранение ───────────────────────────────────────────────────

def save_results(results: list):
    if not results:
        print("\n>>> Данные не собраны.")
        return

    df_new = pd.DataFrame(results)

    # Упорядочиваем колонки: ML-признаки первыми
    priority = [
        "flat_id",
        # Цена
        "price_rub", "price_per_m2", "price_raw",
        # Площадь
        "total_area", "living_area", "kitchen_area",
        "living_ratio", "kitchen_ratio", "non_living_area",
        # Комнаты
        "rooms", "is_studio", "rooms_raw",
        # Этаж
        "floor", "floors_total", "is_first_floor", "is_last_floor",
        "floor_ratio", "floors_above", "floor_raw",
        # Дом
        "build_year", "building_age", "build_year_raw",
        "building_type", "building_type_cat",
        "ceiling_height",
        # Ремонт / удобства
        "renovation", "renovation_cat",
        "has_elevator_bin", "has_parking_obj", "has_balcony_obj",
        "bathroom", "bathroom_separate", "balcony_type",
        "elevator", "parking_type", "heating", "windows",
        # NLP из описания
        "desc_len", "desc_word_count",
        "has_balcony", "has_loggia", "has_parking",
        "has_furniture", "has_renovated", "has_mortgage",
        "has_new_building", "has_view", "has_alarm",
        # Метро / гео
        "metro_station", "metro_minutes", "metro_by_foot",
        "district", "address", "metro_raw",
        # Фото
        "image_count", "image_urls",
        # Служебные
        "url", "scraped_at", "factoids_raw", "description",
    ]
    existing = [c for c in priority if c in df_new.columns]
    rest = [c for c in df_new.columns if c not in existing]
    df_new = df_new[existing + rest]

    if os.path.exists(OUT_CSV):
        df_old = pd.read_csv(OUT_CSV, encoding="utf-8-sig")
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        df_combined = df_combined.drop_duplicates(subset=["flat_id"])
        df_combined.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n>>> Добавлено {len(df_new)}, итого {len(df_combined)} → {OUT_CSV}")
    else:
        df_new.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"\n>>> Сохранено {len(df_new)} объявлений → {OUT_CSV}")

    print(f">>> Признаков (колонок): {len(df_new.columns)}")


# ─── Main ─────────────────────────────────────────────────────────

async def main():
    est = MAX_SEARCH_PAGES * 28
    actual = min(est, MAX_FLATS) if MAX_FLATS else est
    print("=" * 60)
    print(f"ЦИАН Парсер | страниц: {MAX_SEARCH_PAGES} | ~{actual} объявлений | потоков: {CONCURRENCY}")
    print("=" * 60)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        search_page = await context.new_page()
        print("\n>>> Открываем ЦИАН...")
        await search_page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(5)

        print(">>> Собираем ссылки...")
        urls = await get_listing_urls(search_page, SEARCH_URL, MAX_SEARCH_PAGES)
        await search_page.close()

        if MAX_FLATS:
            urls = urls[:MAX_FLATS]
        print(f"\n>>> Итого ссылок: {len(urls)}\n")

        results = []
        semaphore = asyncio.Semaphore(CONCURRENCY)

        async def scrape_with_sem(url):
            async with semaphore:
                p = await context.new_page()
                try:
                    result = await scrape_flat(p, url)
                    if result:
                        results.append(result)
                finally:
                    await p.close()
                    await asyncio.sleep(DELAY_FLAT)

        tasks = [scrape_with_sem(u) for u in urls]
        for coro in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Парсинг"):
            await coro

        await browser.close()

    save_results(results)


if __name__ == "__main__":
    asyncio.run(main())