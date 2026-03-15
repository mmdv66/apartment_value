"""
Скачивает фото из image_urls в CSV и загружает в MinIO S3.
В CSV обновляет колонку image_s3_uris с адресами s3://bucket/...

Запуск: python download_images.py
"""

import asyncio
import os
import io
import httpx
import pandas as pd
import boto3
from botocore.client import Config
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ══════════════════════════════════════════════════════════════════

CSV_PATH            = "data/raw/flats_data.csv"
MAX_PHOTOS_PER_FLAT = 5
CONCURRENCY         = 20
SKIP_EXISTING       = True   # не перекачивать уже загруженные в S3

S3_ENDPOINT         = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET           = os.getenv("S3_BUCKET",   "cian-dataset")
S3_ACCESS_KEY       = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY       = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_PREFIX           = "images"   # папка внутри бакета

# ══════════════════════════════════════════════════════════════════


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_existing_keys(s3, flat_id: str) -> set:
    """Возвращает множество уже загруженных ключей для flat_id."""
    prefix = f"{S3_PREFIX}/{flat_id}/"
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        return {obj["Key"] for obj in resp.get("Contents", [])}
    except Exception:
        return set()


async def download_and_upload(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    s3,
    flat_id: str,
    image_urls: list[str],
) -> list[str]:
    """Скачивает фото и загружает в S3. Возвращает список S3 URI."""
    s3_uris = []
    existing_keys = list_existing_keys(s3, flat_id) if SKIP_EXISTING else set()

    async with semaphore:
        for i, url in enumerate(image_urls[:MAX_PHOTOS_PER_FLAT]):
            ext = url.split(".")[-1].split("?")[0] or "jpg"
            s3_key = f"{S3_PREFIX}/{flat_id}/{i}.{ext}"
            s3_uri = f"s3://{S3_BUCKET}/{s3_key}"

            if SKIP_EXISTING and s3_key in existing_keys:
                s3_uris.append(s3_uri)
                continue

            try:
                r = await client.get(
                    url,
                    headers={"Referer": "https://www.cian.ru/"},
                    timeout=15,
                )
                r.raise_for_status()

                # Загружаем в S3 прямо из памяти (без сохранения на диск)
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_key,
                    Body=io.BytesIO(r.content),
                    ContentType=f"image/{ext}",
                )
                s3_uris.append(s3_uri)

            except Exception as e:
                print(f"    [!] {flat_id}/{i}: {e}")

    return s3_uris


async def main():
    if not os.path.exists(CSV_PATH):
        print(f"[!] Файл не найден: {CSV_PATH}")
        return

    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")

    if "image_urls" not in df.columns:
        print("[!] Колонка image_urls не найдена")
        return

    df = df[df["image_urls"].notna() & (df["image_urls"].str.strip() != "")]
    print(f">>> Объявлений с фото: {len(df)}")
    print(f">>> Макс фото на объявление: {MAX_PHOTOS_PER_FLAT}")
    print(f">>> Параллельность: {CONCURRENCY}")
    print(f">>> S3: {S3_ENDPOINT}/{S3_BUCKET}\n")

    s3 = get_s3_client()

    # Проверяем подключение к S3
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f">>> Бакет {S3_BUCKET} доступен\n")
    except Exception as e:
        print(f"[!] Ошибка подключения к S3: {e}")
        return

    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(
        limits=httpx.Limits(max_connections=CONCURRENCY + 5)
    ) as client:
        tasks = []
        indices = []
        for idx, row in df.iterrows():
            flat_id = str(row["flat_id"])
            urls = [u for u in row["image_urls"].split("|") if u.startswith("http")]
            if urls:
                tasks.append(download_and_upload(client, semaphore, s3, flat_id, urls))
                indices.append(idx)

        results = await tqdm_asyncio.gather(*tasks, desc="Загрузка в S3")

    # Обновляем CSV — добавляем колонку с S3 URI
    df_all = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    if "image_s3_uris" not in df_all.columns:
        df_all["image_s3_uris"] = ""

    for idx, s3_uris in zip(indices, results):
        df_all.loc[idx, "image_s3_uris"] = "|".join(s3_uris)
        df_all.loc[idx, "image_count_s3"] = len(s3_uris)

    df_all.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    total = sum(len(r) for r in results)
    print(f"\n>>> Загружено в S3: {total} фото")
    print(f">>> CSV обновлён: добавлены колонки image_s3_uris, image_count_s3")


if __name__ == "__main__":
    asyncio.run(main())