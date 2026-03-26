import pandera as pa
from pandera.typing import Series
import pandas as pd

# Описываем схему данных
realty_schema = pa.DataFrameSchema({
    "offer_id": pa.Column(str, coerce=True, nullable=False), # ID всегда должен быть
    "price": pa.Column(str, nullable=True),
    "price_numeric": pa.Column(pa.Int, nullable=True, coerce=True), # Числовая цена
    "old_price": pa.Column(str, nullable=True),
    "area": pa.Column(pa.Float, nullable=True, coerce=True), # Площадь как число
    "rooms": pa.Column(str, nullable=True),
    "floor": pa.Column(str, nullable=True, regex=r"^\d+/\d+$"), # Проверка формата "5/9"
    "price_per_m2": pa.Column(str, nullable=True),
    "metro": pa.Column(str, nullable=True),
    "metro_time": pa.Column(str, nullable=True),
    "address": pa.Column(str, nullable=True),
    "author": pa.Column(str, nullable=True),
    "main_image": pa.Column(str, nullable=True, checks=pa.Check.str_startswith("http")),
    "photo_count": pa.Column(pa.Int, nullable=True, coerce=True),
    "badges": pa.Column(str, nullable=True),
    "publish_date": pa.Column(str, nullable=True),
    "url": pa.Column(str, nullable=False, checks=pa.Check.str_contains("realty.yandex.ru"))
})