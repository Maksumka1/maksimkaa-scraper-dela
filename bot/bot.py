import asyncio
import html
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
import redis.asyncio as aioredis
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
REDIS_ADDR = os.getenv("REDIS_ADDR", "redis:6379")
REDIS_PASS = os.getenv("REDIS_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())

pg_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[aioredis.Redis] = None

SET_PATTERN = re.compile(
    r"^/set\s+(?P<from>[^;]+);\s*(?P<to>[^;]+);\s*(?P<weight>\d+(?:[\.,]\d+)?);\s*(?P<price>\d+(?:[\.,]\d+)?)$",
    re.IGNORECASE,
)

TRANSPORT_OPTIONS = [
    "тент",
    "крита",
    "ізотерм",
    "рефрижератор",
    "автобус",
    "автовоз",
    "зерновоз",
    "щеповоз",
    "самоскид",
    "цистерна",
    "контейнеровоз",
    "низькорамник",
    "платформа",
    "маніпулятор",
]

ARCHIVE_MESSAGE_LIMIT = 3800
ARCHIVE_PAUSE_SEC = 1.05


class FilterWizard(StatesGroup):
    waiting_value = State()


def blank_filter() -> Dict[str, Any]:
    return {
        "schema_version": "4",
        "route_from": "",
        "route_to": "",
        "from_all_ukraine": False,
        "from_cities": [],
        "from_regions": [],
        "to_all_ukraine": False,
        "to_cities": [],
        "to_regions": [],
        "min_weight": None,
        "max_weight": None,
        "min_volume": None,
        "max_volume": None,
        "min_length": None,
        "max_length": None,
        "min_width": None,
        "max_width": None,
        "min_height": None,
        "max_height": None,
        "transport_types": [],
        "min_price_km": None,
        "hot_min_price_km": 20.0,
        "allow_incomplete_data": False,
        "return_search_enabled": False,
        "round_trip_only": False,
        "enabled": False,
    }


def normalize_text(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def normalize_region(value: str) -> str:
    value = " ".join((value or "").strip().split())
    value = re.sub(r"\s+область$", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+обл\.?$", "", value, flags=re.IGNORECASE)
    return value.strip(" ,").lower()


def split_values(value: str, *, region: bool = False) -> List[str]:
    parts = re.split(r"[,;\n]+", value or "")
    result: List[str] = []
    seen = set()
    for part in parts:
        normalized = normalize_region(part) if region else normalize_text(part)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def safe_float(value: Any) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    try:
        parsed = float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return None
    if parsed < 0:
        return None
    return parsed


def safe_json_list(raw: str) -> List[str]:
    try:
        value = json.loads(raw or "[]")
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, (str, int, float))]


def filter_from_redis(data: Dict[str, str]) -> Dict[str, Any]:
    result = blank_filter()
    result["schema_version"] = data.get("schema_version", "2")
    result["route_from"] = data.get("route_from", "")
    result["route_to"] = data.get("route_to", "")
    result["from_all_ukraine"] = data.get("from_all_ukraine") in {"1", "true", "yes"}
    result["from_cities"] = safe_json_list(data.get("from_cities", "[]"))
    result["from_regions"] = safe_json_list(data.get("from_regions", "[]"))
    result["to_all_ukraine"] = data.get("to_all_ukraine") in {"1", "true", "yes"}
    result["to_cities"] = safe_json_list(data.get("to_cities", "[]"))
    result["to_regions"] = safe_json_list(data.get("to_regions", "[]"))
    result["min_weight"] = safe_float(data.get("min_weight"))
    result["max_weight"] = safe_float(data.get("max_weight"))
    result["min_volume"] = safe_float(data.get("min_volume"))
    result["max_volume"] = safe_float(data.get("max_volume"))
    result["min_length"] = safe_float(data.get("min_length"))
    result["max_length"] = safe_float(data.get("max_length"))
    result["min_width"] = safe_float(data.get("min_width"))
    result["max_width"] = safe_float(data.get("max_width"))
    result["min_height"] = safe_float(data.get("min_height"))
    result["max_height"] = safe_float(data.get("max_height"))
    result["transport_types"] = safe_json_list(data.get("transport_types", "[]"))
    result["min_price_km"] = safe_float(data.get("min_price_km"))
    hot_value = safe_float(data.get("hot_min_price_km"))
    result["hot_min_price_km"] = 20.0 if hot_value is None else hot_value
    result["allow_incomplete_data"] = data.get("allow_incomplete_data") in {"1", "true", "yes"}
    result["return_search_enabled"] = data.get("return_search_enabled") in {"1", "true", "yes"}
    result["round_trip_only"] = data.get("round_trip_only") in {"1", "true", "yes"}
    result["enabled"] = data.get("enabled") in {"1", "true", "yes"}
    return result


def filter_to_redis(data: Dict[str, Any]) -> Dict[str, str]:
    from_cities = [normalize_text(x) for x in data.get("from_cities", []) if normalize_text(x)]
    from_regions = [normalize_region(x) for x in data.get("from_regions", []) if normalize_region(x)]
    to_cities = [normalize_text(x) for x in data.get("to_cities", []) if normalize_text(x)]
    to_regions = [normalize_region(x) for x in data.get("to_regions", []) if normalize_region(x)]
    transport_types = []
    seen_transport = set()
    for value in data.get("transport_types", []):
        normalized = normalize_text(value)
        if normalized and normalized not in seen_transport:
            seen_transport.add(normalized)
            transport_types.append(normalized)

    def numeric(value: Any) -> str:
        return "" if value is None else str(float(value))

    return {
        "schema_version": "4",
        "route_from": normalize_text(data.get("route_from", "")),
        "route_to": normalize_text(data.get("route_to", "")),
        "from_all_ukraine": "1" if data.get("from_all_ukraine") else "0",
        "from_cities": json.dumps(from_cities, ensure_ascii=False),
        "from_regions": json.dumps(from_regions, ensure_ascii=False),
        "to_all_ukraine": "1" if data.get("to_all_ukraine") else "0",
        "to_cities": json.dumps(to_cities, ensure_ascii=False),
        "to_regions": json.dumps(to_regions, ensure_ascii=False),
        "min_weight": numeric(data.get("min_weight")),
        "max_weight": numeric(data.get("max_weight")),
        "min_volume": numeric(data.get("min_volume")),
        "max_volume": numeric(data.get("max_volume")),
        "min_length": numeric(data.get("min_length")),
        "max_length": numeric(data.get("max_length")),
        "min_width": numeric(data.get("min_width")),
        "max_width": numeric(data.get("max_width")),
        "min_height": numeric(data.get("min_height")),
        "max_height": numeric(data.get("max_height")),
        "transport_types": json.dumps(transport_types, ensure_ascii=False),
        "min_price_km": numeric(data.get("min_price_km")),
        "hot_min_price_km": "0" if data.get("hot_min_price_km") is None else numeric(data.get("hot_min_price_km")),
        "allow_incomplete_data": "1" if data.get("allow_incomplete_data") else "0",
        "return_search_enabled": "1" if (data.get("return_search_enabled") or data.get("round_trip_only")) else "0",
        "round_trip_only": "1" if data.get("round_trip_only") else "0",
        "enabled": "1" if data.get("enabled", True) else "0",
    }


async def get_saved_filter(chat_id: int) -> Dict[str, Any]:
    assert redis_client is not None
    data = await redis_client.hgetall(f"filter:{chat_id}")
    return filter_from_redis(data) if data else blank_filter()


async def get_working_filter(chat_id: int, state: FSMContext) -> Dict[str, Any]:
    data = await state.get_data()
    working = data.get("filter")
    if working:
        return working
    return await get_saved_filter(chat_id)


async def persist_filter(chat_id: int, filter_data: Dict[str, Any]) -> None:
    assert redis_client is not None
    redis_data = filter_to_redis(filter_data)
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.hset(f"filter:{chat_id}", mapping=redis_data)
        pipe.sadd("filters:active_users", str(chat_id))
        pipe.publish("channel:filters:update", str(chat_id))
        await pipe.execute()


def add_location_sql(
    clauses: List[str],
    args: List[Any],
    *,
    city_col: str,
    region_col: str,
    full_col: str,
    all_ukraine: bool,
    cities: List[str],
    regions: List[str],
    legacy: str,
) -> None:
    if all_ukraine:
        return

    alternatives: List[str] = []
    if cities:
        args.append([normalize_text(x) for x in cities])
        alternatives.append(f"LOWER(COALESCE({city_col}, '')) = ANY(${len(args)})")
    if regions:
        args.append([normalize_region(x) for x in regions])
        alternatives.append(f"LOWER(COALESCE({region_col}, '')) = ANY(${len(args)})")
        args.append([f"%{normalize_region(x)}%" for x in regions])
        alternatives.append(f"LOWER(COALESCE({full_col}, '')) LIKE ANY(${len(args)})")
    if legacy:
        args.append(f"%{normalize_text(legacy)}%")
        alternatives.append(f"LOWER(COALESCE({city_col}, '')) LIKE ${len(args)}")

    if alternatives:
        clauses.append("(" + " OR ".join(alternatives) + ")")


def build_archive_query(filter_data: Dict[str, Any]) -> Tuple[str, List[Any]]:
    clauses = ["created_at >= NOW() - INTERVAL '48 HOURS'"]
    args: List[Any] = []

    add_location_sql(
        clauses,
        args,
        city_col="route_from",
        region_col="route_from_region",
        full_col="route_from_full",
        all_ukraine=bool(filter_data.get("from_all_ukraine")),
        cities=filter_data.get("from_cities", []),
        regions=filter_data.get("from_regions", []),
        legacy=filter_data.get("route_from", ""),
    )
    add_location_sql(
        clauses,
        args,
        city_col="route_to",
        region_col="route_to_region",
        full_col="route_to_full",
        all_ukraine=bool(filter_data.get("to_all_ukraine")),
        cities=filter_data.get("to_cities", []),
        regions=filter_data.get("to_regions", []),
        legacy=filter_data.get("route_to", ""),
    )

    min_weight = filter_data.get("min_weight")
    max_weight = filter_data.get("max_weight")
    min_volume = filter_data.get("min_volume")
    max_volume = filter_data.get("max_volume")
    min_price = filter_data.get("min_price_km")
    allow_incomplete = bool(filter_data.get("allow_incomplete_data"))

    if min_weight is not None:
        args.append(min_weight)
        clauses.append(f"weight_t >= ${len(args)}")
    if max_weight is not None:
        args.append(max_weight)
        clauses.append(f"weight_t IS NOT NULL AND weight_t <= ${len(args)}")
    if min_volume is not None:
        args.append(min_volume)
        clauses.append(f"volume_m3 >= ${len(args)}")
    if max_volume is not None:
        args.append(max_volume)
        clauses.append(f"volume_m3 IS NOT NULL AND volume_m3 <= ${len(args)}")

    for min_key, max_key, col in ((
        "min_length", "max_length", "length_m",
    ), (
        "min_width", "max_width", "width_m",
    ), (
        "min_height", "max_height", "height_m",
    )):
        low = filter_data.get(min_key)
        high = filter_data.get(max_key)
        if low is not None:
            args.append(low)
            if allow_incomplete:
                clauses.append(
                    f"({col} IS NULL OR {col} <= 0 OR {col} >= ${len(args)})"
                )
            else:
                clauses.append(f"{col} >= ${len(args)}")
        if high is not None:
            args.append(high)
            if allow_incomplete:
                clauses.append(
                    f"({col} IS NULL OR {col} <= 0 OR {col} <= ${len(args)})"
                )
            else:
                clauses.append(f"{col} IS NOT NULL AND {col} <= ${len(args)}")

    if min_price is not None:
        args.append(min_price)
        if allow_incomplete:
            clauses.append(
                f"(price_per_km_uah IS NULL OR price_per_km_uah <= 0 OR price_per_km_uah >= ${len(args)})"
            )
        else:
            clauses.append(f"price_per_km_uah >= ${len(args)}")

    transport_types = [normalize_text(x) for x in filter_data.get("transport_types", []) if normalize_text(x)]
    if transport_types:
        args.append(transport_types)
        clauses.append(f"transport_types && ${len(args)}::text[]")

    query = f"""
        SELECT route_from, route_to, order_url,route_from_full, route_to_full,
               route_from_region, route_to_region, distance_km, cargo_type,
               weight_t, volume_m3, length_m, width_m, height_m,
               price_uah, price_per_km_uah, tags, transport_types, published_relative, published_at, created_at
        FROM cargo_history
        WHERE {' AND '.join(clauses)}
        ORDER BY created_at DESC
    """
    return query, args


def format_archive_row(row: asyncpg.Record, filter_data: Dict[str, Any]) -> str:
    transport = row["transport_types"] or []
    tags = row["tags"] or []
    hot_min = filter_data.get("hot_min_price_km")
    rate = row["price_per_km_uah"]
    is_hot = hot_min is not None and float(hot_min) > 0 and rate is not None and float(rate) >= float(hot_min)

    dimensions = []
    for key, label in (("length_m", "дов"), ("width_m", "шир"), ("height_m", "вис")):
        value = row[key]
        if value is not None and float(value) > 0:
            dimensions.append(f"{label} {float(value):.2f} м")
    dimension_text = " · ".join(dimensions) if dimensions else "не вказані"

    route = f"{str(row['route_from']).upper()} ➔ {str(row['route_to']).upper()}"
    price_text = (
        f"{int(round(float(row['price_uah']))):,}".replace(",", " ") + " ГРН"
        if row["price_uah"] is not None and float(row["price_uah"]) > 0
        else "СТАВКА НЕ ВКАЗАНА"
    )
    rate_text = f"({float(rate):.2f} ГРН/КМ)" if rate is not None and float(rate) > 0 else ""

    lines = [
        ("🔥 <b>ГАРЯЧА СТАВКА</b> · " if is_hot else "") + f"📍 <b>{html.escape(route)}</b> <code>{row['distance_km'] or 0} КМ</code>",
        f"💰 <b>{html.escape(price_text)}</b>" + (f" <code>{html.escape(rate_text)}</code>" if rate_text else ""),
    ]

    if tags:
        tag_badges = []

        for tag in tags:
            tag = str(tag).strip()
            if not tag:
                continue

            tag_badges.append(
                f"🏷️ <code>[{html.escape(tag)}]</code>"
            )

        lines.append(
            " ".join(tag_badges)
            if tag_badges
            else "🏷️ <code>Теги не вказані</code>"
        )
    else:
        lines.append("🏷️ <code>Теги не вказані</code>")

    lines.append("<blockquote>")
    lines.extend([
        f"📦 <i>Вантаж:</i> {html.escape(str(row['cargo_type'] or '—'))}",
        f"⚖️ <i>Вага / Об'єм:</i> {row['weight_t'] if row['weight_t'] is not None else '—'} т · {row['volume_m3'] if row['volume_m3'] is not None else '—'} м³",
        f"🚛 <i>Тип авто:</i> {html.escape(', '.join(str(x) for x in transport) if transport else 'не вказано')}",
        f"📐 <i>Габарити:</i> {html.escape(dimension_text)}",
    ])

    relative = str(row["published_relative"] or "")
    exact = str(row["published_at"] or "")
    exact_time = exact[-8:] if len(exact) >= 8 else exact
    published = f"{relative} ({exact_time})" if relative and exact_time else (relative or exact_time)
    lines.append(f"⏱ <i>Опубліковано:</i> {html.escape(published)}")
    lines.append("</blockquote>")

    return "\n".join(lines)


def della_keyboard(urls: List[str]) -> Optional[InlineKeyboardMarkup]:
    urls = [url for url in urls if url]
    if not urls:
        return None
    buttons = []
    for index, url in enumerate(urls, 1):
        text = "🔗 Відкрити замовлення на Della" if len(urls) == 1 else f"🔗 Della #{index}"
        buttons.append([InlineKeyboardButton(text=text, url=url)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def send_archive(message: types.Message, filter_data: Dict[str, Any], *, header: bool = True) -> int:
    assert pg_pool is not None
    query, args = build_archive_query(filter_data)
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    if not rows:
        await message.answer("ℹ️ За останні 48 годин відповідних вантажів не знайдено.", reply_markup=bottom_menu())
        return 0

    chunks: List[Tuple[str, List[str]]] = []
    current = ""
    current_urls: List[str] = []
    if header:
        current = "📋 <b>Вантажі за останні 48 годин</b>\n"
        current += f"Знайдено: <b>{len(rows)}</b>\n\n"

    for row in rows:
        block = format_archive_row(row, filter_data)
        if len(current) + len(block) + 2 > ARCHIVE_MESSAGE_LIMIT and current.strip():
            chunks.append((current.rstrip(), current_urls))
            current = ""
            current_urls = []
        current += block + "\n\n"
        if row["order_url"]:
            current_urls.append(str(row["order_url"]))
    if current.strip():
        chunks.append((current.rstrip(), current_urls))

    for index, (chunk, urls) in enumerate(chunks):
        await message.answer(
            chunk,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=della_keyboard(urls),
        )
        if index < len(chunks) - 1:
            await asyncio.sleep(ARCHIVE_PAUSE_SEC)

    return len(rows)



def build_forecast_query(filter_data: Dict[str, Any]) -> Tuple[str, List[Any]]:
    clauses = ["created_at >= NOW() - INTERVAL '180 DAYS'"]
    args: List[Any] = []

    # A return candidate starts where the forward filter ends, and ends where
    # the forward filter starts.
    add_location_sql(
        clauses,
        args,
        city_col="route_from",
        region_col="route_from_region",
        full_col="route_from_full",
        all_ukraine=bool(filter_data.get("to_all_ukraine")),
        cities=filter_data.get("to_cities", []),
        regions=filter_data.get("to_regions", []),
        legacy=filter_data.get("route_to", ""),
    )
    add_location_sql(
        clauses,
        args,
        city_col="route_to",
        region_col="route_to_region",
        full_col="route_to_full",
        all_ukraine=bool(filter_data.get("from_all_ukraine")),
        cities=filter_data.get("from_cities", []),
        regions=filter_data.get("from_regions", []),
        legacy=filter_data.get("route_from", ""),
    )

    allow_incomplete = bool(filter_data.get("allow_incomplete_data"))
    for key, col in (("min_weight", "weight_t"), ("min_volume", "volume_m3")):
        value = filter_data.get(key)
        if value is not None:
            args.append(value)
            clauses.append(f"{col} >= ${len(args)}")
    for key, col in (("min_length", "length_m"), ("min_width", "width_m"), ("min_height", "height_m")):
        value = filter_data.get(key)
        if value is not None:
            args.append(value)
            if allow_incomplete:
                clauses.append(f"({col} IS NULL OR {col} <= 0 OR {col} >= ${len(args)})")
            else:
                clauses.append(f"{col} >= ${len(args)}")

    for key, col in (("max_weight", "weight_t"), ("max_volume", "volume_m3")):
         value = filter_data.get(key)
         if value is not None:
             args.append(value)
             clauses.append(f"{col} IS NOT NULL AND {col} <= ${len(args)}")

    for key, col in (("max_length", "length_m"), ("max_width", "width_m"), ("max_height", "height_m")):
        value = filter_data.get(key)
        if value is not None:
            args.append(value)
            if allow_incomplete:
                clauses.append(f"({col} IS NULL OR {col} <= 0 OR {col} <= ${len(args)})")
            else:
                clauses.append(f"{col} IS NOT NULL AND {col} <= ${len(args)}")

    value = filter_data.get("min_price_km")
    if value is not None:
        args.append(value)
        if allow_incomplete:
            clauses.append(f"(price_per_km_uah IS NULL OR price_per_km_uah <= 0 OR price_per_km_uah >= ${len(args)})")
        else:
            clauses.append(f"price_per_km_uah >= ${len(args)}")


    transport_types = [normalize_text(x) for x in filter_data.get("transport_types", []) if normalize_text(x)]
    if transport_types:
        args.append(transport_types)
        clauses.append(f"transport_types && ${len(args)}::text[]")

    query = f"""
        SELECT route_from, route_to, route_from_region, route_to_region, created_at
        FROM cargo_history
        WHERE {' AND '.join(clauses)}
        ORDER BY created_at DESC
        LIMIT 5000
    """
    return query, args


async def send_forecast(message: types.Message, filter_data: Dict[str, Any]) -> int:
    assert pg_pool is not None
    from forecast import calculate_route_forecasts

    query, args = build_forecast_query(filter_data)
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    forecasts = calculate_route_forecasts(rows)
    if not forecasts:
        await message.answer(
            "📈 <b>Прогноз повернення</b>\n\n"
            "Недостатньо історичних даних для прогнозу за вибраним напрямком.",
            parse_mode="HTML",
            reply_markup=bottom_menu(),
        )
        return 0

    lines = [
        "📈 <b>Прогноз зворотного вантажу</b>",
        "База: до 180 днів історії. Це статистична оцінка, а не гарантія появи вантажу.",
        "",
    ]
    for item in forecasts[:10]:
        route = f"{html.escape(item['route_from'])} ➔ {html.escape(item['route_to'])}"
        region_hint = ""
        if item.get("region_from") or item.get("region_to"):
            region_hint = f"\n{html.escape(item.get('region_from') or '—')} обл. → {html.escape(item.get('region_to') or '—')} обл."
        count = item["count"]
        last_seen = item["last_seen"].astimezone().strftime("%d.%m.%Y %H:%M")
        if item["median_interval_hours"] is None:
            frequency = "недостатньо даних про інтервали"
            expected = "—"
        else:
            frequency = f"медіанний інтервал ≈ {item['median_interval_hours']:.1f} год"
            expected = item["next_expected"].astimezone().strftime("%d.%m.%Y %H:%M")
        window = item.get("expected_window")
        window_text = ""
        if window:
            low = window[0].astimezone().strftime("%d.%m %H:%M")
            high = window[1].astimezone().strftime("%d.%m %H:%M")
            window_text = f"\nВікно: {low} — {high}"
        lines.extend([
            f"<b>{route}</b>{region_hint}",
            f"Рейсів: <b>{count}</b>; останній: {last_seen}",
            frequency,
            f"Очікувана наступна поява: <b>{expected}</b>{window_text}",
            "",
        ])

    await message.answer("\n".join(lines).rstrip(), parse_mode="HTML", reply_markup=bottom_menu())
    return len(forecasts)

def bottom_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⚙️ Налаштувати фільтр"), KeyboardButton(text="📋 Мій фільтр")],
            [KeyboardButton(text="📦 Наявні вантажі (48г)"), KeyboardButton(text="🔄 Зворотний пошук")],
            [KeyboardButton(text="🚛 Туди + назад"), KeyboardButton(text="📈 Прогноз повернення")],
            [KeyboardButton(text="🗑 Очистити фільтр")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⚙️ Налаштувати фільтр", callback_data="cfg")],
            [InlineKeyboardButton(text="📋 Мій фільтр", callback_data="view")],
            [InlineKeyboardButton(text="📦 Наявні вантажі (48г)", callback_data="hist")],
            [InlineKeyboardButton(text="🔄 Зворотний пошук", callback_data="return_toggle")],
            [InlineKeyboardButton(text="🚛 Туди + назад", callback_data="roundtrip_toggle")],
            [InlineKeyboardButton(text="📈 Прогноз повернення", callback_data="forecast")],
            [InlineKeyboardButton(text="🗑 Очистити фільтр", callback_data="clear")],
        ]
    )


def numeric_preset_menu(kind: str) -> InlineKeyboardMarkup:
    presets = {
        "weight": [
            ("1.5 т", "preset:weight:1.5"),
            ("3 т", "preset:weight:3"),
            ("5 т", "preset:weight:5"),
            ("10 т", "preset:weight:10"),
        ],
        "volume": [
            ("20 м³", "preset:volume:20"),
            ("40 м³", "preset:volume:40"),
            ("60 м³", "preset:volume:60"),
            ("80 м³", "preset:volume:80"),
        ],
        "price": [
            ("20 грн/км", "preset:price:20"),
            ("25 грн/км", "preset:price:25"),
            ("30 грн/км", "preset:price:30"),
            ("+5 грн/км", "preset:price:+5"),
        ],
    }
    titles = {
        "weight": "⚖️ <b>Мінімальна маса</b>",
        "volume": "📐 <b>Мінімальний об'єм</b>",
        "price": "💵 <b>Мінімальна ставка / км</b>",
    }
    rows = []
    items = presets[kind]
    for offset in range(0, len(items), 2):
        rows.append([InlineKeyboardButton(text=items[offset][0], callback_data=items[offset][1])] + (
            [InlineKeyboardButton(text=items[offset + 1][0], callback_data=items[offset + 1][1])]
            if offset + 1 < len(items) else []
        ))
    rows.extend([
        [InlineKeyboardButton(text="✏️ Ввести вручну", callback_data=f"custom:{kind}")],
        [InlineKeyboardButton(text="🗑 Очистити", callback_data=f"preset:clear:{kind}")],
        [InlineKeyboardButton(text="↩️ До фільтра", callback_data="cfg")],
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)



def config_menu(filter_data: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📍 Звідки", callback_data="loc:from")],
            [InlineKeyboardButton(text="🎯 Куди", callback_data="loc:to")],
            [InlineKeyboardButton(text="⚖️ Маса", callback_data="val:weight")],
            [InlineKeyboardButton(text="📐 Об'єм", callback_data="val:volume")],
            [InlineKeyboardButton(text="📏 Довжина", callback_data="val:length")],
            [InlineKeyboardButton(text="↔️ Ширина", callback_data="val:width")],
            [InlineKeyboardButton(text="↕️ Висота", callback_data="val:height")],
            [InlineKeyboardButton(text="🚛 Тип транспорту", callback_data="transport")],
            [InlineKeyboardButton(text="💵 Мін. ставка / км", callback_data="val:price")],
            [InlineKeyboardButton(text="🔥 Гаряча ставка", callback_data="val:hot_price")],
            [InlineKeyboardButton(
                text=("📋 Неповні дані: ТАК" if filter_data.get("allow_incomplete_data") else "📋 Неповні дані: НІ"),
                callback_data="incomplete_toggle",
            )],
            [InlineKeyboardButton(
                text=("🔄 Зворотний: ON" if filter_data.get("return_search_enabled") else "🔄 Зворотний: OFF"),
                callback_data="return_toggle_cfg",
            )],
            [InlineKeyboardButton(
                text=("🚛 Туди + назад: ON" if filter_data.get("round_trip_only") else "🚛 Туди + назад: OFF"),
                callback_data="roundtrip_toggle_cfg",
            )],
            [InlineKeyboardButton(text="💾 Зберегти та активувати", callback_data="save")],
            [InlineKeyboardButton(text="↩️ Головне меню", callback_data="menu")],
        ]
    )


def location_menu(side: str, filter_data: Dict[str, Any]) -> InlineKeyboardMarkup:
    prefix = "from" if side == "from" else "to"
    all_ukraine = bool(filter_data.get(f"{prefix}_all_ukraine"))
    cities = filter_data.get(f"{prefix}_cities", [])
    regions = filter_data.get(f"{prefix}_regions", [])
    body = [
        [InlineKeyboardButton(text="🇺🇦 Вся Україна", callback_data=f"loc:{prefix}:all")],
        [InlineKeyboardButton(text="🏙 Додати міста", callback_data=f"loc:{prefix}:city")],
        [InlineKeyboardButton(text="🗺 Додати області", callback_data=f"loc:{prefix}:region")],
        [InlineKeyboardButton(text="🗑 Очистити", callback_data=f"loc:{prefix}:clear")],
        [InlineKeyboardButton(text="✅ Готово", callback_data="cfg")],
    ]
    selected = []
    if all_ukraine:
        selected.append("🇺🇦 Вся Україна")
    if cities:
        selected.append("🏙 " + ", ".join(cities))
    if regions:
        selected.append("🗺 " + ", ".join(regions))
    return InlineKeyboardMarkup(inline_keyboard=body + (
        [[InlineKeyboardButton(text="ℹ️ Вибрано: " + " | ".join(selected)[:45], callback_data="noop")]] if selected else []
    ))


def transport_menu(filter_data: Dict[str, Any]) -> InlineKeyboardMarkup:
    selected = set(filter_data.get("transport_types", []))
    rows = []
    for offset in range(0, len(TRANSPORT_OPTIONS), 2):
        row = []
        for index in range(offset, min(offset + 2, len(TRANSPORT_OPTIONS))):
            transport = TRANSPORT_OPTIONS[index]
            mark = "✅" if transport in selected else "☐"
            row.append(InlineKeyboardButton(text=f"{mark} {transport.title()}", callback_data=f"tr:{index}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🗑 Очистити", callback_data="tr:clear")])
    rows.append([InlineKeyboardButton(text="✅ Готово", callback_data="cfg")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def location_configured(filter_data: Dict[str, Any], prefix: str) -> bool:
    return bool(
        filter_data.get(f"{prefix}_all_ukraine")
        or filter_data.get(f"{prefix}_cities")
        or filter_data.get(f"{prefix}_regions")
        or filter_data.get("route_from" if prefix == "from" else "route_to")
    )


def render_filter(filter_data: Dict[str, Any]) -> str:
    def loc(prefix: str) -> str:
        if filter_data.get(f"{prefix}_all_ukraine"):
            return "Вся Україна"
        values = []
        values += [f"🏙 {x}" for x in filter_data.get(f"{prefix}_cities", [])]
        values += [f"🗺 {x} обл." for x in filter_data.get(f"{prefix}_regions", [])]
        if not values:
            legacy = filter_data.get("route_from" if prefix == "from" else "route_to", "")
            return legacy or "Не задано"
        return ", ".join(values)

    def range_text(min_key: str, max_key: str, unit: str) -> str:
        low = filter_data.get(min_key)
        high = filter_data.get(max_key)
        if low is None and high is None:
            return "не задано"
        if low is None:
            return f"до {high} {unit}"
        if high is None:
            return f"від {low} {unit}"
        return f"{low}–{high} {unit}"

    transports = filter_data.get("transport_types", [])
    hot_min = filter_data.get("hot_min_price_km")
    hot_text = f"від {hot_min:.2f} грн/км" if hot_min is not None and hot_min > 0 else "вимкнена"
    return (
        "<b>📋 Поточний фільтр</b>\n\n"
        f"📍 <b>Звідки:</b> {html.escape(loc('from'))}\n"
        f"🎯 <b>Куди:</b> {html.escape(loc('to'))}\n\n"
        f"⚖️ <b>Маса:</b> {html.escape(range_text('min_weight', 'max_weight', 'т'))}\n"
        f"📐 <b>Об'єм:</b> {html.escape(range_text('min_volume', 'max_volume', 'м³'))}\n"
        f"📏 <b>Габарити:</b> дов {html.escape(range_text('min_length', 'max_length', 'м'))}; шир {html.escape(range_text('min_width', 'max_width', 'м'))}; вис {html.escape(range_text('min_height', 'max_height', 'м'))}\n"
        f"🚛 <b>Транспорт:</b> {html.escape(', '.join(transports) if transports else 'будь-який')}\n\n"
        f"💵 <b>Мін. ставка:</b> {filter_data.get('min_price_km') if filter_data.get('min_price_km') is not None else 'будь-яка'} грн/км\n"
        f"🔥 <b>Гаряча ставка:</b> {html.escape(hot_text)}\n"
        f"📋 <b>Неповні дані:</b> "
        f"{'показувати' if filter_data.get('allow_incomplete_data') else 'не показувати'}\n\n"
        f"🔄 <b>Зворотний пошук:</b> {'увімкнений' if filter_data.get('return_search_enabled') else 'вимкнений'}\n"
        f"🚛 <b>Тільки туди + назад:</b> {'так' if filter_data.get('round_trip_only') else 'ні'}"
    )


async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "👋 <b>Вантажний Радар</b>\n\n"
        "Налаштовуйте фільтр кнопками: області, кілька міст, діапазони маси/об'єму, транспорт, зворотний пошук та режим «туди + назад».\n\n"
        "Старий формат <code>/set Черкаси; Брюховичі; 5.0; 40.0</code> теж підтримується.",
        parse_mode="HTML",
        reply_markup=bottom_menu(),
    )


@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    await cmd_start(message, state)


@dp.message(Command("set"))
async def cmd_set_filter(message: types.Message, state: FSMContext):
    await state.clear()
    match = SET_PATTERN.match(message.text.strip())
    if not match:
        await message.answer(
            "❌ <b>Помилка формату!</b>\n\n"
            "Використовуйте:\n"
            "<code>/set Черкаси; Брюховичі; 5.0; 40.0</code>",
            parse_mode="HTML",
        )
        return

    filter_data = blank_filter()
    filter_data["route_from"] = normalize_text(match.group("from"))
    filter_data["route_to"] = normalize_text(match.group("to"))
    filter_data["min_weight"] = safe_float(match.group("weight"))
    filter_data["min_price_km"] = safe_float(match.group("price"))
    filter_data["enabled"] = True
    chat_id = message.chat.id

    await persist_filter(chat_id, filter_data)
    await message.answer("✅ Старий фільтр збережено. Показую всі відповідні оголошення за 48 годин...", reply_markup=bottom_menu())
    await send_archive(message, filter_data)


@dp.message(Command("clear"))
async def cmd_clear(message: types.Message, state: FSMContext):
    await state.clear()
    chat_id = str(message.chat.id)
    assert redis_client is not None
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.delete(f"filter:{chat_id}")
        pipe.srem("filters:active_users", chat_id)
        pipe.publish("channel:filters:update", chat_id)
        await pipe.execute()
    await message.answer("🗑 Фільтр видалено. Сповіщення зупинено.", reply_markup=bottom_menu())


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("↩️ Редагування скасовано.", reply_markup=bottom_menu())


@dp.callback_query()
async def callbacks(callback: CallbackQuery, state: FSMContext):
    data = callback.data or ""
    chat_id = callback.message.chat.id if callback.message else callback.from_user.id
    filter_data = await get_working_filter(chat_id, state)

    if data == "noop":
        await callback.answer()
        return
    if data == "menu":
        await state.clear()
        await callback.message.edit_text("🏠 <b>Головне меню</b>", parse_mode="HTML", reply_markup=main_menu())
        await callback.answer()
        return
    if data == "cfg":
        await state.clear()
        await state.update_data(filter=filter_data)
        await callback.message.edit_text(render_filter(filter_data), parse_mode="HTML", reply_markup=config_menu(filter_data))
        await callback.answer()
        return
    if data == "view":
        await callback.message.edit_text(render_filter(filter_data), parse_mode="HTML", reply_markup=main_menu())
        await callback.answer()
        return
    if data == "hist":
        if not filter_data.get("enabled"):
            await callback.answer("Спочатку налаштуйте та збережіть фільтр", show_alert=True)
            return
        await callback.answer("Шукаю активні оголошення...")
        await send_archive(callback.message, filter_data)
        return
    if data == "clear":
        assert redis_client is not None
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.delete(f"filter:{chat_id}")
            pipe.srem("filters:active_users", str(chat_id))
            pipe.publish("channel:filters:update", str(chat_id))
            await pipe.execute()
        await state.clear()
        await callback.message.edit_text("🗑 <b>Фільтр очищено.</b>", parse_mode="HTML", reply_markup=main_menu())
        await callback.answer("Готово")
        return

    if data == "incomplete_toggle":
        filter_data["allow_incomplete_data"] = not bool(filter_data.get("allow_incomplete_data"))
        await state.update_data(filter=filter_data)
        await callback.message.edit_text(
            render_filter(filter_data),
            parse_mode="HTML",
            reply_markup=config_menu(filter_data),
        )
        await callback.answer(
            "Показ неповних даних увімкнено"
            if filter_data["allow_incomplete_data"]
            else "Показ неповних даних вимкнено"
        )
        return


    if data in {"return_toggle", "return_toggle_cfg"}:
        if data == "return_toggle":
            filter_data = await get_saved_filter(chat_id)
            if not filter_data.get("enabled"):
                await callback.answer("Спочатку налаштуйте та збережіть фільтр", show_alert=True)
                return
        filter_data["return_search_enabled"] = not bool(filter_data.get("return_search_enabled"))
        if data == "return_toggle":
            await persist_filter(chat_id, filter_data)
        else:
            await state.update_data(filter=filter_data)
        await callback.answer("Зворотний пошук увімкнено" if filter_data["return_search_enabled"] else "Зворотний пошук вимкнено")
        if data == "return_toggle_cfg":
            await callback.message.edit_text(render_filter(filter_data), parse_mode="HTML", reply_markup=config_menu(filter_data))
        else:
            await callback.message.edit_text(render_filter(filter_data), parse_mode="HTML", reply_markup=main_menu())
        return


    if data in {"roundtrip_toggle", "roundtrip_toggle_cfg"}:
        if data == "roundtrip_toggle":
            filter_data = await get_saved_filter(chat_id)
            if not filter_data.get("enabled"):
                await callback.answer("Спочатку налаштуйте та збережіть фільтр", show_alert=True)
                return
        new_value = not bool(filter_data.get("round_trip_only"))
        filter_data["round_trip_only"] = new_value
        if new_value:
            filter_data["return_search_enabled"] = True
        if data == "roundtrip_toggle":
            await persist_filter(chat_id, filter_data)
        else:
            await state.update_data(filter=filter_data)
        await callback.answer("Режим «тільки туди + назад» увімкнено" if new_value else "Режим «тільки туди + назад» вимкнено")
        if data == "roundtrip_toggle_cfg":
            await callback.message.edit_text(render_filter(filter_data), parse_mode="HTML", reply_markup=config_menu(filter_data))
        else:
            await callback.message.edit_text(render_filter(filter_data), parse_mode="HTML", reply_markup=main_menu())
        return

    if data == "forecast":
        if not filter_data.get("enabled"):
            await callback.answer("Спочатку налаштуйте та збережіть фільтр", show_alert=True)
            return
        await callback.answer("Розраховую статистичний прогноз...")
        await send_forecast(callback.message, filter_data)
        return

    if data == "save":
        if not location_configured(filter_data, "from") or not location_configured(filter_data, "to"):
            await callback.answer("Заповніть «Звідки» та «Куди» або виберіть «Вся Україна»", show_alert=True)
            return
        filter_data["enabled"] = True
        await persist_filter(chat_id, filter_data)
        await state.clear()
        await callback.message.edit_text(
            "✅ <b>Фільтр збережено та активовано.</b>\n\n"
            "📦 Нові вантажі надходитимуть у реальному часі.\n"
            "Для архіву за останні 48 годин використовуйте кнопку «Наявні вантажі».",
            parse_mode="HTML",
            reply_markup=main_menu(),
        )
        await callback.answer("Фільтр активовано")
        return

    if data.startswith("loc:"):
        _, side, action = (data.split(":", 2) + [""])[:3]
        if action == "all":
            filter_data[f"{side}_all_ukraine"] = True
            filter_data[f"{side}_cities"] = []
            filter_data[f"{side}_regions"] = []
        elif action == "clear":
            filter_data[f"{side}_all_ukraine"] = False
            filter_data[f"{side}_cities"] = []
            filter_data[f"{side}_regions"] = []
        elif action in {"city", "region"}:
            await state.set_state(FilterWizard.waiting_value)
            await state.update_data(filter=filter_data, input_kind=f"{side}_{action}")
            label = "міста через кому" if action == "city" else "області через кому"
            example = "Калуш, Коломия" if action == "city" else "Івано-Франківська, Львівська"
            await callback.message.answer(f"✍️ Введіть {label}.\nПриклад: <code>{example}</code>\n\nДля скасування: /cancel", parse_mode="HTML")
            await callback.answer()
            return
        elif action == "done":
            pass
        await state.update_data(filter=filter_data)
        await callback.message.edit_text(render_filter(filter_data), parse_mode="HTML", reply_markup=location_menu(side, filter_data))
        await callback.answer()
        return
    
    if data in {"val:weight", "val:volume", "val:price"}:
        kind = data.split(":", 1)[1]
        titles = {
            "weight": "⚖️ <b>Виберіть мінімальну масу</b>",
            "volume": "📐 <b>Виберіть мінімальний об'єм</b>",
            "price": "💵 <b>Виберіть мінімальну ставку / км</b>",
        }
        await callback.message.edit_text(
            titles[kind],
            parse_mode="HTML",
            reply_markup=numeric_preset_menu(kind),
        )
        await callback.answer()
        return
    
    if data.startswith("preset:"):
        _, action, raw_value = (data.split(":", 2) + [""])[:3]
        if action == "clear":
            if raw_value == "weight":
                filter_data["min_weight"] = None
                filter_data["max_weight"] = None
            elif raw_value == "volume":
                filter_data["min_volume"] = None
                filter_data["max_volume"] = None
            elif raw_value == "price":
                filter_data["min_price_km"] = None
            else:
                await callback.answer("Невідомий preset", show_alert=True)
                return
        elif action == "weight":
            filter_data["min_weight"] = safe_float(raw_value)
            filter_data["max_weight"] = None
        elif action == "volume":
            filter_data["min_volume"] = safe_float(raw_value)
            filter_data["max_volume"] = None
        elif action == "price":
            if raw_value == "+5":
                filter_data["min_price_km"] = (filter_data.get("min_price_km") or 0) + 5
            else:
                filter_data["min_price_km"] = safe_float(raw_value)
        else:
            await callback.answer("Невідомий preset", show_alert=True)
            return

        await state.update_data(filter=filter_data)
        await callback.message.edit_text(
            render_filter(filter_data),
            parse_mode="HTML",
            reply_markup=config_menu(filter_data),
        )
        await callback.answer("Значення встановлено")
        return

    if data.startswith("custom:"):
        kind = data.split(":", 1)[1]
        if kind not in {"weight", "volume", "price"}:
            await callback.answer("Невідомий режим", show_alert=True)
            return
        await state.set_state(FilterWizard.waiting_value)
        await state.update_data(filter=filter_data, input_kind=kind)
        prompts = {
            "weight": "⚖️ Введіть <code>мін;макс</code> у тоннах. Наприклад: <code>5;20</code>.",
            "volume": "📐 Введіть <code>мін;макс</code> у м³. Наприклад: <code>20;80</code>.",
            "price": "💵 Введіть мінімальну ставку грн/км. Для вимкнення — <code>0</code>.",
        }
        await callback.message.answer(prompts[kind], parse_mode="HTML")
        await callback.answer()
        return

        
    
    if data in {"val:length", "val:width", "val:height"}:
        labels = {"val:length": ("довжину", "м"), "val:width": ("ширину", "м"), "val:height": ("висоту", "м")}
        label, unit = labels[data]
        await state.set_state(FilterWizard.waiting_value)
        await state.update_data(filter=filter_data, input_kind=data.split(":", 1)[1])
        await callback.message.answer(f"📏 Введіть <code>мін;макс</code> у {unit} для {label}.\nНаприклад: <code>2;4</code>.", parse_mode="HTML")
        await callback.answer()
        return

    if data == "val:hot_price":
        await state.set_state(FilterWizard.waiting_value)
        await state.update_data(filter=filter_data, input_kind="hot_price")
        await callback.message.answer("🔥 Введіть поріг гарячої ставки грн/км. Наприклад <code>20</code>. Для вимкнення — <code>0</code>.", parse_mode="HTML")
        await callback.answer()
        return

    if data == "transport":
        await callback.message.edit_text("🚛 <b>Виберіть потрібні типи транспорту</b>\nМожна вибрати декілька.", parse_mode="HTML", reply_markup=transport_menu(filter_data))
        await callback.answer()
        return
    if data.startswith("tr:"):
        action = data.split(":", 1)[1]
        if action == "clear":
            filter_data["transport_types"] = []
        else:
            index = int(action)
            value = TRANSPORT_OPTIONS[index]
            selected = list(filter_data.get("transport_types", []))
            if value in selected:
                selected.remove(value)
            else:
                selected.append(value)
            filter_data["transport_types"] = selected
        await state.update_data(filter=filter_data)
        await callback.message.edit_text("🚛 <b>Виберіть потрібні типи транспорту</b>\nМожна вибрати декілька.", parse_mode="HTML", reply_markup=transport_menu(filter_data))
        await callback.answer()
        return

    await callback.answer("Невідома команда")


@dp.message(F.text.in_({
    "⚙️ Налаштувати фільтр",
    "📋 Мій фільтр",
    "📦 Наявні вантажі (48г)",
    "🔄 Зворотний пошук",
    "🚛 Туди + назад",
    "📈 Прогноз повернення",
    "🗑 Очистити фільтр",
}))
async def bottom_menu_handler(message: types.Message, state: FSMContext):
    text = (message.text or "").strip()
    chat_id = message.chat.id

    if text == "⚙️ Налаштувати фільтр":
        filter_data = await get_saved_filter(chat_id)
        await state.clear()
        await state.update_data(filter=filter_data)
        await message.answer(render_filter(filter_data), parse_mode="HTML", reply_markup=config_menu(filter_data))
        return

    if text == "📋 Мій фільтр":
        await message.answer(render_filter(await get_saved_filter(chat_id)), parse_mode="HTML", reply_markup=bottom_menu())
        return

    if text == "📦 Наявні вантажі (48г)":
        filter_data = await get_saved_filter(chat_id)
        if not filter_data.get("enabled"):
            await message.answer("Спочатку налаштуйте та збережіть фільтр", reply_markup=bottom_menu())
            return
        await send_archive(message, filter_data)
        return

    if text == "🔄 Зворотний пошук":
        filter_data = await get_saved_filter(chat_id)
        if not filter_data.get("enabled"):
            await message.answer("Спочатку налаштуйте та збережіть фільтр", reply_markup=bottom_menu())
            return
        filter_data["return_search_enabled"] = not bool(filter_data.get("return_search_enabled"))
        await persist_filter(chat_id, filter_data)
        await message.answer(render_filter(filter_data), parse_mode="HTML", reply_markup=bottom_menu())
        return

    if text == "🚛 Туди + назад":
        filter_data = await get_saved_filter(chat_id)
        if not filter_data.get("enabled"):
            await message.answer("Спочатку налаштуйте та збережіть фільтр", reply_markup=bottom_menu())
            return
        new_value = not bool(filter_data.get("round_trip_only"))
        filter_data["round_trip_only"] = new_value
        if new_value:
            filter_data["return_search_enabled"] = True
        await persist_filter(chat_id, filter_data)
        await message.answer(render_filter(filter_data), parse_mode="HTML", reply_markup=bottom_menu())
        return

    if text == "📈 Прогноз повернення":
        filter_data = await get_saved_filter(chat_id)
        if not filter_data.get("enabled"):
            await message.answer("Спочатку налаштуйте та збережіть фільтр", reply_markup=bottom_menu())
            return
        await send_forecast(message, filter_data)
        return

    if text == "🗑 Очистити фільтр":
        assert redis_client is not None
        async with redis_client.pipeline(transaction=True) as pipe:
            pipe.delete(f"filter:{chat_id}")
            pipe.srem("filters:active_users", str(chat_id))
            pipe.publish("channel:filters:update", str(chat_id))
            await pipe.execute()
        await state.clear()
        await message.answer("🗑 Фільтр очищено.", reply_markup=bottom_menu())


@dp.message(FilterWizard.waiting_value, F.text)
async def wizard_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    filter_data = data.get("filter") or await get_saved_filter(message.chat.id)
    input_kind = data.get("input_kind", "")
    text = (message.text or "").strip()

    if input_kind.endswith("_city"):
        side = input_kind.split("_", 1)[0]
        values = split_values(text)
        if not values:
            await message.answer("❌ Не вдалося знайти жодного міста. Спробуйте ще раз.")
            return
        current = filter_data.get(f"{side}_cities", [])
        filter_data[f"{side}_cities"] = list(dict.fromkeys(current + values))
        filter_data[f"{side}_all_ukraine"] = False
    elif input_kind.endswith("_region"):
        side = input_kind.split("_", 1)[0]
        values = split_values(text, region=True)
        if not values:
            await message.answer("❌ Не вдалося знайти жодної області. Спробуйте ще раз.")
            return
        current = filter_data.get(f"{side}_regions", [])
        filter_data[f"{side}_regions"] = list(dict.fromkeys(current + values))
        filter_data[f"{side}_all_ukraine"] = False
    elif input_kind in {"weight", "volume", "length", "width", "height"}:
        parts = [x.strip() for x in text.split(";", 1)]
        if len(parts) != 2:
            await message.answer("❌ Формат: <code>мін;макс</code>. Наприклад <code>5;20</code>.", parse_mode="HTML")
            return
        low = safe_float(parts[0]) if parts[0] else None
        high = safe_float(parts[1]) if parts[1] else None
        if (parts[0] and low is None) or (parts[1] and high is None):
            await message.answer("❌ Значення повинні бути невід'ємними числами. Наприклад: <code>5;20</code>.", parse_mode="HTML")
            return
        if low is not None and high is not None and low > high:
            await message.answer("❌ Мінімум не може бути більшим за максимум.")
            return
        range_keys = {
            "weight": ("min_weight", "max_weight"),
            "volume": ("min_volume", "max_volume"),
            "length": ("min_length", "max_length"),
            "width": ("min_width", "max_width"),
            "height": ("min_height", "max_height"),
        }
        min_key, max_key = range_keys[input_kind]
        filter_data[min_key] = low
        filter_data[max_key] = high
    elif input_kind == "price":
        value = safe_float(text)
        if value is None and text:
            await message.answer("❌ Введіть невід'ємне число, наприклад <code>40</code>.", parse_mode="HTML")
            return
        filter_data["min_price_km"] = None if value in {None, 0} else value

    elif input_kind == "hot_price":
        value = safe_float(text)
        if value is None and text:
            await message.answer("❌ Введіть невід'ємне число, наприклад <code>20</code>.", parse_mode="HTML")
            return
        filter_data["hot_min_price_km"] = None if value in {None, 0} else value
    else:
        await state.clear()
        await message.answer("ℹ️ Неочікуваний режим налаштування.", reply_markup=main_menu())
        return

    await state.clear()
    await state.update_data(filter=filter_data)
    await message.answer(render_filter(filter_data), parse_mode="HTML", reply_markup=config_menu(filter_data))


async def init_services():
    global pg_pool, redis_client
    host, port = REDIS_ADDR.split(":")

    for attempt in range(1, 11):
        try:
            redis_client = aioredis.Redis(
                host=host, port=int(port), password=REDIS_PASS, decode_responses=True
            )
            await redis_client.ping()
            logging.info("Успішно підключено до Redis.")
            break
        except Exception as exc:
            logging.warning("Redis не готовий (спроба %s/10): %s", attempt, exc)
            await asyncio.sleep(2)
    else:
        raise ConnectionError("Не вдалося підключитися до Redis після 10 спроб.")

    for attempt in range(1, 11):
        try:
            pg_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
            async with pg_pool.acquire() as conn:
                await conn.execute("SELECT 1")
            logging.info("Успішно підключено до PostgreSQL.")
            break
        except Exception as exc:
            logging.warning("PostgreSQL не готовий (спроба %s/10): %s", attempt, exc)
            await asyncio.sleep(2)
    else:
        raise ConnectionError("Не вдалося підключитися до PostgreSQL після 10 спроб.")


async def main():
    await init_services()
    try:
        await dp.start_polling(bot)
    finally:
        if pg_pool:
            await pg_pool.close()
        if redis_client:
            await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())