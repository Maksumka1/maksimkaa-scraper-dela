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
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

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
        "schema_version": "2",
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
        "transport_types": [],
        "min_price_km": None,
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
    result["transport_types"] = safe_json_list(data.get("transport_types", "[]"))
    result["min_price_km"] = safe_float(data.get("min_price_km"))
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
        "schema_version": "2",
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
        "transport_types": json.dumps(transport_types, ensure_ascii=False),
        "min_price_km": numeric(data.get("min_price_km")),
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
    if min_price is not None:
        args.append(min_price)
        clauses.append(f"price_per_km_uah >= ${len(args)}")

    transport_types = [normalize_text(x) for x in filter_data.get("transport_types", []) if normalize_text(x)]
    if transport_types:
        args.append(transport_types)
        clauses.append(f"transport_types && ${len(args)}::text[]")

    query = f"""
        SELECT route_from, route_to, order_url,route_from_full, route_to_full,
               route_from_region, route_to_region, distance_km, cargo_type,
               weight_t, volume_m3, price_uah, price_per_km_uah,
               transport_types, published_relative, created_at
        FROM cargo_history
        WHERE {' AND '.join(clauses)}
        ORDER BY created_at DESC
    """
    return query, args


def format_archive_row(row: asyncpg.Record) -> str:
    transport = row["transport_types"] or []
    lines = [
        "📦 <b>[Архів 48г]</b>",
        f"📍 <b>{html.escape(str(row['route_from']))} ➔ {html.escape(str(row['route_to']))}</b> ({row['distance_km'] or 0} км)",
        f"Вантаж: {html.escape(str(row['cargo_type'] or '—'))} | {row['weight_t'] if row['weight_t'] is not None else '—'} т | {row['volume_m3'] if row['volume_m3'] is not None else '—'} м³",
        f"💰 <b>{row['price_uah'] if row['price_uah'] is not None else '—'} грн</b> ({row['price_per_km_uah'] if row['price_per_km_uah'] is not None else '—'} грн/км)",
    ]
    if transport:
        lines.append(f"🚛 {html.escape(', '.join(str(x) for x in transport))}")

    if row["order_url"]:
        lines.append(
            f'🔗 <a href="{html.escape(str(row["order_url"]), quote=True)}">Відкрити замовлення на Della</a>'
        )
    
    lines.append(f"⏱ {html.escape(str(row['published_relative'] or ''))}")
    return "\n".join(lines)


async def send_archive(message: types.Message, filter_data: Dict[str, Any], *, header: bool = True) -> int:
    assert pg_pool is not None
    query, args = build_archive_query(filter_data)
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *args)

    if not rows:
        await message.answer("ℹ️ За останні 48 годин відповідних вантажів не знайдено.", reply_markup=main_menu())
        return 0

    chunks: List[str] = []
    current = ""
    if header:
        current = "📋 <b>Знайдені активні оголошення за останні 48 годин</b>\n"
        current += f"Знайдено: <b>{len(rows)}</b>\n\n"

    for row in rows:
        block = format_archive_row(row)
        if len(current) + len(block) + 2 > ARCHIVE_MESSAGE_LIMIT and current.strip():
            chunks.append(current.rstrip())
            current = ""
        current += block + "\n\n"
    if current.strip():
        chunks.append(current.rstrip())

    for index, chunk in enumerate(chunks):
        await message.answer(chunk, parse_mode="HTML")
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

    for key, col in (("min_weight", "weight_t"), ("min_volume", "volume_m3"), ("min_price_km", "price_per_km_uah")):
        value = filter_data.get(key)
        if value is not None:
            args.append(value)
            clauses.append(f"{col} >= ${len(args)}")
    for key, col in (("max_weight", "weight_t"), ("max_volume", "volume_m3")):
        value = filter_data.get(key)
        if value is not None:
            args.append(value)
            clauses.append(f"{col} IS NOT NULL AND {col} <= ${len(args)}")

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
            reply_markup=main_menu(),
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

    await message.answer("\n".join(lines).rstrip(), parse_mode="HTML", reply_markup=main_menu())
    return len(forecasts)

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


def config_menu(filter_data: Dict[str, Any]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📍 Звідки", callback_data="loc:from")],
            [InlineKeyboardButton(text="🎯 Куди", callback_data="loc:to")],
            [InlineKeyboardButton(text="⚖️ Маса", callback_data="val:weight")],
            [InlineKeyboardButton(text="📐 Об'єм", callback_data="val:volume")],
            [InlineKeyboardButton(text="🚛 Тип транспорту", callback_data="transport")],
            [InlineKeyboardButton(text="💵 Мін. ставка / км", callback_data="val:price")],
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
    return (
        "<b>📋 Поточний фільтр</b>\n\n"
        f"📍 <b>Звідки:</b> {html.escape(loc('from'))}\n"
        f"🎯 <b>Куди:</b> {html.escape(loc('to'))}\n"
        f"⚖️ <b>Маса:</b> {html.escape(range_text('min_weight', 'max_weight', 'т'))}\n"
        f"📐 <b>Об'єм:</b> {html.escape(range_text('min_volume', 'max_volume', 'м³'))}\n"
        f"🚛 <b>Транспорт:</b> {html.escape(', '.join(transports) if transports else 'будь-який')}\n"
        f"💵 <b>Ставка:</b> {filter_data.get('min_price_km') if filter_data.get('min_price_km') is not None else 'будь-яка'} грн/км\n"
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
        reply_markup=main_menu(),
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
    await message.answer("✅ Старий фільтр збережено. Показую всі відповідні оголошення за 48 годин...", reply_markup=main_menu())
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
    await message.answer("🗑 Фільтр видалено. Сповіщення зупинено.", reply_markup=main_menu())


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("↩️ Редагування скасовано.", reply_markup=main_menu())


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

    if data == "val:weight":
        await state.set_state(FilterWizard.waiting_value)
        await state.update_data(filter=filter_data, input_kind="weight")
        await callback.message.answer("⚖️ Введіть <code>мін;макс</code> у тоннах.\nНаприклад: <code>5;20</code> або <code>5;</code> або <code>;20</code>.", parse_mode="HTML")
        await callback.answer()
        return
    if data == "val:volume":
        await state.set_state(FilterWizard.waiting_value)
        await state.update_data(filter=filter_data, input_kind="volume")
        await callback.message.answer("📐 Введіть <code>мін;макс</code> у м³.\nНаприклад: <code>20;80</code>.", parse_mode="HTML")
        await callback.answer()
        return
    if data == "val:price":
        await state.set_state(FilterWizard.waiting_value)
        await state.update_data(filter=filter_data, input_kind="price")
        await callback.message.answer("💵 Введіть мінімальну ставку грн/км. Для вимкнення — <code>0</code>.", parse_mode="HTML")
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
    elif input_kind in {"weight", "volume"}:
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
        min_key, max_key = (("min_weight", "max_weight") if input_kind == "weight" else ("min_volume", "max_volume"))
        filter_data[min_key] = low
        filter_data[max_key] = high
    elif input_kind == "price":
        value = safe_float(text)
        if value is None and text:
            await message.answer("❌ Введіть невід'ємне число, наприклад <code>40</code>.", parse_mode="HTML")
            return
        filter_data["min_price_km"] = None if value in {None, 0} else value
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
