import asyncio
import logging
import os
import re
import asyncpg
import redis.asyncio as aioredis
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

logging.basicConfig(level=logging.INFO)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
REDIS_ADDR = os.getenv("REDIS_ADDR", "redis:6379")
REDIS_PASS = os.getenv("REDIS_PASSWORD")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=TOKEN)
dp = Dispatcher()

pg_pool: asyncpg.Pool = None
redis_client: aioredis.Redis = None

SET_PATTERN = re.compile(
    r"^/set\s+(?P<from>[^;]+);\s*(?P<to>[^;]+);\s*(?P<weight>\d+(?:[\.,]\d+)?);\s*(?P<price>\d+(?:[\.,]\d+)?)$",
    re.IGNORECASE,
)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 <b>Вантажний Радар [Production]</b>\n\n"
        "Встановіть фільтр моніторингу:\n"
        "<code>/set Черкаси; Брюховичі; 5.0; 40.0</code>\n\n"
        "Формат: <i>Звідки; Куди; Мін. вага (т); Мін. ставка (грн/км)</i>\n"
        "Скинути активний фільтр: <code>/clear</code>",
        parse_mode="HTML"
    )

@dp.message(Command("set"))
async def cmd_set_filter(message: types.Message):
    match = SET_PATTERN.match(message.text.strip())
    if not match:
        await message.answer(
            "❌ <b>Помилка формату!</b>\n\n"
            "Використовуйте крапку з комою як розділювач:\n"
            "<code>/set Черкаси; Брюховичі; 5.0; 40.0</code>",
            parse_mode="HTML"
        )
        return

    route_from = match.group("from").strip()
    route_to = match.group("to").strip()
    min_weight = float(match.group("weight").replace(",", "."))
    min_price_km = float(match.group("price").replace(",", "."))
    chat_id = message.chat.id

    await message.answer("🔍 Пошук активних рейсів в архіві за 48 годин...")

    query = """
        SELECT route_from, route_to, distance_km, cargo_type, weight_t, 
               volume_m3, price_uah, price_per_km_uah, published_relative
        FROM cargo_history
        WHERE created_at >= NOW() - INTERVAL '48 HOURS'
          AND route_from ILIKE $1
          AND route_to ILIKE $2
          AND weight_t >= $3
          AND price_per_km_uah >= $4
        ORDER BY created_at DESC
        LIMIT 5;
    """

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            query,
            f"%{route_from}%",
            f"%{route_to}%",
            min_weight,
            min_price_km
        )

    if rows:
        for r in rows:
            text = (
                f"📦 <b>[Архів 48г]</b>\n"
                f"📍 <b>{r['route_from']} ➔ {r['route_to']}</b> ({r['distance_km']} км)\n"
                f"Вантаж: {r['cargo_type']} | {r['weight_t']} т | {r['volume_m3']} м³\n"
                f"💰 <b>{r['price_uah']} грн</b> (<b>{r['price_per_km_uah']} грн/км</b>)\n"
                f"⏱ {r['published_relative']}"
            )
            await message.answer(text, parse_mode="HTML")
    else:
        await message.answer("ℹ️ За останні 48 годин відповідних вантажів у базі не знайдено.")

    filter_data = {
        "route_from": route_from.lower(),
        "route_to": route_to.lower(),
        "min_weight": str(min_weight),
        "min_price_km": str(min_price_km),
        "enabled": "1"
    }

    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.hset(f"filter:{chat_id}", mapping=filter_data)
        pipe.sadd("filters:active_users", str(chat_id))
        pipe.publish("channel:filters:update", str(chat_id))
        await pipe.execute()

    await message.answer(
        f"✅ <b>Моніторинг активовано:</b>\n"
        f"📍 {route_from} ➔ {route_to}\n"
        f"⚖️ Від {min_weight} т | 💵 Від {min_price_km} грн/км\n\n"
        f"Нові оголошення надходитимуть сюди в реальному часі.",
        parse_mode="HTML"
    )

@dp.message(Command("clear"))
async def cmd_clear(message: types.Message):
    chat_id = str(message.chat.id)
    async with redis_client.pipeline(transaction=True) as pipe:
        pipe.delete(f"filter:{chat_id}")
        pipe.srem("filters:active_users", chat_id)
        pipe.publish("channel:filters:update", chat_id)
        await pipe.execute()

    await message.answer("🗑 Фільтр видалено. Сповіщення зупинено.")

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
        except Exception as e:
            logging.warning(f"Redis не готовий (спроба {attempt}/10): {e}")
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
        except Exception as e:
            logging.warning(f"PostgreSQL не готовий (спроба {attempt}/10): {e}")
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