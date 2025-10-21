import datetime
import requests
import time
from supabase import create_client
import os

# === Supabase настройки ===
# ⚠️ ЗАМЕНИ ЭТИ СТРОКИ НА ТВОИ ЗНАЧЕНИЯ!
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

# === Конфигурация данных ===
CRYPTO_IDS = {
    "DOGE": "dogecoin",
    "SHIB": "shiba-inu",
    "PEPE": "pepe",
    "BONK": "bonk"
}

CITIES = {
    "Moscow": (55.7558, 37.6176),
    "Saint Petersburg": (59.9343, 30.3351),
    "Novosibirsk": (55.0084, 82.9357),
    "Yekaterinburg": (56.8389, 60.6057),
    "Kazan": (55.8304, 49.0661),
    "Rostov-on-Don": (47.2313, 39.7233)
}

# === Инициализация Supabase ===
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Дата ===
yesterday = datetime.date.today() - datetime.timedelta(days=1)
date_for_coingecko = yesterday.strftime("%d-%m-%Y")
date_for_openmeteo = yesterday.isoformat()

print("Загружаем данные за:", yesterday)

# === 1. Криптовалюты ===
print("\n--- Криптовалюты ---")
crypto_records = []
for symbol, coin_id in CRYPTO_IDS.items():
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history?date={date_for_coingecko}&localization=false"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        price = data.get("market_data", {}).get("current_price", {}).get("usd")
        if price is not None:
            crypto_records.append({
                "date": date_for_openmeteo,
                "symbol": symbol,
                "price_usd": price
            })
            print(f"✅ {symbol}: ${price}")
        else:
            print(f"❌ {symbol}: цена не найдена")
    else:
        print(f"❌ Ошибка CoinGecko для {symbol}: {response.status_code}")
    time.sleep(1)

# Запись в базу (игнорируем дубли)
if crypto_records:
    try:
        supabase.table("crypto_prices").upsert(crypto_records, on_conflict="date,symbol").execute()
        print("✅ Криптовалюты сохранены в Supabase")
    except Exception as e:
        print("❌ Ошибка записи крипты:", e)

# === 2. Погода ===
print("\n--- Погода ---")
weather_records = []
for city, (lat, lng) in CITIES.items():
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lng}"
        f"&start_date={date_for_openmeteo}&end_date={date_for_openmeteo}"
        f"&daily=temperature_2m_max,temperature_2m_min,temperature_2m_mean"
        f"&timezone=Europe/Moscow"
    )
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        daily = data.get("daily")
        if daily and len(daily["time"]) > 0:
            record = {
                "date": date_for_openmeteo,
                "city": city,
                "temp_avg": daily["temperature_2m_mean"][0],
                "temp_min": daily["temperature_2m_min"][0],
                "temp_max": daily["temperature_2m_max"][0]
            }
            weather_records.append(record)
            print(f"✅ {city}: avg={record['temp_avg']}°C")
        else:
            print(f"❌ {city}: данные пусты")
    else:
        print(f"❌ Ошибка Open-Meteo для {city}: {response.status_code}")
    time.sleep(1)

# Запись в базу (игнорируем дубли)
if weather_records:
    try:
        supabase.table("weather_data").upsert(weather_records, on_conflict="date,city").execute()
        print("✅ Погода сохранена в Supabase")
    except Exception as e:
        print("❌ Ошибка записи погоды:", e)

print("\n✅ Pipeline завершён: данные собраны и сохранены.")