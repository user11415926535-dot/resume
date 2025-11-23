import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import os
import sys
import time
import sqlite3
from datetime import datetime, timedelta
from contextlib import closing
import asyncio
import html
from telethon import TelegramClient
from telethon.sessions import StringSession

# URL поиска
URL = "https://hh.ru/search/resume?area=1&area=2&exp_period=all_time&logic=normal&no_magic=true&ored_clusters=true&pos=full_text&search_period=3&text=Python+%D1%80%D0%B0%D0%B7%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%87%D0%B8%D0%BA&order_by=publication_time"

# Путь к постоянному хранилищу в Amvera
DATA_DIR = os.environ.get("AMVERA_DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(DATA_DIR, "hh_results.txt")
DB_FILE = os.path.join(DATA_DIR, "hh_resumes.db")

# Telegram configuration
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH")
SESSION_STRING = os.getenv("TELETHON_SESSION_STRING")
DEST_CHANNEL = os.getenv("DEST_CHANNEL")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

# Global Telegram client
tg_client = None

def init_db():
    """Инициализация базы данных"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS resumes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    title TEXT,
                    context TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_url ON resumes(url)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_created_at ON resumes(created_at)')

def cleanup_old_resumes():
    """Удаление резюме старше 14 дней"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with conn:
            cutoff_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d %H:%M:%S')
            conn.execute('DELETE FROM resumes WHERE created_at < ?', (cutoff_date,))
            deleted_count = conn.total_changes
            if deleted_count > 0:
                print(f"🗑️ Удалено {deleted_count} старых резюме")

def save_resume(url, title, context):
    """Сохранение резюме в базу данных"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        with conn:
            try:
                conn.execute(
                    'INSERT INTO resumes (url, title, context) VALUES (?, ?, ?)',
                    (url, title, context)
                )
                return True  # Новое резюме
            except sqlite3.IntegrityError:
                return False  # Уже существует

def get_today_stats():
    """Получение статистики за сегодня"""
    with closing(sqlite3.connect(DB_FILE)) as conn:
        cursor = conn.cursor()
        today = datetime.now().strftime('%Y-%m-%d')
        cursor.execute(
            'SELECT COUNT(*) FROM resumes WHERE DATE(created_at) = ?',
            (today,)
        )
        today_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM resumes')
        total_count = cursor.fetchone()[0]
        
        return today_count, total_count

def fetch_page(url: str, timeout: int = 15) -> str:
    """Загрузка страницы"""
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    return resp.text

def extract_resumes(html: str, base_url: str) -> list:
    """Извлечение резюме из HTML"""
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    seen = set()
    results = []

    for a in anchors:
        href = a["href"]
        if "/resume/" not in href:
            continue
            
        full = urljoin(base_url, href.split("?")[0])
        if full in seen:
            continue
        seen.add(full)

        title = a.get_text(strip=True)
        if not title:
            title = a.find_parent().get_text(" ", strip=True)[:120]

        parent = a.find_parent()
        context = ""
        if parent:
            for sub_a in parent.find_all("a"):
                sub_a.extract()
            context = parent.get_text(" ", strip=True)

        context = re.sub(r"\s+", " ", context).strip()

        results.append({
            "title": title,
            "url": full,
            "context": context[:800],
        })

    # Альтернативный поиск если основной не сработал
    if not results:
        cards = soup.find_all(attrs={"data-qa": re.compile("resume-serp__resume|serp-item")})
        for c in cards:
            a = c.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            full = urljoin(base_url, href.split("?")[0])
            if full in seen:
                continue
            seen.add(full)
            title = a.get_text(strip=True) or c.get_text(" ", strip=True)[:120]
            context = c.get_text(" ", strip=True)
            results.append({"title": title, "url": full, "context": context[:800]})

    return results

async def send_to_telegram(results: list, new_count: int, today_count: int, total_count: int):
    """Отправка результатов в Telegram канал"""
    if not tg_client or not DEST_CHANNEL:
        print("❌ Telegram клиент не инициализирован или канал не указан")
        return

    try:
        # Формируем сообщение
        message = f"**📊 Новые резюме разработчиков Python**\n\n"
        message += f"🎯 Новых за сессию: {new_count}\n"
        message += f"📅 Всего за сегодня: {today_count}\n"
        message += f"💾 Всего в базе: {total_count}\n"
        message += f"⏰ Время парсинга: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"

        if new_count > 0:
            message += "**🔍 Найдены новые резюме:**\n\n"
            
            for i, resume in enumerate([r for r in results if r.get('is_new', False)], 1):
                # Экранируем специальные символы для Markdown
                title = html.escape(resume['title'])
                url = resume['url']
                
                message += f"{i}. [{title}]({url})\n"
                
                # Отправляем сообщения порциями, если их много
                if i % 10 == 0 and i < new_count:
                    await tg_client.send_message(DEST_CHANNEL, message, parse_mode='md', link_preview=False)
                    message = "**Продолжение:**\n\n"
                    await asyncio.sleep(1)  # Задержка между сообщениями
            
            # Отправляем оставшуюся часть
            if message.strip() and "Найдены новые резюме" in message:
                await tg_client.send_message(DEST_CHANNEL, message, parse_mode='md', link_preview=False)
        else:
            message += "ℹ️ Новых резюме не найдено."
            await tg_client.send_message(DEST_CHANNEL, message, parse_mode='md')

        print(f"✅ Результаты отправлены в Telegram канал {DEST_CHANNEL}")

    except Exception as e:
        print(f"❌ Ошибка при отправке в Telegram: {e}")

def save_results(results: list, filename: str):
    """Сохранение результатов в файл"""
    today_count, total_count = get_today_stats()
    new_today = sum(1 for r in results if r.get('is_new', False))
    
    sep = "=" * 40 + "\n"
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write("📊 СТАТИСТИКА ПАРСИНГА\n")
        f.write("=" * 20 + "\n")
        f.write(f"🎯 За сегодня найдено: {new_today} новых резюме\n")
        f.write(f"📅 Всего за сегодня: {today_count} резюме\n")
        f.write(f"💾 Всего в базе: {total_count} резюме\n")
        f.write(f"🔗 Источник: {URL}\n")
        f.write(f"⏰ Время парсинга: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        if not results:
            f.write("❌ На первой странице не найдено резюме (или изменилась структура страницы).\n")
            return
            
        new_resumes = [r for r in results if r.get('is_new', False)]
        if new_resumes:
            for i, r in enumerate(new_resumes, 1):
                f.write(sep)
                f.write(f"Резюме #{i}\n")
                f.write(f"🏷️  {r['title']}\n")
                f.write(f"🔗 Ссылка: {r['url']}\n")
            f.write(sep)
        else:
            f.write("ℹ️ Новых резюме не найдено.\n")

    print(f"💾 Результаты сохранены в {filename}")
    print(f"🎯 Новых резюме: {new_today}")
    print(f"📊 Всего обработано: {len(results)}")

async def init_telegram():
    """Инициализация Telegram клиента"""
    global tg_client
    
    if not all([API_ID, API_HASH, SESSION_STRING, DEST_CHANNEL]):
        print("❌ Не все переменные окружения для Telegram настроены")
        return False
    
    try:
        tg_client = TelegramClient(
            StringSession(SESSION_STRING),
            API_ID,
            API_HASH
        )
        
        await tg_client.start()
        print("✅ Telegram клиент успешно инициализирован")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка инициализации Telegram клиента: {e}")
        return False

async def main():
    # Инициализация Telegram
    telegram_ready = await init_telegram()
    
    # Инициализация базы данных
    init_db()
    cleanup_old_resumes()
    
    try:
        html = fetch_page(URL)
    except Exception as e:
        print("❌ Ошибка при скачивании страницы:", e, file=sys.stderr)
        sys.exit(1)

    # Извлечение резюме
    raw_results = extract_resumes(html, URL)
    
    # Проверка уникальности и сохранение в БД
    processed_results = []
    for resume in raw_results:
        is_new = save_resume(resume['url'], resume['title'], resume['context'])
        resume['is_new'] = is_new
        processed_results.append(resume)
        
        if is_new:
            print(f"✅ НОВОЕ: {resume['title']}")
        else:
            print(f"ℹ️  ПОВТОР: {resume['title']}")

    # Сохранение результатов в файл
    save_results(processed_results, OUTPUT_FILE)
    
    # Отправка в Telegram
    if telegram_ready:
        today_count, total_count = get_today_stats()
        new_count = sum(1 for r in processed_results if r.get('is_new', False))
        await send_to_telegram(processed_results, new_count, today_count, total_count)
    
    # Закрытие Telegram клиента
    if tg_client:
        await tg_client.disconnect()

if __name__ == "__main__":

    asyncio.run(main())
