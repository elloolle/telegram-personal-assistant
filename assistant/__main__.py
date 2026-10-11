import argparse
import asyncio
import datetime as dt
import getpass
import json
import logging
import os
from pathlib import Path
import re

from dotenv import dotenv_values, set_key
from telethon import errors

from . import __version__
from .app import Assistant, MENU, error_text
from .auth import USER_SESSION, BOT_SESSION, SOURCE_APIS, make_user, make_bot, setup_user
from .lock import instance_lock
from .store import Store


ROOT = Path(__file__).resolve().parent.parent


def configuration(interactive=False):
    env_path = ROOT / ".env"
    values = {**dotenv_values(env_path), **os.environ}
    if not values.get("TG_BOT_TOKEN") and interactive:
        token = getpass.getpass("Токен BotFather (ввод скрыт): ").strip()
        if not re.fullmatch(r"\d+:[A-Za-z0-9_-]{20,}", token):
            raise ValueError("Некорректный формат токена BotFather.")
        env_path.touch(mode=0o600, exist_ok=True)
        if os.name != "nt":
            env_path.chmod(0o600)
        set_key(str(env_path), "TG_BOT_TOKEN", token)
        values["TG_BOT_TOKEN"] = token
    if not values.get("TG_BOT_TOKEN"):
        raise ValueError("Не заполнено TG_BOT_TOKEN. Выполните: python -m assistant setup")
    if not re.fullmatch(r"\d+:[A-Za-z0-9_-]{20,}", values["TG_BOT_TOKEN"]):
        raise ValueError("Некорректный формат TG_BOT_TOKEN.")
    data = Path(values.get("DATA_DIR") or "data").expanduser()
    if not data.is_absolute():
        data = ROOT / data
    data.mkdir(mode=0o700, parents=True, exist_ok=True)
    return values, data


async def main(args):
    os.umask(0o077)
    print(f"Помощник v{__version__}. Каталог программы: {ROOT}", flush=True)
    values, data = configuration(interactive=args.command == "setup")
    with instance_lock(data / "instance.lock"):
        user = make_user(data)
        bot = make_bot(data)
        store = None
        try:
            if args.command == "setup":
                print("API ID/Hash вводить не нужно. Помощник использует встроенный шаблон OpenTele2.")
                print(f"Отдельный файл входа: {data / (USER_SESSION + '.session')}")
                me = await setup_user(user, data, tdata=args.tdata, account_number=args.account,
                                      source_api=args.source_api, open_browser=not args.no_browser,
                                      login=args.login)
            else:
                await user.connect()
                if not await user.is_user_authorized():
                    raise ValueError("Отдельная сессия не подключена. Выполните: python -m assistant setup")
                me = await user.get_me()
            if me.bot:
                raise ValueError("Личная сессия не должна принадлежать боту.")
            store = Store(data / "queue.sqlite3")
            if store.get("owner_id", me.id) != me.id:
                raise ValueError("Очередь принадлежит другому аккаунту. Используйте отдельный DATA_DIR.")
            store.set("owner_id", me.id)
            if args.command == "setup":
                profile = data / "assistant-user-info.json"
                if not profile.exists():
                    profile.write_text(json.dumps({
                        "owner_id": me.id, "profile": "desktop", "device": "Personal Telegram Assistant",
                        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    }, ensure_ascii=False, indent=2), encoding="utf-8")
                print("В Telegram → Настройки → Устройства проверьте отдельный вход Personal Telegram Assistant.")
            await bot.start(bot_token=values["TG_BOT_TOKEN"])
            bot_me = await bot.get_me()
            if not bot_me.bot or bot_me.id != int(values["TG_BOT_TOKEN"].split(":", 1)[0]):
                raise ValueError(f"Подключён другой бот. Остановите программу и удалите только {BOT_SESSION}.session в каталоге данных, затем повторите setup.")
            if args.command == "setup":
                print(f"Готово. Откройте https://t.me/{bot_me.username} и нажмите Start.")
                print("Для запуска: python -m assistant run")
                return
            app = Assistant(user, bot, store, me.id)
            print(f"Помощник запущен: https://t.me/{bot_me.username}")
            stop_hint = "stop.command" if os.environ.get("TG_ASSISTANT_SUPERVISED") == "1" else "Ctrl+C"
            print(f"Откройте бота → Start → Настройка. Для остановки: {stop_hint}.")
            await app.tell(f"Помощник v{__version__} запущен. /version — проверить версию, /settings — настройка.", buttons=MENU)
            await app.run()
        finally:
            try:
                await bot.disconnect()
            finally:
                await user.disconnect()
                if store:
                    store.close()


def parse_args():
    parser = argparse.ArgumentParser(description="Telegram-помощник. Отдельный вход без своих API ID/Hash.")
    parser.add_argument("command", choices=["setup", "run"], help="setup: создать/проверить отдельную сессию; run: запустить бота")
    parser.add_argument("--login", choices=["ask", "qr", "tdata"], default="ask",
                        help="Способ входа при setup; по умолчанию программа спрашивает")
    parser.add_argument("--tdata", help="Путь к tdata AyuGram Desktop для создания НОВОЙ сессии")
    parser.add_argument("--account", type=int, help="Номер аккаунта в tdata, начиная с 1")
    parser.add_argument("--source-api", choices=list(SOURCE_APIS), default="desktop",
                        help="API-профиль исходного AyuGram, если вы меняли его в настройках")
    parser.add_argument("--no-browser", action="store_true", help="Показать QR в терминале без открытия браузера")
    args = parser.parse_args()
    if args.tdata and args.login == "qr":
        parser.error("--tdata и --login qr не используются вместе")
    if args.account is not None and args.account < 1:
        parser.error("--account должен быть положительным числом")
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main(parse_args()))
    except KeyboardInterrupt:
        print("\nПомощник остановлен. Очередь сохранена.")
    except (ValueError, errors.RPCError, OSError) as exc:
        print(error_text(exc))
        raise SystemExit(1)
