"""Create an independent authorization; never persist an AyuGram auth key."""

import asyncio
import contextlib
import datetime as dt
import getpass
from pathlib import Path
import platform
import webbrowser

from opentele2.api import API, UseCurrentSession
from opentele2.td import TDesktop
from opentele2.tl import TelegramClient
import qrcode
from qrcode.image.svg import SvgPathImage
from telethon import errors, functions
from telethon.sessions import MemorySession


USER_SESSION = "assistant-user"
BOT_SESSION = "assistant-bot"
SOURCE_APIS = {"desktop": API.TelegramDesktop, "android": API.TelegramAndroid,
               "ios": API.TelegramIOS, "macos": API.TelegramMacOS}


def helper_api():
    api = API.TelegramDesktop.copy()
    # Stable and descriptive identity instead of a randomly generated device.
    api.device_model = "Personal Telegram Assistant"
    api.system_version = f"{platform.system()} {platform.release()}"
    api.app_version = "1.1.0"
    return api


def make_user(data):
    return TelegramClient(str(data / USER_SESSION), api=helper_api(),
                          auto_post_login=False, flood_sleep_threshold=0,
                          request_retries=1, catch_up=True)


def make_bot(data):
    return TelegramClient(str(data / BOT_SESSION), api=helper_api(),
                          auto_post_login=False, flood_sleep_threshold=0,
                          sequential_updates=True, request_retries=1)


class QRDisplay:
    """Temporary local-only display, automatically cleaned after login."""
    def __init__(self, data, open_browser=True):
        self.svg = data / "login-qr.svg"
        self.html = data / "login-qr.html"
        self.open_browser = open_browser
        self.opened = False

    def show(self, url):
        qr = qrcode.QRCode(box_size=8, border=4)
        qr.add_data(url)
        qr.make(fit=True)
        qr.make_image(image_factory=SvgPathImage).save(str(self.svg))
        if not self.html.exists():
            self.html.write_text('''<!doctype html><meta charset="utf-8">
<title>Вход помощника Telegram</title>
<style>body{font:18px system-ui;max-width:650px;margin:40px auto;padding:20px}
img{width:340px;max-width:100%;background:white}small{color:#555}</style>
<h1>Отдельная сессия помощника</h1>
<p>На телефоне: Telegram / AyuGram → Настройки → Устройства → Подключить устройство.</p>
<img id="qr" src="login-qr.svg" alt="QR-код входа">
<p>Отсканируйте QR и подтвердите вход в свой аккаунт. Код обновляется автоматически.</p>
<small>После успешного входа закройте эту страницу. Не пересылайте QR другим людям.</small>
<script>setInterval(()=>{document.getElementById('qr').src='login-qr.svg?v='+Date.now()},1000)</script>
''', encoding="utf-8")
        if not self.opened:
            self.opened = True
            print(f"QR-страница: {self.html.resolve()}")
            print("На телефоне: Настройки → Устройства → Подключить устройство → отсканируйте QR.")
            if self.open_browser:
                webbrowser.open(self.html.resolve().as_uri())
        try:
            qr.print_ascii(invert=True)
        except UnicodeEncodeError:
            print("QR доступен в локальной странице, указанной выше.")

    def close(self):
        self.svg.unlink(missing_ok=True)
        self.html.unlink(missing_ok=True)


async def finish_password(client, prompt=getpass.getpass):
    for attempt in range(3):
        password = await asyncio.to_thread(prompt, "Пароль двухэтапной защиты Telegram (ввод скрыт): ")
        try:
            return await client.sign_in(password=password)
        except errors.PasswordHashInvalidError:
            print("Неверный пароль двухэтапной защиты.")
        finally:
            password = None
    raise ValueError("Пароль не принят. Запустите setup ещё раз.")


async def qr_authorize(client, *, display=None, accept_token=None, rounds=10,
                       password_prompt=getpass.getpass):
    for _ in range(rounds):
        qr = await client.qr_login()
        seconds = max(1, min(60, (qr.expires - dt.datetime.now(dt.timezone.utc)).total_seconds()))
        waiter = asyncio.create_task(qr.wait(timeout=seconds))
        try:
            # QRLogin.wait must register its update handler BEFORE acceptance.
            await asyncio.sleep(0)
            if display:
                display.show(qr.url)
            if accept_token:
                await accept_token(qr.token)
            return await waiter
        except asyncio.TimeoutError:
            print("QR истёк. Создаю новый код…")
        except errors.SessionPasswordNeededError:
            return await finish_password(client, password_prompt)
        finally:
            if not waiter.done():
                waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await waiter
    raise ValueError("Вход не подтверждён. Запустите setup ещё раз.")


async def create_from_tdata(client, path, *, account_number=None, source_api="desktop",
                            prompt=getpass.getpass, choose=input):
    path = Path(path).expanduser().resolve()
    if not path.is_dir():
        raise ValueError("Папка tdata не найдена. Укажите путь к самой папке tdata.")
    passcode = await asyncio.to_thread(prompt, "Локальный код-пароль AyuGram Desktop (Enter, если не задан): ")
    try:
        desktop = TDesktop(str(path), api=SOURCE_APIS[source_api], passcode=passcode or None)
    except Exception as exc:
        raise ValueError(f"Не удалось открыть tdata ({type(exc).__name__}). Проверьте путь, локальный код-пароль и совместимость версии AyuGram.") from None
    finally:
        passcode = None
    if not desktop.isLoaded() or not desktop.accounts:
        raise ValueError("В tdata нет авторизованных аккаунтов.")
    if account_number is None:
        if len(desktop.accounts) == 1:
            account_number = 1
        else:
            print("Аккаунты в tdata:")
            for number, account in enumerate(desktop.accounts, 1):
                print(f"  {number}: Telegram ID {account.UserId}")
            answer = await asyncio.to_thread(choose, "Номер нужного аккаунта: ")
            if not answer.isdigit():
                raise ValueError("Номер аккаунта должен быть числом.")
            account_number = int(answer)
    if not 1 <= account_number <= len(desktop.accounts):
        raise ValueError("Такого номера аккаунта в tdata нет.")
    account = desktop.accounts[account_number - 1]
    # Source key lives only in RAM, briefly, to approve a NEW login token.
    # No source session file and no call to SaveTData are made.
    try:
        source = await TelegramClient.FromTDesktop(
            account, session=MemorySession(), flag=UseCurrentSession,
            auto_post_login=False, flood_sleep_threshold=0, request_retries=1)
    except Exception as exc:
        raise ValueError(f"Не удалось загрузить исходную сессию ({type(exc).__name__}). Попробуйте QR-вход.") from None
    try:
        await source.connect()
        if not await source.is_user_authorized():
            raise ValueError("Исходная сессия AyuGram уже не авторизована.")

        async def approve(token):
            await source(functions.auth.AcceptLoginTokenRequest(token))

        me = await qr_authorize(client, accept_token=approve, password_prompt=prompt)
        if me.id != account.UserId:
            raise ValueError("Telegram вернул другой аккаунт при создании сессии.")
        if client.session.auth_key.key == source.session.auth_key.key:
            raise ValueError("Новая сессия получила исходный ключ. Запуск остановлен.")
        print("Создан отдельный ключ входа. Исходный ключ AyuGram в файлы помощника не записан.")
        return me
    finally:
        try:
            await source.disconnect()
        finally:
            source.session.close()


async def setup_user(client, data, *, tdata=None, account_number=None,
                     source_api="desktop", open_browser=True, login="ask"):
    await client.connect()
    if await client.is_user_authorized():
        print("Отдельная сессия помощника уже подключена; использую её.")
        return await client.get_me()
    if not tdata and login == "ask":
        answer = await asyncio.to_thread(input,
            "Как войти? 1 — QR с телефона; 2 — новая сессия из tdata AyuGram Desktop [1]: ")
        if answer.strip() not in ("", "1", "2"):
            raise ValueError("Выберите 1 или 2 и повторите setup.")
        login = "tdata" if answer.strip() == "2" else "qr"
    if not tdata and login == "tdata":
        value = (await asyncio.to_thread(input, "Путь к папке tdata (AyuGram Desktop должен быть закрыт): ")).strip()
        if len(value) > 1 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if not value:
            raise ValueError("Путь к папке tdata не указан.")
        tdata = value
    if tdata:
        return await create_from_tdata(client, tdata, account_number=account_number,
                                       source_api=source_api)
    display = QRDisplay(data, open_browser=open_browser)
    try:
        return await qr_authorize(client, display=display)
    finally:
        display.close()
