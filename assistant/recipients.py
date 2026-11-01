import asyncio
import math
import time

from telethon import errors, types, utils


def identity(person):
    return {"user_id": person.id, "access_hash": person.access_hash,
            "username": person.username or "",
            "display_name": " ".join(utils.get_display_name(person).split())[:120]}


def recipient_label(recipient):
    if recipient["username"]:
        return "@" + recipient["username"]
    return recipient["display_name"] or f"ID {recipient['user_id']}"


def recipient_link(recipient):
    if recipient["username"]:
        return "https://t.me/" + recipient["username"]
    return None


def eligible(person):
    return (isinstance(person, types.User) and not person.bot and not person.is_self
            and not person.deleted and person.access_hash is not None)


async def load_directory(app, *, force=False):
    cached = app.store.get("chat_directory")
    if not force and cached is not None and time.time() - app.store.get("chat_directory_at", 0) < 300:
        return cached
    contacts = []
    seen = set()
    try:
        wait = app.store.get("send_not_before", 0) - time.time()
        if wait > 0:
            raise ValueError(f"Telegram на паузе ещё {math.ceil(wait)} сек. Откройте список позже.")
        await app.tell("Загружаю список личных чатов. Сообщения при этом не отмечаются прочитанными.")
        count = 0
        async for dialog in app.user.iter_dialogs(limit=None):
            wait = app.store.get("send_not_before", 0) - time.time()
            if wait > 0:
                raise ValueError(f"Telegram на паузе ещё {math.ceil(wait)} сек. Откройте список позже.")
            person = dialog.entity
            if eligible(person) and person.id not in seen:
                contacts.append(identity(person))
                seen.add(person.id)
            count += 1
            if count % 100 == 0:
                await asyncio.sleep(1)
                wait = app.store.get("send_not_before", 0) - time.time()
                if wait > 0:
                    raise ValueError(f"Telegram на паузе ещё {math.ceil(wait)} сек. Откройте список позже.")
        app.store.set("chat_directory", contacts)
        app.store.set("chat_directory_at", time.time())
        return contacts
    except errors.FloodWaitError as exc:
        wait = app.defer_for_flood(exc)
        raise ValueError(f"Telegram ограничил загрузку чатов. Пауза {wait} сек.; попробуйте позже.") from None
