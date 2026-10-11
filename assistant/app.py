import asyncio
import contextlib
import io
import json
import logging
import math
import re
import sqlite3
import time
from collections import defaultdict

from telethon import Button, errors, events, functions, types

from . import __version__
from .folders import Folders, NAMES


log = logging.getLogger(__name__)
READ_DELAY = 3
RECORD_INTERVAL = 4
MUTE_FOREVER = 2**31 - 1
SCENARIO = "Стикер + ГС1 + ГС2"
MENU = [[Button.text("⚙️ Настройка"), Button.text("📋 Статус")],
        [Button.text("⏸ Пауза"), Button.text("▶️ Продолжить")],
        [Button.text("❌ Отменить отправку")]]
SENDING = ("sticker_pending", "voice_pending", "second_pending")
WAITING = ("reply_waiting", "second_confirmation")
STAGES = {"awaiting_confirmation": "вы уже писали этому человеку: требуется подтверждение",
          "sticker_pending": "ожидает стикера", "voice_pending": "ожидает ГС1",
          "reply_waiting": "ГС1 отправлено, ждём новый ответ",
          "second_confirmation": "ответ получен: ждём вашу кнопку «Отправить ГС2»",
          "second_pending": "подтверждено: готовится ГС2", "second_done": "ГС2 отправлено",
          "voice_forbidden": "голосовые запрещены: отправки остановлены",
          "done": "отправки завершены", "replied": "получен ответ", "cancelled": "отменено"}


def parse_username(text):
    match = re.fullmatch(r"(?:@|https?://t\.me/)?([A-Za-z][A-Za-z0-9_]{0,31})/?", text.strip())
    return match[1] if match else None


def error_text(exc):
    if isinstance(exc, errors.FloodWaitError):
        return f"Telegram просит подождать {exc.seconds} сек."
    if isinstance(exc, errors.PeerFloodError):
        return "Telegram ограничил отправку сообщений. Очередь поставлена на паузу."
    if isinstance(exc, ValueError):
        return str(exc)[:200]
    if isinstance(exc, errors.RPCError):
        detail = exc.message
        if detail in ("BAD_REQUEST", "FORBIDDEN", "UNAUTHORIZED", "RPC_CALL_FAIL", "INTERNAL", "FLOOD"):
            detail = type(exc).__name__
        return f"Telegram: {detail}"
    return f"Ошибка {type(exc).__name__}. Проверьте подключение и журнал приложения."


def peer_for(job):
    return types.InputPeerUser(job["user_id"], job["access_hash"])


def sent_message_id(result, random_id):
    for update in getattr(result, "updates", []):
        if isinstance(update, types.UpdateMessageID) and update.random_id == random_id:
            return update.id
        if isinstance(update, types.UpdateNewMessage) and update.message.out:
            return update.message.id
    return getattr(result, "id", None)



def voice_duration(message):
    if message and message.voice:
        duration = next((a.duration for a in message.document.attributes
                         if isinstance(a, types.DocumentAttributeAudio) and a.voice), None)
        if duration is not None and math.isfinite(duration) and duration >= 0:
            return float(duration)
    raise ValueError("Не удалось определить длительность голосового. Выберите запись заново.")


class Assistant:
    def __init__(self, user, bot, store, owner_id):
        self.user = user
        self.bot = bot
        self.store = store
        self.owner_id = owner_id
        self.folders = Folders(user)
        self.locks = defaultdict(asyncio.Lock)
        self.ready = False
        self.last_history_scan = 0
        self.wake = asyncio.Event()
        self.user.add_event_handler(self.on_incoming, events.NewMessage(incoming=True))
        self.bot.add_event_handler(self.on_message, events.NewMessage(incoming=True))
        self.bot.add_event_handler(self.on_callback, events.CallbackQuery())

    async def tell(self, text, **kwargs):
        # Notifications must never roll back an already completed send.
        try:
            await self.bot.send_message(self.owner_id, text, parse_mode=None, **kwargs)
            return True
        except Exception as exc:
            log.warning("Owner notification failed: %s", type(exc).__name__)
            return False


    def owned(self, event):
        return event.is_private and event.sender_id == self.owner_id


    async def settings(self):
        await self.tell("Режимы переписки. Выберите режим для настройки:",
                        buttons=[[Button.inline(SCENARIO, b"scenario:two_voice")]])

    async def scenario_settings(self):
        labels = [f"{title}: {'задано' if self.store.get(key) else 'не задано'}"
                  for key, title in (("sticker", "Стикер"), ("voice", "ГС1"), ("second", "ГС2"))]
        await self.tell(SCENARIO + "\n" + "\n".join(labels) + "\n\n"
                        "Перед каждой отправкой: прочитать входящие → подождать 3 секунды. "
                        "Перед ГС — индикатор записи на длительность сообщения.\n"
                        "После ответа на ГС1 откройте переписку, ответьте человеку и нажмите «Отправить ГС2» в боте.\n"
                        "При запрете голосовых отправки останавливаются. Настройки действуют на новые задания.",
                        buttons=[[Button.inline("Выбрать стикер", b"sticker")],
                                 [Button.inline("Задать ГС1", b"voice")],
                                 [Button.inline("Задать ГС2", b"second")],
                                 [Button.inline("← Режимы переписки", b"settings")]])

    async def on_message(self, event):
        if not self.owned(event):
            return
        text = (event.raw_text or "").strip()
        try:
            if text in ("/start", "/help"):
                self.store.set("mode", None)
                await self.tell(f"Помощник v{__version__}.\n"
                                "Настройка → «Стикер + ГС1 + ГС2»: выберите стикер и обе записи.\n"
                                "Пришлите @username. Перед отправками прочитаю входящие и подожду 3 секунды. "
                                "Перед каждым ГС покажу индикатор записи на его длительность.\n"
                                "После ответа на ГС1 пришлю username и кнопку «Отправить ГС2». "
                                "Откройте чат, ответьте человеку, затем нажмите кнопку.\n"
                                "При запрете голосовых ничего дополнительно не отправляю.\n"
                                "Чаты архивируются, уведомления отключаются. Повторный username запрашивает подтверждение повтора.\n"
                                "/settings, /status, /version, /pause, /resume, /cancel ID, /retry ID.", buttons=MENU)
            elif text == "/version":
                await self.tell(f"Помощник v{__version__}. Режим: {SCENARIO}.")
            elif text in ("⚙️ Настройка", "/settings", "🔀 Режим переписки"):
                self.store.set("mode", None)
                await self.settings()
            elif text in ("📋 Статус", "/status"):
                await self.status()
            elif text in ("⏸ Пауза", "/pause"):
                self.store.set("paused", True)
                for job in self.store.jobs():
                    async with self.locks[job["user_id"]]:
                        job = self.store.by_user(job["user_id"])
                        if not job:
                            continue
                        await self.stop_recording(job, reset=True)
                        self.store.reset_gate(job["id"])
                await self.tell("Отправки приостановлены. Индикаторы записи выключены.")
            elif text in ("▶️ Продолжить", "/resume"):
                self.store.set("paused", False)
                await self.tell("Очередь продолжена. Перед отправкой снова проверю и прочитаю входящие.")
            elif re.fullmatch(r"/(cancel|retry) \d+", text):
                action, job_id = text.split()
                await self.job_action(action, int(job_id))
            elif text == "❌ Отменить отправку":
                await self.status()
                await self.tell("Чтобы отменить задание: /cancel ID. ID указан в статусе.")
            elif text == "/cancel":
                self.store.set("mode", None)
                await self.tell("Ввод настройки отменён. Для отмены задания: /cancel ID.", buttons=MENU)
            elif self.store.get("mode") in ("sticker", "voice", "second"):
                await self.capture_media(event.message, self.store.get("mode"))
            elif text.startswith("/"):
                await self.tell("Неизвестная команда. /help — инструкция.")
            else:
                username = parse_username(text)
                if username:
                    await self.add_target(username)
                else:
                    await self.tell("Пришлите @username или нажмите «⚙️ Настройка».", buttons=MENU)
        except Exception as exc:
            log.warning("Bot command failed: %s", type(exc).__name__)
            await self.tell(error_text(exc))
        finally:
            self.wake.set()

    async def on_callback(self, event):
        if event.sender_id != self.owner_id:
            await event.answer("Доступ только владельцу.", alert=True)
            return
        await event.answer()
        try:
            data = event.data.decode()
            if data == "settings":
                self.store.set("mode", None)
                await self.settings()
            elif data == "scenario:two_voice":
                self.store.set("mode", None)
                await self.scenario_settings()
            elif re.fullmatch(r"send_second:\d+:\d+", data):
                _, job_id, token = data.split(":")
                await self.confirm_second(int(job_id), int(token))
            elif re.fullmatch(r"second_cancel:\d+", data):
                await self.job_action("/cancel", int(data.split(":")[1]))
            elif re.fullmatch(r"repeat_(confirm|cancel):\d+:\d+", data):
                action, job_id, token = data.split(":")
                await self.repeat_action(action, int(job_id), int(token))
            elif re.fullmatch(r"(confirm|dismiss):\d+", data):
                action, job_id = data.split(":")
                await self.confirmation_action(action, int(job_id))
            elif data in ("sticker", "voice", "second"):
                self.store.set("mode", data)
                name = {"sticker": "стикер", "voice": "первое голосовое", "second": "второе голосовое"}[data]
                await self.tell(f"Отправьте {name} для режима «{SCENARIO}». /cancel — отменить ввод.")
            elif data in ("saved", "saved_second"):
                self.store.set("mode", None)
                messages = await self.user.get_messages("me", limit=10, filter=types.InputMessagesFilterVoice())
                prefix = "saved_second" if data == "saved_second" else "saved"
                buttons = [[Button.inline(f"#{m.id} · {voice_duration(m):g} сек.", f"{prefix}:{m.id}".encode())]
                           for m in messages if m and m.voice]
                await self.tell("Выберите голосовое из Избранного:" if buttons else "В Избранном нет голосовых.",
                                buttons=buttons or None)
            elif re.fullmatch(r"(saved|saved_second):\d+", data):
                prefix, message_id = data.split(":")
                message = await self.user.get_messages("me", ids=int(message_id))
                voice_duration(message)
                kind = "second" if prefix == "saved_second" else "voice"
                self.store.set(kind, int(message_id))
                self.store.set("mode", None)
                await self.tell("ГС2 выбрано." if kind == "second" else "ГС1 выбрано.", buttons=[[
                    Button.inline("Прослушать", b"preview_second" if kind == "second" else b"preview_voice")]])
                await self.scenario_settings()
            elif data in ("preview_voice", "preview_second"):
                message_id = self.store.get("second" if data == "preview_second" else "voice")
                message = await self.user.get_messages("me", ids=message_id) if message_id else None
                voice_duration(message)
                buffer = io.BytesIO(await self.user.download_media(message, bytes))
                buffer.name = "preview.ogg"
                await self.bot.send_file(self.owner_id, buffer, voice_note=True,
                                         attributes=message.document.attributes)
            else:
                await self.tell("Эта кнопка устарела. Откройте /settings или /status.")
        except Exception as exc:
            log.warning("Settings failed: %s", type(exc).__name__)
            await self.tell(error_text(exc))
        finally:
            self.wake.set()


    async def capture_media(self, message, kind):
        media = message.sticker if kind == "sticker" else message.voice
        if not media:
            raise ValueError("Нужен именно стикер." if kind == "sticker" else
                             "Нужно именно голосовое сообщение, не аудиофайл и не видеокружок.")
        if getattr(message, "noforwards", False):
            raise ValueError("У сообщения запрещена пересылка. Выберите другую запись.")
        # Read the user's copy: bot and user message IDs/file references differ.
        # Reuse Telegram's existing document to retain sticker/voice metadata.
        bot = await self.bot.get_me()
        original = None
        for attempt in range(3):
            recent = await self.user.get_messages(bot.username, limit=100)
            original = next((item for item in recent
                             if item.out and item.document
                             and item.document.id == media.id
                             and abs((item.date - message.date).total_seconds()) <= 5
                             and (item.sticker if kind == "sticker" else item.voice)), None)
            if original:
                break
            if attempt < 2:
                await asyncio.sleep(0.5)
        if not original:
            raise ValueError("Не удалось найти сообщение в вашем чате с ботом. "
                             "Отправьте стикер или голосовое ещё раз и не удаляйте сообщение до сохранения.")
        saved = await self.user.send_file("me", original.media)
        if saved and not (saved.sticker if kind == "sticker" else saved.voice):
            saved = await self.user.get_messages("me", ids=saved.id)
        if not saved or not (saved.sticker if kind == "sticker" else saved.voice):
            raise ValueError("Не удалось сохранить медиа с исходным типом. "
                             "Настройка не изменена. Попробуйте отправить сообщение боту ещё раз.")
        self.store.set(kind, saved.id)
        self.store.set("mode", None)
        await self.tell("Стикер сохранён." if kind == "sticker" else
                        "Голосовое сохранено. Получатель увидит его без пометки о пересылке.")
        await self.scenario_settings()


    async def add_target(self, username):
        if not self.ready:
            raise ValueError("Идёт восстановление очереди. Повторите чуть позже.")
        if self.store.get("paused", False):
            raise ValueError("Очередь на паузе. Нажмите «▶️ Продолжить».")
        sources = [self.store.get(key) for key in ("sticker", "voice", "second")]
        if not all(sources):
            raise ValueError("Настройка → «Стикер + ГС1 + ГС2»: задайте стикер и обе записи.")
        media = await self.user.get_messages("me", ids=sources)
        if len(media) != 3 or not media[0] or not media[0].sticker:
            raise ValueError("Стикер удалён из Избранного. Задайте его заново.")
        first_duration, second_duration = voice_duration(media[1]), voice_duration(media[2])
        person = await self.user.get_entity(username)
        if not isinstance(person, types.User) or person.bot or person.is_self or person.deleted:
            raise ValueError("Нужен username обычного пользователя.")
        async with self.locks[person.id]:
            existing = self.store.by_user(person.id)
            if existing:
                if existing["stage"] == "awaiting_confirmation":
                    await self.request_confirmation(existing)
                    return
                token = self.store.request_repeat(existing["id"], dict(
                    user_id=person.id, access_hash=person.access_hash, username=person.username or username,
                    sticker_source=sources[0], voice_source=sources[1], second_source=sources[2],
                    first_duration=first_duration, second_duration=second_duration))
                await self.notify_repeat(existing, token)
                return
            latest = await self.user.get_messages(person, limit=1)
            baseline = latest[0].id if latest else 0
            previous = await self.previous_outgoing(person)
            job = self.store.add(user_id=person.id, access_hash=person.access_hash, username=person.username or username,
                                 sticker_source=sources[0], voice_source=sources[1], second_source=sources[2],
                                 baseline=baseline, first_duration=first_duration, second_duration=second_duration,
                                 history_checked=True, stage="awaiting_confirmation" if previous else "sticker_pending")
        if previous:
            await self.request_confirmation(job)
        else:
            await self.tell(f"Задание #{job['id']} для @{job['username']} принято. Сначала проверю запрет голосовых и прочитаю входящие.")


    async def previous_outgoing(self, peer, **kwargs):
        # Scan until an outgoing message is found, including old pages of history.
        async for message in self.user.iter_messages(peer, **kwargs):
            if isinstance(message, types.Message) and message.out:
                return message
        return None

    async def notify_repeat(self, job, token):
        await self.tell(f"⚠️ @{job['username']} уже добавлен(а). Задание #{job['id']}.\n"
                        "Подтвердите повтор: заново отправить стикер и ГС1. "
                        "ГС2 потребует нового ответа и отдельной кнопки «Отправить ГС2».\n"
                        "Подтверждение заменит текущее задание новым циклом.",
                        buttons=[[Button.inline("✅ Подтвердить повтор", f"repeat_confirm:{job['id']}:{token}".encode())],
                                 [Button.inline("Отмена повтора", f"repeat_cancel:{job['id']}:{token}".encode())]])

    async def repeat_action(self, action, job_id, token):
        job = self.store.job(job_id)
        if not job:
            await self.tell("Этот повтор уже подтверждён или запрос устарел.")
            return
        async with self.locks[job["user_id"]]:
            job = self.store.job(job_id)
            request = self.store.repeat_request(job_id)
            if not job or not request or request["token"] != token:
                await self.tell("Этот повтор уже подтверждён или запрос устарел.")
                return
            if action == "repeat_cancel":
                self.store.cancel_repeat(job_id, token)
                await self.tell("Повтор отменён. Текущее задание сохранено.")
                return
            payload = json.loads(request["payload"])
            sources = [payload[key] for key in ("sticker_source", "voice_source", "second_source")]
            media = await self.user.get_messages("me", ids=sources)
            if len(media) != 3 or not media[0] or not media[0].sticker:
                raise ValueError("Стикер для повтора удалён. Задайте медиа и снова пришлите username.")
            voice_duration(media[1])
            voice_duration(media[2])
            latest = await self.user.get_messages(peer_for(job), limit=1)
            baseline = latest[0].id if latest else 0
            await self.stop_recording(job, reset=True)
            new = self.store.restart(job_id, token, baseline)
            if new:
                await self.tell(f"Повтор для @{new['username']} подтверждён. Новое задание #{new['id']}. "
                                "Сначала прочитаю входящие и подожду 3 секунды.")


    async def request_confirmation(self, job):
        await self.tell(f"⚠️ Вы уже писали @{job['username']}. Задание #{job['id']}.\n"
                        "Подтвердите начало сценария: стикер → ГС1 → ответ → ваше подтверждение ГС2.",
                        buttons=[[Button.inline("Подтвердить начало", f"confirm:{job['id']}".encode())],
                                 [Button.inline("Отмена", f"dismiss:{job['id']}".encode())]])

    async def confirmation_action(self, action, job_id):
        job = self.store.job(job_id)
        if not job:
            await self.tell("Запрос уже отменён или обработан.")
            return
        async with self.locks[job["user_id"]]:
            job = self.store.job(job_id)
            if not job or job["stage"] != "awaiting_confirmation":
                await self.tell("Запрос уже отменён или обработан.")
                return
            if action == "dismiss":
                self.store.dismiss_confirmation(job_id)
                await self.tell("Начало переписки отменено.")
                return
            self.store.update(job_id, stage="sticker_pending", history_confirmed=1)
            self.store.reset_gate(job_id)
            await self.tell(f"#{job_id}: начало переписки подтверждено.")

    async def notify_second(self, job, *, force=False):
        if job["stage"] != "second_confirmation":
            return
        if not force and (job["second_send_notified"] or job["second_notice_try"] > time.time()):
            return
        self.store.update(job["id"], second_notice_try=time.time() + 30)
        question = "\n⚠️ В ответе есть вопрос." if job["second_question"] > job["second_approved"] else ""
        sent = await self.tell(f"@{job['username']} ответил(а) после ГС1. Задание #{job['id']}.{question}\n"
                               "Откройте переписку и ответьте человеку. Затем вернитесь сюда и нажмите «Отправить ГС2». "
                               "До вашего нажатия ГС2 не отправляется.",
                               buttons=[[Button.url("Открыть переписку", f"https://t.me/{job['username']}")],
                                        [Button.inline("🎙 Отправить ГС2", f"send_second:{job['id']}:{job['second_token']}".encode())],
                                        [Button.inline("Отменить задание", f"second_cancel:{job['id']}".encode())]])
        if sent:
            self.store.update(job["id"], second_send_notified=1)

    async def confirm_second(self, job_id, token):
        job = self.store.job(job_id)
        if not job:
            raise ValueError("Такого задания нет.")
        async with self.locks[job["user_id"]]:
            job = self.store.job(job_id)
            if not job or job["stage"] != "second_confirmation":
                await self.tell("ГС2 уже подтверждено, отправлено или задание остановлено.")
                return
            await self.reconcile(job)
            job = self.store.job(job_id)
            if token != job["second_token"]:
                await self.tell("Эта кнопка устарела. Используйте последнюю кнопку или /status.")
                await self.notify_second(job, force=True)
                return
            self.store.update(job_id, stage="second_pending", second_send_approved=1,
                              second_approved=job["second_question"], error="", next_try=0)
            self.store.reset_gate(job_id)
            await self.tell(f"#{job_id}: ГС2 подтверждено. Прочитаю входящие, подожду 3 секунды, покажу запись и отправлю ГС2.")

    async def job_action(self, action, job_id):
        job = self.store.job(job_id)
        if not job:
            raise ValueError("Такого задания нет.")
        async with self.locks[job["user_id"]]:
            job = self.store.job(job_id)
            if not job:
                raise ValueError("Задание уже заменено повтором. Откройте /status.")
            if action == "/cancel":
                request = self.store.repeat_request(job_id)
                if request:
                    self.store.cancel_repeat(job_id, request["token"])
                await self.stop_recording(job, reset=True)
                self.store.update(job_id, stage="cancelled", error="", archive_pending=0, mute_pending=0,
                                  desired_folder=job["applied_folder"], notice_pending=0)
                await self.tell(f"#{job_id}: задание отменено. Уже отправленные сообщения остаются в чате.")
            else:
                if job["stage"] in ("cancelled", "voice_forbidden", "second_done", "done", "replied"):
                    raise ValueError("Это задание завершено или остановлено. Для нового цикла пришлите username и подтвердите повтор.")
                if not job["second_source"] and self.store.get("second"):
                    self.store.update(job_id, second_source=self.store.get("second"))
                self.store.update(job_id, error="", next_try=0, read_error="", read_next_try=0,
                                  archive_next_try=0, mute_next_try=0, folder_next_try=0, second_notice_try=0)
                await self.tell(f"#{job_id}: повторю незавершённый шаг. Подтверждение ГС2 команда /retry не заменяет.")

    async def status(self):
        heading = f"Помощник v{__version__}\n" + ("⏸ Очередь на паузе" if self.store.get("paused", False) else "▶️ Очередь работает")
        jobs = self.store.jobs()
        lines = [heading, f"Всего заданий: {len(jobs)}. Последние 20:"]
        for job in jobs[:20]:
            line = f"#{job['id']} @{job['username']} — {STAGES.get(job['stage'], job['stage'])}"
            if job["recording_kind"]:
                line += f"; 🎙 осталось {max(0, math.ceil(job['recording_until'] - time.time()))} сек. записи"
            if job["desired_folder"]:
                line += f"; {NAMES[job['desired_folder']]}"
            for field, label in (("archive_done", "архив"), ("mute_done", "🔕")):
                if job[field]:
                    line += "; " + label
            reason = job["error"] or job["read_error"] or job["archive_error"] or job["mute_error"] or job["folder_error"]
            if reason:
                line += "\nОшибка: " + reason
            lines.append(line)
            if job["stage"] == "second_confirmation":
                await self.notify_second(job, force=True)
            request = self.store.repeat_request(job["id"])
            if request:
                await self.notify_repeat(job, request["token"])
        chunk = ""
        for line in lines:
            if len((chunk + line).encode("utf-16-le")) // 2 > 3800:
                await self.tell(chunk)
                chunk = ""
            chunk += ("\n" if chunk else "") + line
        if chunk:
            await self.tell(chunk)

    def observe_reply(self, job, message):
        if not isinstance(message, types.Message) or message.out or message.id <= job["baseline"]:
            return
        changes = {}
        if not job["replied"]:
            changes.update(replied=1, desired_folder=2, folder_next_try=0)
        if (job["stage"] in WAITING + ("second_pending",) and job["second_baseline"]
                and message.id > job["second_baseline"]):
            changes["second_reply"] = max(job["second_reply"], message.id)
            text = message.message or ""
            question = any(mark in text for mark in ("?", "？", "؟"))
            if question and message.id > job["second_question"]:
                changes.update(second_question=message.id)
                if job["stage"] in ("second_confirmation", "second_pending"):
                    changes.update(stage="second_confirmation", second_send_approved=0,
                                   second_token=job["second_token"] + 1, second_send_notified=0, second_notice_try=0)
            if job["stage"] == "reply_waiting":
                changes.update(stage="second_confirmation", second_send_approved=0,
                               second_token=job["second_token"] + 1, second_send_notified=0, second_notice_try=0)
        if changes:
            self.store.update(job["id"], **changes)

    async def reconcile(self, job):
        if job["stage"] not in SENDING + WAITING:
            return
        bounds = {"min_id": job["baseline"], "reverse": True}
        if job["stage"] in WAITING + ("second_pending",) and job["second_baseline"]:
            bounds["min_id"] = job["second_baseline"]
        latest_id = max(bounds["min_id"], job["read_gate_id"])
        async for message in self.user.iter_messages(peer_for(job), **bounds):
            self.observe_reply(self.store.job(job["id"]), message)
            latest_id = max(latest_id, message.id)
        return latest_id

    async def on_incoming(self, event):
        if not event.is_private or not isinstance(event.message, types.Message):
            return
        job = self.store.by_user(event.sender_id)
        if not job or job["stage"] not in SENDING + WAITING or event.id <= job["baseline"]:
            return
        async with self.locks[job["user_id"]]:
            job = self.store.by_user(event.sender_id)
            if not job or job["stage"] not in SENDING + WAITING or event.id <= job["baseline"]:
                return
            self.observe_reply(job, event.message)
            job = self.store.job(job["id"])
            if job["stage"] in SENDING:
                self.store.queue_read(job["id"], event.id)
                await self.sync_reads(self.store.job(job["id"]))
            else:
                await self.stop_recording(job, reset=True)
            await self.notify_second(self.store.job(job["id"]))
        self.wake.set()

    async def sync_reads(self, job):
        if job["read_target"] <= job["read_done"]:
            return True
        if job["read_next_try"] > time.time():
            return False
        try:
            result = await self.user(functions.messages.ReadHistoryRequest(peer=peer_for(job), max_id=job["read_target"]))
            if result is False:
                raise ValueError("Telegram не подтвердил прочтение.")
            self.store.update(job["id"], read_done=job["read_target"], read_error="", read_next_try=0,
                              read_gate_id=max(job["read_gate_id"], job["read_target"]), read_gate_at=time.time())
            return True
        except Exception as exc:
            wait = exc.seconds + 1 if isinstance(exc, errors.FloodWaitError) else 30
            reason = error_text(exc)
            self.store.update(job["id"], read_error=reason, read_next_try=time.time() + wait)
            if isinstance(exc, errors.FloodWaitError):
                self.store.set("send_not_before", time.time() + wait)
            if reason != job["read_error"]:
                await self.tell(f"#{job['id']}: не удалось прочитать входящие. Отправку задержал. {reason}")
            return False

    async def before_send(self, job, latest_id=None):
        if latest_id is None:
            latest = await self.user.get_messages(peer_for(job), limit=1)
            latest_id = latest[0].id if latest else 0
        if job["read_gate_at"] is None or latest_id > job["read_gate_id"]:
            self.store.queue_read(job["id"], latest_id)
            job = self.store.job(job["id"])
            if not await self.sync_reads(job):
                return False
            job = self.store.job(job["id"])
            if job["read_gate_at"] is None or latest_id > job["read_gate_id"]:
                # Even a previously read chat has a fresh three-second gate for this step.
                if latest_id:
                    result = await self.user(functions.messages.ReadHistoryRequest(peer=peer_for(job), max_id=latest_id))
                    if result is False:
                        raise ValueError("Telegram не подтвердил прочтение.")
                self.store.update(job["id"], read_gate_id=latest_id, read_gate_at=time.time())
        job = self.store.job(job["id"])
        return time.time() >= job["read_gate_at"] + READ_DELAY

    async def voice_allowed(self, job):
        result = await self.user(functions.users.GetFullUserRequest(
            id=types.InputUser(job["user_id"], job["access_hash"])))
        if result.full_user.voice_messages_forbidden:
            await self.forbid_voice(job)
            return False
        return True

    async def forbid_voice(self, job):
        self.store.update(job["id"], stage="voice_forbidden", notice_pending=1, error="", next_try=0)
        await self.stop_recording(self.store.job(job["id"]), reset=True)
        await self.notify_forbidden(self.store.job(job["id"]))

    async def notify_forbidden(self, job):
        if not job["notice_pending"] or job["notice_try"] > time.time():
            return
        self.store.update(job["id"], notice_try=time.time() + 30)
        if await self.tell(f"⚠️ @{job['username']} не принимает голосовые. Задание #{job['id']} остановлено. "
                           "Дальнейших сообщений этому человеку не отправляю."):
            self.store.update(job["id"], notice_pending=0)

    async def prepare_recording(self, job, kind):
        if job["record_complete"]:
            return True
        if job["recording_kind"]:
            if time.time() >= job["recording_until"]:
                self.store.update(job["id"], record_complete=1)
                return True
            if job["recording_next"] <= time.time():
                accepted = await self.user(functions.messages.SetTypingRequest(
                    peer=peer_for(job), action=types.SendMessageRecordAudioAction()))
                if not accepted:
                    raise ValueError("Telegram не подтвердил индикатор записи.")
                self.store.update(job["id"], recording_next=time.time() + RECORD_INTERVAL)
            return False
        duration = job["first_duration"] if kind == "voice" else job["second_duration"]
        if duration is None:
            source = await self.user.get_messages("me", ids=job[f"{kind}_source"])
            duration = voice_duration(source)
            self.store.update(job["id"], **{"first_duration" if kind == "voice" else "second_duration": duration})
        accepted = await self.user(functions.messages.SetTypingRequest(
            peer=peer_for(job), action=types.SendMessageRecordAudioAction()))
        if not accepted:
            raise ValueError("Telegram не подтвердил индикатор записи.")
        now = time.time()
        self.store.update(job["id"], recording_kind=kind, recording_until=now + duration, recording_next=now + RECORD_INTERVAL)
        return False

    async def stop_recording(self, job, *, reset=False):
        if job["recording_kind"]:
            try:
                await self.user(functions.messages.SetTypingRequest(peer=peer_for(job), action=types.SendMessageCancelAction()))
            except Exception as exc:
                log.warning("Recording cancellation failed: %s", type(exc).__name__)
            self.store.update(job["id"], recording_kind="", recording_until=0, recording_next=0)
        if reset:
            self.store.update(job["id"], record_complete=0)

    async def copy_saved(self, job, kind):
        result = await self.user(functions.messages.ForwardMessagesRequest(
            from_peer=types.InputPeerSelf(), id=[job[f"{kind}_source"]], to_peer=peer_for(job),
            random_id=[job[f"{kind}_random"]], drop_author=True, drop_media_captions=True))
        return sent_message_id(result, job[f"{kind}_random"])

    async def after_send(self, job, kind, message_id):
        if kind == "sticker":
            changes = {"stage": "voice_pending", "sticker_message": message_id,
                       "desired_folder": 2 if job["replied"] else 1}
        elif kind == "voice":
            changes = {"stage": "reply_waiting", "voice_message": message_id, "second_baseline": message_id or 0}
        else:
            changes = {"stage": "second_done", "second_message": message_id}
        self.store.update(job["id"], **changes, error="", next_try=0, archive_pending=1, archive_done=0,
                          archive_next_try=0, mute_pending=1, mute_done=0, mute_next_try=0)
        self.store.reset_gate(job["id"])
        await self.stop_recording(self.store.job(job["id"]), reset=True)
        await self.sync_mute(self.store.job(job["id"]))
        await self.sync_archive(self.store.job(job["id"]))
        label = {"sticker": "стикер", "voice": "ГС1", "second": "ГС2"}[kind]
        await self.tell(f"#{job['id']}: {label} отправлено @{job['username']}."
                        + (" Жду нового ответа для запроса ГС2." if kind == "voice" else ""))

    async def send_step(self, job):
        if job["stage"] not in SENDING:
            return
        if not job["second_source"]:
            raise ValueError("Для старого задания не настроено ГС2. Задайте запись и выполните /retry ID.")
        if job["stage"] == "second_pending" and not job["second_send_approved"]:
            self.store.update(job["id"], stage="second_confirmation", second_send_notified=0)
            await self.stop_recording(self.store.job(job["id"]), reset=True)
            return
        if job["recording_kind"]:
            complete = await self.prepare_recording(job, "voice" if job["stage"] == "voice_pending" else "second")
            if not complete:
                await self.sync_reads(self.store.job(job["id"]))
                return
            job = self.store.job(job["id"])
        if job["read_gate_at"] is not None and time.time() < job["read_gate_at"] + READ_DELAY:
            if job["record_complete"]:
                await self.stop_recording(job)
            await self.sync_reads(job)
            return
        if job["stage"] == "sticker_pending" and not job["history_confirmed"]:
            bounds = {"min_id": job["baseline"]} if job["history_checked"] else {}
            if await self.previous_outgoing(peer_for(job), **bounds):
                self.store.update(job["id"], stage="awaiting_confirmation", history_checked=1)
                await self.request_confirmation(self.store.job(job["id"]))
                return
            self.store.update(job["id"], history_checked=1)
        if job["read_gate_at"] is None:
            latest_id = await self.reconcile(job)
            job = self.store.job(job["id"])
            if job["stage"] not in SENDING:
                await self.stop_recording(job, reset=True)
                return
            if not await self.before_send(job, latest_id):
                return
            job = self.store.job(job["id"])
        if not await self.voice_allowed(job):
            return
        latest_id = await self.reconcile(self.store.job(job["id"]))
        job = self.store.job(job["id"])
        if job["stage"] not in SENDING or self.store.get("paused", False):
            await self.stop_recording(job, reset=True)
            return
        if not await self.before_send(job, latest_id):
            if job["record_complete"]:
                await self.stop_recording(self.store.job(job["id"]))
            return
        kind = {"sticker_pending": "sticker", "voice_pending": "voice", "second_pending": "second"}[job["stage"]]
        if kind != "sticker" and not await self.prepare_recording(self.store.job(job["id"]), kind):
            return
        message_id = await self.copy_saved(self.store.job(job["id"]), kind)
        await self.after_send(self.store.job(job["id"]), kind, message_id)


    async def sync_mute(self, job):
        if not job["mute_pending"] or job["mute_next_try"] > time.time():
            return
        try:
            peer = types.InputNotifyPeer(peer_for(job))
            result = await self.user(functions.account.UpdateNotifySettingsRequest(
                peer=peer, settings=types.InputPeerNotifySettings(mute_until=MUTE_FOREVER)))
            if not result:
                raise ValueError("Telegram не принял отключение уведомлений.")
            confirmed = await self.user(functions.account.GetNotifySettingsRequest(peer=peer))
            until = confirmed.mute_until
            until = until.timestamp() if hasattr(until, "timestamp") else (until or 0)
            if until <= time.time():
                raise ValueError("Telegram пока не подтвердил отключение уведомлений.")
            self.store.update(job["id"], mute_pending=0, mute_done=1, mute_error="", mute_next_try=0)
        except Exception as exc:
            wait = exc.seconds if isinstance(exc, errors.FloodWaitError) else 30
            reason = error_text(exc)
            self.store.update(job["id"], mute_error=reason, mute_next_try=time.time() + wait + 1)
            if job["mute_error"] != reason:
                await self.tell(f"#{job['id']}: не удалось отключить уведомления в чате. {reason} Повторю позже.")


    async def sync_archive(self, job):
        if not job["archive_pending"] or job["archive_next_try"] > time.time():
            return
        try:
            await self.user(functions.folders.EditPeerFoldersRequest(folder_peers=[
                types.InputFolderPeer(peer=peer_for(job), folder_id=1)]))
            response = await self.user(functions.messages.GetPeerDialogsRequest(peers=[
                types.InputDialogPeer(peer=peer_for(job))]))
            dialog = next((item for item in response.dialogs
                           if isinstance(item.peer, types.PeerUser) and item.peer.user_id == job["user_id"]), None)
            if not dialog or getattr(dialog, "folder_id", None) != 1:
                raise ValueError("Telegram пока не подтвердил перенос в архив.")
            self.store.update(job["id"], archive_pending=0, archive_done=1,
                              archive_error="", archive_next_try=0)
        except Exception as exc:
            wait = exc.seconds if isinstance(exc, errors.FloodWaitError) else 30
            reason = error_text(exc)
            self.store.update(job["id"], archive_error=reason, archive_next_try=time.time() + wait + 1)
            if job["archive_error"] != reason:
                await self.tell(f"#{job['id']}: не удалось перенести переписку в архив. {reason} Повторю позже.")


    async def sync_folder(self, job):
        if not job["desired_folder"] or job["applied_folder"] == job["desired_folder"]:
            return
        if job["folder_next_try"] > time.time():
            return
        try:
            await self.folders.move(peer_for(job), job["desired_folder"])
            self.store.update(job["id"], applied_folder=job["desired_folder"],
                              folder_error="", folder_next_try=0)
        except Exception as exc:
            wait = exc.seconds if isinstance(exc, errors.FloodWaitError) else 60
            reason = error_text(exc)
            self.store.update(job["id"], folder_error=reason, folder_next_try=time.time() + wait)
            if job["folder_error"] != reason:
                await self.tell(f"#{job['id']}: не удалось изменить папку. {reason} Повторю позже.")


    async def process_job(self, job_id):
        job = self.store.job(job_id)
        if not job:
            return
        async with self.locks[job["user_id"]]:
            job = self.store.job(job_id)
            if not job:
                return
            await self.notify_forbidden(job)
            if job["stage"] in ("cancelled", "awaiting_confirmation"):
                return
            await self.sync_mute(job)
            await self.sync_archive(self.store.job(job_id))
            await self.sync_folder(self.store.job(job_id))
            job = self.store.job(job_id)
            if job["stage"] == "voice_forbidden":
                return
            if self.store.get("paused", False) or job["error"] or job["next_try"] > time.time():
                await self.stop_recording(job, reset=True)
                return
            try:
                if job["stage"] == "reply_waiting" and not job["second_baseline"]:
                    # Retrying the same random_id recovers an omitted Telegram message ID.
                    message_id = await self.copy_saved(job, "voice")
                    if not message_id:
                        raise ValueError("Telegram не вернул ID ГС1. Ожидание ответа остановлено; повторите /retry ID.")
                    self.store.update(job_id, second_baseline=message_id, voice_message=message_id)
                await self.send_step(self.store.job(job_id))
            except errors.RPCError as exc:
                if isinstance(exc, (errors.VoiceMessagesForbiddenError, errors.ChatSendVoicesForbiddenError)) or exc.message in (
                        "VOICE_MESSAGES_FORBIDDEN", "CHAT_SEND_VOICES_FORBIDDEN"):
                    await self.forbid_voice(self.store.job(job_id))
                elif isinstance(exc, errors.FloodWaitError):
                    log.warning("Rate limit for job %s: request=%s wait=%s stage=%s", job_id,
                                type(exc.request).__name__, exc.seconds, self.store.job(job_id)["stage"])
                    deadline = time.time() + exc.seconds + 1
                    self.store.update(job_id, next_try=deadline)
                    self.store.set("send_not_before", deadline)
                    await self.stop_recording(self.store.job(job_id), reset=True)
                    self.store.reset_gate(job_id)
                    await self.tell(f"#{job_id}: {error_text(exc)} Отправку отложил.")
                elif isinstance(exc, errors.ServerError):
                    await self.temporary_failure(job_id)
                else:
                    await self.failed_job(job_id, exc)
            except (ConnectionError, OSError, asyncio.TimeoutError):
                await self.temporary_failure(job_id)
            except Exception as exc:
                await self.failed_job(job_id, exc)
            await self.sync_folder(self.store.job(job_id))
            await self.notify_second(self.store.job(job_id))

    async def temporary_failure(self, job_id):
        self.store.update(job_id, next_try=time.time() + 30)
        await self.stop_recording(self.store.job(job_id), reset=True)
        self.store.reset_gate(job_id)
        log.warning("Temporary send failure for job %s", job_id)

    async def failed_job(self, job_id, exc):
        self.store.update(job_id, error=error_text(exc))
        await self.stop_recording(self.store.job(job_id), reset=True)
        self.store.reset_gate(job_id)
        if isinstance(exc, errors.PeerFloodError):
            self.store.set("paused", True)
        await self.tell(f"#{job_id}: {error_text(exc)} После устранения причины: /retry {job_id}.")

    async def recover_replies(self):
        for job in self.store.jobs():
            if job["stage"] not in WAITING + ("second_pending",):
                continue
            async with self.locks[job["user_id"]]:
                try:
                    job = self.store.job(job["id"])
                    if not job:
                        continue
                    await self.reconcile(job)
                    job = self.store.job(job["id"])
                    if job["stage"] != "second_pending":
                        await self.stop_recording(job, reset=True)
                    await self.notify_second(self.store.job(job["id"]))
                except errors.FloodWaitError as exc:
                    self.store.set("send_not_before", time.time() + exc.seconds + 1)
                    break
                except Exception as exc:
                    log.warning("Reply lookup failed: %s", type(exc).__name__)

    def next_tick(self):
        now = time.time()
        deadlines = [now + 1]
        for job in self.store.jobs():
            if job["recording_kind"]:
                deadlines.extend((job["recording_until"], job["recording_next"]))
            if job["stage"] in SENDING and job["read_gate_at"] is not None:
                deadline = job["read_gate_at"] + READ_DELAY
                if deadline > now:
                    deadlines.append(deadline)
        return max(0.01, min(deadline for deadline in deadlines if deadline > now) - now)

    async def worker(self):
        # A fresh process must observe and read the chat again before any send.
        for job in self.store.jobs():
            async with self.locks[job["user_id"]]:
                job = self.store.by_user(job["user_id"])
                if job and job["stage"] in SENDING:
                    await self.stop_recording(job, reset=True)
                    self.store.reset_gate(job["id"])
        await self.recover_replies()
        self.ready = True
        while True:
            self.wake.clear()
            try:
                if self.user.is_connected():
                    if self.store.get("send_not_before", 0) <= time.time():
                        if time.monotonic() - self.last_history_scan > 30:
                            self.last_history_scan = time.monotonic()
                            await self.recover_replies()
                        for job in self.store.jobs():
                            if self.store.get("send_not_before", 0) > time.time():
                                break
                            await self.process_job(job["id"])
            except Exception as exc:
                log.warning("Worker iteration failed: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=self.next_tick())
            except asyncio.TimeoutError:
                pass

    async def run(self):
        worker = asyncio.create_task(self.worker())
        try:
            await asyncio.wait([self.user.disconnected, self.bot.disconnected], return_when=asyncio.FIRST_COMPLETED)
        finally:
            worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker
            for job in self.store.jobs():
                await self.stop_recording(job, reset=True)
