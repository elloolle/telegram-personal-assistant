import asyncio
import contextlib
import io
import json
import logging
import math
import re
import secrets
import sqlite3
import time
from collections import defaultdict

from telethon import Button, errors, events, functions, types

from . import __version__
from .folders import Folders, NAMES
from .store import validate_opening_text


log = logging.getLogger(__name__)
READ_DELAY = 3
FOLLOWUP_DELAY = 1
FIRST_VOICE_DELAY = 1
RECORD_INTERVAL = 3
MUTE_FOREVER = 2**31 - 1
SCENARIO = "Текст или стикер + ГС1 + ГС2 + ГС3"
VOICE_MODES = {"two_voice": "Старый: ГС1; ГС3 вручную по username",
               "three_voice": "Новый: ГС1 + ГС2 подряд; ГС3 вручную по username"}
MENU = [[Button.text("⚙️ Настройка"), Button.text("📋 Статус")],
        [Button.text("⏸ Пауза"), Button.text("▶️ Продолжить")],
        [Button.text("📨 Очередь"), Button.text("❌ Отменить отправку")],
        [Button.text("🎙 Отправить ГС3")],
        [Button.text("📹 Отправить подтверждающий кружок")]]
SENDING = ("sticker_pending", "voice_pending", "followup_pending", "second_pending")
WAITING = ("reply_waiting", "second_confirmation")
STAGES = {"awaiting_confirmation": "вы уже писали этому человеку: требуется подтверждение",
          "sticker_pending": "ожидает первого сообщения", "voice_pending": "ожидает ГС1",
          "followup_pending": "ГС1 отправлено, готовится ГС2",
          "reply_waiting": "начальные ГС отправлены; ГС3 вручную по username",
          "second_confirmation": "ответ получен; ГС3 отправляется вручную по username",
          "second_pending": "подтверждено: готовится ГС3", "second_done": "ГС3 отправлено",
          "voice_forbidden": "голосовые запрещены: отправки остановлены",
          "done": "отправки завершены", "replied": "получен ответ", "cancelled": "отменено"}


def parse_username(text):
    match = re.fullmatch(r"(?:@|https?://t\.me/)?([A-Za-z][A-Za-z0-9_]{0,31})/?", text.strip())
    return match[1] if match else None


def cancel_button(job):
    return Button.inline("❌ Отменить отправку", f"cancel_job:{job['id']}".encode())


def circle_button(job, *, show_username=False):
    title = f"📹 Кружок для @{job['username']}" if show_username else "📹 Отправить подтверждающий кружок"
    return Button.inline(title, f"send_circle:{job['id']}:{job['circle_token']}".encode())


def media_for(message, kind):
    if not message:
        return None
    return message.sticker if kind == "sticker" else message.video_note if kind == "circle" else message.voice


def recording_action(kind):
    return types.SendMessageRecordRoundAction() if kind == "circle" else types.SendMessageRecordAudioAction()


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


def circle_duration(message):
    if message and message.video_note:
        duration = next((a.duration for a in message.document.attributes
                         if isinstance(a, types.DocumentAttributeVideo) and a.round_message), None)
        if duration is not None and math.isfinite(duration) and duration >= 0:
            return float(duration)
    raise ValueError("Нужен видеокружок с известной длительностью, а не обычное видео. Задайте кружок заново.")


class Assistant:
    def __init__(self, user, bot, store, owner_id):
        self.user = user
        self.bot = bot
        self.store = store
        self.owner_id = owner_id
        self.folders = Folders(user)
        self.locks = defaultdict(asyncio.Lock)
        self.queue_lock = asyncio.Lock()
        self.recording_tasks = {}
        self.recording_locks = defaultdict(asyncio.Lock)
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
        mode = self.store.get("voice_send_mode", "two_voice")
        await self.tell("Выберите режим для новых заданий:\n"
                        "Первое сообщение в обоих режимах — текст или стикер на ваш выбор.\n"
                        "Старый: первое сообщение → чтение → 1 секунда → запись ГС1 → ГС1 → новый ответ → ваша кнопка → ГС3.\n"
                        "Новый: первое сообщение → чтение → 1 секунда → запись ГС1 → ГС1 → чтение → 1 секунда → запись ГС2 → ГС2 → "
                        "новый ответ → ваша кнопка → ГС3.\n"
                        "Прежнее ГС2 теперь называется ГС3 и сохранено.",
                        buttons=[[Button.inline(("✅ " if mode == key else "") + title,
                                                f"voice_mode:{key}".encode())]
                                 for key, title in (("two_voice", "Старый режим"), ("three_voice", "Новый режим"))])

    async def scenario_settings(self):
        mode = self.store.get("voice_send_mode", "two_voice")
        opening_kind = self.store.get("opening_kind", "sticker")
        labels = [f"{title}: {'задано' if self.store.get(key) else 'не задано'}"
                  for key, title in (("sticker", "Стикер"), ("opening_text", "Текст"), ("voice", "ГС1"), ("followup", "ГС2"), ("second", "ГС3"), ("circle", "Кружок для подтверждения личности"))]
        labels.append("Первое сообщение: " + ("текст" if opening_kind == "text" else "стикер"))
        if opening_kind == "text" and self.store.get("opening_text"):
            labels.append("Текст: " + self.store.get("opening_text")[:500])
        await self.tell(SCENARIO + "\n" + "\n".join(labels) + f"\nОтправка: {VOICE_MODES[mode]}\n\n"
                        "Перед отправками: прочитать входящие → подождать 3 секунды (перед ГС1 и ГС2 — 1 секунду). "
                        "Перед ГС — индикатор записи на длительность сообщения.\n"
                        "В режиме «ГС1 + ГС2 подряд» после ГС1 прочитаю все доступные входящие, "
                        "подожду 1 секунду и начну запись ГС2 без отдельной кнопки.\n"
                        "После нового ответа на начальные ГС откройте переписку, ответьте человеку "
                        "затем нажмите «🎙 Отправить ГС3» и пришлите username в боте. Прежнее ГС2 сохранено как ГС3.\n"
                        "Кружок отправляется отдельно вашей кнопкой: чтение входящих → 3 секунды → "
                        "анимация записи видеосообщения на длительность кружка → отправка.\n"
                        "При запрете голосовых отправки останавливаются. Настройки действуют на новые задания.",
                        buttons=[[Button.inline(("✅ " if opening_kind == "sticker" else "") + "Первое: стикер", b"opening_kind:sticker"),
                                  Button.inline(("✅ " if opening_kind == "text" else "") + "Первое: текст", b"opening_kind:text")],
                                 [Button.inline("Выбрать стикер", b"sticker"), Button.inline("Задать текст", b"opening_text")],
                                 [Button.inline("Задать ГС1", b"voice")],
                                 [Button.inline("Задать ГС2", b"followup")],
                                 [Button.inline("Задать ГС3", b"second")],
                                 [Button.inline("Задать подтверждающий кружок", b"circle")],
                                 [Button.inline(("✅ " if mode == "two_voice" else "") + "Старый: только ГС1", b"voice_mode:two_voice")],
                                 [Button.inline(("✅ " if mode == "three_voice" else "") + "Новый: ГС1 + ГС2 подряд", b"voice_mode:three_voice")],
                                 [Button.inline("← Режимы переписки", b"settings")]])

    async def on_message(self, event):
        if not self.owned(event):
            return
        text = (event.raw_text or "").strip()
        try:
            if text in ("/start", "/help"):
                self.store.set("mode", None)
                await self.tell(f"Помощник v{__version__}.\n"
                                "В настройках выберите первое сообщение: текст или стикер, затем задайте голосовые.\n"
                                "Выберите старый или новый режим. В новом режиме после ГС1 "
                                "прочитаю входящие, подожду 1 секунду и покажу запись ГС2.\n"
                                "Пришлите @username. Перед отправками прочитаю входящие и подожду 3 секунды (перед ГС1 и ГС2 — 1 секунду). "
                                "Перед каждым ГС покажу индикатор записи на его длительность.\n"
                                "ГС3 отправляется вручную: нажмите «🎙 Отправить ГС3» и пришлите username. "
                                "Откройте чат, ответьте человеку, затем нажмите кнопку.\n"
                                "При запрете голосовых ничего дополнительно не отправляю.\n"
                                "Чаты архивируются, уведомления отключаются. Повторный username запрашивает подтверждение повтора.\n"
                                "«📨 Очередь» → до 5 или все ожидающие чаты.\n"
                                "«❌ Отменить отправку» → пришлите username для остановки его задания.\n"
                                "«📹 Отправить подтверждающий кружок» → пришлите username из очереди. "
                                "Кружок задаётся в настройках; перед отправкой прочитаю сообщения и покажу запись видео.\n"
                                "/settings, /queue, /status, /version, /pause, /resume, /cancel ID, /retry ID.", buttons=MENU)
            elif text == "/version":
                await self.tell(f"Помощник v{__version__}. Режим: {SCENARIO}. "
                                f"Отправка: {VOICE_MODES[self.store.get('voice_send_mode', 'two_voice')]}.")
            elif text in ("⚙️ Настройка", "/settings", "🔀 Режим переписки"):
                self.store.set("mode", None)
                await self.settings()
            elif text in ("📋 Статус", "/status"):
                await self.status()
            elif text in ("📨 Очередь", "/queue"):
                await self.queue_menu()
            elif text in ("🎙 Отправить ГС3", "/gs3"):
                await self.prompt_third_target()
            elif text == "📹 Отправить подтверждающий кружок":
                self.store.set("mode", "circle_target")
                await self.tell("Пришлите @username из очереди, которому отправить кружок для подтверждения личности. "
                                "Можно прислать ссылку t.me/username. /cancel — отменить ввод.")
            elif text == "❌ Отменить отправку":
                self.store.set("mode", "cancel_target")
                await self.tell("Пришлите @username, для которого нужно отменить все дальнейшие отправки. "
                                "Можно прислать ссылку t.me/username. /cancel — выйти из отмены.")
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
            elif text == "/cancel":
                self.store.set("mode", None)
                await self.tell("Ввод отменён. Для отмены задания: «❌ Отменить отправку» или /cancel ID.", buttons=MENU)
            elif self.store.get("mode") == "cancel_target" and not text.startswith("/"):
                username = parse_username(text)
                if not username:
                    raise ValueError("Пришлите корректный @username или /cancel для выхода.")
                job = self.store.by_username(username)
                if not job:
                    raise ValueError("Задание для этого username не найдено. Проверьте имя или отправьте /cancel.")
                await self.job_action("/cancel", job["id"])
                self.store.set("mode", None)
            elif self.store.get("mode") == "circle_target" and not text.startswith("/"):
                username = parse_username(text)
                job = self.store.by_username(username) if username else None
                if not job:
                    raise ValueError("Пришлите username существующего задания или /cancel для выхода.")
                await self.request_circle(job["id"], job["circle_token"])
                self.store.set("mode", None)
            elif self.store.get("mode") == "third_target" and not text.startswith("/"):
                username = parse_username(text)
                if not username:
                    raise ValueError("Пришлите корректный @username или ссылку t.me/username. /cancel — выйти.")
                await self.request_third_target(username)
            elif self.store.get("mode") == "opening_text":
                if getattr(getattr(event, "message", None), "media", None):
                    raise ValueError("Пришлите текст отдельным сообщением, без вложений.")
                opening_text = event.raw_text or ""
                validate_opening_text(opening_text)
                self.store.set("opening_text", opening_text)
                self.store.set("opening_kind", "text")
                self.store.set("mode", None)
                await self.tell("Текст сохранён и выбран первым сообщением для новых заданий.")
                await self.scenario_settings()
            elif self.store.get("mode") in ("sticker", "voice", "followup", "second", "circle"):
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
            elif data.startswith("voice_mode:") and data.split(":", 1)[1] in VOICE_MODES:
                self.store.set("voice_send_mode", data.split(":", 1)[1])
                self.store.set("mode", None)
                await self.scenario_settings()
            elif data in ("opening_kind:sticker", "opening_kind:text", "opening_text"):
                if data == "opening_text" or (data == "opening_kind:text" and not self.store.get("opening_text")):
                    self.store.set("mode", "opening_text")
                    await self.tell("Пришлите текст первого сообщения. Он будет отправлен как обычный текст. /cancel — отменить ввод.")
                else:
                    self.store.set("opening_kind", data.split(":", 1)[1])
                    self.store.set("mode", None)
                    await self.scenario_settings()
            elif data == "third_target":
                await self.prompt_third_target()
            elif data in ("queue:five", "queue:all"):
                await self.send_queue_requests(5 if data == "queue:five" else None)
            elif re.fullmatch(r"send_second:\d+:\d+", data):
                await self.prompt_third_target()
            elif re.fullmatch(r"send_circle:\d+:\d+", data):
                _, job_id, token = data.split(":")
                await self.request_circle(int(job_id), int(token))
            elif re.fullmatch(r"cancel_job:\d+", data):
                await self.job_action("/cancel", int(data.split(":")[1]))
            elif re.fullmatch(r"repeat_(confirm|cancel):\d+:\d+", data):
                action, job_id, token = data.split(":")
                await self.repeat_action(action, int(job_id), int(token))
            elif re.fullmatch(r"(confirm|dismiss):\d+", data):
                action, job_id = data.split(":")
                await self.confirmation_action(action, int(job_id))
            elif data in ("sticker", "voice", "followup", "second", "circle"):
                self.store.set("mode", data)
                name = {"sticker": "стикер", "voice": "первое голосовое", "followup": "второе голосовое",
                        "second": "третье голосовое (прежнее ГС2)", "circle": "видеокружок для подтверждения личности"}[data]
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
                await self.tell("ГС3 выбрано." if kind == "second" else "ГС1 выбрано.", buttons=[[
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
        media = media_for(message, kind)
        if not media:
            raise ValueError("Нужен именно стикер." if kind == "sticker" else
                             "Нужен именно видеокружок, а не обычное видео или голосовое." if kind == "circle" else
                             "Нужно именно голосовое сообщение, не аудиофайл и не видеокружок.")
        if getattr(message, "noforwards", False):
            raise ValueError("У сообщения запрещена пересылка. Выберите другую запись.")
        # Read the user's copy: bot and user message IDs/file references differ.
        # Reuse Telegram's existing document to retain its media type and attributes.
        bot = await self.bot.get_me()
        original = None
        for attempt in range(3):
            recent = await self.user.get_messages(bot.username, limit=100)
            original = next((item for item in recent
                             if item.out and item.document
                             and item.document.id == media.id
                             and abs((item.date - message.date).total_seconds()) <= 5
                             and media_for(item, kind)), None)
            if original:
                break
            if attempt < 2:
                await asyncio.sleep(0.5)
        if not original:
            raise ValueError("Не удалось найти сообщение в вашем чате с ботом. "
                             "Отправьте сообщение ещё раз и не удаляйте его до сохранения.")
        saved = await self.user.send_file("me", original.media)
        if saved and not media_for(saved, kind):
            saved = await self.user.get_messages("me", ids=saved.id)
        if not saved or not media_for(saved, kind):
            raise ValueError("Не удалось сохранить медиа с исходным типом. "
                             "Настройка не изменена. Попробуйте отправить сообщение боту ещё раз.")
        if kind == "circle":
            circle_duration(saved)
        self.store.set(kind, saved.id)
        if kind == "sticker":
            self.store.set("opening_kind", "sticker")
        self.store.set("mode", None)
        await self.tell("Стикер сохранён." if kind == "sticker" else
                        "Подтверждающий кружок сохранён. Отправьте его отдельной кнопкой для нужного чата." if kind == "circle" else
                        "Голосовое сохранено. Получатель увидит его без пометки о пересылке.")
        await self.scenario_settings()


    async def add_target(self, username):
        if not self.ready:
            raise ValueError("Идёт восстановление очереди. Повторите чуть позже.")
        if self.store.get("paused", False):
            raise ValueError("Очередь на паузе. Нажмите «▶️ Продолжить».")
        dialogue_mode = self.store.get("voice_send_mode", "two_voice")
        opening_kind = self.store.get("opening_kind", "sticker")
        opening_text = self.store.get("opening_text", "") if opening_kind == "text" else ""
        if opening_kind == "text":
            validate_opening_text(opening_text)
        keys = ["voice", "second"]
        if opening_kind == "sticker":
            keys.insert(0, "sticker")
        if dialogue_mode == "three_voice":
            keys.append("followup")
        sources = {key: self.store.get(key) for key in keys}
        if not all(sources.values()):
            raise ValueError("В настройках задайте первое сообщение, ГС1 и ГС3, "
                             "а для отправки пары — также новое ГС2.")
        messages = await self.user.get_messages("me", ids=list(sources.values()))
        if len(messages) != len(sources):
            raise ValueError("Сохранённые записи недоступны. Задайте их заново.")
        media = dict(zip(keys, messages))
        if opening_kind == "sticker" and (not media["sticker"] or not media["sticker"].sticker):
            raise ValueError("Стикер удалён из Избранного. Задайте его заново.")
        first_duration, second_duration = voice_duration(media["voice"]), voice_duration(media["second"])
        followup_source = sources.get("followup")
        followup_duration = voice_duration(media["followup"]) if dialogue_mode == "three_voice" else None
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
                    sticker_source=sources.get("sticker", 0), voice_source=sources["voice"], second_source=sources["second"],
                    first_duration=first_duration, second_duration=second_duration,
                    opening_kind=opening_kind, opening_text=opening_text,
                    dialogue_mode=dialogue_mode, followup_source=followup_source, followup_duration=followup_duration))
                await self.notify_repeat(existing, token)
                return
            latest = await self.user.get_messages(person, limit=1)
            baseline = latest[0].id if latest else 0
            previous = await self.previous_outgoing(person)
            job = self.store.add(user_id=person.id, access_hash=person.access_hash, username=person.username or username,
                                 sticker_source=sources.get("sticker", 0), voice_source=sources["voice"], second_source=sources["second"],
                                 baseline=baseline, first_duration=first_duration, second_duration=second_duration,
                                 opening_kind=opening_kind, opening_text=opening_text,
                                 dialogue_mode=dialogue_mode, followup_source=followup_source, followup_duration=followup_duration,
                                 history_checked=True, stage="awaiting_confirmation" if previous else "sticker_pending")
        if previous:
            await self.request_confirmation(job)
        else:
            await self.tell(f"Задание #{job['id']} для @{job['username']} принято. {VOICE_MODES[job['dialogue_mode']]}. "
                            "Сначала проверю запрет голосовых и прочитаю входящие.",
                            buttons=[[cancel_button(job)], [circle_button(job)]])


    async def previous_outgoing(self, peer, **kwargs):
        # Scan until an outgoing message is found, including old pages of history.
        async for message in self.user.iter_messages(peer, **kwargs):
            if isinstance(message, types.Message) and message.out:
                return message
        return None

    async def notify_repeat(self, job, token):
        request = self.store.repeat_request(job["id"])
        mode = json.loads(request["payload"]).get("dialogue_mode", "two_voice") if request else "two_voice"
        await self.tell(f"⚠️ @{job['username']} уже добавлен(а). Задание #{job['id']}.\n"
                        f"Подтвердите повтор: заново отправить первое сообщение и начальные ГС. {VOICE_MODES[mode]}.\n"
                        "ГС3 потребует нового ответа после начальных ГС и отдельной кнопки «Отправить ГС3».\n"
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
            opening_kind = payload.get("opening_kind", "sticker")
            if opening_kind == "text":
                validate_opening_text(payload["opening_text"])
            keys = ["voice_source", "second_source"]
            if opening_kind == "sticker":
                keys.insert(0, "sticker_source")
            if payload.get("dialogue_mode") == "three_voice":
                keys.append("followup_source")
            sources = [payload[key] for key in keys]
            media = await self.user.get_messages("me", ids=sources)
            if len(media) != len(sources):
                raise ValueError("Записи для повтора недоступны. Задайте их и снова пришлите username.")
            if opening_kind == "sticker" and (not media[0] or not media[0].sticker):
                raise ValueError("Стикер для повтора удалён. Задайте медиа и снова пришлите username.")
            for message in media[1:] if opening_kind == "sticker" else media:
                voice_duration(message)
            latest = await self.user.get_messages(peer_for(job), limit=1)
            baseline = latest[0].id if latest else 0
            await self.stop_recording(job, reset=True)
            new = self.store.restart(job_id, token, baseline)
            if new:
                await self.tell(f"Повтор для @{new['username']} подтверждён. Новое задание #{new['id']}. "
                                "Сначала прочитаю входящие и подожду 3 секунды.")


    async def request_confirmation(self, job):
        await self.tell(f"⚠️ Вы уже писали @{job['username']}. Задание #{job['id']}.\n"
                        f"Подтвердите начало сценария: {'текст' if job['opening_kind'] == 'text' else 'стикер'} → {VOICE_MODES[job['dialogue_mode']]}.",
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
                await self.stop_recording(job, reset=True)
                self.store.dismiss_confirmation(job_id)
                await self.tell("Начало переписки отменено.")
                return
            self.store.update(job_id, stage="sticker_pending", history_confirmed=1)
            self.store.reset_gate(job_id)
            await self.tell(f"#{job_id}: начало переписки подтверждено.", buttons=[[cancel_button(job)], [circle_button(job)]])

    async def prompt_third_target(self):
        self.store.set("mode", "third_target")
        await self.tell("Пришлите @username или ссылку t.me/username, кому отправить ГС3. "
                        "Это сразу поставит выбранное ГС3 на отправку с анимацией записи. /cancel — выйти.")

    async def request_third_target(self, username):
        if not self.ready:
            raise ValueError("Идёт восстановление очереди. Повторите чуть позже.")
        source = self.store.get("second")
        if not source:
            raise ValueError("Сначала в настройках нажмите «Задать ГС3» и пришлите голосовое.")
        duration = voice_duration(await self.user.get_messages("me", ids=source))
        person = await self.user.get_entity(username)
        if not isinstance(person, types.User) or person.bot or person.is_self or person.deleted:
            raise ValueError("Нужен username обычного пользователя.")
        async with self.locks[person.id]:
            job = self.store.by_user(person.id)
            if job and ((job["circle_state"] == "pending" and job["stage"] not in WAITING) or job["stage"] in
                        ("awaiting_confirmation", "sticker_pending", "voice_pending", "followup_pending")):
                raise ValueError("У этого пользователя ещё готовятся начальные сообщения или кружок. Дождитесь их отправки.")
            if job and job["stage"] == "second_pending" and job["second_send_approved"]:
                self.store.set("mode", None)
                await self.tell(f"ГС3 для @{job['username']} уже поставлено на отправку. Повтор не создан.")
                return
            if job and job["stage"] in WAITING + ("second_pending",):
                if job["circle_state"] != "pending":
                    await self.stop_recording(job, reset=True)
                self.store.update(job["id"], stage="second_pending", second_send_approved=1,
                                  second_source=source, second_duration=duration, second_random=secrets.randbits(63),
                                  second_message=None, second_token=job["second_token"] + 1, error="", next_try=0,
                                  access_hash=person.access_hash, username=person.username or username)
                if job["circle_state"] != "pending":
                    self.store.reset_gate(job["id"])
                job = self.store.job(job["id"])
            else:
                latest = await self.user.get_messages(person, limit=1)
                job = self.store.add_manual_third(user_id=person.id, access_hash=person.access_hash,
                                                  username=person.username or username, source=source,
                                                  duration=duration, baseline=latest[0].id if latest else 0)
            self.store.set("mode", None)
            await self.tell(f"#{job['id']} @{job['username']}: ГС3 поставлено на отправку. "
                            "Прочитаю входящие, подожду 3 секунды, покажу запись и отправлю голосовое.",
                            buttons=[[cancel_button(job)]])
        self.wake.set()

    def second_queue(self):
        return [job for job in reversed(self.store.jobs())
                if job["stage"] == "second_confirmation" and job["desired_folder"] == 2
                and job["applied_folder"] == 2 and not job["second_send_approved"]]

    async def queue_menu(self):
        count = len(self.second_queue())
        await self.tell(f"📨 В процессе: {count} чатов ожидают ручной отправки ГС3.\n"
                        "Посмотрите список, затем нажмите «🎙 Отправить ГС3» и пришлите нужный username.",
                        buttons=[[Button.inline("Показать до 5 чатов", b"queue:five")],
                                 [Button.inline("Показать все чаты", b"queue:all")],
                                 [Button.inline("🎙 Отправить ГС3", b"third_target")]])

    async def send_queue_requests(self, limit):
        if self.queue_lock.locked():
            await self.tell("Запросы из очереди уже отправляются. Дождитесь окончания.")
            return
        async with self.queue_lock:
            jobs = self.second_queue()
            if limit is not None:
                jobs = jobs[:limit]
            if not jobs:
                await self.tell("В папке «В процессе» нет заданий, ожидающих подтверждения ГС3.")
                return
            sent = 0
            for index, snapshot in enumerate(jobs):
                if index:
                    await asyncio.sleep(1.1)
                async with self.locks[snapshot["user_id"]]:
                    job = self.store.job(snapshot["id"])
                    if (not job or job["stage"] != "second_confirmation" or job["desired_folder"] != 2
                            or job["applied_folder"] != 2 or job["second_send_approved"]):
                        continue
                    if not await self.tell(f"@{job['username']} · задание #{job['id']}",
                                           buttons=[[Button.url("Открыть переписку", f"https://t.me/{job['username']}")]]):
                        await self.tell(f"Отправлено запросов: {sent}. Доставка прервалась; повторите через «Очередь» позже.")
                        return
                    sent += 1
            await asyncio.sleep(1.1)
            await self.tell(f"Показано чатов: {sent}. Для отправки нажмите «🎙 Отправить ГС3» и пришлите username.",
                            buttons=[[Button.inline("🎙 Отправить ГС3", b"third_target")]])

    async def request_circle(self, job_id, token):
        job = self.store.job(job_id)
        if not job:
            raise ValueError("Задание не найдено или заменено повтором. Откройте /status.")
        async with self.locks[job["user_id"]]:
            job = self.store.job(job_id)
            if not job:
                raise ValueError("Задание заменено повтором. Откройте /status.")
            if job["stage"] in ("cancelled", "voice_forbidden"):
                raise ValueError("Задание остановлено; кружок не отправляется. Для нового цикла подтвердите повтор.")
            if token != job["circle_token"] or job["circle_state"] == "pending":
                await self.tell("Этот кружок уже подтверждён или отправлен. Для нового запроса откройте /status.")
                return
            if job["error"]:
                raise ValueError(f"Сначала устраните ошибку задания и выполните /retry {job_id}.")
            source = self.store.get("circle")
            if not source:
                raise ValueError("Сначала в настройках нажмите «Задать подтверждающий кружок» и пришлите видеокружок боту.")
            message = await self.user.get_messages("me", ids=source)
            duration = circle_duration(message)
            if job["stage"] == "sticker_pending" and not job["history_confirmed"] and not job["opening_attempted"]:
                bounds = {"min_id": job["baseline"]} if job["history_checked"] else {}
                if not await self.previous_outgoing(peer_for(job), **bounds):
                    self.store.update(job_id, history_confirmed=1)
            await self.stop_recording(job, reset=True)
            self.store.update(job_id, circle_state="pending", circle_source=source, circle_duration=duration,
                              circle_random=secrets.randbits(63), circle_message=None, circle_token=token + 1)
            self.store.reset_gate(job_id)
            await self.tell(f"#{job_id} @{job['username']}: отправка кружка подтверждена. "
                            "Прочитаю входящие, подожду 3 секунды и покажу запись видеосообщения перед отправкой.",
                            buttons=[[cancel_button(job)]])

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
                                  desired_folder=job["applied_folder"], notice_pending=0,
                                  second_send_approved=0, second_send_notified=0, second_notice_try=0,
                                  second_token=job["second_token"] + 1, read_target=job["read_done"],
                                  circle_state="cancelled", circle_token=job["circle_token"] + 1,
                                  read_gate_id=-1, read_gate_at=None, due=None, next_try=0)
                await self.tell(f"#{job_id} @{job['username']}: дальнейшие отправки отменены. "
                                "Уже отправленные сообщения остаются в чате.", buttons=MENU)
            else:
                if job["stage"] in ("cancelled", "voice_forbidden") or (
                        job["stage"] in ("second_done", "done", "replied") and job["circle_state"] != "pending"):
                    raise ValueError("Это задание завершено или остановлено. Для нового цикла пришлите username и подтвердите повтор.")
                if not job["second_source"] and self.store.get("second"):
                    self.store.update(job_id, second_source=self.store.get("second"))
                self.store.update(job_id, error="", next_try=0, read_error="", read_next_try=0,
                                  archive_next_try=0, mute_next_try=0, folder_next_try=0, second_notice_try=0)
                await self.tell(f"#{job_id}: повторю незавершённый шаг. Подтверждение ГС3 команда /retry не заменяет.")

    async def status(self):
        heading = f"Помощник v{__version__}\n" + ("⏸ Очередь на паузе" if self.store.get("paused", False) else "▶️ Очередь работает")
        jobs = self.store.jobs()
        lines = [heading, f"Всего заданий: {len(jobs)}. Последние 20:"]
        for job in jobs[:20]:
            line = f"#{job['id']} @{job['username']} — {STAGES.get(job['stage'], job['stage'])}"
            if job["opening_kind"] == "text":
                line += "; первое сообщение: текст"
            if job["circle_state"]:
                line += "; кружок: " + {"pending": "ожидает отправки", "done": "отправлен", "cancelled": "отменён"}.get(job["circle_state"], job["circle_state"])
            if job["dialogue_mode"] == "three_voice":
                line += "; ГС1 + ГС2 подряд"
            if job["recording_kind"]:
                icon = "📹" if job["recording_kind"] == "circle" else "🎙"
                line += f"; {icon} осталось {max(0, math.ceil(job['recording_until'] - time.time()))} сек. записи"
            if job["desired_folder"]:
                line += f"; {NAMES.get(job['desired_folder'], 'архив')}"
            for field, label in (("archive_done", "архив"), ("mute_done", "🔕")):
                if job[field]:
                    line += "; " + label
            reason = job["error"] or job["read_error"] or job["archive_error"] or job["mute_error"] or job["folder_error"]
            if reason:
                line += "\nОшибка: " + reason
            lines.append(line)
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
            buttons = [[circle_button(job, show_username=True)] for job in jobs[:20]
                       if job["stage"] not in ("cancelled", "voice_forbidden")]
            await self.tell(chunk, buttons=buttons or None)

    def observe_reply(self, job, message):
        if not isinstance(message, types.Message) or message.out or message.id <= job["baseline"]:
            return
        changes = {}
        if not job["replied"]:
            changes.update(replied=1, desired_folder=2, folder_next_try=0)
        if (job["stage"] in WAITING + ("second_pending",) and job["second_baseline"]
                and message.id > job["second_baseline"]):
            changes["second_reply"] = max(job["second_reply"], message.id)
            if job["stage"] == "reply_waiting":
                changes.update(stage="second_confirmation", second_send_approved=0,
                               second_token=job["second_token"] + 1, second_send_notified=0, second_notice_try=0)
        if changes:
            self.store.update(job["id"], **changes)

    async def reconcile(self, job):
        completed = ("second_done", "done", "replied")
        if job["stage"] not in SENDING + WAITING + completed and job["circle_state"] != "pending":
            return
        bounds = {"min_id": job["baseline"], "reverse": True}
        if job["circle_state"] != "pending" and job["stage"] in WAITING + ("second_pending",) and job["second_baseline"]:
            bounds["min_id"] = job["second_baseline"]
        if job["stage"] in completed and job["circle_state"] != "pending":
            bounds["min_id"] = max(bounds["min_id"], job["second_message"] or 0)
        latest_id = max(bounds["min_id"], job["read_gate_id"])
        async for message in self.user.iter_messages(peer_for(job), **bounds):
            self.observe_reply(self.store.job(job["id"]), message)
            latest_id = max(latest_id, message.id)
        return latest_id

    async def on_incoming(self, event):
        if not event.is_private or not isinstance(event.message, types.Message):
            return
        job = self.store.by_user(event.sender_id)
        completed = ("second_done", "done", "replied")
        if not job or (job["stage"] not in SENDING + WAITING + completed and job["circle_state"] != "pending") or event.id <= job["baseline"]:
            return
        async with self.locks[job["user_id"]]:
            job = self.store.by_user(event.sender_id)
            if not job or (job["stage"] not in SENDING + WAITING + completed and job["circle_state"] != "pending") or event.id <= job["baseline"]:
                return
            self.observe_reply(job, event.message)
            job = self.store.job(job["id"])
            if job["stage"] in completed and job["circle_state"] != "pending":
                await self.sync_folder(job)
                return
            if job["stage"] in SENDING or job["circle_state"] == "pending":
                self.store.queue_read(job["id"], event.id)
                await self.sync_reads(self.store.job(job["id"]))
            else:
                await self.stop_recording(job, reset=True)
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

    def read_delay(self, job):
        if job["circle_state"] == "pending":
            return READ_DELAY
        if job["stage"] == "voice_pending":
            return FIRST_VOICE_DELAY
        return FOLLOWUP_DELAY if job["stage"] == "followup_pending" else READ_DELAY

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
                # Even a previously read chat has a fresh pause for this step.
                if latest_id:
                    result = await self.user(functions.messages.ReadHistoryRequest(peer=peer_for(job), max_id=latest_id))
                    if result is False:
                        raise ValueError("Telegram не подтвердил прочтение.")
                self.store.update(job["id"], read_gate_id=latest_id, read_gate_at=time.time())
        job = self.store.job(job["id"])
        return time.time() >= job["read_gate_at"] + self.read_delay(job)

    async def voice_allowed(self, job):
        result = await self.user(functions.users.GetFullUserRequest(
            id=types.InputUser(job["user_id"], job["access_hash"])))
        if result.full_user.voice_messages_forbidden:
            await self.forbid_voice(job)
            return False
        return True

    async def forbid_voice(self, job):
        changes = {"circle_state": "cancelled", "circle_token": job["circle_token"] + 1} if job["circle_state"] == "pending" else {}
        self.store.update(job["id"], stage="voice_forbidden", notice_pending=1, error="", next_try=0, **changes)
        await self.stop_recording(self.store.job(job["id"]), reset=True)
        await self.notify_forbidden(self.store.job(job["id"]))

    async def notify_forbidden(self, job):
        if not job["notice_pending"] or job["notice_try"] > time.time():
            return
        self.store.update(job["id"], notice_try=time.time() + 30)
        if await self.tell(f"⚠️ @{job['username']} не принимает голосовые. Задание #{job['id']} остановлено. "
                           "Дальнейших сообщений этому человеку не отправляю."):
            self.store.update(job["id"], notice_pending=0)

    async def refresh_recording(self, job_id, *, force=False):
        async with self.recording_locks[job_id]:
            job = self.store.job(job_id)
            if not job or not job["recording_kind"]:
                return
            if not force and job["recording_next"] > time.time():
                return
            accepted = await self.user(functions.messages.SetTypingRequest(
                peer=peer_for(job), action=recording_action(job["recording_kind"])))
            if not accepted:
                raise ValueError("Telegram не подтвердил индикатор записи.")
            self.store.update(job_id, recording_next=time.time() + RECORD_INTERVAL)

    def keep_recording(self, job_id):
        task = self.recording_tasks.get(job_id)
        if task is None or task.done():
            self.recording_tasks[job_id] = asyncio.create_task(self.recording_heartbeat(job_id))

    async def recording_heartbeat(self, job_id):
        # Independent of slow history/permission/send requests and other queued chats.
        while True:
            job = self.store.job(job_id)
            if not job or not job["recording_kind"]:
                return
            await asyncio.sleep(max(0.05, job["recording_next"] - time.time()))
            try:
                await self.refresh_recording(job_id)
            except Exception as exc:
                log.warning("Recording refresh failed for job %s: %s", job_id, type(exc).__name__)
                wait = exc.seconds + 1 if isinstance(exc, errors.FloodWaitError) else RECORD_INTERVAL
                await asyncio.sleep(wait)

    async def prepare_recording(self, job, kind):
        if job["record_complete"]:
            if job["recording_kind"]:
                self.keep_recording(job["id"])
                await self.refresh_recording(job["id"])
            return True
        if job["recording_kind"]:
            self.keep_recording(job["id"])
            if time.time() >= job["recording_until"]:
                self.store.update(job["id"], record_complete=1)
                # Cover the final checks even when the last regular refresh is old.
                await self.refresh_recording(job["id"], force=True)
                return True
            await self.refresh_recording(job["id"])
            return False
        duration_key = {"voice": "first_duration", "followup": "followup_duration", "second": "second_duration",
                        "circle": "circle_duration"}[kind]
        duration = job[duration_key]
        if duration is None:
            source = await self.user.get_messages("me", ids=job[f"{kind}_source"])
            duration = circle_duration(source) if kind == "circle" else voice_duration(source)
            self.store.update(job["id"], **{duration_key: duration})
        accepted = await self.user(functions.messages.SetTypingRequest(
            peer=peer_for(job), action=recording_action(kind)))
        if not accepted:
            raise ValueError("Telegram не подтвердил индикатор записи.")
        now = time.time()
        self.store.update(job["id"], recording_kind=kind, recording_until=now + duration, recording_next=now + RECORD_INTERVAL)
        self.keep_recording(job["id"])
        return False

    async def stop_recording(self, job, *, reset=False):
        task = self.recording_tasks.pop(job["id"], None)
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if job["recording_kind"]:
            try:
                await self.user(functions.messages.SetTypingRequest(peer=peer_for(job), action=types.SendMessageCancelAction()))
            except Exception as exc:
                log.warning("Recording cancellation failed: %s", type(exc).__name__)
            self.store.update(job["id"], recording_kind="", recording_until=0, recording_next=0)
        if reset:
            self.store.update(job["id"], record_complete=0)

    async def copy_saved(self, job, kind):
        if kind == "sticker" and job["opening_kind"] == "text":
            validate_opening_text(job["opening_text"])
            result = await self.user(functions.messages.SendMessageRequest(
                peer=peer_for(job), message=job["opening_text"], random_id=job["sticker_random"], no_webpage=True))
            return sent_message_id(result, job["sticker_random"])
        result = await self.user(functions.messages.ForwardMessagesRequest(
            from_peer=types.InputPeerSelf(), id=[job[f"{kind}_source"]], to_peer=peer_for(job),
            random_id=[job[f"{kind}_random"]], drop_author=True, drop_media_captions=True))
        return sent_message_id(result, job[f"{kind}_random"])

    async def after_send(self, job, kind, message_id):
        if kind == "sticker":
            changes = {"stage": "voice_pending", "sticker_message": message_id,
                       "desired_folder": 2 if job["replied"] else 0}
        elif kind == "voice":
            changes = {"stage": "reply_waiting", "voice_message": message_id, "second_baseline": message_id or 0}
            if job["dialogue_mode"] == "three_voice":
                changes.update(stage="followup_pending", second_baseline=0)
        elif kind == "followup":
            changes = {"stage": "reply_waiting", "followup_message": message_id, "second_baseline": message_id or 0}
        else:
            changes = {"stage": "second_done", "second_message": message_id}
        self.store.update(job["id"], **changes, error="", next_try=0, archive_pending=1, archive_done=0,
                          archive_next_try=0, mute_pending=1, mute_done=0, mute_next_try=0)
        self.store.reset_gate(job["id"])
        await self.stop_recording(self.store.job(job["id"]), reset=True)
        if kind == "sticker" or (kind == "voice" and job["dialogue_mode"] == "three_voice"):
            # Start the next voice's read/pause gate immediately after the preceding message.
            latest_id = await self.reconcile(self.store.job(job["id"]))
            await self.before_send(self.store.job(job["id"]), latest_id)
        await self.sync_mute(self.store.job(job["id"]))
        await self.sync_archive(self.store.job(job["id"]))
        label = {"sticker": "стикер", "voice": "ГС1", "followup": "ГС2", "second": "ГС3"}[kind]
        if kind == "sticker" and job["opening_kind"] == "text":
            label = "первое текстовое сообщение"
        await self.tell(f"#{job['id']}: {label} отправлено @{job['username']}."
                        + (" Готовлю ГС2: чтение входящих → пауза 1 секунда → запись."
                           if kind == "voice" and job["dialogue_mode"] == "three_voice" else
                           " После ответа чат попадёт в «В процессе». ГС3 отправляется кнопкой и username." if kind in ("voice", "followup") else ""),
                        buttons=[[cancel_button(job)], [circle_button(job)]] if kind != "second" else [[circle_button(job)]])

    async def send_circle_step(self, job):
        if job["recording_kind"]:
            if not await self.prepare_recording(job, "circle"):
                await self.sync_reads(self.store.job(job["id"]))
                return
            job = self.store.job(job["id"])
        if job["read_gate_at"] is not None and time.time() < job["read_gate_at"] + READ_DELAY:
            await self.sync_reads(job)
            return
        latest_id = await self.reconcile(job)
        job = self.store.job(job["id"])
        if self.store.get("paused", False) or job["circle_state"] != "pending":
            await self.stop_recording(job, reset=True)
            return
        if not await self.before_send(job, latest_id):
            return
        if not await self.prepare_recording(self.store.job(job["id"]), "circle"):
            return
        message_id = await self.copy_saved(self.store.job(job["id"]), "circle")
        self.store.update(job["id"], circle_state="done", circle_message=message_id, error="", next_try=0,
                          archive_pending=1, archive_done=0, archive_next_try=0,
                          mute_pending=1, mute_done=0, mute_next_try=0)
        await self.stop_recording(self.store.job(job["id"]), reset=True)
        self.store.reset_gate(job["id"])
        await self.tell(f"#{job['id']}: подтверждающий кружок отправлен @{job['username']}.",
                        buttons=[[cancel_button(job)], [circle_button(self.store.job(job["id"]))]])

    async def send_step(self, job):
        if job["stage"] not in SENDING:
            return
        if not job["second_source"]:
            raise ValueError("Для старого задания не настроено ГС3. Задайте запись и выполните /retry ID.")
        if job["dialogue_mode"] == "three_voice" and not job["followup_source"]:
            raise ValueError("Для пары ГС1 + ГС2 не настроено ГС2. Пришлите username и подтвердите новый цикл.")
        if job["stage"] == "second_pending" and not job["second_send_approved"]:
            self.store.update(job["id"], stage="second_confirmation", second_send_notified=0)
            await self.stop_recording(self.store.job(job["id"]), reset=True)
            return
        if job["recording_kind"]:
            complete = await self.prepare_recording(job, job["recording_kind"])
            if not complete:
                await self.sync_reads(self.store.job(job["id"]))
                return
            job = self.store.job(job["id"])
        if job["read_gate_at"] is not None and time.time() < job["read_gate_at"] + self.read_delay(job):
            await self.sync_reads(job)
            return
        if job["stage"] == "sticker_pending" and not job["history_confirmed"] and not job["opening_attempted"]:
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
            return
        kind = {"sticker_pending": "sticker", "voice_pending": "voice", "followup_pending": "followup",
                "second_pending": "second"}[job["stage"]]
        if kind != "sticker" and not await self.prepare_recording(self.store.job(job["id"]), kind):
            return
        if kind == "sticker":
            # A lost acknowledgement must retry the same random_id, even if our message is now in history.
            self.store.update(job["id"], opening_attempted=1)
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

    async def sync_folder_layout(self):
        if self.store.get("single_process_folder_layout", False) or self.store.get("folder_layout_retry", 0) > time.time():
            return
        try:
            destination = await self.folders.normalize()
            if destination is None:
                for job in self.store.jobs():
                    if job["desired_folder"] == 2:
                        self.store.update(job["id"], applied_folder=0)
            self.store.set("single_process_folder_layout", True)
        except Exception as exc:
            wait = exc.seconds + 1 if isinstance(exc, errors.FloodWaitError) else 60
            self.store.set("folder_layout_retry", time.time() + wait)
            if isinstance(exc, errors.FloodWaitError):
                self.store.set("send_not_before", time.time() + wait)
            log.warning("Folder migration delayed: %s", type(exc).__name__)


    async def process_job(self, job_id):
        job = self.store.job(job_id)
        if not job:
            return
        async with self.locks[job["user_id"]]:
            job = self.store.job(job_id)
            if not job:
                return
            await self.notify_forbidden(job)
            if job["stage"] == "cancelled" or (job["stage"] == "awaiting_confirmation" and job["circle_state"] != "pending"):
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
                if job["stage"] == "reply_waiting" and not job["second_baseline"] and job["circle_state"] != "pending":
                    # Retrying the same random_id recovers an omitted Telegram message ID.
                    kind = "followup" if job["dialogue_mode"] == "three_voice" else "voice"
                    message_id = await self.copy_saved(job, kind)
                    if not message_id:
                        raise ValueError("Telegram не вернул ID начального ГС. Ожидание ответа остановлено; повторите /retry ID.")
                    self.store.update(job_id, second_baseline=message_id, **{f"{kind}_message": message_id})
                if job["circle_state"] == "pending":
                    await self.send_circle_step(self.store.job(job_id))
                else:
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
            completed_waiting = job["stage"] in ("second_done", "done", "replied") and not job["replied"]
            if job["stage"] not in WAITING + ("second_pending",) and job["circle_state"] != "pending" and not completed_waiting:
                continue
            async with self.locks[job["user_id"]]:
                try:
                    job = self.store.job(job["id"])
                    if not job:
                        continue
                    await self.reconcile(job)
                    job = self.store.job(job["id"])
                    if job["stage"] != "second_pending" and job["circle_state"] != "pending":
                        await self.stop_recording(job, reset=True)
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
            if (job["stage"] in SENDING or job["circle_state"] == "pending") and job["read_gate_at"] is not None:
                deadline = job["read_gate_at"] + self.read_delay(job)
                if deadline > now:
                    deadlines.append(deadline)
        return max(0.01, min(deadline for deadline in deadlines if deadline > now) - now)

    async def worker(self):
        # A fresh process must observe and read the chat again before any send.
        for job in self.store.jobs():
            async with self.locks[job["user_id"]]:
                job = self.store.by_user(job["user_id"])
                if job and (job["stage"] in SENDING or job["circle_state"] == "pending"):
                    await self.stop_recording(job, reset=True)
                    self.store.reset_gate(job["id"])
        await self.recover_replies()
        self.ready = True
        while True:
            self.wake.clear()
            try:
                if self.user.is_connected():
                    if self.store.get("send_not_before", 0) <= time.time():
                        await self.sync_folder_layout()
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
