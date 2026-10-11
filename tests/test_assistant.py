import asyncio
import datetime as dt
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from telethon import errors, functions, types, utils

from assistant.app import Assistant, READ_DELAY, parse_username, voice_duration
from assistant.folders import Folders, place_peer, title_text
from assistant.store import Store


def incoming(message_id=200, text='Привет', sender=123, out=False):
    return types.Message(id=message_id, peer_id=types.PeerUser(sender),
                         date=dt.datetime.now(dt.timezone.utc), message=text, out=out)


def media_message(kind, message_id=500, duration=7):
    attributes = ([types.DocumentAttributeAudio(duration=duration, voice=True)] if kind == 'voice' else
                  [types.DocumentAttributeSticker(alt='🙂', stickerset=types.InputStickerSetEmpty())])
    document = types.Document(id=77, access_hash=88, file_reference=b'bot-only-reference',
                              date=dt.datetime.now(dt.timezone.utc), mime_type='audio/ogg' if kind == 'voice' else 'image/webp',
                              size=4, dc_id=2, attributes=attributes)
    return types.Message(id=message_id, peer_id=types.PeerUser(42), date=dt.datetime.now(dt.timezone.utc),
                         message='', media=types.MessageMediaDocument(document=document))


class FakeUser:
    def __init__(self, clock):
        self.clock = clock
        self.operations = []
        self.sent = []
        self.typing = []
        self.reads = []
        self.history_queries = []
        self.history = {123: [incoming(100)]}
        self.saved = {10: media_message('sticker', 10), 11: media_message('voice', 11),
                      12: media_message('voice', 12, duration=11)}
        self.forbidden = set()
        self.failures = {}
        self.returns_false = set()
        self.archive = {}
        self.muted = {}
        self.ignore_archive = False
        self.ignore_mute = False
        self.ids = {}
        self.next_id = 1000
        self.add_event_handler = lambda *args: None
        self.is_connected = lambda: True

    async def __call__(self, request):
        self.operations.append((self.clock[0], request))
        bytes(request)
        cls = type(request)
        if cls in self.failures:
            raise self.failures.pop(cls)
        if cls in self.returns_false:
            return False
        if isinstance(request, functions.users.GetFullUserRequest):
            return SimpleNamespace(full_user=SimpleNamespace(voice_messages_forbidden=request.id.user_id in self.forbidden))
        if isinstance(request, functions.messages.ReadHistoryRequest):
            self.reads.append((self.clock[0], request))
            return types.messages.AffectedMessages(pts=1, pts_count=1)
        if isinstance(request, functions.messages.SetTypingRequest):
            self.typing.append((self.clock[0], request))
            return True
        if isinstance(request, functions.messages.ForwardMessagesRequest):
            self.sent.append((self.clock[0], request))
            random_id = request.random_id[0]
            if random_id not in self.ids:
                self.next_id += 1
                self.ids[random_id] = self.next_id
                self.history.setdefault(request.to_peer.user_id, []).append(incoming(self.next_id, '', request.to_peer.user_id, out=True))
            return SimpleNamespace(updates=[types.UpdateMessageID(id=self.ids[random_id], random_id=random_id)])
        if isinstance(request, functions.account.UpdateNotifySettingsRequest):
            if not self.ignore_mute:
                self.muted[request.peer.peer.user_id] = request.settings.mute_until
            return True
        if isinstance(request, functions.account.GetNotifySettingsRequest):
            return types.PeerNotifySettings(mute_until=self.muted.get(request.peer.peer.user_id, 0))
        if isinstance(request, functions.folders.EditPeerFoldersRequest):
            if not self.ignore_archive:
                for item in request.folder_peers:
                    self.archive[item.peer.user_id] = item.folder_id
            return SimpleNamespace()
        if isinstance(request, functions.messages.GetPeerDialogsRequest):
            user_id = request.peers[0].peer.user_id
            return SimpleNamespace(dialogs=[SimpleNamespace(peer=types.PeerUser(user_id), folder_id=self.archive.get(user_id, 0))])
        raise AssertionError(f'Unexpected API request {type(request).__name__}')

    async def get_messages(self, peer, *, ids=None, limit=None, **kwargs):
        if peer == 'me':
            if isinstance(ids, list):
                return [self.saved.get(message_id) for message_id in ids]
            if ids is not None:
                return self.saved.get(ids)
            return [m for m in self.saved.values() if m.voice]
        user_id = peer.id if isinstance(peer, types.User) else peer.user_id
        self.history_queries.append((self.clock[0], user_id))
        return sorted(self.history.get(user_id, []), key=lambda m: m.id, reverse=True)[:limit]

    async def iter_messages(self, peer, **kwargs):
        user_id = peer.id if isinstance(peer, types.User) else peer.user_id
        self.history_queries.append((self.clock[0], user_id))
        for message in sorted(self.history.get(user_id, []), key=lambda m: m.id):
            if message.id > kwargs.get('min_id', 0):
                yield message

    async def get_entity(self, username):
        return types.User(id=456, access_hash=99, username=username)


class ScenarioTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'queue.sqlite3'
        self.clock = [1000.0]
        self.time_patch = patch('assistant.app.time.time', side_effect=lambda: self.clock[0])
        self.time_patch.start()
        self.store = Store(self.path)
        self.user = FakeUser(self.clock)
        self.bot = SimpleNamespace(add_event_handler=lambda *args: None, send_message=AsyncMock())
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.folders.move = AsyncMock()
        self.job = self.store.add(user_id=123, access_hash=55, username='alice', sticker_source=10,
                                  voice_source=11, second_source=12, baseline=100, first_duration=7,
                                  second_duration=11, history_checked=True)
        self.app.ready = True

    async def asyncTearDown(self):
        self.store.close()
        self.time_patch.stop()
        self.temp.cleanup()

    def state(self):
        return self.store.job(self.job['id'])

    async def tick(self, seconds=0):
        self.clock[0] += seconds
        await self.app.process_job(self.job['id'])

    async def sticker(self):
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.state()['stage'], 'voice_pending')

    async def first(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        await self.tick(4)
        await self.tick(3)
        self.assertEqual(self.state()['stage'], 'reply_waiting')

    async def receive(self, message_id, text='Привет', *, live=True, sender=123):
        message = incoming(message_id, text, sender)
        self.user.history.setdefault(sender, []).append(message)
        if live:
            await self.app.on_incoming(SimpleNamespace(is_private=True, sender_id=sender, id=message_id, message=message))

    async def callback(self, data, sender=42):
        await self.app.on_callback(SimpleNamespace(sender_id=sender, answer=AsyncMock(), data=data.encode()))

    async def approve(self):
        await self.callback(f"send_second:{self.job['id']}:{self.state()['second_token']}")

    async def command(self, text, sender=42):
        await self.app.on_message(SimpleNamespace(is_private=True, sender_id=sender, raw_text=text))

    async def finish_second(self):
        await self.tick()
        await self.tick(3)
        await self.tick(4)
        await self.tick(4)
        await self.tick(3)

    async def test_full_scenario_obeys_read_pause_recording_and_owner_confirmation(self):
        await self.tick()
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.user.reads[0][1].max_id, 100)
        await self.tick(2.999)
        self.assertEqual(self.user.sent, [])
        await self.tick(.001)
        self.assertEqual(self.user.sent[-1][0], 1003)
        await self.tick()
        await self.tick(3)
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordAudioAction)
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(4)
        await self.tick(2.999)
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(.001)
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11]])
        self.assertAlmostEqual(self.user.sent[-1][0], 1013)
        await self.receive(1100)
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 2)
        await self.approve()
        await self.finish_second()
        self.assertEqual(self.state()['stage'], 'second_done')
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11], [12]])
        self.assertTrue(all(r.drop_author and r.drop_media_captions for _, r in self.user.sent))
        self.assertEqual(self.user.archive[123], 1)
        self.assertGreater(self.user.muted[123], self.clock[0])

    async def test_fractional_duration_is_not_rounded_to_worker_second(self):
        self.store.update(self.job['id'], first_duration=1.25)
        await self.sticker()
        await self.tick()
        await self.tick(3)
        start = self.clock[0]
        self.assertLessEqual(self.app.next_tick(), 1)
        await self.tick(1)
        self.assertAlmostEqual(self.app.next_tick(), .25)
        await self.tick(.249)
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(.001)
        self.assertAlmostEqual(self.user.sent[-1][0] - start, 1.25)

    async def test_recording_renews_without_extending_duration(self):
        self.store.update(self.job['id'], first_duration=13)
        await self.sticker()
        await self.tick()
        await self.tick(3)
        start = self.clock[0]
        for _ in range(3):
            await self.tick(4)
        await self.tick(1)
        record = [(t, r) for t, r in self.user.typing if isinstance(r.action, types.SendMessageRecordAudioAction)]
        cancel = [(t, r) for t, r in self.user.typing if isinstance(r.action, types.SendMessageCancelAction)]
        self.assertEqual([t - start for t, _ in record], [0, 4, 8, 12])
        self.assertEqual(cancel[-1][0] - start, 13)
        self.assertEqual(self.user.sent[-1][0] - start, 13)

    async def test_voice_is_dispatched_before_indicator_cancellation_with_one_final_history_scan(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        start = self.clock[0]
        queries = len(self.user.history_queries)
        operation = len(self.user.operations)
        allowed = self.app.voice_allowed
        history = self.user.iter_messages

        async def delayed_allowed(job):
            self.clock[0] += .25
            return await allowed(job)

        async def delayed_history(peer, **kwargs):
            self.clock[0] += .25
            async for message in history(peer, **kwargs):
                yield message

        with patch.object(self.app, 'voice_allowed', side_effect=delayed_allowed), \
                patch.object(self.user, 'iter_messages', side_effect=delayed_history):
            await self.tick(7)
        self.assertEqual(len(self.user.history_queries) - queries, 1)
        self.assertAlmostEqual(self.user.sent[-1][0] - start, 7.5)
        operations = [request for _, request in self.user.operations[operation:]]
        sent = next(i for i, request in enumerate(operations) if isinstance(request, functions.messages.ForwardMessagesRequest))
        cancelled = next(i for i, request in enumerate(operations) if isinstance(request, functions.messages.SetTypingRequest)
                         and isinstance(request.action, types.SendMessageCancelAction))
        self.assertLess(sent, cancelled)

    async def test_question_received_during_final_privacy_check_blocks_second_voice(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        await self.tick()
        await self.tick(3)
        allowed = self.app.voice_allowed

        async def question_during_request(job):
            await self.receive(1101, 'Почему?', live=False)
            return await allowed(job)

        with patch.object(self.app, 'voice_allowed', side_effect=question_during_request):
            await self.tick(11)
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        self.assertFalse(self.state()['second_send_approved'])
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 2)

    async def test_long_voice_does_not_poll_history_every_second_and_still_reads_before_send(self):
        self.store.update(self.job['id'], first_duration=109)
        await self.sticker()
        await self.tick()
        queries = len(self.user.history_queries)
        await self.tick(1)
        await self.tick(1)
        self.assertEqual(len(self.user.history_queries), queries)
        await self.tick(1)
        start = self.clock[0]
        queries = len(self.user.history_queries)
        for second in range(1, 109):
            await self.tick(1)
            if second == 50:
                await self.receive(1100)
                self.assertEqual(self.user.reads[-1][1].max_id, 1100)
        self.assertEqual(len(self.user.history_queries), queries)
        self.assertEqual(len(self.user.sent), 1)
        await self.receive(1101, live=False)
        await self.tick(1)
        self.assertEqual(self.user.reads[-1][1].max_id, 1101)
        cancel = [(t, r) for t, r in self.user.typing if isinstance(r.action, types.SendMessageCancelAction)]
        self.assertEqual(cancel[-1][0] - start, 109)
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(3)
        self.assertEqual(len(self.user.sent), 2)

    async def test_long_second_voice_checks_missed_question_before_delivery(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        self.store.update(self.job['id'], second_duration=109)
        await self.tick()
        await self.tick(3)
        queries = len(self.user.history_queries)
        for second in range(1, 109):
            await self.tick(1)
            if second % 30 == 0:
                await self.app.recover_replies()
        self.assertLessEqual(len(self.user.history_queries) - queries, 3)
        await self.receive(1101, 'Почему?', live=False)
        await self.tick(1)
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        self.assertFalse(self.state()['second_send_approved'])
        self.assertEqual(len(self.user.sent), 2)

    async def test_new_reply_before_sticker_restarts_three_seconds_and_reads_old_history(self):
        self.user.history[123] = [incoming(50), incoming(100)]
        await self.tick()
        await self.receive(200)
        await self.tick(2)
        await self.receive(201)
        await self.tick(2.999)
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.user.reads[-1][1].max_id, 201)
        await self.tick(.001)
        self.assertEqual(self.user.sent[0][1].id, [10])

    async def test_reply_during_recording_is_read_and_voice_waits_three_seconds(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        start = self.clock[0]
        await self.tick(6)
        await self.receive(1100)
        await self.tick(1)
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(1.999)
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(.001)
        self.assertEqual(self.user.sent[-1][0] - start, 9)
        self.assertEqual(self.state()['stage'], 'reply_waiting')
        self.assertFalse(any('ответил(а) после ГС1' in c.args[1] for c in self.bot.send_message.call_args_list))

    async def test_missed_reply_just_before_voice_is_read_and_delays_sending(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        await self.tick(6)
        await self.receive(1100, live=False)
        await self.tick(1)
        self.assertEqual(len(self.user.sent), 1)
        self.assertEqual(self.user.reads[-1][1].max_id, 1100)
        await self.tick(3)
        self.assertEqual(len(self.user.sent), 2)

    async def test_early_reply_never_starts_or_cancels_first_voice(self):
        await self.sticker()
        await self.receive(1100, 'Почему?')
        await self.tick()
        await self.tick(3)
        await self.tick(7)
        self.assertEqual(self.state()['stage'], 'reply_waiting')
        self.assertEqual(len(self.user.sent), 2)
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 2)
        await self.receive(1200)
        self.assertEqual(self.state()['stage'], 'second_confirmation')

    async def test_second_request_contains_username_link_and_button(self):
        await self.first()
        await self.receive(1100)
        call = next(c for c in self.bot.send_message.call_args_list if 'ответил(а) после ГС1' in c.args[1])
        self.assertIn('@alice', call.args[1])
        self.assertEqual(call.kwargs['buttons'][0][0].type.url, 'https://t.me/alice')
        self.assertIn('send_second:', call.kwargs['buttons'][1][0].type.data.decode())
        self.assertFalse(self.state()['recording_kind'])

    async def test_manual_owner_reply_then_approval_still_has_read_and_recording_steps(self):
        await self.first()
        await self.receive(1100)
        self.user.history[123].append(incoming(1200, 'Мой ручной ответ', out=True))
        await self.approve()
        await self.tick()
        self.assertEqual(self.user.reads[-1][1].max_id, 1200)
        await self.tick(2.999)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(.001)
        self.assertEqual(self.state()['recording_kind'], 'second')
        await self.tick(11)
        self.assertEqual(self.state()['stage'], 'second_done')

    async def test_owner_only_approval_and_duplicate_button_does_not_duplicate_voice(self):
        await self.first()
        await self.receive(1100)
        data = f"send_second:{self.job['id']}:{self.state()['second_token']}"
        await self.callback(data, sender=999)
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        await asyncio.gather(self.callback(data), self.callback(data))
        await self.finish_second()
        await self.callback(data)
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 3)

    async def test_new_question_during_second_recording_revokes_approval(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        await self.tick()
        await self.tick(3)
        old_token = self.state()['second_token']
        await self.receive(1101, 'А почему?')
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        self.assertFalse(self.state()['recording_kind'])
        await self.callback(f"send_second:{self.job['id']}:{old_token}")
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 2)
        await self.approve()
        await self.finish_second()
        self.assertEqual(len(self.user.sent), 3)

    async def test_regular_new_reply_during_second_does_not_revoke_but_restarts_read_wait(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        await self.tick()
        await self.tick(3)
        await self.tick(10)
        await self.receive(1101, 'Хорошо')
        await self.tick(1)
        self.assertEqual(len(self.user.sent), 2)
        self.assertEqual(self.state()['stage'], 'second_pending')
        await self.tick(2)
        self.assertEqual(len(self.user.sent), 3)

    async def test_known_voice_restriction_prevents_even_sticker(self):
        self.user.forbidden.add(123)
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.user.typing, [])
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertTrue(any('не принимает голосовые' in c.args[1] for c in self.bot.send_message.call_args_list))
        with self.assertRaises(ValueError):
            await self.app.job_action('/retry', self.job['id'])

    async def test_voice_rpc_restriction_stops_without_any_text_replacement(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        self.user.failures[functions.messages.ForwardMessagesRequest] = errors.VoiceMessagesForbiddenError(request=None)
        await self.tick(7)
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertEqual(len(self.user.sent), 1)
        self.assertFalse(self.state()['recording_kind'])
        self.assertFalse(any(isinstance(r, functions.messages.SendMessageRequest) for _, r in self.user.operations))

    async def test_second_rpc_restriction_stops_without_fallback(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        await self.tick()
        await self.tick(3)
        self.user.failures[functions.messages.ForwardMessagesRequest] = errors.ChatSendVoicesForbiddenError(request=None)
        await self.tick(11)
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertEqual(len(self.user.sent), 2)

    async def test_restriction_enabled_while_recording_prevents_voice(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        self.user.forbidden.add(123)
        await self.tick(7)
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertEqual(len(self.user.sent), 1)

    async def test_failed_restriction_notification_retries_after_restart(self):
        self.user.forbidden.add(123)
        await self.tick()
        self.bot.send_message.side_effect = ConnectionError('offline')
        await self.tick(3)
        self.assertTrue(self.state()['notice_pending'])
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        self.bot.send_message.side_effect = None
        await self.tick(30)
        self.assertFalse(self.state()['notice_pending'])
        self.assertEqual(self.user.sent, [])

    async def test_read_failure_blocks_all_sends_and_then_requires_three_seconds(self):
        self.user.failures[functions.messages.ReadHistoryRequest] = ConnectionError('offline')
        await self.tick()
        await self.tick(29)
        self.assertEqual(self.user.sent, [])
        await self.tick(1)
        self.assertEqual(self.user.sent, [])
        await self.tick(2.999)
        self.assertEqual(self.user.sent, [])
        await self.tick(.001)
        self.assertEqual(len(self.user.sent), 1)

    async def test_failed_typing_does_not_start_clock_or_send_voice(self):
        await self.sticker()
        await self.tick()
        self.user.returns_false.add(functions.messages.SetTypingRequest)
        await self.tick(3)
        self.assertTrue(self.state()['error'])
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 1)

    async def test_flood_wait_blocks_later_jobs_and_keeps_stage(self):
        await self.tick()
        self.user.failures[functions.users.GetFullUserRequest] = errors.FloodWaitError(request=None, capture=20)
        await self.tick(3)
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.state()['stage'], 'sticker_pending')
        self.assertEqual(self.store.get('send_not_before'), self.clock[0] + 21)

    async def test_transient_send_failure_retains_random_id_and_rereads_before_retry(self):
        await self.tick()
        random_id = self.state()['sticker_random']
        self.user.failures[functions.messages.ForwardMessagesRequest] = ConnectionError('offline')
        await self.tick(3)
        self.assertEqual(self.user.sent, [])
        await self.tick(30)
        self.assertEqual(self.user.sent, [])
        await self.tick(3)
        self.assertEqual(self.user.sent[-1][1].random_id, [random_id])

    async def test_pause_clears_recording_and_resume_restarts_read_and_full_duration(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        await self.tick(2)
        await self.command('/pause')
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 1)
        await self.command('/resume')
        await self.tick()
        await self.tick(3)
        start = self.clock[0]
        await self.tick(7)
        self.assertEqual(self.user.sent[-1][0] - start, 7)

    async def test_cancel_clears_recording_and_never_sends_rest(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        await self.app.job_action('/cancel', self.job['id'])
        await self.tick(100)
        self.assertEqual(self.state()['stage'], 'cancelled')
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 1)

    async def test_retry_does_not_approve_second_and_status_restores_request(self):
        await self.first()
        await self.receive(1100)
        await self.app.job_action('/retry', self.job['id'])
        self.bot.send_message.reset_mock()
        await self.app.status()
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 2)
        self.assertTrue(any(c.kwargs.get('buttons') for c in self.bot.send_message.call_args_list))

    async def test_restart_restores_held_approval_without_auto_send(self):
        await self.first()
        await self.receive(1100)
        self.store.close()
        self.store = Store(self.path)
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.folders.move = AsyncMock()
        await self.app.recover_replies()
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 2)
        await self.approve()
        await self.finish_second()
        self.assertEqual(len(self.user.sent), 3)

    async def test_offline_answer_recovers_request(self):
        await self.first()
        await self.receive(1100, live=False)
        await self.app.recover_replies()
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        self.assertTrue(self.state()['second_send_notified'])
        self.assertEqual(len(self.user.sent), 2)

    async def test_finished_chat_new_answer_stays_unread_and_never_restarts(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        await self.finish_second()
        read_count = len(self.user.reads)
        await self.receive(1400)
        await self.tick(100)
        self.assertEqual(len(self.user.reads), read_count)
        self.assertEqual(len(self.user.sent), 3)

    async def test_unrelated_chat_and_service_events_are_ignored(self):
        await self.receive(1100, sender=999)
        await self.app.on_incoming(SimpleNamespace(is_private=True, sender_id=123, id=1200,
                                   message=SimpleNamespace()))
        self.assertFalse(self.state()['replied'])
        self.assertEqual(self.user.reads, [])

    async def test_settings_has_one_mode_then_its_three_media(self):
        await self.command('/settings')
        call = self.bot.send_message.call_args
        self.assertEqual(len(call.kwargs['buttons']), 1)
        self.assertEqual(call.kwargs['buttons'][0][0].type.data, b'scenario:two_voice')
        await self.callback('scenario:two_voice')
        buttons = [b.type.data for row in self.bot.send_message.call_args.kwargs['buttons'] for b in row]
        self.assertIn(b'sticker', buttons)
        self.assertIn(b'voice', buttons)
        self.assertIn(b'second', buttons)
        self.assertNotIn(b'saved', buttons)
        self.assertNotIn(b'saved_second', buttons)
        self.assertNotIn(b'texts', buttons)
        await self.callback('texts')
        self.assertIsNone(self.store.get('mode'))

    async def test_missing_second_voice_prevents_new_job(self):
        self.store.set('sticker', 10)
        self.store.set('voice', 11)
        with self.assertRaises(ValueError):
            await self.app.add_target('bob')
        self.assertIsNone(self.store.by_user(456))

    async def test_new_job_snapshots_all_three_media_and_durations(self):
        for key, value in (('sticker', 10), ('voice', 11), ('second', 12)):
            self.store.set(key, value)
        await self.app.add_target('bob')
        job = self.store.by_user(456)
        self.assertEqual(job['first_duration'], 7)
        self.assertEqual(job['second_duration'], 11)
        self.assertEqual(job['dialogue_mode'], 'two_voice')
        self.store.set('second', 99)
        self.assertEqual(self.store.job(job['id'])['second_source'], 12)
        with self.assertRaises(ValueError):
            await self.app.add_target('bob')

    async def request_repeat(self):
        for key, value in (('sticker', 10), ('voice', 11), ('second', 12)):
            self.store.set(key, value)
        self.user.get_entity = AsyncMock(return_value=types.User(id=123, access_hash=55, username='alice'))
        await self.app.add_target('alice')
        return self.store.repeat_request(self.job['id'])['token']

    async def test_repeat_requires_owner_button_and_new_reply_then_separate_second_approval(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        await self.finish_second()
        old = self.state()
        self.user.next_id = 1200
        token = await self.request_repeat()
        await self.tick(20)
        self.assertEqual(len(self.user.sent), 3)
        self.assertEqual(self.state()['stage'], 'second_done')
        await self.callback(f"repeat_confirm:{old['id']}:{token}", sender=999)
        self.assertEqual(self.state()['stage'], 'second_done')
        await self.callback(f"repeat_confirm:{old['id']}:{token}")
        self.assertIsNone(self.store.job(old['id']))
        self.job = self.store.by_user(123)
        self.assertGreater(self.job['id'], old['id'])
        self.assertNotEqual(self.job['sticker_random'], old['sticker_random'])
        self.assertNotEqual(self.job['voice_random'], old['voice_random'])
        self.assertNotEqual(self.job['second_random'], old['second_random'])
        self.assertFalse(self.job['second_send_approved'])
        self.assertEqual(self.job['baseline'], 1100)
        self.assertTrue(self.store.db.execute('SELECT 1 FROM job_history WHERE job_id=?', (old['id'],)).fetchone())
        await self.first()
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 5)
        self.assertEqual(self.state()['stage'], 'reply_waiting')
        await self.receive(1300)
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 5)
        await self.approve()
        await self.finish_second()
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11], [12], [10], [11], [12]])

    async def test_duplicate_confirmation_and_old_buttons_cannot_touch_new_cycle(self):
        self.store.update(self.job['id'], stage='cancelled')
        old_id = self.job['id']
        token = await self.request_repeat()
        await asyncio.gather(self.callback(f'repeat_confirm:{old_id}:{token}'),
                             self.callback(f'repeat_confirm:{old_id}:{token}'))
        self.job = self.store.by_user(123)
        self.assertEqual(self.job['id'], old_id + 1)
        self.assertEqual(len(self.store.jobs()), 1)
        await self.callback(f'send_second:{old_id}:0')
        await self.callback(f'second_cancel:{old_id}')
        await self.app.process_job(old_id)
        self.assertEqual(self.state()['stage'], 'sticker_pending')
        self.assertEqual(self.user.sent, [])
        await self.sticker()
        self.assertEqual(len(self.user.sent), 1)

    async def test_repeat_cancel_leaves_existing_cycle_and_recording_intact(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        deadline = self.state()['recording_until']
        token = await self.request_repeat()
        self.assertEqual(self.state()['recording_until'], deadline)
        await self.callback(f"repeat_cancel:{self.job['id']}:{token}")
        self.assertIsNone(self.store.repeat_request(self.job['id']))
        self.assertEqual(self.state()['recording_until'], deadline)
        await self.tick(7)
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11]])

    async def test_repeat_confirm_stops_old_recording_and_preserves_pause(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        token = await self.request_repeat()
        old_id = self.job['id']
        self.store.set('paused', True)
        await self.callback(f'repeat_confirm:{old_id}:{token}')
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageCancelAction)
        self.job = self.store.by_user(123)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 1)
        self.store.set('paused', False)
        await self.first()
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [10], [11]])

    async def test_repeat_survives_restart_and_status_restores_latest_button(self):
        first_token = await self.request_repeat()
        token = await self.request_repeat()
        self.assertNotEqual(first_token, token)
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        self.bot.send_message.reset_mock()
        await self.app.status()
        buttons = [button.type.data for call in self.bot.send_message.call_args_list
                   for row in call.kwargs.get('buttons', []) for button in row]
        self.assertIn(f"repeat_confirm:{self.job['id']}:{token}".encode(), buttons)
        await self.callback(f"repeat_confirm:{self.job['id']}:{first_token}")
        self.assertIsNotNone(self.state())
        await self.callback(f"repeat_confirm:{self.job['id']}:{token}")
        self.assertIsNone(self.state())
        self.assertEqual(self.store.by_user(123)['stage'], 'sticker_pending')

    async def test_repeat_uses_snapshot_and_baseline_at_confirmation(self):
        token = await self.request_repeat()
        self.store.set('voice', 99)
        self.user.history[123].append(incoming(150, 'Мой ответ', out=True))
        await self.callback(f"repeat_confirm:{self.job['id']}:{token}")
        self.job = self.store.by_user(123)
        self.assertEqual(self.job['voice_source'], 11)
        self.assertEqual(self.job['baseline'], 150)
        await self.app.on_incoming(SimpleNamespace(is_private=True, sender_id=123, id=149, message=incoming(149)))
        self.assertFalse(self.state()['replied'])
        await self.sticker()
        self.assertEqual(self.user.reads[-1][1].max_id, 150)

    async def test_repeat_cannot_bypass_voice_prohibition(self):
        self.store.update(self.job['id'], stage='voice_forbidden')
        token = await self.request_repeat()
        await self.callback(f"repeat_confirm:{self.job['id']}:{token}")
        self.job = self.store.by_user(123)
        self.user.forbidden.add(123)
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertEqual(self.user.sent, [])

    async def test_deleted_repeat_media_preserves_old_job_and_request(self):
        self.store.update(self.job['id'], stage='second_done')
        token = await self.request_repeat()
        del self.user.saved[11]
        await self.callback(f"repeat_confirm:{self.job['id']}:{token}")
        self.assertEqual(self.state()['stage'], 'second_done')
        self.assertEqual(self.store.repeat_request(self.job['id'])['token'], token)
        self.assertEqual(self.user.sent, [])

    async def test_failed_repeat_transaction_rolls_back_old_job_and_request(self):
        token = await self.request_repeat()
        with patch.object(self.store, '_insert_job', side_effect=sqlite3.OperationalError('disk full')):
            with self.assertRaises(sqlite3.OperationalError):
                self.store.restart(self.job['id'], token, 150)
        self.assertIsNotNone(self.state())
        self.assertEqual(self.store.repeat_request(self.job['id'])['token'], token)
        self.assertFalse(self.store.db.execute('SELECT 1 FROM job_history').fetchone())

    async def test_pause_waiting_for_lock_handles_replaced_job(self):
        token = await self.request_repeat()
        async with self.app.locks[123]:
            pause = asyncio.create_task(self.command('/pause'))
            await asyncio.sleep(0)
            self.store.restart(self.job['id'], token, 100)
        await pause
        self.assertTrue(self.store.get('paused'))
        self.assertIn('Отправки приостановлены', self.bot.send_message.call_args.args[1])

    async def test_previous_outgoing_requires_confirmation_before_any_send_or_read(self):
        self.user.history[123].append(incoming(150, 'Мой текст', out=True))
        await self.tick()
        self.assertEqual(self.state()['stage'], 'awaiting_confirmation')
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.user.reads, [])
        await self.callback(f"confirm:{self.job['id']}", sender=999)
        self.assertEqual(self.state()['stage'], 'awaiting_confirmation')
        await self.callback(f"confirm:{self.job['id']}")
        await self.tick()
        await self.tick(3)
        self.assertEqual(len(self.user.sent), 1)

    async def test_blocked_recipient_error_does_not_trigger_replacement(self):
        await self.tick()
        self.user.failures[functions.messages.ForwardMessagesRequest] = errors.UserIsBlockedError(request=None)
        await self.tick(3)
        self.assertTrue(self.state()['error'])
        self.assertEqual(self.user.sent, [])
        self.assertNotEqual(self.state()['stage'], 'voice_forbidden')

    async def test_mute_and_archive_failures_retry_without_resending(self):
        await self.tick()
        self.user.ignore_archive = True
        self.user.ignore_mute = True
        await self.tick(3)
        self.assertTrue(self.state()['archive_pending'])
        self.assertTrue(self.state()['mute_pending'])
        self.store.set('paused', True)
        self.user.ignore_archive = False
        self.user.ignore_mute = False
        await self.tick(31)
        self.assertEqual(len(self.user.sent), 1)
        self.assertTrue(self.state()['archive_done'])
        self.assertTrue(self.state()['mute_done'])

    async def test_approval_is_isolated_to_its_chat(self):
        await self.first()
        await self.receive(1100)
        other = self.store.add(user_id=456, access_hash=99, username='bob', sticker_source=10,
                               voice_source=11, second_source=12, baseline=100, first_duration=7,
                               second_duration=11)
        self.store.update(other['id'], stage='second_confirmation', second_baseline=1001, second_reply=1100, second_token=1)
        await self.approve()
        await self.finish_second()
        await self.app.process_job(other['id'])
        self.assertEqual(self.store.job(other['id'])['stage'], 'second_confirmation')
        self.assertFalse(self.store.job(other['id'])['second_send_approved'])
        self.assertEqual(len(self.user.sent), 3)

    async def test_worker_restart_rereads_and_restarts_interrupted_recording(self):
        await self.sticker()
        await self.tick()
        await self.tick(3)
        await self.tick(2)
        self.assertTrue(self.state()['recording_kind'])
        self.store.close()
        self.store = Store(self.path)
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.folders.move = AsyncMock()

        async def end_iteration(awaitable, **kwargs):
            awaitable.close()
            raise asyncio.CancelledError

        with patch('assistant.app.asyncio.wait_for', side_effect=end_iteration):
            with self.assertRaises(asyncio.CancelledError):
                await self.app.worker()
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(3)
        start = self.clock[0]
        await self.tick(7)
        self.assertEqual(self.user.sent[-1][0] - start, 7)

    async def test_second_request_delivery_failure_retries_without_sending_voice(self):
        await self.first()
        self.bot.send_message.side_effect = ConnectionError('offline')
        await self.receive(1100)
        self.assertFalse(self.state()['second_send_notified'])
        await self.tick(29)
        self.assertEqual(len(self.user.sent), 2)
        self.bot.send_message.side_effect = None
        await self.tick(1)
        self.assertTrue(self.state()['second_send_notified'])
        self.assertEqual(len(self.user.sent), 2)

    async def test_missing_legacy_durations_are_read_from_correct_saved_voices(self):
        self.store.update(self.job['id'], first_duration=None, second_duration=None)
        await self.first()
        self.assertEqual(self.state()['first_duration'], 7)
        await self.receive(1100)
        await self.approve()
        await self.finish_second()
        self.assertEqual(self.state()['second_duration'], 11)
        self.assertEqual(len(self.user.sent), 3)

    async def test_lost_first_message_id_recovers_with_same_id_without_replaying_intro(self):
        original = self.user.__call__
        missed = []

        class LoseFirstVoiceId:
            async def __call__(inner, request):
                result = await original(request)
                if isinstance(request, functions.messages.ForwardMessagesRequest) and request.id == [11] and not missed:
                    missed.append(True)
                    return SimpleNamespace()
                return result

            def __getattr__(inner, key):
                return getattr(self.user, key)

        self.app.user = LoseFirstVoiceId()
        await self.first()
        self.assertEqual(self.state()['second_baseline'], 0)
        self.assertEqual(len(self.user.ids), 2)
        await self.tick()
        self.assertGreater(self.state()['second_baseline'], 0)
        self.assertEqual(len(self.user.ids), 2)
        self.assertEqual(self.user.sent[-1][1].random_id, self.user.sent[-2][1].random_id)
        self.assertEqual(self.state()['stage'], 'reply_waiting')

    async def test_setup_copies_user_document_without_upload_for_all_media_types(self):
        self.bot.get_me = AsyncMock(return_value=SimpleNamespace(username="my_controller_bot"))
        self.bot.download_media = AsyncMock()
        for kind, mime in (("sticker", "image/webp"),
                           ("sticker", "application/x-tgsticker"),
                           ("sticker", "video/webm"), ("voice", "audio/ogg")):
            with self.subTest(kind=kind, mime=mime):
                message = media_message(kind, 500)
                original = media_message(kind, 900)
                original.out = True
                original.date = message.date
                original.document.mime_type = mime
                original.document.file_reference = b"user-side-reference"
                saved = media_message(kind, 600)
                unrelated = media_message(kind, 500)  # Same ID in the other account is irrelevant.
                unrelated.out = True
                unrelated.document.id = 999
                self.user.get_messages = AsyncMock(return_value=[unrelated, original])
                self.user.send_file = AsyncMock(return_value=saved)
                self.store.set("mode", kind)
                await self.app.capture_media(message, kind)
                self.user.get_messages.assert_awaited_once_with("my_controller_bot", limit=100)
                self.user.send_file.assert_awaited_once_with("me", original.media)
                request_media = utils.get_input_media(self.user.send_file.call_args.args[1])
                self.assertIsInstance(request_media, types.InputMediaDocument)
                self.assertEqual(request_media.id.file_reference, b"user-side-reference")
                self.bot.download_media.assert_not_awaited()
                self.assertEqual(self.store.get(kind), saved.id)
                self.assertIsNone(self.store.get("mode"))


    async def test_setup_retries_history_and_ignores_incoming_and_stale_matches(self):
        message = media_message("sticker")
        original = media_message("sticker", 900)
        original.out = True
        original.date = message.date
        incoming_copy = media_message("sticker", 901)
        stale = media_message("sticker", 902)
        stale.out = True
        stale.date = message.date - dt.timedelta(days=1)
        self.bot.get_me = AsyncMock(return_value=SimpleNamespace(username="my_controller_bot"))
        self.user.get_messages = AsyncMock(side_effect=[[incoming_copy, stale], [original]])
        self.user.send_file = AsyncMock(return_value=media_message("sticker", 600))
        with patch("assistant.app.asyncio.sleep", new_callable=AsyncMock):
            await self.app.capture_media(message, "sticker")
        self.assertEqual(self.user.get_messages.await_count, 2)
        self.user.send_file.assert_awaited_once_with("me", original.media)


    async def test_setup_missing_original_keeps_previous_setting(self):
        self.store.set("sticker", 11)
        self.store.set("mode", "sticker")
        self.bot.get_me = AsyncMock(return_value=SimpleNamespace(username="my_controller_bot"))
        self.user.get_messages = AsyncMock(return_value=[])
        self.user.send_file = AsyncMock()
        with patch("assistant.app.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaisesRegex(ValueError, "Не удалось найти"):
                await self.app.capture_media(media_message("sticker"), "sticker")
        self.user.send_file.assert_not_awaited()
        self.assertEqual(self.store.get("sticker"), 11)
        self.assertEqual(self.store.get("mode"), "sticker")


    async def test_setup_refetches_saved_media_before_reporting_type_loss(self):
        message = media_message("sticker")
        original = media_message("sticker", 900)
        original.out = True
        original.date = message.date
        incomplete = types.Message(id=600, peer_id=types.PeerUser(42), message="")
        saved = media_message("sticker", 600)
        self.bot.get_me = AsyncMock(return_value=SimpleNamespace(username="my_controller_bot"))
        self.user.get_messages = AsyncMock(side_effect=[[original], saved])
        self.user.send_file = AsyncMock(return_value=incomplete)
        await self.app.capture_media(message, "sticker")
        self.assertEqual(self.store.get("sticker"), 600)
        self.user.get_messages.assert_awaited_with("me", ids=600)


    async def test_setup_type_loss_keeps_previous_setting(self):
        self.store.set("sticker", 11)
        original = media_message("sticker", 900)
        original.out = True
        self.bot.get_me = AsyncMock(return_value=SimpleNamespace(username="my_controller_bot"))
        incomplete = types.Message(id=600, peer_id=types.PeerUser(42), message="")
        self.user.get_messages = AsyncMock(side_effect=[[original], incomplete])
        self.user.send_file = AsyncMock(return_value=incomplete)
        with self.assertRaisesRegex(ValueError, "Настройка не изменена"):
            await self.app.capture_media(media_message("sticker"), "sticker")
        self.assertEqual(self.store.get("sticker"), 11)


    async def test_choose_existing_saved_voice_by_id(self):
        self.user.get_messages = AsyncMock(return_value=media_message("voice", 555))
        event = SimpleNamespace(sender_id=42, answer=AsyncMock(), data=b"saved:555")
        await self.app.on_callback(event)
        self.assertEqual(self.store.get("voice"), 555)
        self.user.get_messages.assert_awaited_once_with("me", ids=555)



class FolderTests(unittest.IsolatedAsyncioTestCase):
    async def test_folder_move_preserves_other_peers_flags_and_pins(self):
        target = types.InputPeerUser(123, 55)
        other = types.InputPeerUser(456, 66)
        folder = types.DialogFilter(id=2, title=types.TextWithEntities("В процессе 1", []),
                    pinned_peers=[target, other], include_peers=[target, other],
                    exclude_peers=[], contacts=True, emoticon="🟢")
        place_peer(folder, target, False)
        self.assertEqual(folder.include_peers, [other])
        self.assertEqual(folder.pinned_peers, [other])
        self.assertEqual(folder.exclude_peers, [target])
        self.assertTrue(folder.contacts)
        self.assertEqual(folder.emoticon, "🟢")
        place_peer(folder, target, True)
        self.assertEqual(folder.exclude_peers, [])
        self.assertEqual(folder.include_peers, [other, target])

    async def test_create_folder_and_correct_update_order(self):
        peer = types.InputPeerUser(123, 55)
        existing = types.DialogFilter(id=2, title=types.TextWithEntities("В процессе 1", []),
                    pinned_peers=[], include_peers=[peer], exclude_peers=[])
        requests = []

        async def client(request):
            requests.append(request)
            if isinstance(request, functions.messages.GetDialogFiltersRequest):
                return SimpleNamespace(filters=[types.DialogFilterDefault(), existing])
            # Serialization catches incompatible Telegram schema usage offline.
            self.assertIsInstance(bytes(request), bytes)
            return True

        await Folders(client).move(peer, 2)
        self.assertEqual(title_text(requests[1].filter), "В процессе 2")
        self.assertEqual(requests[1].filter.include_peers, [peer])
        self.assertEqual(requests[2].id, existing.id)
        self.assertIsNone(requests[2].filter, "Telegram rejects empty folders without category rules")

    async def test_nonempty_source_folder_is_kept(self):
        peer, other = types.InputPeerUser(123, 55), types.InputPeerUser(456, 66)
        existing = types.DialogFilter(id=2, title=types.TextWithEntities("В процессе 1", []),
                    pinned_peers=[other], include_peers=[peer, other], exclude_peers=[], color=3)
        requests = []

        async def client(request):
            requests.append(request)
            if isinstance(request, functions.messages.GetDialogFiltersRequest):
                return SimpleNamespace(filters=[existing])
            return True

        await Folders(client).move(peer, 2)
        self.assertIs(requests[2].filter, existing)
        self.assertEqual(existing.include_peers, [other])
        self.assertEqual(existing.pinned_peers, [other])
        self.assertEqual(existing.color, 3)

    async def test_duplicate_folder_names_fail_without_writing(self):
        folder = lambda i: types.DialogFilter(id=i, title=types.TextWithEntities("В процессе 1", []),
                                pinned_peers=[], include_peers=[], exclude_peers=[])
        client = AsyncMock(return_value=SimpleNamespace(filters=[folder(2), folder(3)]))
        with self.assertRaises(ValueError):
            await Folders(client).move(types.InputPeerUser(123, 55), 1)
        self.assertEqual(client.await_count, 1)



class InputTests(unittest.TestCase):
    def test_username_forms(self):
        for value in ["@alice", "alice", "https://t.me/alice", "https://t.me/alice/"]:
            self.assertEqual(parse_username(value), "alice")
        for value in ["@alice @bob", "https://evil.example/alice", "/start", "t.me/+invite"]:
            self.assertIsNone(parse_username(value))

