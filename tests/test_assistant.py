import asyncio
import datetime as dt
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from telethon import errors, functions, types, utils

from assistant.app import Assistant, READ_DELAY, parse_username, voice_duration, circle_duration, peer_for
from assistant.folders import Folders, place_peer, title_text
from assistant.store import Store


def incoming(message_id=200, text='Привет', sender=123, out=False):
    return types.Message(id=message_id, peer_id=types.PeerUser(sender),
                         date=dt.datetime.now(dt.timezone.utc), message=text, out=out)


def media_message(kind, message_id=500, duration=7):
    attributes = ([types.DocumentAttributeAudio(duration=duration, voice=True)] if kind == 'voice' else
                  [types.DocumentAttributeVideo(duration=duration, w=240, h=240, round_message=True)] if kind == 'circle' else
                  [types.DocumentAttributeSticker(alt='🙂', stickerset=types.InputStickerSetEmpty())])
    document = types.Document(id=77, access_hash=88, file_reference=b'bot-only-reference',
                              date=dt.datetime.now(dt.timezone.utc), mime_type='audio/ogg' if kind == 'voice' else 'video/mp4' if kind == 'circle' else 'image/webp',
                              size=4, dc_id=2, attributes=attributes)
    return types.Message(id=message_id, peer_id=types.PeerUser(42), date=dt.datetime.now(dt.timezone.utc),
                         message='', media=types.MessageMediaDocument(document=document))


class FakeUser:
    def __init__(self, clock):
        self.clock = clock
        self.operations = []
        self.sent = []
        self.texts = []
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
        if isinstance(request, functions.messages.SendMessageRequest):
            self.texts.append((self.clock[0], request))
            if request.random_id not in self.ids:
                self.next_id += 1
                self.ids[request.random_id] = self.next_id
                self.history.setdefault(request.peer.user_id, []).append(
                    incoming(self.next_id, request.message, request.peer.user_id, out=True))
            return SimpleNamespace(updates=[types.UpdateMessageID(id=self.ids[request.random_id], random_id=request.random_id)])
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
        return types.User(id=123 if username.lower() == 'alice' else int(username[4:]) if username.startswith('user') and username[4:].isdigit() else 456, access_hash=99, username=username)


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
        self.app.folders.normalize = AsyncMock()
        self.job = self.store.add(user_id=123, access_hash=55, username='alice', sticker_source=10,
                                  voice_source=11, second_source=12, baseline=100, first_duration=7,
                                  second_duration=11, history_checked=True)
        self.app.ready = True

    async def asyncTearDown(self):
        await self.stop_heartbeats()
        self.store.close()
        self.time_patch.stop()
        self.temp.cleanup()

    async def stop_heartbeats(self):
        tasks = list(self.app.recording_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.app.recording_tasks.clear()

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
        await self.tick(1)
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
        self.store.set('second', self.state()['second_source'])
        await self.app.request_third_target(self.job['username'])

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
        await self.tick(.999)
        self.assertFalse(self.state()['recording_kind'])
        self.assertAlmostEqual(self.app.next_tick(), .01)
        await self.tick(.001)
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordAudioAction)
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(4)
        await self.tick(2.999)
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(.001)
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11]])
        self.assertAlmostEqual(self.user.sent[-1][0], 1011)
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
        await self.tick(1)
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
        await self.tick(1)
        start = self.clock[0]
        for _ in range(3):
            await self.tick(4)
        await self.tick(1)
        record = [(t, r) for t, r in self.user.typing if isinstance(r.action, types.SendMessageRecordAudioAction)]
        cancel = [(t, r) for t, r in self.user.typing if isinstance(r.action, types.SendMessageCancelAction)]
        self.assertEqual([t - start for t, _ in record], [0, 4, 8, 12, 13])
        self.assertEqual(cancel[-1][0] - start, 13)
        self.assertEqual(self.user.sent[-1][0] - start, 13)

    async def test_voice_is_dispatched_before_indicator_cancellation_with_one_final_history_scan(self):
        await self.sticker()
        await self.tick()
        await self.tick(1)
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

    async def start_test_voice(self, kind, duration=1):
        self.user.saved[13] = media_message('voice', 13, duration=duration)
        self.store.update(self.job['id'],
                          stage={'voice': 'voice_pending', 'followup': 'followup_pending',
                                 'second': 'second_pending'}[kind],
                          dialogue_mode='three_voice' if kind == 'followup' else 'two_voice',
                          followup_source=13, followup_duration=duration, followup_random=123456,
                          first_duration=duration, second_duration=duration, second_baseline=0,
                          second_send_approved=int(kind == 'second'),
                          recording_kind='', record_complete=0,
                          read_gate_id=100, read_gate_at=self.clock[0] - 3)
        self.user.history[123] = [incoming(100)]
        await self.tick()
        self.assertEqual(self.state()['recording_kind'], kind)

    async def test_all_voices_keep_indicator_during_slow_final_checks_and_delivery(self):
        for kind in ('voice', 'followup', 'second'):
            with self.subTest(kind=kind), patch('assistant.app.RECORD_INTERVAL', .01):
                await self.start_test_voice(kind)
                permission = asyncio.Event()
                permission_refreshed = asyncio.Event()
                delivery = asyncio.Event()
                delivery_refreshed = asyncio.Event()
                refresh = self.app.refresh_recording
                allowed = self.app.voice_allowed
                copy = self.app.copy_saved
                deadline = self.state()['recording_until']
                operation = len(self.user.operations)

                async def observe_refresh(job_id, **kwargs):
                    previous = len(self.user.typing)
                    await refresh(job_id, **kwargs)
                    if len(self.user.typing) > previous:
                        if delivery.is_set():
                            delivery_refreshed.set()
                        elif permission.is_set():
                            permission_refreshed.set()

                async def slow_permission(job):
                    permission.set()
                    self.clock[0] += 4
                    await asyncio.wait_for(permission_refreshed.wait(), .5)
                    self.assertEqual(self.state()['recording_until'], deadline)
                    return await allowed(job)

                async def slow_delivery(job, selected):
                    delivery.set()
                    self.clock[0] += 4
                    await asyncio.wait_for(delivery_refreshed.wait(), .5)
                    self.assertEqual(selected, kind)
                    self.assertEqual(self.state()['recording_kind'], kind)
                    return await copy(job, selected)

                with patch.object(self.app, 'refresh_recording', side_effect=observe_refresh), \
                        patch.object(self.app, 'voice_allowed', side_effect=slow_permission), \
                        patch.object(self.app, 'copy_saved', side_effect=slow_delivery):
                    await self.tick(1)
                self.assertTrue(permission_refreshed.is_set())
                self.assertTrue(delivery_refreshed.is_set())
                operations = [r for _, r in self.user.operations[operation:]]
                sent = next(i for i, r in enumerate(operations) if isinstance(r, functions.messages.ForwardMessagesRequest))
                cancelled = next(i for i, r in enumerate(operations) if isinstance(r, functions.messages.SetTypingRequest)
                                 and isinstance(r.action, types.SendMessageCancelAction))
                self.assertLess(sent, cancelled)
                self.assertNotIn(self.job['id'], self.app.recording_tasks)

    async def test_all_voices_keep_indicator_during_pause_after_late_incoming(self):
        for kind in ('voice', 'followup', 'second'):
            with self.subTest(kind=kind):
                await self.start_test_voice(kind)
                sent_before = len(self.user.sent)
                cancel_before = sum(isinstance(r.action, types.SendMessageCancelAction) for _, r in self.user.typing)
                await self.receive(1100, live=False)
                await self.tick(1)
                self.assertEqual(self.state()['recording_kind'], kind)
                self.assertTrue(self.state()['record_complete'])
                self.assertEqual(self.state()['read_done'], 1100)
                await self.tick((3 if kind == 'second' else 1) - .001)
                self.assertEqual(len(self.user.sent), sent_before)
                self.assertEqual(sum(isinstance(r.action, types.SendMessageCancelAction) for _, r in self.user.typing), cancel_before)
                await self.tick(.001)
                self.assertEqual(len(self.user.sent), sent_before + 1)
                self.assertFalse(self.state()['recording_kind'])

    async def test_recording_refreshes_while_worker_is_busy_elsewhere(self):
        with patch('assistant.app.RECORD_INTERVAL', .01):
            await self.start_test_voice('voice', duration=30)
            refreshed = asyncio.Event()
            refresh = self.app.refresh_recording

            async def observe_refresh(job_id, **kwargs):
                await refresh(job_id, **kwargs)
                refreshed.set()

            self.clock[0] += 4
            with patch.object(self.app, 'refresh_recording', side_effect=observe_refresh):
                await asyncio.wait_for(refreshed.wait(), .5)
            self.assertEqual(self.state()['recording_kind'], 'voice')
            self.assertEqual(len(self.user.sent), 0)
            await self.command('/pause')
            self.assertEqual(self.app.recording_tasks, {})
            self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageCancelAction)

    async def test_voice_send_failure_stops_background_indicator_and_retry_restarts_recording(self):
        await self.start_test_voice('voice')
        self.user.failures[functions.messages.ForwardMessagesRequest] = ConnectionError('offline')
        await self.tick(1)
        self.assertEqual(self.app.recording_tasks, {})
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 0)
        await self.tick(30)
        await self.tick(3)
        self.assertEqual(self.state()['recording_kind'], 'voice')
        await self.tick(1)
        self.assertEqual(len(self.user.sent), 1)

    async def test_question_during_final_check_keeps_approval_but_waits_after_reading(self):
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
        self.assertEqual(self.state()['stage'], 'second_pending')
        self.assertTrue(self.state()['second_send_approved'])
        self.assertEqual(self.state()['recording_kind'], 'second')
        self.assertEqual(len(self.user.sent), 2)
        await self.tick(3)
        self.assertEqual(len(self.user.sent), 3)

    async def test_long_voice_does_not_poll_history_every_second_and_still_reads_before_send(self):
        self.store.update(self.job['id'], first_duration=109)
        await self.sticker()
        await self.tick()
        queries = len(self.user.history_queries)
        await self.tick(.999)
        self.assertEqual(len(self.user.history_queries), queries)
        await self.tick(.001)
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
        self.assertEqual(cancel, [])
        self.assertEqual(self.state()['recording_kind'], 'voice')
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(3)
        self.assertEqual(len(self.user.sent), 2)

    async def test_long_second_voice_reads_missed_question_without_revoking_approval(self):
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
        self.assertEqual(self.state()['stage'], 'second_pending')
        self.assertTrue(self.state()['second_send_approved'])
        self.assertEqual(len(self.user.sent), 2)
        await self.tick(3)
        self.assertEqual(len(self.user.sent), 3)

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

    async def test_reply_during_first_recording_is_read_and_voice_waits_one_second(self):
        await self.sticker()
        await self.tick()
        await self.tick(1)
        start = self.clock[0]
        await self.tick(6)
        await self.receive(1100)
        await self.tick(.999)
        self.assertEqual(self.state()['recording_kind'], 'voice')
        self.assertEqual(len(self.user.sent), 1)
        await self.tick(.001)
        self.assertEqual(self.user.sent[-1][0] - start, 7)
        self.assertEqual(self.state()['stage'], 'reply_waiting')
        self.assertFalse(any('ответил(а) после начальных ГС' in c.args[1] for c in self.bot.send_message.call_args_list))

    async def test_missed_reply_just_before_voice_is_read_and_delays_sending(self):
        await self.sticker()
        await self.tick()
        await self.tick(1)
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

    async def test_reply_never_sends_a_third_voice_confirmation_message(self):
        await self.first()
        self.bot.send_message.reset_mock()
        await self.receive(1100)
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        self.assertEqual(self.bot.send_message.call_count, 0)
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 2)

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
        self.store.set("second", 12)
        await asyncio.gather(self.app.request_third_target("alice"), self.app.request_third_target("alice"))
        await self.finish_second()
        await self.callback(data)
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 3)

    async def test_question_marks_during_second_recording_keep_approval_and_timer(self):
        await self.first()
        await self.receive(1100)
        await self.approve()
        await self.tick()
        await self.tick(3)
        old_token = self.state()['second_token']
        deadline = self.state()['recording_until']
        await self.receive(1101, 'А почему? ？ ؟')
        self.assertEqual(self.state()['stage'], 'second_pending')
        self.assertTrue(self.state()['recording_kind'])
        self.assertTrue(self.state()['second_send_approved'])
        self.assertEqual(self.state()['recording_until'], deadline)
        self.assertEqual(self.state()['second_token'], old_token)
        await self.callback(f"send_second:{self.job['id']}:{old_token}")
        await self.finish_second()
        self.assertEqual(len(self.user.sent), 3)

    async def test_question_before_approval_does_not_warn_or_invalidate_existing_button(self):
        await self.first()
        await self.receive(1100, 'Почему?')
        token = self.state()['second_token']
        notices = self.bot.send_message.call_count
        await self.receive(1101, 'Когда？ Как؟')
        self.assertEqual(self.state()['second_token'], token)
        self.assertEqual(self.bot.send_message.call_count, notices)
        self.assertFalse(any('В ответе есть вопрос' in call.args[1] for call in self.bot.send_message.call_args_list))
        await self.approve()
        self.assertTrue(self.state()['second_send_approved'])

    def add_queue_job(self, user_id, stage='second_confirmation', folder=2):
        job = self.store.add(user_id=user_id, access_hash=55, username=f'user{user_id}',
                             sticker_source=10, voice_source=11, second_source=12, baseline=100,
                             first_duration=7, second_duration=11, history_checked=True)
        self.store.update(job['id'], stage=stage, desired_folder=2, applied_folder=folder,
                          second_baseline=1001, second_reply=1100, second_token=1,
                          second_send_approved=int(stage == 'second_pending'))
        return self.store.job(job['id'])

    def queue_prompts(self):
        return [call for call in self.bot.send_message.call_args_list if call.args[1].startswith('@user') and ' · задание #' in call.args[1]]

    async def test_queue_menu_has_two_options_and_main_menu_has_cancel_button(self):
        self.add_queue_job(456)
        await self.command('/start')
        menu = [button.button.text for row in self.bot.send_message.call_args.kwargs['buttons'] for button in row]
        self.assertIn('📨 Очередь', menu)
        self.assertIn('❌ Отменить отправку', menu)
        await self.command('📨 Очередь')
        self.assertIn('В процессе: 1', self.bot.send_message.call_args.args[1])
        buttons = [button.type.data for row in self.bot.send_message.call_args.kwargs['buttons'] for button in row]
        self.assertEqual(buttons, [b'queue:five', b'queue:all', b'third_target'])
        self.assertEqual(self.queue_prompts(), [])
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.user.history_queries, [])

    async def test_queue_five_sends_oldest_five_separate_prompts_without_voice_approval(self):
        jobs = [self.add_queue_job(user_id) for user_id in range(456, 463)]
        self.add_queue_job(500, stage='second_pending')
        self.add_queue_job(501, stage='reply_waiting')
        self.add_queue_job(502, stage='second_done')
        self.add_queue_job(503, folder=1)
        with patch('assistant.app.asyncio.sleep', new_callable=AsyncMock):
            await self.callback('queue:five')
        prompts = self.queue_prompts()
        self.assertEqual(len(prompts), 5)
        self.assertEqual([call.args[1].split(' · ')[0] for call in prompts],
                         [f"@{job['username']}" for job in jobs[:5]])
        self.assertTrue(all(len(call.kwargs['buttons']) == 1 for call in prompts))
        self.assertTrue(all(not self.store.job(job['id'])['second_send_approved'] for job in jobs))
        self.assertEqual(self.user.sent, [])
        self.store.set('second', 12)
        await self.app.request_third_target(jobs[0]['username'])
        self.assertEqual(self.store.job(jobs[0]['id'])['stage'], 'second_pending')
        self.assertEqual(len(self.app.second_queue()), 6)
        self.assertTrue(all(self.store.job(job['id'])['stage'] == 'second_confirmation' for job in jobs[1:]))

    async def test_queue_all_resends_existing_prompts_with_pacing_and_no_media_sends(self):
        jobs = [self.add_queue_job(user_id) for user_id in range(456, 463)]
        for job in jobs:
            self.store.update(job['id'], second_send_notified=1)
        with patch('assistant.app.asyncio.sleep', new_callable=AsyncMock) as sleep:
            await self.callback('queue:all')
        self.assertEqual(len(self.queue_prompts()), 7)
        self.assertEqual(sleep.await_count, 7)
        self.assertTrue(all(call.args == (1.1,) for call in sleep.await_args_list))
        self.assertEqual(self.user.sent, [])
        self.assertTrue(all(self.store.job(job['id'])['stage'] == 'second_confirmation' for job in jobs))

    async def test_empty_queue_and_unauthorized_queue_click_send_no_prompts(self):
        await self.callback('queue:five')
        self.assertIn('нет заданий', self.bot.send_message.call_args.args[1])
        self.add_queue_job(456)
        await self.callback('queue:all', sender=999)
        self.assertEqual(self.queue_prompts(), [])
        self.assertEqual(self.user.sent, [])

    async def test_queue_skips_job_approved_during_batch(self):
        first = self.add_queue_job(456)
        second = self.add_queue_job(457)

        async def approve_between_prompts(seconds):
            self.store.set('second', 12)
            await self.app.request_third_target(second['username'])

        with patch('assistant.app.asyncio.sleep', side_effect=approve_between_prompts):
            await self.callback('queue:all')
        self.assertEqual(len(self.queue_prompts()), 1)
        self.assertIn(f"#{first['id']}", self.queue_prompts()[0].args[1])
        self.assertEqual(self.store.job(second['id'])['stage'], 'second_pending')

    async def test_queue_delivery_failure_stops_batch_without_approving_any_voice(self):
        jobs = [self.add_queue_job(456), self.add_queue_job(457)]
        self.bot.send_message.side_effect = ConnectionError('offline')
        with patch('assistant.app.asyncio.sleep', new_callable=AsyncMock):
            await self.callback('queue:all')
        self.assertEqual(len(self.queue_prompts()), 1)
        self.assertTrue(all(self.store.job(job['id'])['stage'] == 'second_confirmation' for job in jobs))
        self.assertFalse(self.store.job(jobs[0]['id'])['second_send_notified'])

    async def test_queue_rejects_concurrent_batch_and_obsolete_cancel_button(self):
        job = self.add_queue_job(456)
        async with self.app.queue_lock:
            await self.callback('queue:all')
        self.assertIn('уже отправляются', self.bot.send_message.call_args.args[1])
        self.assertEqual(self.queue_prompts(), [])
        await self.callback(f"second_cancel:{job['id']}")
        self.assertEqual(self.store.job(job['id'])['stage'], 'second_confirmation')

    async def test_queue_survives_restart_and_excludes_completed_or_replaced_jobs(self):
        waiting = self.add_queue_job(456)
        self.add_queue_job(457, stage='cancelled')
        self.add_queue_job(458, stage='voice_forbidden')
        await self.stop_heartbeats()
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        with patch('assistant.app.asyncio.sleep', new_callable=AsyncMock):
            await self.callback('queue:all')
        self.assertEqual(len(self.queue_prompts()), 1)
        self.assertIn(f"#{waiting['id']}", self.queue_prompts()[0].args[1])

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

    async def test_manual_third_button_and_username_send_only_selected_voice(self):
        self.store.set('second', 12)
        await self.command('🎙 Отправить ГС3')
        self.assertEqual(self.store.get('mode'), 'third_target')
        self.assertEqual(self.user.sent, [])
        await self.command('https://t.me/bob')
        self.job = self.store.by_user(456)
        self.assertTrue(self.job['manual_third_only'])
        self.assertTrue(self.job['second_send_approved'])
        self.assertEqual(self.job['stage'], 'second_pending')
        self.assertIsNone(self.store.get('mode'))
        await self.tick()
        await self.tick(2.999)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(.001)
        self.assertEqual(self.state()['recording_kind'], 'second')
        await self.tick(10.999)
        self.assertEqual(self.user.sent, [])
        await self.tick(.001)
        self.assertEqual([request.id for _, request in self.user.sent], [[12]])
        self.assertEqual(self.state()['stage'], 'second_done')
        self.assertEqual(self.user.archive[456], 1)
        self.assertEqual(self.state()['desired_folder'], 0)

    async def test_manual_third_for_existing_chat_uses_current_template_without_restarting_initials(self):
        await self.first()
        self.store.set('second', 12)
        await self.command('/gs3')
        await self.command('@alice')
        self.assertEqual(self.state()['stage'], 'second_pending')
        self.assertTrue(self.state()['second_send_approved'])
        await self.finish_second()
        self.assertEqual([request.id for _, request in self.user.sent], [[10], [11], [12]])

    async def test_manual_third_cannot_interrupt_initial_voice_or_pending_circle(self):
        self.store.set('second', 12)
        await self.command('/gs3')
        await self.command('@alice')
        self.assertEqual(self.state()['stage'], 'sticker_pending')
        self.assertEqual(self.store.get('mode'), 'third_target')
        self.assertEqual(self.user.sent, [])

    async def test_manual_third_validates_target_media_and_owner_and_can_cancel_input(self):
        self.store.set('second', 12)
        await self.command('/gs3', sender=999)
        self.assertIsNone(self.store.get('mode'))
        await self.callback('third_target', sender=999)
        self.assertIsNone(self.store.get('mode'))
        await self.callback('third_target')
        await self.command('@bob @alice')
        self.assertIsNone(self.store.by_user(456))
        self.assertEqual(self.store.get('mode'), 'third_target')
        del self.user.saved[12]
        await self.command('@bob')
        self.assertIsNone(self.store.by_user(456))
        await self.command('/cancel')
        self.assertIsNone(self.store.get('mode'))

    async def test_manual_third_duplicate_input_preserves_random_id_and_duration(self):
        self.store.set('second', 12)
        await self.app.request_third_target('bob')
        job = self.store.by_user(456)
        await self.app.request_third_target('bob')
        self.assertEqual(self.store.by_user(456)['second_random'], job['second_random'])
        self.assertEqual(self.store.by_user(456)['id'], job['id'])
        self.assertEqual(len(self.store.jobs()), 2)

    async def test_manual_third_replaces_completed_job_only_after_explicit_new_request(self):
        self.store.set('second', 12)
        await self.app.request_third_target('bob')
        old = self.store.by_user(456)
        self.store.update(old['id'], stage='second_done', second_message=1500)
        await self.app.request_third_target('bob')
        new = self.store.by_user(456)
        self.assertNotEqual(old['id'], new['id'])
        self.assertNotEqual(old['second_random'], new['second_random'])
        self.assertIsNone(self.store.job(old['id']))
        self.assertIsNotNone(self.store.db.execute('SELECT 1 FROM job_history WHERE job_id=?', (old['id'],)).fetchone())

    async def test_manual_third_survives_restart_and_respects_pause_and_voice_restrictions(self):
        self.store.set('second', 12)
        self.store.set('paused', True)
        await self.app.request_third_target('bob')
        self.job = self.store.by_user(456)
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        self.assertTrue(self.state()['second_send_approved'])
        await self.tick(100)
        self.assertEqual(self.user.sent, [])
        self.store.set('paused', False)
        self.user.forbidden.add(456)
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertEqual(self.user.sent, [])

    async def test_initial_message_archives_without_creating_first_stage_folder_and_reply_moves_to_process(self):
        await self.sticker()
        self.assertEqual(self.user.archive[123], 1)
        self.assertEqual(self.state()['desired_folder'], 0)
        self.app.folders.move.assert_not_awaited()
        await self.receive(1100)
        await self.tick()
        self.assertEqual(self.state()['desired_folder'], 2)
        self.app.folders.move.assert_awaited_with(peer_for(self.state()), 2)

    async def test_answer_after_standalone_third_moves_to_process_without_reading_or_sending_again(self):
        self.store.set('second', 12)
        await self.app.request_third_target('bob')
        self.job = self.store.by_user(456)
        await self.finish_second()
        reads = len(self.user.reads)
        await self.receive(2000, sender=456)
        self.assertEqual(self.state()['desired_folder'], 2)
        self.assertEqual(len(self.user.reads), reads)
        self.assertEqual([request.id for _, request in self.user.sent], [[12]])

    async def test_offline_answer_after_standalone_voice_is_recovered_without_notification_or_reading(self):
        self.store.set('second', 12)
        await self.app.request_third_target('bob')
        self.job = self.store.by_user(456)
        await self.finish_second()
        read_count = len(self.user.reads)
        await self.receive(2000, sender=456, live=False)
        self.bot.send_message.reset_mock()
        await self.app.recover_replies()
        self.assertEqual(self.state()['desired_folder'], 2)
        self.assertTrue(self.state()['replied'])
        self.assertEqual(len(self.user.reads), read_count)
        self.assertEqual(self.bot.send_message.call_count, 0)

    async def test_folder_layout_failure_retries_without_losing_approved_sends(self):
        self.app.folders.normalize.side_effect = ConnectionError('offline')
        await self.app.sync_folder_layout()
        self.assertFalse(self.store.get('single_process_folder_layout', False))
        self.assertGreater(self.store.get('folder_layout_retry'), self.clock[0])
        self.app.folders.normalize.side_effect = None
        self.app.folders.normalize.return_value = None
        await self.app.sync_folder_layout()
        self.assertEqual(self.app.folders.normalize.await_count, 1)
        self.clock[0] += 60
        await self.app.sync_folder_layout()
        self.assertTrue(self.store.get('single_process_folder_layout'))

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
        await self.tick(1)
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
        await self.tick(1)
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
        await self.stop_heartbeats()
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
        await self.tick(1)
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
        await self.tick(1)
        await self.app.job_action('/cancel', self.job['id'])
        await self.tick(100)
        self.assertEqual(self.state()['stage'], 'cancelled')
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(len(self.user.sent), 1)

    async def test_cancel_by_username_button_only_stops_selected_user_case_insensitively(self):
        other = self.add_queue_job(456)
        self.store.set('sticker', 10)
        await self.command('❌ Отменить отправку')
        await self.command('https://t.me/ALICE')
        self.assertEqual(self.state()['stage'], 'cancelled')
        self.assertEqual(self.store.job(other['id'])['stage'], 'second_confirmation')
        self.assertEqual(self.store.get('sticker'), 10)
        self.assertIsNone(self.store.get('mode'))
        self.assertIn('@alice', self.bot.send_message.call_args.args[1])
        await self.tick(100)
        self.assertEqual(self.user.sent, [])

    async def test_cancel_input_invalid_unknown_and_exit_never_add_targets(self):
        self.app.add_target = AsyncMock()
        await self.command('❌ Отменить отправку', sender=999)
        self.assertIsNone(self.store.get('mode'))
        await self.command('❌ Отменить отправку')
        for text in ('invalid name!', '@unknown'):
            await self.command(text)
            self.assertEqual(self.store.get('mode'), 'cancel_target')
            self.assertEqual(self.state()['stage'], 'sticker_pending')
        self.app.add_target.assert_not_awaited()
        await self.command('/cancel')
        self.assertIsNone(self.store.get('mode'))
        self.assertEqual(self.state()['stage'], 'sticker_pending')

    async def test_cancel_callback_revokes_approved_second_and_pending_repeat_across_restart(self):
        await self.first()
        await self.receive(1100)
        second_token = self.state()['second_token']
        await self.approve()
        await self.tick()
        await self.tick(3)
        repeat_token = await self.request_repeat()
        data = f"cancel_job:{self.job['id']}"
        await self.callback(data, sender=999)
        self.assertEqual(self.state()['stage'], 'second_pending')
        await asyncio.gather(self.callback(data), self.callback(data))
        self.assertEqual(self.state()['stage'], 'cancelled')
        self.assertFalse(self.state()['recording_kind'])
        self.assertFalse(self.state()['second_send_approved'])
        self.assertIsNone(self.store.repeat_request(self.job['id']))
        self.assertTrue(any(isinstance(r.action, types.SendMessageCancelAction) for _, r in self.user.typing))
        await self.stop_heartbeats()
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        await self.callback(f"send_second:{self.job['id']}:{second_token}")
        await self.callback(f"repeat_confirm:{self.job['id']}:{repeat_token}")
        await self.receive(1200)
        await self.tick(100)
        self.assertEqual(self.state()['stage'], 'cancelled')
        self.assertEqual(len(self.user.sent), 2)

    async def test_cancel_stale_job_button_does_not_cancel_replacement(self):
        old_id = self.job['id']
        token = await self.request_repeat()
        await self.callback(f'repeat_confirm:{old_id}:{token}')
        self.job = self.store.by_user(123)
        await self.callback(f'cancel_job:{old_id}')
        self.assertEqual(self.state()['stage'], 'sticker_pending')
        self.assertEqual(self.user.sent, [])

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
        await self.stop_heartbeats()
        self.store.close()
        self.store = Store(self.path)
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.folders.move = AsyncMock()
        self.app.folders.normalize = AsyncMock()
        await self.app.recover_replies()
        self.app.ready = True
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
        self.assertFalse(self.state()['second_send_notified'])
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

    async def test_settings_has_old_and_new_modes_and_four_media(self):
        await self.command('/settings')
        call = self.bot.send_message.call_args
        self.assertEqual(len(call.kwargs['buttons']), 2)
        self.assertEqual(call.kwargs['buttons'][0][0].type.data, b'voice_mode:two_voice')
        self.assertEqual(call.kwargs['buttons'][1][0].type.data, b'voice_mode:three_voice')
        await self.callback('voice_mode:three_voice')
        self.assertEqual(self.store.get('voice_send_mode'), 'three_voice')
        buttons = [b.type.data for row in self.bot.send_message.call_args.kwargs['buttons'] for b in row]
        self.assertIn(b'sticker', buttons)
        self.assertIn(b'opening_kind:sticker', buttons)
        self.assertIn(b'opening_kind:text', buttons)
        self.assertIn(b'opening_text', buttons)
        self.assertIn(b'circle', buttons)
        self.assertIn(b'voice', buttons)
        self.assertIn(b'followup', buttons)
        self.assertIn(b'second', buttons)
        self.assertNotIn(b'saved', buttons)
        self.assertNotIn(b'saved_second', buttons)
        self.assertNotIn(b'texts', buttons)
        await self.callback('texts')
        self.assertIsNone(self.store.get('mode'))

    async def make_text_target(self, mode='two_voice'):
        self.user.saved[13] = media_message('voice', 13, duration=5.25)
        for key, value in (('voice', 11), ('second', 12), ('followup', 13)):
            self.store.set(key, value)
        await self.callback(f'voice_mode:{mode}')
        await self.callback('opening_text')
        await self.command('Привет 👋\nКак дела?')
        self.user.history[456] = [incoming(100, sender=456)]
        await self.app.add_target('bob')
        self.job = self.store.by_user(456)

    async def request_test_circle(self, stage='second_done', duration=5.25):
        self.store.update(self.job['id'], stage=stage)
        self.user.saved[14] = media_message('circle', 14, duration=duration)
        self.store.set('circle', 14)
        await self.callback(f'send_circle:{self.job["id"]}:{self.state()["circle_token"]}')
        self.assertEqual(self.state()['circle_state'], 'pending')

    async def test_circle_reads_all_previous_messages_then_records_round_video_for_full_duration(self):
        await self.receive(1100, live=False)
        await self.receive(1101, live=False)
        await self.request_test_circle()
        await self.tick()
        self.assertEqual(self.user.reads[-1][1].max_id, 1101)
        await self.tick(2.999)
        self.assertEqual(self.user.typing, [])
        await self.tick(.001)
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordRoundAction)
        start = self.clock[0]
        await self.tick(3)
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordRoundAction)
        await self.tick(2.249)
        self.assertEqual(self.user.sent, [])
        await self.tick(.001)
        self.assertEqual(self.user.sent[0][0] - start, 5.25)
        self.assertEqual(self.user.sent[0][1].id, [14])
        self.assertTrue(self.user.sent[0][1].drop_author)
        self.assertEqual(self.state()['circle_state'], 'done')
        self.assertEqual(self.state()['stage'], 'second_done')
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(self.app.recording_tasks, {})
        operations = [r for _, r in self.user.operations]
        sent = next(i for i, r in enumerate(operations) if isinstance(r, functions.messages.ForwardMessagesRequest))
        cancelled = next(i for i, r in enumerate(operations) if isinstance(r, functions.messages.SetTypingRequest)
                         and isinstance(r.action, types.SendMessageCancelAction))
        self.assertLess(sent, cancelled)

    async def test_circle_button_is_owner_only_and_duplicate_or_old_click_never_sends_twice(self):
        self.user.saved[14] = media_message('circle', 14)
        self.store.set('circle', 14)
        self.store.update(self.job['id'], stage='second_done')
        data = f'send_circle:{self.job["id"]}:0'
        await self.callback(data, sender=999)
        self.assertEqual(self.state()['circle_state'], '')
        await asyncio.gather(self.callback(data), self.callback(data))
        random_id = self.state()['circle_random']
        await self.tick()
        await self.tick(3)
        await self.tick(7)
        await self.callback(data)
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 1)
        self.assertEqual(self.state()['circle_random'], random_id)

    async def test_circle_can_be_requested_from_menu_and_status_for_its_username(self):
        self.user.saved[14] = media_message('circle', 14)
        self.store.set('circle', 14)
        self.store.update(self.job['id'], stage='second_done')
        await self.command('📹 Отправить подтверждающий кружок')
        self.assertEqual(self.store.get('mode'), 'circle_target')
        await self.command('@alice')
        self.assertEqual(self.state()['circle_state'], 'pending')
        self.assertIsNone(self.store.get('mode'))
        await self.command('/status')
        buttons = [b.type.data for row in self.bot.send_message.call_args.kwargs['buttons'] for b in row]
        self.assertIn(f'send_circle:{self.job["id"]}:1'.encode(), buttons)

    async def test_circle_capture_preserves_round_video_metadata_in_saved_messages(self):
        await self.callback('circle')
        self.assertEqual(self.store.get('mode'), 'circle')
        message = media_message('circle', 500, duration=5.25)
        original = media_message('circle', 900, duration=5.25)
        original.out = True
        original.date = message.date
        saved = media_message('circle', 600, duration=5.25)
        self.bot.get_me = AsyncMock(return_value=SimpleNamespace(username='controller_bot'))
        self.user.get_messages = AsyncMock(return_value=[original])
        self.user.send_file = AsyncMock(return_value=saved)
        await self.app.capture_media(message, 'circle')
        self.assertEqual(self.store.get('circle'), 600)
        self.user.send_file.assert_awaited_once_with('me', original.media)
        self.assertEqual(circle_duration(saved), 5.25)

    async def test_circle_rejects_ordinary_video_audio_deleted_media_and_missing_configuration(self):
        await self.callback(f'send_circle:{self.job["id"]}:0')
        self.assertEqual(self.state()['circle_state'], '')
        regular = media_message('circle')
        regular.document.attributes[0].round_message = False
        for message in (regular, media_message('voice'), None):
            with self.assertRaises(ValueError):
                circle_duration(message)
        self.store.set('circle', 999)
        await self.callback(f'send_circle:{self.job["id"]}:0')
        self.assertEqual(self.state()['circle_state'], '')
        with self.assertRaisesRegex(ValueError, 'видеокружок'):
            await self.app.capture_media(regular, 'circle')

    async def test_incoming_during_round_recording_is_read_keeps_video_indicator_and_read_pause(self):
        await self.request_test_circle(duration=5)
        await self.tick()
        await self.tick(3)
        await self.tick(4)
        await self.receive(1100)
        await self.tick(1)
        self.assertEqual(self.state()['recording_kind'], 'circle')
        self.assertEqual(self.user.reads[-1][1].max_id, 1100)
        await self.tick(1.999)
        self.assertEqual(self.user.sent, [])
        await self.tick(.001)
        self.assertEqual(len(self.user.sent), 1)
        self.assertFalse(any(isinstance(r.action, types.SendMessageRecordAudioAction) for _, r in self.user.typing))

    async def test_circle_indicator_keeps_refreshing_during_slow_delivery(self):
        with patch('assistant.app.RECORD_INTERVAL', .01):
            await self.request_test_circle(duration=1)
            await self.tick()
            await self.tick(3)
            renewed = asyncio.Event()
            sending = asyncio.Event()
            refresh = self.app.refresh_recording
            copy = self.app.copy_saved

            async def observe_refresh(job_id, **kwargs):
                await refresh(job_id, **kwargs)
                if sending.is_set():
                    renewed.set()

            async def slow_delivery(job, kind):
                self.assertEqual(kind, 'circle')
                sending.set()
                self.clock[0] += 4
                await asyncio.wait_for(renewed.wait(), .5)
                self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordRoundAction)
                return await copy(job, kind)

            with patch.object(self.app, 'refresh_recording', side_effect=observe_refresh), \
                    patch.object(self.app, 'copy_saved', side_effect=slow_delivery):
                await self.tick(1)
            self.assertTrue(renewed.is_set())
            self.assertEqual(self.state()['circle_state'], 'done')

    async def test_circle_read_failure_blocks_recording_until_successful_read_and_pause(self):
        await self.request_test_circle()
        self.user.failures[functions.messages.ReadHistoryRequest] = ConnectionError('offline')
        await self.tick()
        await self.tick(29)
        self.assertEqual(self.user.typing, [])
        await self.tick(1)
        await self.tick(2.999)
        self.assertEqual(self.user.typing, [])
        await self.tick(.001)
        self.assertEqual(self.state()['recording_kind'], 'circle')

    async def test_circle_cancel_stops_recording_and_invalidates_queued_request(self):
        await self.request_test_circle()
        await self.tick()
        await self.tick(3)
        await self.callback(f'cancel_job:{self.job["id"]}')
        self.assertEqual(self.state()['circle_state'], 'cancelled')
        self.assertEqual(self.app.recording_tasks, {})
        await self.callback(f'send_circle:{self.job["id"]}:0')
        await self.tick(100)
        self.assertEqual(self.user.sent, [])

    async def test_circle_interruption_preserves_main_voice_approval_and_resumes_it_after_circle(self):
        await self.first()
        await self.receive(1100)
        await self.request_test_circle(stage='second_confirmation', duration=1)
        await self.tick()
        await self.tick(3)
        deadline = self.state()['recording_until']
        await self.approve()
        self.assertEqual(self.state()['recording_kind'], 'circle')
        self.assertEqual(self.state()['recording_until'], deadline)
        self.assertTrue(self.state()['second_send_approved'])
        await self.tick(1)
        self.assertEqual(self.state()['stage'], 'second_pending')
        await self.finish_second()
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11], [14], [12]])

    async def test_circle_can_interrupt_first_voice_without_simultaneous_audio_and_round_indicators(self):
        await self.sticker()
        await self.tick(1)
        await self.request_test_circle(stage='voice_pending', duration=1)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick()
        await self.tick(3)
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordRoundAction)
        await self.tick(1)
        await self.tick()
        await self.tick(1)
        self.assertEqual(self.state()['recording_kind'], 'voice')
        await self.tick(7)
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [14], [11]])

    async def test_circle_restart_rereads_chat_restarts_full_round_recording_and_keeps_snapshot(self):
        await self.request_test_circle()
        await self.tick()
        await self.tick(3)
        await self.tick(2)
        random_id = self.state()['circle_random']
        self.store.set('circle', 999)
        await self.stop_heartbeats()
        self.store.close()
        self.store = Store(self.path)
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.folders.move = AsyncMock()
        self.app.folders.normalize = AsyncMock()

        async def end_iteration(awaitable, **kwargs):
            awaitable.close()
            raise asyncio.CancelledError

        with patch('assistant.app.asyncio.wait_for', side_effect=end_iteration):
            with self.assertRaises(asyncio.CancelledError):
                await self.app.worker()
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(self.state()['circle_random'], random_id)
        self.assertEqual(self.state()['circle_source'], 14)
        await self.tick(3)
        await self.tick(5.25)
        self.assertEqual([r.id for _, r in self.user.sent], [[14]])

    async def test_circle_uncertain_delivery_retries_same_random_id_after_read_pause_and_full_recording(self):
        await self.request_test_circle(duration=1)
        await self.tick()
        await self.tick(3)
        copy = self.app.copy_saved

        async def delivered_then_disconnected(job, kind):
            await copy(job, kind)
            raise ConnectionError('lost acknowledgement')

        with patch.object(self.app, 'copy_saved', side_effect=delivered_then_disconnected):
            await self.tick(1)
        random_id = self.state()['circle_random']
        self.assertEqual(self.app.recording_tasks, {})
        await self.tick(30)
        await self.tick(3)
        await self.tick(1)
        self.assertEqual(len(self.user.sent), 2)
        self.assertEqual(self.user.sent[0][1].random_id, self.user.sent[1][1].random_id)
        self.assertEqual(self.state()['circle_random'], random_id)
        self.assertEqual(self.state()['circle_state'], 'done')
        self.assertEqual(len([m for m in self.user.history[123] if m.out]), 1)

    async def test_round_video_forbidden_stops_indicator_and_retry_works_for_completed_main_job(self):
        await self.request_test_circle(duration=1)
        await self.tick()
        await self.tick(3)
        self.user.failures[functions.messages.ForwardMessagesRequest] = errors.BadRequestError(
            request=None, message='CHAT_SEND_ROUNDVIDEOS_FORBIDDEN')
        await self.tick(1)
        self.assertTrue(self.state()['error'])
        self.assertEqual(self.app.recording_tasks, {})
        await self.app.job_action('/retry', self.job['id'])
        await self.tick()
        await self.tick(3)
        await self.tick(1)
        self.assertEqual(self.state()['circle_state'], 'done')

    async def test_circle_pause_resume_rereads_and_restarts_full_video_duration(self):
        await self.request_test_circle()
        await self.tick()
        await self.tick(3)
        await self.tick(2)
        await self.command('/pause')
        self.assertFalse(self.state()['recording_kind'])
        self.assertEqual(self.app.recording_tasks, {})
        await self.tick(100)
        self.assertEqual(self.user.sent, [])
        await self.command('/resume')
        await self.tick()
        await self.tick(3)
        start = self.clock[0]
        await self.tick(5.249)
        self.assertEqual(self.user.sent, [])
        await self.tick(.001)
        self.assertEqual(self.user.sent[0][0] - start, 5.25)

    async def test_periodic_reply_recovery_does_not_cancel_round_recording_or_approve_final_voice(self):
        await self.first()
        await self.receive(1100)
        await self.request_test_circle(stage='second_confirmation', duration=35)
        await self.tick()
        await self.tick(3)
        deadline = self.state()['recording_until']
        await self.app.recover_replies()
        self.assertEqual(self.state()['recording_kind'], 'circle')
        self.assertEqual(self.state()['recording_until'], deadline)
        self.assertFalse(self.state()['second_send_approved'])

    async def test_circle_before_opening_does_not_mistake_our_circle_for_prior_correspondence(self):
        await self.request_test_circle(stage='sticker_pending', duration=1)
        await self.tick()
        await self.tick(3)
        await self.tick(1)
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.state()['stage'], 'voice_pending')
        self.assertEqual([r.id for _, r in self.user.sent], [[14], [10]])

    async def test_dismissing_initial_confirmation_stops_pending_circle_before_deleting_job(self):
        await self.request_test_circle(stage='awaiting_confirmation', duration=5)
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.state()['recording_kind'], 'circle')
        await self.callback(f'dismiss:{self.job["id"]}')
        self.assertIsNone(self.state())
        self.assertEqual(self.app.recording_tasks, {})
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageCancelAction)
        self.assertEqual(self.user.sent, [])

    async def test_old_mode_opens_with_text_then_first_voice_after_one_second_and_confirmed_final(self):
        await self.make_text_target()
        self.assertEqual(self.job['opening_kind'], 'text')
        self.assertEqual(self.job['sticker_source'], 0)
        await self.tick()
        await self.tick(2.999)
        self.assertEqual(self.user.texts, [])
        await self.tick(.001)
        self.assertEqual(len(self.user.texts), 1)
        self.assertEqual(self.user.texts[0][1].message, 'Привет 👋\nКак дела?')
        self.assertEqual(self.user.sent, [])
        await self.tick(.999)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(.001)
        self.assertEqual(self.state()['recording_kind'], 'voice')
        await self.tick(7)
        self.assertEqual(self.state()['stage'], 'reply_waiting')
        await self.receive(1100, sender=456)
        await self.tick(100)
        self.assertEqual([r.id for _, r in self.user.sent], [[11]])
        await self.approve()
        await self.finish_second()
        self.assertEqual([r.id for _, r in self.user.sent], [[11], [12]])
        self.assertEqual(len(self.user.texts), 1)

    async def test_new_mode_opens_with_text_then_pair_and_separately_confirmed_final(self):
        await self.make_text_target('three_voice')
        await self.tick()
        await self.tick(3)
        await self.tick(1)
        await self.tick(7)
        self.assertEqual(self.state()['stage'], 'followup_pending')
        await self.tick(1)
        self.assertEqual(self.state()['recording_kind'], 'followup')
        await self.tick(5.25)
        self.assertEqual(self.state()['stage'], 'reply_waiting')
        await self.receive(1100, sender=456)
        await self.tick(100)
        self.assertFalse(self.state()['second_send_approved'])
        self.assertEqual([r.id for _, r in self.user.sent], [[11], [13]])
        await self.approve()
        await self.finish_second()
        self.assertEqual([r.id for _, r in self.user.sent], [[11], [13], [12]])
        self.assertEqual(len(self.user.texts), 1)

    async def test_opening_choice_and_text_are_snapshotted_and_sticker_can_be_selected_again(self):
        await self.make_text_target()
        await self.callback('opening_text')
        await self.command('Другой текст')
        await self.callback('opening_kind:sticker')
        self.assertEqual(self.store.get('opening_kind'), 'sticker')
        self.assertEqual(self.store.get('opening_text'), 'Другой текст')
        self.assertEqual(self.state()['opening_kind'], 'text')
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.user.texts[0][1].message, 'Привет 👋\nКак дела?')
        self.assertEqual(self.user.sent, [])
        await self.callback('opening_kind:text')
        self.assertEqual(self.store.get('opening_kind'), 'text')
        self.assertEqual(self.store.get('opening_text'), 'Другой текст')

    async def test_invalid_opening_text_and_cancel_leave_saved_text_unchanged(self):
        await self.callback('opening_text')
        await self.command('Исходный текст')
        await self.callback('opening_text')
        for text in (' ', 'x' * 4097, '🙂' * 2049):
            await self.command(text)
            self.assertEqual(self.store.get('opening_text'), 'Исходный текст')
            self.assertEqual(self.store.get('mode'), 'opening_text')
        await self.app.on_message(SimpleNamespace(is_private=True, sender_id=42, raw_text='Подпись',
                                                 message=media_message('sticker')))
        self.assertEqual(self.store.get('opening_text'), 'Исходный текст')
        await self.command('/cancel')
        self.assertIsNone(self.store.get('mode'))
        self.assertEqual(self.store.get('opening_text'), 'Исходный текст')

    async def test_opening_text_setup_is_owner_only_and_missing_text_prompts_for_input(self):
        await self.callback('opening_kind:text', sender=999)
        self.assertIsNone(self.store.get('mode'))
        await self.callback('opening_kind:text')
        self.assertEqual(self.store.get('mode'), 'opening_text')
        await self.command('Чужой текст', sender=999)
        self.assertIsNone(self.store.get('opening_text'))
        await self.command('Наш текст')
        self.assertEqual(self.store.get('opening_kind'), 'text')

    async def test_text_retry_uses_same_random_id_after_uncertain_delivery_and_restart(self):
        await self.make_text_target()
        await self.tick()
        copy = self.app.copy_saved

        async def delivered_then_disconnected(job, kind):
            await copy(job, kind)
            raise ConnectionError('lost acknowledgement')

        with patch.object(self.app, 'copy_saved', side_effect=delivered_then_disconnected):
            await self.tick(3)
        random_id = self.user.texts[0][1].random_id
        await self.stop_heartbeats()
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        await self.tick(30)
        await self.tick(3)
        self.assertEqual(len(self.user.texts), 2)
        self.assertEqual(self.user.texts[-1][1].random_id, random_id)
        self.assertEqual(self.state()['stage'], 'voice_pending')
        self.assertEqual(len([m for m in self.user.history[456] if m.out]), 1)

    async def test_text_opening_respects_voice_restriction_and_cancel(self):
        await self.make_text_target()
        self.user.forbidden.add(456)
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertEqual(self.user.texts, [])
        await self.callback(f'cancel_job:{self.job["id"]}')
        await self.tick(100)
        self.assertEqual(self.user.texts, [])

    async def test_text_repeat_keeps_requested_text_even_after_setting_changes(self):
        await self.make_text_target('three_voice')
        await self.app.add_target('bob')
        token = self.store.repeat_request(self.job['id'])['token']
        await self.callback('opening_text')
        await self.command('Изменённый текст')
        await self.callback('opening_kind:sticker')
        await self.callback(f'repeat_confirm:{self.job["id"]}:{token}')
        self.job = self.store.by_user(456)
        self.assertEqual(self.job['opening_kind'], 'text')
        self.assertEqual(self.job['opening_text'], 'Привет 👋\nКак дела?')
        self.assertEqual(self.job['dialogue_mode'], 'three_voice')
        await self.tick()
        await self.tick(3)
        self.assertEqual(self.user.texts[0][1].message, 'Привет 👋\nКак дела?')

    async def pair_first(self):
        self.user.saved[13] = media_message('voice', 13, duration=5.25)
        self.store.update(self.job['id'], dialogue_mode='three_voice', followup_source=13,
                          followup_duration=5.25, followup_random=123456)
        await self.sticker()
        await self.tick()
        await self.tick(1)
        await self.tick(4)
        await self.tick(3)
        self.assertEqual(self.state()['stage'], 'followup_pending')

    async def pair(self):
        await self.pair_first()
        await self.tick(1)
        await self.tick(4)
        await self.tick(1.25)
        self.assertEqual(self.state()['stage'], 'reply_waiting')

    async def test_new_mode_sends_pair_with_read_pause_full_recording_then_requires_third_approval(self):
        await self.pair_first()
        first_sent_at = self.user.sent[-1][0]
        self.assertEqual(self.state()['read_gate_at'], first_sent_at)
        self.assertGreaterEqual(self.user.reads[-1][1].max_id, self.state()['voice_message'])
        self.assertFalse(self.state()['second_send_approved'])
        await self.tick(.999)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(.001)
        self.assertEqual(self.state()['recording_kind'], 'followup')
        self.assertEqual(self.state()['recording_until'], first_sent_at + 1 + 5.25)
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordAudioAction)
        await self.tick(4)
        self.assertIsInstance(self.user.typing[-1][1].action, types.SendMessageRecordAudioAction)
        await self.tick(1.249)
        self.assertEqual(len(self.user.sent), 2)
        await self.tick(.001)
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11], [13]])
        self.assertEqual(self.state()['second_baseline'], self.state()['followup_message'])
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 3)
        await self.receive(1100)
        self.assertEqual(self.state()['stage'], 'second_confirmation')
        self.assertFalse(any('ответил(а) после начальных ГС' in call.args[1] for call in self.bot.send_message.call_args_list))
        await self.tick(100)
        self.assertEqual(len(self.user.sent), 3)
        await self.approve()
        await self.finish_second()
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11], [13], [12]])
        self.assertEqual(self.state()['stage'], 'second_done')

    async def test_pair_reads_all_missed_incoming_after_first_before_recording_second(self):
        copy = self.app.copy_saved

        async def messages_during_send(job, kind):
            result = await copy(job, kind)
            if kind == 'voice':
                await self.receive(1100, live=False)
                await self.receive(1101, live=False)
                self.user.next_id = 1101
            return result

        with patch.object(self.app, 'copy_saved', side_effect=messages_during_send):
            await self.pair_first()
        self.assertEqual(self.state()['read_done'], 1101)
        self.assertEqual(self.user.reads[-1][1].max_id, 1101)
        self.assertFalse(self.state()['recording_kind'])
        self.assertFalse(self.state()['second_reply'])
        await self.tick(1)
        self.assertEqual(self.state()['recording_kind'], 'followup')
        await self.tick(5.25)
        await self.app.recover_replies()
        self.assertEqual(self.state()['stage'], 'reply_waiting')
        await self.receive(1200)
        self.assertEqual(self.state()['stage'], 'second_confirmation')

    async def test_new_incoming_between_pair_delays_second_recording_one_second_from_read(self):
        await self.pair_first()
        await self.tick(.5)
        await self.receive(1100)
        self.user.next_id = 1100
        read_at = self.clock[0]
        await self.tick(.999)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(.001)
        self.assertEqual(self.user.typing[-1][0], read_at + 1)
        self.assertEqual(self.state()['recording_kind'], 'followup')
        self.assertFalse(self.state()['second_send_approved'])

    async def test_pair_read_failure_blocks_second_until_read_succeeds_and_one_second_passes(self):
        read = self.app.before_send

        async def fail_pair_read(job, latest_id=None):
            if job['stage'] == 'followup_pending':
                self.user.failures[functions.messages.ReadHistoryRequest] = ConnectionError('offline')
            return await read(job, latest_id)

        with patch.object(self.app, 'before_send', side_effect=fail_pair_read):
            await self.pair_first()
        self.assertTrue(self.state()['read_error'])
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(29)
        self.assertEqual(len(self.user.sent), 2)
        await self.tick(1)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(1)
        self.assertEqual(self.state()['recording_kind'], 'followup')

    async def test_cancellation_between_pair_prevents_second_and_third(self):
        await self.pair_first()
        await self.callback(f"cancel_job:{self.job['id']}")
        await self.tick(100)
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11]])
        self.assertEqual(self.state()['stage'], 'cancelled')

    async def test_pair_restriction_before_second_stops_further_voices(self):
        await self.pair_first()
        self.user.forbidden.add(123)
        await self.tick(1)
        self.assertEqual(self.state()['stage'], 'voice_forbidden')
        self.assertEqual(len(self.user.sent), 2)

    async def test_pair_mode_and_random_id_survive_restart_during_second_recording(self):
        await self.pair_first()
        await self.tick(1)
        await self.tick(2)
        random_id = self.state()['followup_random']
        await self.stop_heartbeats()
        self.store.close()
        self.store = Store(self.path)
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.folders.move = AsyncMock()
        self.app.folders.normalize = AsyncMock()

        async def end_iteration(awaitable, **kwargs):
            awaitable.close()
            raise asyncio.CancelledError

        with patch('assistant.app.asyncio.wait_for', side_effect=end_iteration):
            with self.assertRaises(asyncio.CancelledError):
                await self.app.worker()
        self.assertEqual(self.state()['dialogue_mode'], 'three_voice')
        self.assertEqual(self.state()['followup_random'], random_id)
        self.assertFalse(self.state()['recording_kind'])
        await self.tick(1)
        self.assertEqual(self.state()['recording_kind'], 'followup')
        await self.tick(5.25)
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11], [13]])
        self.assertEqual(self.state()['stage'], 'reply_waiting')

    async def test_new_job_snapshots_pair_mode_and_all_four_media(self):
        self.user.saved[13] = media_message('voice', 13, duration=5.25)
        for key, value in (('sticker', 10), ('voice', 11), ('second', 12), ('followup', 13)):
            self.store.set(key, value)
        await self.callback('voice_mode:three_voice')
        await self.app.add_target('bob')
        job = self.store.by_user(456)
        self.assertEqual(job['dialogue_mode'], 'three_voice')
        self.assertEqual(job['followup_source'], 13)
        self.assertEqual(job['followup_duration'], 5.25)
        self.assertEqual(job['second_source'], 12)
        self.assertNotIn(job['followup_random'], (0, job['voice_random'], job['second_random']))
        await self.callback('voice_mode:two_voice')
        self.store.set('followup', 99)
        self.assertEqual(self.store.job(job['id'])['dialogue_mode'], 'three_voice')
        self.assertEqual(self.store.job(job['id'])['followup_source'], 13)
        self.assertEqual(self.state()['dialogue_mode'], 'two_voice')

    async def test_new_mode_requires_new_second_but_old_mode_keeps_existing_final_voice(self):
        for key, value in (('sticker', 10), ('voice', 11), ('second', 12)):
            self.store.set(key, value)
        await self.callback('voice_mode:three_voice')
        with self.assertRaises(ValueError):
            await self.app.add_target('bob')
        await self.callback('voice_mode:two_voice')
        await self.app.add_target('bob')
        job = self.store.by_user(456)
        self.assertEqual(job['dialogue_mode'], 'two_voice')
        self.assertEqual(job['second_source'], 12)
        self.assertIsNone(job['followup_source'])

    async def test_repeat_snapshots_new_mode_but_keeps_third_unapproved(self):
        self.user.saved[13] = media_message('voice', 13, duration=5.25)
        self.store.set('followup', 13)
        await self.callback('voice_mode:three_voice')
        token = await self.request_repeat()
        await self.callback('voice_mode:two_voice')
        old_id = self.job['id']
        await self.callback(f'repeat_confirm:{old_id}:{token}')
        self.job = self.store.by_user(123)
        self.assertEqual(self.job['dialogue_mode'], 'three_voice')
        self.assertEqual(self.job['followup_source'], 13)
        self.assertFalse(self.job['second_send_approved'])
        await self.pair()
        self.assertEqual([r.id for _, r in self.user.sent], [[10], [11], [13]])

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
        await self.tick(1)
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
        await self.tick(1)
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
        await self.stop_heartbeats()
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
        self.assertIn(150, [request.max_id for _, request in self.user.reads])
        self.assertEqual(self.user.reads[-1][1].max_id, self.state()['sticker_message'])

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
        await self.tick(1)
        await self.tick(2)
        self.assertTrue(self.state()['recording_kind'])
        await self.stop_heartbeats()
        self.store.close()
        self.store = Store(self.path)
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.folders.move = AsyncMock()
        self.app.folders.normalize = AsyncMock()

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

    async def test_waiting_third_voice_stays_silent_even_after_notice_retry_deadline(self):
        await self.first()
        self.bot.send_message.reset_mock()
        self.bot.send_message.side_effect = ConnectionError('offline')
        await self.receive(1100)
        await self.tick(31)
        self.assertFalse(self.state()['second_send_notified'])
        self.assertEqual(self.bot.send_message.call_count, 0)
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
    async def test_rejected_archive_keeps_first_stage_folder(self):
        folder = self.folder(2, 'В процессе 1', [types.InputPeerUser(123, 55)])
        requests = []
        async def client(request):
            requests.append(request)
            if isinstance(request, functions.messages.GetDialogFiltersRequest):
                return SimpleNamespace(filters=[folder])
            return False
        with self.assertRaises(ValueError):
            await Folders(client).normalize()
        self.assertEqual(len(requests), 2)
        self.assertIsInstance(requests[-1], functions.folders.EditPeerFoldersRequest)

    def folder(self, folder_id, name, peers=None, pinned=None):
        return types.DialogFilter(id=folder_id, title=types.TextWithEntities(name, []),
                                  pinned_peers=pinned or [], include_peers=peers or [], exclude_peers=[])

    async def execute(self, folders, *, peer=None, fail_delete=False):
        self.requests = []
        async def call(request):
            self.requests.append(request)
            bytes(request)
            if isinstance(request, functions.messages.GetDialogFiltersRequest):
                return SimpleNamespace(filters=folders)
            if fail_delete and isinstance(request, functions.messages.UpdateDialogFilterRequest) and request.filter is None:
                raise ConnectionError('offline')
            return True
        manager = Folders(call)
        if peer:
            await manager.move(peer, 2)
        else:
            await manager.normalize()

    async def test_rename_existing_reply_folder_keeps_members_and_pins(self):
        peer = types.InputPeerUser(123, 55)
        old = self.folder(3, 'В процессе 2', [peer], [peer])
        await self.execute([old])
        update = self.requests[1]
        self.assertEqual(update.id, 3)
        self.assertEqual(title_text(update.filter), 'В процессе')
        self.assertEqual(update.filter.pinned_peers, [peer])
        self.assertEqual(update.filter.include_peers, [peer])

    async def test_old_first_folder_members_are_archived_before_folder_is_removed(self):
        peer = types.InputPeerUser(123, 55)
        await self.execute([self.folder(2, 'В процессе 1', [peer], [peer])])
        self.assertIsInstance(self.requests[1], functions.folders.EditPeerFoldersRequest)
        self.assertEqual(len(self.requests[1].folder_peers), 1)
        self.assertEqual(self.requests[1].folder_peers[0].folder_id, 1)
        self.assertEqual(self.requests[2].id, 2)
        self.assertIsNone(self.requests[2].filter)

    async def test_existing_new_and_legacy_reply_folders_merge_before_old_is_deleted(self):
        first = types.InputPeerUser(123, 55)
        second = types.InputPeerUser(456, 66)
        destination = self.folder(4, 'В процессе', [first], [first])
        old = self.folder(3, 'В процессе 2', [second], [second])
        await self.execute([destination, old])
        self.assertEqual({p.user_id for p in self.requests[1].filter.include_peers}, {123, 456})
        self.assertEqual({p.user_id for p in self.requests[1].filter.pinned_peers}, {123, 456})
        self.assertEqual(self.requests[2].id, 3)
        self.assertIsNone(self.requests[2].filter)

    async def test_move_only_creates_single_reply_folder_and_preserves_unrelated_filters(self):
        peer = types.InputPeerUser(123, 55)
        unrelated = self.folder(2, 'Личное', [types.InputPeerUser(456, 66)])
        await self.execute([unrelated], peer=peer)
        self.assertEqual(title_text(self.requests[-1].filter), 'В процессе')
        self.assertEqual(self.requests[-1].id, 3)
        self.assertEqual(unrelated.include_peers[0].user_id, 456)

    async def test_first_stage_does_not_create_a_folder(self):
        client = AsyncMock()
        await Folders(client).move(types.InputPeerUser(123, 55), 1)
        client.assert_not_awaited()

    async def test_duplicate_folder_names_fail_without_writing(self):
        with self.assertRaises(ValueError):
            await self.execute([self.folder(2, 'В процессе'), self.folder(3, 'В процессе')])
        self.assertEqual(len(self.requests), 1)

    async def test_failed_deletion_does_not_delete_the_merged_destination(self):
        first = types.InputPeerUser(123, 55)
        second = types.InputPeerUser(456, 66)
        with self.assertRaises(ConnectionError):
            await self.execute([self.folder(2, 'В процессе', [first]),
                                self.folder(3, 'В процессе 2', [second])], fail_delete=True)
        self.assertEqual(self.requests[1].id, 2)
        self.assertIsNotNone(self.requests[1].filter)
        self.assertEqual(self.requests[2].id, 3)


class InputTests(unittest.TestCase):
    def test_username_forms(self):
        for value in ["@alice", "alice", "https://t.me/alice", "https://t.me/alice/"]:
            self.assertEqual(parse_username(value), "alice")
        for value in ["@alice @bob", "https://evil.example/alice", "/start", "t.me/+invite"]:
            self.assertIsNone(parse_username(value))

