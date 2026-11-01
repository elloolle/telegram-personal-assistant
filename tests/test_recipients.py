import asyncio
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from telethon import errors, types

from assistant.app import Assistant
from assistant.recipients import load_directory, recipient_label, recipient_link
from assistant.store import Store
from test_assistant import FakeUser, incoming, media_message


class DirectoryUser(FakeUser):
    def __init__(self, clock):
        super().__init__(clock)
        self.people = {456: types.User(id=456, access_hash=99, first_name='Анна', last_name='Иванова'),
                       457: types.User(id=457, access_hash=100, first_name='Анна', last_name='Иванова'),
                       458: types.User(id=458, access_hash=101, first_name='Борис', username='boris')}
        self.dialog_entities = list(self.people.values())
        self.directory_requests = []
        self.directory_error = None
        self.resolved = []

    async def iter_dialogs(self, **kwargs):
        self.directory_requests.append(kwargs)
        for entity in self.dialog_entities:
            yield SimpleNamespace(entity=entity)
        if self.directory_error:
            raise self.directory_error

    async def get_entity(self, target):
        self.resolved.append(target)
        if isinstance(target, types.InputPeerUser):
            return self.people[target.user_id]
        return await super().get_entity(target)


class RecipientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'queue.sqlite3'
        self.clock = [1000.0]
        self.time_patch = patch('assistant.app.time.time', side_effect=lambda: self.clock[0])
        self.time_patch.start()
        self.store = Store(self.path)
        self.user = DirectoryUser(self.clock)
        self.bot = SimpleNamespace(add_event_handler=lambda *args: None, send_message=AsyncMock())
        self.app = Assistant(self.user, self.bot, self.store, 42)
        self.app.ready = True
        self.app.folders.move = AsyncMock()
        self.app.folders.normalize = AsyncMock()
        for key, value in [('sticker', 10), ('voice', 11), ('second', 12), ('circle', 14)]:
            self.store.set(key, value)
        self.user.saved[14] = media_message('circle', 14, duration=4)

    async def asyncTearDown(self):
        tasks = list(self.app.recording_tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.store.close()
        self.time_patch.stop()
        self.temp.cleanup()

    async def command(self, text, sender=42):
        await self.app.on_message(SimpleNamespace(is_private=True, sender_id=sender, raw_text=text))

    async def callback(self, text, sender=42):
        await self.app.on_callback(SimpleNamespace(sender_id=sender, data=text.encode(), answer=AsyncMock()))

    def token(self):
        return self.store.get('chat_picker')['token']

    def buttons(self):
        return [button for row in self.bot.send_message.call_args.kwargs['buttons'] for button in row]

    async def choose(self, user_id=456):
        await self.command('/chats')
        state = self.store.get('chat_picker')
        index = next(i for i, item in enumerate(state['contacts']) if item['user_id'] == user_id)
        await self.callback(f'chat_pick:{state["token"]}:{index}')
        return f'chat_action:{state["token"]}:{index}:'

    async def test_choose_without_username_then_third_voice_obeys_read_record_send(self):
        action = await self.choose()
        self.assertEqual(self.store.jobs(), [])
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.user.reads, [])
        self.assertIn('ID 456', self.bot.send_message.call_args.args[1])
        await self.callback(action + 'third')
        job = self.store.by_user(456)
        self.assertEqual(job['username'], '')
        self.assertEqual(job['display_name'], 'Анна Иванова')
        self.assertEqual(recipient_label(job), 'Анна Иванова')
        self.assertIsNone(recipient_link(job))
        self.assertTrue(job['manual_third_only'])
        self.assertEqual(self.user.resolved[-1].user_id, 456)
        self.user.history[456] = [incoming(120, sender=456)]
        await self.app.process_job(job['id'])
        self.assertEqual(self.user.reads[-1][1].peer.user_id, 456)
        self.clock[0] += 2.999
        await self.app.process_job(job['id'])
        self.assertEqual(self.user.sent, [])
        self.clock[0] += .001
        await self.app.process_job(job['id'])
        self.assertEqual(self.store.job(job['id'])['recording_kind'], 'second')
        self.clock[0] += 11
        await self.app.process_job(job['id'])
        self.assertEqual(len(self.user.sent), 1)
        self.assertEqual(self.user.sent[0][1].to_peer.user_id, 456)
        self.assertEqual(self.user.sent[0][1].id, [12])
        self.assertEqual(self.store.job(job['id'])['stage'], 'second_done')

    async def test_search_same_names_and_pagination_do_not_send_or_read(self):
        self.user.dialog_entities += [types.User(id=i, access_hash=i, first_name=f'Имя {i}') for i in range(500, 518)]
        await self.command('👤 Выбрать чат')
        token = self.token()
        await self.callback(f'chat_page:{token}:2')
        self.assertIn('3/3', self.bot.send_message.call_args.args[1])
        await self.callback(f'chat_search:{token}')
        await self.command('аНнА')
        labels = [button.text for button in self.buttons()]
        self.assertTrue(any('ID 456' in label for label in labels))
        self.assertTrue(any('ID 457' in label for label in labels))
        self.assertEqual(len([label for label in labels if 'Анна' in label]), 2)
        self.assertEqual(self.user.sent, [])
        self.assertEqual(self.user.reads, [])
        self.assertEqual(self.user.history_queries, [])
        self.assertEqual(len(self.user.directory_requests), 1)

    async def test_directory_filters_nonhuman_and_duplicates_and_includes_all_folders(self):
        self.user.dialog_entities += [types.User(id=42, access_hash=1, is_self=True),
                                      types.User(id=900, access_hash=1, bot=True),
                                      types.User(id=901, access_hash=1, deleted=True),
                                      types.User(id=902, first_name='Нет доступа'),
                                      types.Channel(id=903, title='Канал', photo=types.ChatPhotoEmpty(), date=None),
                                      self.user.people[456]]
        directory = await load_directory(self.app)
        self.assertEqual([item['user_id'] for item in directory], [456, 457, 458])
        self.assertEqual(self.user.directory_requests, [{'limit': None}])
        await load_directory(self.app)
        self.assertEqual(len(self.user.directory_requests), 1)
        self.clock[0] += 301
        await load_directory(self.app)
        self.assertEqual(len(self.user.directory_requests), 2)

    async def test_card_requires_explicit_action_and_is_single_use(self):
        action = await self.choose()
        await asyncio.gather(self.callback(action + 'third'), self.callback(action + 'third'))
        job = self.store.by_user(456)
        self.assertEqual(len(self.store.jobs()), 1)
        random_id = job['second_random']
        await self.callback(action + 'start')
        self.assertEqual(self.store.by_user(456)['second_random'], random_id)
        self.assertIsNone(self.store.repeat_request(job['id']))

    async def test_cancel_new_list_back_and_expiry_invalidate_old_actions(self):
        action = await self.choose()
        await self.command('/cancel')
        await self.callback(action + 'third')
        self.assertEqual(self.store.jobs(), [])
        action = await self.choose()
        await self.command('/chats')
        await self.callback(action + 'third')
        self.assertEqual(self.store.jobs(), [])
        action = await self.choose()
        await self.callback(f'chat_page:{self.token()}:0')
        await self.callback(action + 'third')
        self.assertEqual(self.store.jobs(), [])
        action = await self.choose()
        self.clock[0] += 601
        await self.callback(action + 'third')
        self.assertEqual(self.store.jobs(), [])

    async def test_nonowner_cannot_load_pick_or_send(self):
        await self.command('/chats', sender=999)
        self.assertEqual(self.user.directory_requests, [])
        action = await self.choose()
        await self.callback(action + 'third', sender=999)
        self.assertEqual(self.store.jobs(), [])
        await self.callback(action + 'third')
        self.assertIsNotNone(self.store.by_user(456))

    async def test_initial_scenario_keeps_previous_correspondence_warning_and_name_on_repeat(self):
        self.user.history[456] = [incoming(100, sender=456, out=True)]
        action = await self.choose()
        await self.callback(action + 'start')
        job = self.store.by_user(456)
        self.assertEqual(job['stage'], 'awaiting_confirmation')
        self.assertIn('Анна Иванова', self.bot.send_message.call_args.args[1])
        self.assertEqual(self.user.sent, [])
        await self.callback(f'confirm:{job["id"]}')
        action = await self.choose()
        await self.callback(action + 'start')
        request = self.store.repeat_request(job['id'])
        self.assertIsNotNone(request)
        await self.callback(f'repeat_confirm:{job["id"]}:{request["token"]}')
        new = self.store.by_user(456)
        self.assertNotEqual(new['id'], job['id'])
        self.assertEqual(new['display_name'], 'Анна Иванова')
        self.assertEqual(new['username'], '')

    async def test_existing_job_can_send_circle_and_cancel_without_username(self):
        self.store.add_manual_third(user_id=456, access_hash=99, username='', display_name='Анна Иванова',
                                    source=12, duration=11, baseline=0)
        action = await self.choose()
        await self.callback(action + 'circle')
        self.assertEqual(self.store.by_user(456)['circle_state'], 'pending')
        action = await self.choose()
        await self.callback(action + 'cancel')
        self.assertEqual(self.store.by_user(456)['stage'], 'cancelled')
        self.assertEqual(self.store.by_user(456)['circle_state'], 'cancelled')

    async def test_flood_wait_on_directory_preserves_previous_cache_and_starts_global_backoff(self):
        directory = await load_directory(self.app)
        self.user.directory_error = errors.FloodWaitError(request=None, capture=15)
        with self.assertRaisesRegex(ValueError, '120'):
            await load_directory(self.app, force=True)
        self.assertEqual(self.store.get('chat_directory'), directory)
        self.assertEqual(self.store.get('send_not_before'), self.clock[0] + 120)
        requests = len(self.user.directory_requests)
        with self.assertRaisesRegex(ValueError, '120'):
            await load_directory(self.app, force=True)
        self.assertEqual(len(self.user.directory_requests), requests)

    async def test_new_action_respects_existing_cooldown_and_can_be_retried_after_it(self):
        action = await self.choose()
        self.app.defer_for_flood(errors.FloodWaitError(request=None, capture=15))
        await self.callback(action + 'third')
        self.assertEqual(self.store.jobs(), [])
        self.assertEqual(self.user.resolved, [])
        self.clock[0] += 120
        await self.callback(action + 'third')
        self.assertIsNotNone(self.store.by_user(456))

    async def test_directory_stops_before_next_page_if_worker_starts_cooldown(self):
        self.user.dialog_entities = [types.User(id=i, access_hash=i, first_name=str(i)) for i in range(500, 701)]

        async def start_cooldown(_):
            self.app.defer_for_flood(errors.FloodWaitError(request=None, capture=15))

        with patch('assistant.recipients.asyncio.sleep', side_effect=start_cooldown):
            with self.assertRaisesRegex(ValueError, '120'):
                await load_directory(self.app)
        self.assertIsNone(self.store.get('chat_directory'))

    async def test_refresh_search_no_matches_and_reset(self):
        await self.command('/chats')
        token = self.token()
        await self.callback(f'chat_search:{token}')
        await self.command('Никого такого')
        self.assertIn('Совпадений нет', self.bot.send_message.call_args.args[1])
        await self.callback(f'chat_reset:{token}')
        self.assertIn('Личные чаты: 3.', self.bot.send_message.call_args.args[1])
        await self.callback(f'chat_refresh:{token}')
        self.assertNotEqual(self.token(), token)
        self.assertEqual(len(self.user.directory_requests), 2)

    async def test_directory_and_recipient_survive_restart_without_fabricating_username(self):
        action = await self.choose()
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        await self.callback(action + 'third')
        job = self.store.by_user(456)
        self.assertEqual(job['display_name'], 'Анна Иванова')
        random_id = job['second_random']
        self.store.close()
        self.store = Store(self.path)
        self.app.store = self.store
        self.assertEqual(self.store.by_user(456)['second_random'], random_id)
        self.assertIsNone(recipient_link(self.store.by_user(456)))

    async def test_private_chat_is_selected_by_id_after_username_changes(self):
        action = await self.choose(458)
        self.user.people[458].username = None
        await self.callback(action + 'third')
        job = self.store.by_user(458)
        self.assertEqual(job['username'], '')
        self.assertEqual(job['display_name'], 'Борис')
        self.assertIsInstance(self.user.resolved[-1], types.InputPeerUser)
        self.assertEqual(self.user.resolved[-1].user_id, 458)

    async def test_queue_and_status_use_name_and_chat_picker_without_fake_profile_url(self):
        job = self.store.add_manual_third(user_id=456, access_hash=99, username='', display_name='Анна Иванова',
                                         source=12, duration=11, baseline=0)
        self.store.update(job['id'], stage='second_confirmation', desired_folder=2,
                          applied_folder=2, second_send_approved=0)
        await self.command('/status')
        self.assertIn('Анна Иванова', self.bot.send_message.call_args.args[1])
        with patch('assistant.app.asyncio.sleep', new=AsyncMock()):
            await self.app.send_queue_requests(5)
        cards = [call for call in self.bot.send_message.call_args_list if ' · задание #' in call.args[1]]
        self.assertEqual(cards[-1].kwargs['buttons'][0][0].type.data, b'chat_open')

    async def test_forbidden_voice_still_stops_without_sending(self):
        action = await self.choose()
        self.user.forbidden.add(456)
        await self.callback(action + 'third')
        job = self.store.by_user(456)
        await self.app.process_job(job['id'])
        self.clock[0] += 3
        await self.app.process_job(job['id'])
        self.assertEqual(self.store.by_user(456)['stage'], 'voice_forbidden')
        self.assertEqual(self.user.sent, [])
