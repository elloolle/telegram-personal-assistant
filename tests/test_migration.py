import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from assistant.store import Store


class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'queue.sqlite3'
        self.store = None

    def tearDown(self):
        if self.store:
            self.store.close()
        self.temp.cleanup()

    def legacy(self):
        stages = ['sticker_pending', 'voice_pending', 'text_pending', 'reply_waiting', 'second_confirmation',
                  'second_pending', 'second_text_pending', 'second_done', 'done', 'replied', 'cancelled']
        with sqlite3.connect(self.path) as db:
            db.executescript('''
                CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL UNIQUE,
                    access_hash INTEGER NOT NULL, username TEXT NOT NULL,
                    sticker_source INTEGER NOT NULL, voice_source INTEGER NOT NULL,
                    sticker_random INTEGER NOT NULL, voice_random INTEGER NOT NULL,
                    baseline INTEGER NOT NULL, started REAL NOT NULL, stage TEXT NOT NULL,
                    due REAL, replied INTEGER NOT NULL DEFAULT 0, cancel_on_reply INTEGER NOT NULL,
                    voice_delay REAL, fallback_payload TEXT, second_payload TEXT,
                    dialogue_mode TEXT NOT NULL DEFAULT 'standard', second_source INTEGER,
                    second_send_approved INTEGER NOT NULL DEFAULT 0
                );
            ''')
            for key, value in [('sticker', 10), ('voice', 11), ('second', 12), ('fallback_texts', ['old']),
                               ('text_draft', ['old draft']), ('mode', 'texts'), ('dialogue_mode', 'standard')]:
                db.execute('INSERT INTO settings VALUES (?, ?)', (key, json.dumps(value)))
            for i, stage in enumerate(stages, 1):
                db.execute('''INSERT INTO jobs (user_id, access_hash, username, sticker_source, voice_source,
                              sticker_random, voice_random, baseline, started, stage, due, cancel_on_reply,
                              voice_delay, fallback_payload, second_payload, second_send_approved)
                              VALUES (?, 55, 'testuser', 10, 11, ?, ?, 100, 1000, ?, 2000, 1, 22, 'old texts', 'old texts', 1)''',
                           (100+i, 1000+i, 2000+i, stage))
        return stages

    def test_old_text_chains_are_stopped_and_their_payloads_removed(self):
        self.legacy()
        self.store = Store(self.path)
        for job_id in (3, 7):
            job = self.store.job(job_id)
            self.assertEqual(job['stage'], 'voice_forbidden')
            self.assertTrue(job['notice_pending'])
        for job in self.store.jobs():
            self.assertIsNone(job['fallback_payload'])
            self.assertIsNone(job['second_payload'])
        for key in ('fallback_texts', 'text_draft', 'mode', 'dialogue_mode'):
            self.assertIsNone(self.store.get(key))
        self.assertEqual(self.store.get('sticker'), 10)
        self.assertEqual(self.store.get('voice'), 11)
        self.assertEqual(self.store.get('second'), 12)

    def test_old_approved_second_requires_new_button_and_completed_jobs_stay_complete(self):
        stages = self.legacy()
        self.store = Store(self.path)
        for job_id in (5, 6):
            job = self.store.job(job_id)
            self.assertEqual(job['stage'], 'second_confirmation')
            self.assertFalse(job['second_send_approved'])
            self.assertFalse(job['second_send_notified'])
            self.assertEqual(job['second_token'], 1)
        for job_id in (8, 9, 10, 11):
            self.assertEqual(self.store.job(job_id)['stage'], stages[job_id-1])
        for job_id in (1, 2, 4, 5, 6):
            job = self.store.job(job_id)
            self.assertEqual(job['dialogue_mode'], 'two_voice')
            self.assertEqual(job['second_source'], 12)
            self.assertFalse(job['cancel_on_reply'])
            self.assertIsNone(job['read_gate_at'])
        ids = [job['second_random'] for job in self.store.jobs()]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertNotIn(0, ids)

    def test_migration_is_once_and_does_not_revoke_current_approval_on_reopen(self):
        self.legacy()
        self.store = Store(self.path)
        self.store.update(6, stage='second_pending', second_send_approved=1, second_token=9)
        random_id = self.store.job(6)['second_random']
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.job(6)['stage'], 'second_pending')
        self.assertTrue(self.store.job(6)['second_send_approved'])
        self.assertEqual(self.store.job(6)['second_token'], 9)
        self.assertEqual(self.store.job(6)['second_random'], random_id)

    def test_new_job_rejects_invalid_durations_and_requires_second_source(self):
        self.store = Store(self.path)
        for duration in (-1, float('nan'), float('inf')):
            with self.assertRaises(ValueError):
                self.store.add(user_id=123, access_hash=55, username='testuser', sticker_source=10,
                               voice_source=11, second_source=12, baseline=0, first_duration=duration)
        with self.assertRaises(ValueError):
            self.store.add(user_id=123, access_hash=55, username='testuser', sticker_source=10,
                           voice_source=11, second_source=None, baseline=0)
