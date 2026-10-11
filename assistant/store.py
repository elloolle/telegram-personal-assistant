import json
import math
from pathlib import Path
import secrets
import sqlite3
import time


COLUMNS = {
    'voice_delay': 'REAL',
    'desired_folder': 'INTEGER NOT NULL DEFAULT 0',
    'applied_folder': 'INTEGER NOT NULL DEFAULT 0',
    'next_try': 'REAL NOT NULL DEFAULT 0',
    'folder_next_try': 'REAL NOT NULL DEFAULT 0',
    'error': "TEXT NOT NULL DEFAULT ''",
    'folder_error': "TEXT NOT NULL DEFAULT ''",
    'archive_pending': 'INTEGER NOT NULL DEFAULT 0',
    'archive_done': 'INTEGER NOT NULL DEFAULT 0',
    'archive_next_try': 'REAL NOT NULL DEFAULT 0',
    'archive_error': "TEXT NOT NULL DEFAULT ''",
    'mute_pending': 'INTEGER NOT NULL DEFAULT 0',
    'mute_done': 'INTEGER NOT NULL DEFAULT 0',
    'mute_next_try': 'REAL NOT NULL DEFAULT 0',
    'mute_error': "TEXT NOT NULL DEFAULT ''",
    'history_checked': 'INTEGER NOT NULL DEFAULT 0',
    'history_confirmed': 'INTEGER NOT NULL DEFAULT 0',
    'read_target': 'INTEGER NOT NULL DEFAULT 0',
    'read_done': 'INTEGER NOT NULL DEFAULT 0',
    'read_next_try': 'REAL NOT NULL DEFAULT 0',
    'read_error': "TEXT NOT NULL DEFAULT ''",
    'read_gate_id': 'INTEGER NOT NULL DEFAULT -1',
    'read_gate_at': 'REAL',
    'dialogue_mode': "TEXT NOT NULL DEFAULT 'two_voice'",
    'second_source': 'INTEGER',
    'second_random': 'INTEGER NOT NULL DEFAULT 0',
    'second_message': 'INTEGER',
    'second_baseline': 'INTEGER NOT NULL DEFAULT 0',
    'second_reply': 'INTEGER NOT NULL DEFAULT 0',
    'second_question': 'INTEGER NOT NULL DEFAULT 0',
    'second_approved': 'INTEGER NOT NULL DEFAULT 0',
    'second_token': 'INTEGER NOT NULL DEFAULT 0',
    'second_send_approved': 'INTEGER NOT NULL DEFAULT 0',
    'second_send_notified': 'INTEGER NOT NULL DEFAULT 0',
    'second_notice_try': 'REAL NOT NULL DEFAULT 0',
    'first_duration': 'REAL',
    'second_duration': 'REAL',
    'recording_kind': "TEXT NOT NULL DEFAULT ''",
    'recording_until': 'REAL NOT NULL DEFAULT 0',
    'recording_next': 'REAL NOT NULL DEFAULT 0',
    'record_complete': 'INTEGER NOT NULL DEFAULT 0',
    'notice_pending': 'INTEGER NOT NULL DEFAULT 0',
    'notice_try': 'REAL NOT NULL DEFAULT 0',
    'sticker_message': 'INTEGER',
    'voice_message': 'INTEGER',
}


class Store:
    def __init__(self, path: Path):
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS repeat_requests (
                job_id INTEGER PRIMARY KEY, token INTEGER NOT NULL, payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS job_history (
                job_id INTEGER PRIMARY KEY, replaced_at REAL NOT NULL, snapshot TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                access_hash INTEGER NOT NULL,
                username TEXT NOT NULL,
                sticker_source INTEGER NOT NULL,
                voice_source INTEGER NOT NULL,
                sticker_random INTEGER NOT NULL,
                voice_random INTEGER NOT NULL,
                baseline INTEGER NOT NULL,
                started REAL NOT NULL,
                stage TEXT NOT NULL DEFAULT 'sticker_pending',
                due REAL,
                replied INTEGER NOT NULL DEFAULT 0,
                cancel_on_reply INTEGER NOT NULL DEFAULT 0
            );
        ''')
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(jobs)')}
        for name, declaration in COLUMNS.items():
            if name not in columns:
                self.db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {declaration}')
        self.db.commit()
        self.migrate_scenario_once()

    def get(self, key, default=None):
        row = self.db.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO settings VALUES (?, ?)', (key, json.dumps(value)))

    def migrate_scenario_once(self):
        if self.get('scenario_v4', False):
            return
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(jobs)')}
        with self.db:
            self.db.execute("DELETE FROM settings WHERE key IN ('fallback_texts', 'text_draft', 'cancel_on_reply', 'record_audio', 'dialogue_mode')")
            self.db.execute("DELETE FROM settings WHERE key='mode' AND value='\"texts\"'")
            self.db.execute("UPDATE jobs SET stage='voice_forbidden', notice_pending=1, error='' WHERE stage IN ('text_pending', 'second_text_pending')")
            self.db.execute("""UPDATE jobs SET stage='second_confirmation', second_send_approved=0,
                               second_send_notified=0, second_notice_try=0, second_token=second_token+1
                               WHERE stage IN ('second_pending', 'second_confirmation')""")
            self.db.execute("""UPDATE jobs SET dialogue_mode='two_voice', cancel_on_reply=0,
                               second_source=COALESCE(second_source, ?),
                               recording_kind='', recording_until=0, recording_next=0,
                               record_complete=0, read_gate_id=-1, read_gate_at=NULL, due=NULL
                               WHERE stage IN ('sticker_pending', 'awaiting_confirmation', 'voice_pending',
                                               'reply_waiting', 'second_pending', 'second_confirmation')""", (self.get('second'),))
            for name in ('fallback_payload', 'second_payload'):
                if name in columns:
                    self.db.execute(f'UPDATE jobs SET {name}=NULL')
            for row in self.db.execute('SELECT id FROM jobs WHERE second_random=0').fetchall():
                self.db.execute('UPDATE jobs SET second_random=? WHERE id=?', (secrets.randbits(63), row['id']))
            self.db.execute('INSERT OR REPLACE INTO settings VALUES (?, ?)', ('scenario_v4', 'true'))

    def add(self, **values):
        with self.db:
            job_id = self._insert_job(**values)
        return self.job(job_id)

    def _insert_job(self, *, user_id, access_hash, username, sticker_source, voice_source, second_source,
            baseline, first_duration=None, second_duration=None, stage='sticker_pending', history_checked=False):
        if stage not in ('sticker_pending', 'awaiting_confirmation') or not second_source:
            raise ValueError('Задайте стикер, ГС1 и ГС2.')
        for duration in (first_duration, second_duration):
            if duration is not None and (duration < 0 or not math.isfinite(duration)):
                raise ValueError('Некорректная длительность голосового.')
        cursor = self.db.execute('''
                INSERT INTO jobs (user_id, access_hash, username, sticker_source, voice_source,
                  second_source, sticker_random, voice_random, second_random, baseline, started,
                  cancel_on_reply, dialogue_mode, first_duration, second_duration, stage, history_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'two_voice', ?, ?, ?, ?)
            ''', (user_id, access_hash, username, sticker_source, voice_source, second_source,
                  secrets.randbits(63), secrets.randbits(63), secrets.randbits(63), baseline,
                  time.time(), first_duration, second_duration, stage, int(history_checked)))
        return cursor.lastrowid

    def request_repeat(self, job_id, payload):
        token = secrets.randbits(63)
        with self.db:
            self.db.execute('INSERT OR REPLACE INTO repeat_requests VALUES (?, ?, ?)',
                            (job_id, token, json.dumps(payload)))
        return token

    def repeat_request(self, job_id):
        return self.db.execute('SELECT * FROM repeat_requests WHERE job_id=?', (job_id,)).fetchone()

    def cancel_repeat(self, job_id, token):
        with self.db:
            cursor = self.db.execute('DELETE FROM repeat_requests WHERE job_id=? AND token=?', (job_id, token))
        return bool(cursor.rowcount)

    def restart(self, job_id, token, baseline):
        with self.db:
            old = self.job(job_id)
            request = self.repeat_request(job_id)
            if not old or not request or request['token'] != token:
                return None
            payload = json.loads(request['payload'])
            payload.update(baseline=baseline, stage='sticker_pending', history_checked=True)
            self.db.execute('INSERT INTO job_history VALUES (?, ?, ?)',
                            (job_id, time.time(), json.dumps(dict(old))))
            self.db.execute('DELETE FROM jobs WHERE id=?', (job_id,))
            new_id = self._insert_job(**payload)
            self.db.execute('UPDATE jobs SET history_confirmed=1 WHERE id=?', (new_id,))
            self.db.execute('DELETE FROM repeat_requests WHERE job_id=?', (job_id,))
        return self.job(new_id)

    def job(self, job_id):
        return self.db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()

    def by_user(self, user_id):
        return self.db.execute('SELECT * FROM jobs WHERE user_id=?', (user_id,)).fetchone()

    def jobs(self):
        return self.db.execute('SELECT * FROM jobs ORDER BY id DESC').fetchall()

    def update(self, job_id, **values):
        allowed = {row[1] for row in self.db.execute('PRAGMA table_info(jobs)')}
        if not values or not set(values).issubset(allowed - {'id', 'user_id'}):
            raise ValueError('Invalid job fields')
        assignments = ', '.join(f'{key}=?' for key in values)
        with self.db:
            self.db.execute(f'UPDATE jobs SET {assignments} WHERE id=?', (*values.values(), job_id))

    def reset_gate(self, job_id):
        self.update(job_id, read_gate_id=-1, read_gate_at=None)

    def dismiss_confirmation(self, job_id):
        with self.db:
            self.db.execute("DELETE FROM jobs WHERE id=? AND stage='awaiting_confirmation'", (job_id,))

    def queue_read(self, job_id, message_id):
        if message_id > 0:
            with self.db:
                self.db.execute('UPDATE jobs SET read_target=MAX(read_target, ?) WHERE id=?', (message_id, job_id))

    def close(self):
        self.db.close()
