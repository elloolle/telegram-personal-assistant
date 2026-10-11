import os
from pathlib import Path
import plistlib
import signal
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from assistant.service import agent_settings, file_snapshot, ready, supervise


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="bot service ")
        self.root = Path(self.temp.name)
        (self.root / "assistant").mkdir()
        (self.root / "assistant/app.py").write_text("value = 1\n")
        (self.root / "data").mkdir()
        self.events = self.root / "data/events.txt"
        self.script = self.root / "child.py"
        self.script.write_text("""import os
from pathlib import Path
import signal
import sys
import time

events = Path(sys.argv[1])
def record(text):
    with events.open('a') as handle:
        handle.write(text + '\\n')
def stop(*args):
    record('stopped ' + str(os.getpid()))
    sys.exit(0)
signal.signal(signal.SIGINT, stop)
record('started ' + str(os.getpid()))
while True:
    time.sleep(0.02)
""")
        self.stop = threading.Event()
        self.result = []
        self.thread = None

    def tearDown(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=20)
            self.assertFalse(self.thread.is_alive())
        self.temp.cleanup()

    def start(self):
        def run():
            self.result.append(supervise(self.root, [sys.executable, str(self.script), str(self.events)],
                                         self.stop, poll_interval=0.02, restart_delay=0.1, settle_delay=0.15))
        self.thread = threading.Thread(target=run)
        self.thread.start()
        self.wait_for(lambda: len(self.started()) == 1)

    def lines(self):
        return self.events.read_text().splitlines() if self.events.exists() else []

    def started(self):
        return [int(line.split()[1]) for line in self.lines() if line.startswith("started ")]

    def wait_for(self, predicate):
        deadline = time.monotonic() + 5
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("Supervisor condition was not reached")
            time.sleep(0.02)

    def test_crashed_child_is_restarted_and_shutdown_stops_the_new_child(self):
        self.start()
        first = self.started()[0]
        os.kill(first, signal.SIGTERM)
        self.wait_for(lambda: len(self.started()) == 2)
        second = self.started()[1]
        self.assertNotEqual(first, second)
        self.stop.set()
        self.thread.join(timeout=5)
        self.assertIn(f"stopped {second}", self.lines())
        self.assertEqual(self.result, ["stop"])
        with self.assertRaises(ProcessLookupError):
            os.kill(second, 0)

    def test_code_changes_stop_child_gracefully_before_supervisor_reload(self):
        self.start()
        child = self.started()[0]
        (self.root / "assistant/app.py").write_text("value = 2\n")
        self.thread.join(timeout=5)
        self.assertEqual(self.result, ["reload"])
        self.assertIn(f"stopped {child}", self.lines())
        self.assertEqual(len(self.started()), 1)

    def test_runtime_data_and_unchanged_code_do_not_restart_child(self):
        self.start()
        (self.root / "data/queue.sqlite3").write_bytes(b"changed")
        (self.root / "assistant/__pycache__").mkdir()
        (self.root / "assistant/__pycache__/cache.py").write_text("changed")
        (self.root / "assistant/app.py").write_text("value = 1\n")
        time.sleep(0.4)
        self.assertEqual(len(self.started()), 1)
        self.assertTrue(self.thread.is_alive())

    def test_update_burst_waits_until_files_stop_changing(self):
        self.start()
        for value in range(2, 6):
            (self.root / "assistant/app.py").write_text(f"value = {value}\n")
            time.sleep(0.06)
            self.assertTrue(self.thread.is_alive())
        self.thread.join(timeout=5)
        self.assertEqual(self.result, ["reload"])

    def test_settings_changes_trigger_reload_without_exposing_token_in_snapshot(self):
        self.start()
        (self.root / ".env").write_text("TG_BOT_TOKEN='secret value'\n")
        self.thread.join(timeout=5)
        self.assertEqual(self.result, ["reload"])
        self.assertNotIn("secret value", repr(file_snapshot(self.root)))

    def test_plist_preserves_space_and_unicode_paths_and_has_restart_policy(self):
        root = self.root / "Прога с пробелами"
        settings = agent_settings(root, root / ".venv/bin/python", self.root / "logs")
        restored = plistlib.loads(plistlib.dumps(settings))
        self.assertEqual(restored["WorkingDirectory"], str(root))
        self.assertEqual(restored["ProgramArguments"][0], str(root / ".venv/bin/python"))
        self.assertTrue(restored["KeepAlive"])
        self.assertTrue(restored["RunAtLoad"])
        self.assertEqual(restored["ThrottleInterval"], 15)
        self.assertNotIn("TG_BOT_TOKEN", restored["EnvironmentVariables"])

    def test_ready_requires_saved_token_and_both_authorized_sessions(self):
        token = "123456:" + "a" * 30
        (self.root / ".env").write_text(f"TG_BOT_TOKEN='{token}'\nDATA_DIR='data'\n")
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(ready(self.root))
            for name in ("assistant-user.session", "assistant-bot.session"):
                db = sqlite3.connect(self.root / "data" / name)
                db.execute("CREATE TABLE sessions (auth_key BLOB)")
                db.execute("INSERT INTO sessions VALUES (?)", (b"saved-key",))
                db.commit()
                db.close()
            self.assertTrue(ready(self.root))
            db = sqlite3.connect(self.root / "data/assistant-user.session")
            db.execute("UPDATE sessions SET auth_key=NULL")
            db.commit()
            db.close()
            self.assertFalse(ready(self.root))
