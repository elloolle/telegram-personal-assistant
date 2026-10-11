import asyncio
import contextlib
import datetime as dt
import io
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from telethon import errors, functions
from telethon.sessions import MemorySession

from assistant import auth
from assistant.__main__ import configuration
from assistant.lock import instance_lock


class FakeQR:
    url = "tg://login?token=TEST_ONLY_NOT_A_REAL_LOGIN_TOKEN"
    token = b"test-only"

    def __init__(self, user_id=123, failure=None):
        self.expires = dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=30)
        self.registered = False
        self.accepted = asyncio.Event()
        self.failure = failure
        self.user_id = user_id

    async def wait(self, timeout=None):
        self.registered = True
        if self.failure:
            raise self.failure
        await self.accepted.wait()
        return SimpleNamespace(id=self.user_id, bot=False)


class AuthTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        self.output = contextlib.redirect_stdout(io.StringIO())
        self.output.__enter__()

    async def asyncTearDown(self):
        self.output.__exit__(None, None, None)
        self.temp.cleanup()

    async def test_waiter_is_registered_before_source_accepts_login(self):
        qr = FakeQR()
        client = SimpleNamespace(qr_login=AsyncMock(return_value=qr))

        async def approve(token):
            self.assertTrue(qr.registered, "otherwise the server login update can be lost")
            self.assertEqual(token, qr.token)
            qr.accepted.set()

        me = await auth.qr_authorize(client, accept_token=approve)
        self.assertEqual(me.id, 123)

    async def test_expired_qr_is_regenerated(self):
        first = FakeQR(failure=asyncio.TimeoutError())
        second = FakeQR()
        client = SimpleNamespace(qr_login=AsyncMock(side_effect=[first, second]))
        display = Mock()

        def show(url):
            if second.registered:
                second.accepted.set()

        display.show.side_effect = show
        me = await auth.qr_authorize(client, display=display, rounds=2)
        self.assertEqual(me.id, 123)
        self.assertEqual(client.qr_login.await_count, 2)
        self.assertEqual(display.show.call_count, 2)

    async def test_2fa_prompt_retries_wrong_password(self):
        qr = FakeQR(failure=errors.SessionPasswordNeededError(request=None))
        user = SimpleNamespace(id=123)
        client = SimpleNamespace(qr_login=AsyncMock(return_value=qr),
            sign_in=AsyncMock(side_effect=[errors.PasswordHashInvalidError(request=None), user]))
        prompt = Mock(side_effect=["test-wrong", "test-correct"])
        result = await auth.qr_authorize(client, password_prompt=prompt)
        self.assertIs(result, user)
        self.assertEqual(prompt.call_count, 2)

    async def test_qr_files_are_removed_after_failed_login(self):
        qr = FakeQR(failure=ValueError("test failure"))
        client = SimpleNamespace(connect=AsyncMock(), is_user_authorized=AsyncMock(return_value=False),
                                 qr_login=AsyncMock(return_value=qr))
        with self.assertRaisesRegex(ValueError, "test failure"):
            await auth.setup_user(client, self.path, login="qr", open_browser=False)
        self.assertFalse((self.path / "login-qr.svg").exists())
        self.assertFalse((self.path / "login-qr.html").exists())

    async def test_existing_helper_session_does_not_load_tdata(self):
        client = SimpleNamespace(connect=AsyncMock(), is_user_authorized=AsyncMock(return_value=True),
                                 get_me=AsyncMock(return_value=SimpleNamespace(id=123)))
        with patch.object(auth, "create_from_tdata", new_callable=AsyncMock) as create:
            await auth.setup_user(client, self.path, tdata="must-not-be-read")
            create.assert_not_awaited()

    async def tdata_case(self, *, same_key=False, account_number=1):
        source_path = self.path / "tdata"
        source_path.mkdir()
        original = source_path / "dummy-original"
        original.write_bytes(b"not actual Telegram data")
        accounts = [SimpleNamespace(UserId=123), SimpleNamespace(UserId=456)]
        desktop = SimpleNamespace(isLoaded=lambda: True, accounts=accounts)
        qr = FakeQR(user_id=accounts[account_number - 1].UserId)
        target = SimpleNamespace(qr_login=AsyncMock(return_value=qr),
                                 session=SimpleNamespace(auth_key=SimpleNamespace(key=b"old" if same_key else b"new")))

        async def approve(request):
            self.assertIsInstance(request, functions.auth.AcceptLoginTokenRequest)
            self.assertTrue(qr.registered)
            qr.accepted.set()

        source = AsyncMock(side_effect=approve)
        source.is_user_authorized.return_value = True
        source.session = SimpleNamespace(auth_key=SimpleNamespace(key=b"old"), close=Mock())
        with patch.object(auth, "TDesktop", return_value=desktop) as load, \
             patch.object(auth.TelegramClient, "FromTDesktop", new_callable=AsyncMock, return_value=source) as convert:
            if same_key:
                with self.assertRaisesRegex(ValueError, "исходный ключ"):
                    await auth.create_from_tdata(target, source_path, account_number=account_number,
                                                 prompt=lambda _: "")
            else:
                result = await auth.create_from_tdata(target, source_path, account_number=account_number,
                                                      prompt=lambda _: "")
                self.assertEqual(result.id, accounts[account_number - 1].UserId)
            self.assertIsInstance(convert.call_args.kwargs["session"], MemorySession)
            self.assertIs(convert.call_args.args[0], accounts[account_number - 1])
            self.assertFalse(convert.call_args.kwargs["auto_post_login"])
            self.assertEqual(load.call_args.args, (str(source_path.resolve()),))
        source.disconnect.assert_awaited_once()
        source.session.close.assert_called_once()
        self.assertEqual(original.read_bytes(), b"not actual Telegram data")
        self.assertEqual(list(source_path.iterdir()), [original])

    async def test_tdata_creates_different_key_and_keeps_source_in_memory(self):
        await self.tdata_case()

    async def test_tdata_rejects_reusing_source_key(self):
        await self.tdata_case(same_key=True)

    async def test_tdata_can_select_second_account(self):
        await self.tdata_case(account_number=2)

    async def test_legacy_session_file_is_not_opened(self):
        old = self.path / "user.session"
        old.write_bytes(b"legacy file must stay untouched")
        client = auth.make_user(self.path)
        try:
            self.assertEqual(Path(client.session.filename).name, "assistant-user.session")
            self.assertTrue(client.api_id)
            self.assertTrue(client.api_hash)
        finally:
            client.session.close()
        self.assertEqual(old.read_bytes(), b"legacy file must stay untouched")


class ConfigAndLockTests(unittest.TestCase):
    def test_configuration_needs_only_bot_token_and_accepts_no_api_keys(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            with patch("assistant.__main__.ROOT", root), patch.dict(os.environ, {
                    "TG_BOT_TOKEN": "12345:TEST_ONLY_NOT_A_REAL_BOT_TOKEN"}, clear=True):
                values, data = configuration()
                self.assertNotIn("TG_API_ID", values)
                self.assertNotIn("TG_API_HASH", values)
                self.assertEqual(data, root / "data")

    def test_qr_display_is_local_and_cleanup_is_idempotent(self):
        with tempfile.TemporaryDirectory() as folder, contextlib.redirect_stdout(io.StringIO()):
            display = auth.QRDisplay(Path(folder), open_browser=False)
            display.show(FakeQR.url)
            html = display.html.read_text(encoding="utf-8")
            self.assertIn("login-qr.svg", html)
            self.assertNotIn("https://", html)
            self.assertNotIn(FakeQR.url, html)
            self.assertTrue(display.svg.read_text().lstrip().startswith("<?xml"))
            display.close()
            display.close()
            self.assertEqual(list(Path(folder).iterdir()), [])

    def test_second_process_lock_is_rejected_and_released(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "lock"
            with instance_lock(path):
                with self.assertRaises(ValueError):
                    with instance_lock(path):
                        self.fail("second instance must not enter")
            with instance_lock(path):
                pass


if __name__ == "__main__":
    unittest.main()
