"""macOS launch agent and a supervisor that reloads changed application files."""

import argparse
import hashlib
import os
from pathlib import Path
import plistlib
import re
import signal
import sqlite3
import subprocess
import sys
import threading
import time

from dotenv import dotenv_values

from .lock import instance_lock


ROOT = Path(__file__).resolve().parent.parent
LABEL = "local.personal.telegram-assistant"


def data_directory(root):
    values = {**dotenv_values(root / ".env"), **os.environ}
    data = Path(values.get("DATA_DIR") or "data").expanduser()
    return data if data.is_absolute() else root / data


def ready(root):
    values = {**dotenv_values(root / ".env"), **os.environ}
    if not re.fullmatch(r"\d+:[A-Za-z0-9_-]{20,}", values.get("TG_BOT_TOKEN", "")):
        return False
    for name in ("assistant-user.session", "assistant-bot.session"):
        path = data_directory(root) / name
        if not path.is_file():
            return False
        try:
            db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
            try:
                row = db.execute("SELECT auth_key FROM sessions LIMIT 1").fetchone()
                if not row or not row[0]:
                    return False
            finally:
                db.close()
        except sqlite3.Error:
            return False
    return True


def file_snapshot(root):
    # Runtime data, logs, caches and tests must not restart the bot.
    files = list((root / "assistant").rglob("*.py"))
    files += [root / ".env", root / "requirements.txt"]
    snapshot = []
    for path in sorted(files):
        if "__pycache__" in path.parts or path.name.startswith(".codex-update-"):
            continue
        try:
            snapshot.append((str(path.relative_to(root)), hashlib.sha256(path.read_bytes()).digest()))
        except FileNotFoundError:
            continue
    return tuple(snapshot)


def stop_child(child):
    if child is None or child.poll() is not None:
        return
    try:
        child.send_signal(signal.SIGINT)
        child.wait(timeout=15)
    except subprocess.TimeoutExpired:
        child.terminate()
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
    except ProcessLookupError:
        child.wait()


def supervise(root, command, stop, *, poll_interval=1, restart_delay=15, settle_delay=2):
    baseline = file_snapshot(root)
    pending = None
    changed_at = 0
    retry_at = 0
    child = None
    try:
        while not stop.is_set():
            now = time.monotonic()
            current = file_snapshot(root)
            if current != baseline:
                if current != pending:
                    pending, changed_at = current, now
                elif now - changed_at >= settle_delay:
                    print("Код или настройки изменились. Перезапускаю помощника.", flush=True)
                    return "reload"
            else:
                pending = None
            if child is not None and child.poll() is not None:
                print(f"Помощник завершился (код {child.returncode}). Повторный запуск через {restart_delay:g} сек.", flush=True)
                child = None
                retry_at = now + restart_delay
            if child is None and pending is None and now >= retry_at:
                child = subprocess.Popen(command, cwd=root, stdin=subprocess.DEVNULL,
                                         env={**os.environ, "TG_ASSISTANT_SUPERVISED": "1"})
                print(f"Запущен процесс помощника: {child.pid}.", flush=True)
            stop.wait(poll_interval)
    finally:
        stop_child(child)
    return "stop"


def launchctl(*args, check=True):
    result = subprocess.run(["/bin/launchctl", *args], capture_output=True, text=True)
    if check and result.returncode:
        raise ValueError((result.stderr or result.stdout or "Ошибка launchctl").strip()[:500])
    return result


def domain():
    return f"gui/{os.getuid()}"


def agent_path():
    return Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"


def log_directory():
    return Path.home() / "Library" / "Logs" / "TelegramAssistant"


def agent_settings(root, python, logs):
    return {
        "Label": LABEL,
        "ProgramArguments": [str(python), "-u", "-m", "assistant.service", "supervise"],
        "WorkingDirectory": str(root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 15,
        "ExitTimeOut": 30,
        "ProcessType": "Background",
        "Umask": 0o077,
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
        "StandardOutPath": str(logs / "service.log"),
        "StandardErrorPath": str(logs / "service-error.log"),
    }


def loaded():
    return launchctl("print", f"{domain()}/{LABEL}", check=False).returncode == 0


def uninstall():
    if loaded():
        launchctl("bootout", f"{domain()}/{LABEL}")
    agent_path().unlink(missing_ok=True)
    print("Помощник остановлен. Автозапуск отключён; настройки и очередь сохранены.")


def install(root):
    if not ready(root):
        raise ValueError("Сначала сохраните токен и выполните вход через start.command.")
    if loaded():
        launchctl("bootout", f"{domain()}/{LABEL}")
    data = data_directory(root)
    # Do not take over a manually launched instance or a still-stopping service.
    for attempt in range(30):
        try:
            with instance_lock(data / "instance.lock"):
                break
        except ValueError:
            if attempt == 29:
                raise ValueError("Помощник уже запущен вручную. Остановите его через Ctrl+C и повторите start.command.") from None
            time.sleep(0.5)
    logs = log_directory()
    logs.mkdir(parents=True, exist_ok=True, mode=0o700)
    path = agent_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".plist.tmp")
    with temporary.open("wb") as handle:
        plistlib.dump(agent_settings(root, sys.executable, logs), handle)
    temporary.chmod(0o600)
    os.replace(temporary, path)
    launchctl("enable", f"{domain()}/{LABEL}")
    launchctl("bootstrap", domain(), str(path))
    print("Автозапуск включён. Бот работает в фоне и перезапускается после сбоев и изменений кода.")
    print("Для полной остановки откройте stop.command. Закрывать это окно Terminal можно.")
    print(f"Журнал: {logs / 'service.log'}")


def main():
    parser = argparse.ArgumentParser(description="Автозапуск Telegram-помощника на macOS")
    parser.add_argument("command", choices=("ready", "install", "restart", "stop", "status", "supervise"))
    args = parser.parse_args()
    if args.command == "ready":
        return 0 if ready(ROOT) else 1
    if sys.platform != "darwin":
        raise ValueError("Фоновый автозапуск доступен на macOS.")
    if args.command in ("install", "restart"):
        install(ROOT)
    elif args.command == "stop":
        uninstall()
    elif args.command == "status":
        result = launchctl("print", f"{domain()}/{LABEL}", check=False)
        if result.returncode:
            print("Фоновый запуск выключен. Для включения откройте start.command.")
        else:
            for line in result.stdout.splitlines():
                if re.match(r"\s*(state|pid|last exit code) =", line):
                    print(line.strip())
        print(f"Журнал: {log_directory()}")
    else:
        stop = threading.Event()
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        with instance_lock(data_directory(ROOT) / "supervisor.lock"):
            action = supervise(ROOT, [sys.executable, "-u", "-m", "assistant", "run"], stop)
        if action == "reload" and not stop.is_set():
            os.execv(sys.executable, [sys.executable, "-u", "-m", "assistant.service", "supervise"])
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr, flush=True)
        raise SystemExit(1)
