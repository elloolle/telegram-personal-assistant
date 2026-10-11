#!/bin/bash
set -eu
cd -- "$(dirname -- "$0")"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else "Нужен Python 3.10 или новее.")'
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv
fi
.venv/bin/python -m pip install -r requirements.txt
if [ "$#" -gt 0 ]; then
  .venv/bin/python -m assistant.service stop
  .venv/bin/python -m assistant setup "$@"
elif ! .venv/bin/python -m assistant.service ready; then
  .venv/bin/python -m assistant setup
fi
.venv/bin/python -m assistant.service install
