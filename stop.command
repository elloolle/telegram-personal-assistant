#!/bin/bash
set -eu
cd -- "$(dirname -- "$0")"
.venv/bin/python -m assistant.service stop
