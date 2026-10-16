#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
# Dataset policy is explicit and overrides any older local dotenv selection.
export HYDROMANCER_COINS=SOL,BTC,ETH
export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"
exec .venv/bin/python -m hydromancer_eda.main
