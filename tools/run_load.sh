#!/usr/bin/env bash
# Run the Quantic loader unattended for hours or days.
#  - keeps the Mac awake (caffeinate), logs to a file
#  - if the loader crashes, waits 60 s and restarts it; it resumes from its checkpoints
#  - stops for good when the loader finishes or on Ctrl-C
# Usage: tools/run_load.sh <logfile> <loader args...>
set -u
cd "$(dirname "$0")/.."                       # run from the repo root
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  if [ -x .venv/bin/python ]; then PY=.venv/bin/python; else PY=python3; fi   # repo venv first: has the SDK
fi
log="$1"; shift
trap 'echo "$(date "+%F %T") run_load: stopped by user" | tee -a "$log"; exit 130' INT TERM
attempt=0
while true; do
  attempt=$((attempt + 1))
  echo "$(date "+%F %T") run_load: attempt $attempt: $PY loader/quantic_pos_gen.py $*" | tee -a "$log"
  caffeinate -dimsu "$PY" loader/quantic_pos_gen.py "$@" 2>&1 | tee -a "$log"
  rc=${PIPESTATUS[0]}
  if [ "$rc" -eq 0 ]; then
    echo "$(date "+%F %T") run_load: finished OK" | tee -a "$log"; exit 0
  fi
  echo "$(date "+%F %T") run_load: loader exited with $rc; restarting in 60 s (resumes from checkpoints)" | tee -a "$log"
  sleep 60
done
