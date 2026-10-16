#!/usr/bin/env bash
# Pushes the page's data after Airflow's morning run. Airflow writes site/data/ into this
# folder; pushing it triggers the pages workflow, which publishes the page.
set -euo pipefail
cd "$(dirname "$0")/.."

git pull --quiet --ff-only
git add site/data
if git diff --cached --quiet; then
  echo "No new data to publish."
else
  git commit --quiet -m "data: rain to $(python3 -c 'import json; print(json.load(open("site/data/latest.json"))["date"])')"
  git push --quiet
  echo "Published."
fi
