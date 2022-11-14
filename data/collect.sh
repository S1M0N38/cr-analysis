#!/bin/bash
set -e

csv_path="db-hour/$(date '+%Y%m%dT%H%M%S').csv"

# collect battles into csv_path file
python collect.py                                 \
  --quiet                                         \
  --players 100000                                \
  --requests 13                                   \
  --output "$csv_path"

# sort csv file by battles datetime,
# remove duplication and compress.
sort --unique "$csv_path" | gzip > "$csv_path.gz"

# remove csv file keeping only the compress version.
[[ -f "$csv_path.gz" ]] && rm "$csv_path"
