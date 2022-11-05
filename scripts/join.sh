#!/bin/bash
set -e

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  input_files="$(date --date='yesterday' '+%Y%m%d')*.csv.gz"
  output_file="$(date --date='yesterday' '+%Y%m%d').csv.gz"
elif [[ "$OSTYPE" == "darwin"* ]]; then
  input_files="$(date -v -1d '+%Y%m%d')*.csv.gz"
  output_file="$(date -v -1d '+%Y%m%d').csv.gz"
else
  echo "Unknown platform"
fi

python join.py                 \
  --quiet                      \
  --input-files "$input_files" \
  --input-dir "db-hour"        \
  --output-file "$output_file" \
  --output-dir "db-day"        \
  --remove                     \
  --compress                   \
