#!/bin/bash
#
# This script only works on FreeBSD or macOS
for i in {0..5}; do
  datetime_max="$(date -v -$((i))'d' '+%Y%m%d')"
  datetime_min="$(date -v -$((i+1))'d' '+%Y%m%d')"
  input_file="$datetime_min.csv.gz"
  echo -e "\nCreating $input_file ..."
  python join.py                   \
    --input-dir "db-day"           \
    --output-file "$input_file"    \
    --output-dir "db"              \
    --compress                     \
    --force                        \
    --datetime-min "$datetime_min" \
    --datetime-max "$datetime_max"
done
