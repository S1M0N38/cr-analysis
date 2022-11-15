#!/bin/bash

# This script only works on FreeBSD or macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
  echo "This script can be run only on macOS"
  exit 1
fi

# Unzip all file in db-day, `look` only works with files.
gunzip --keep $(find 'db-day' -type f -name '*.csv.gz')

# CSV files to iterate trought
csv_files=$(find 'db-day' -type f -name '*.csv')

# Search battle up to 20 days ago
for i in {0..20}; do

  datetime="$(date -v -$((i))'d' '+%Y%m%d')"

  # TODO: don't iterate trough all csv files, but only
  # on ones that are <= datetime

  # create a command for merge battle on same datetime.
  cmd="sort --merge --unique"
  for csv_file in $csv_files; do
    cmd="$cmd <(look '$datetime' '$csv_file')"
  done

  # execute command and save result in output_file.
  eval "$cmd" | gzip -c > "db/$datetime.csv.gz"

  echo "[$((i+1))/20] created 'db/$datetime.csv.gz'"

done

# Remove uncompressed file to save space.
rm $csv_files
