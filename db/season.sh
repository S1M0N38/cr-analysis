#!/bin/bash

set -e

# This script only works on FreeBSD or macOS
if [[ "$OSTYPE" != "darwin"* ]]; then
  echo "This script can be run only on macOS."
  exit 1
fi

output_dir="$1-$2"

# Ensure db directory exists
[ ! -d $output_dir ] && mkdir $output_dir

# Unzip all file in db-day, `look` only works with files.
gunzip --keep $(find 'days' -type f -name '*.csv.gz')

# CSV files to iterate trought
csv_files=$(find 'days' -type f -name '*.csv')

i=-1
while
  ((i++))
  datetime="$(date -v +$((i))'d' -jf '%Y%m%d' $1 '+%Y%m%d')"
  [[ $datetime < "$(date -v '+1d' -jf '%Y%m%d' $2 '+%Y%m%d')" ]]
do
  # TODO: don't iterate trough all csv files, but only
  # on ones that are <= datetime

  # create a command for merge battle on same datetime.
  cmd="sort --merge --unique"
  for csv_file in $csv_files; do
    cmd="$cmd <(look '$datetime' '$csv_file')"
  done

  # execute command and save result in output_file.
  eval "$cmd" | gzip -c > "$output_dir/$datetime.csv.gz"

  echo "created '$output_dir/$datetime.csv.gz' [$((i+1))]"
done

# Remove uncompressed file to save space.
rm $csv_files
