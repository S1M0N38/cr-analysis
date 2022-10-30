#!/bin/bash
set -e

if [[ "$PWD" != */cr-analysis/data ]]; then
  echo "You are not in the right directory."
  echo "Follow instructions in README"
  exit 1
fi

echo -e "\nCollect battles from 100 players [1/2]"
python collect.py --players 50 --database-dir "db-test" --compress

echo -e "\nCollect battles from 100 players [2/2]"
python collect.py --players 50 --database-dir "db-test" --compress

echo -e "\nJoin multiple .csv.gz into a single one"
python join.py                \
  --input-dir "db-test"       \
  --output-dir "db-test"      \
  --output-file "test.csv.gz" \
  --compress                  \
  --force                     \
  --remove

if [ -f "$PWD/db-test/test.csv.gz" ]; then
  echo -e "\nAll tests pass. You're ready to collect."
  rm "$PWD/db-test/test.csv.gz"
  exit 0
else
  echo -e "\nUnknown error occured."
  exit 1
fi
