#!/bin/bash
python join.py                                                 \
  --quiet                                                      \
  --input-files "$(date --date='yesterday' '+%Y%m%d')*.csv.gz" \
  --input-dir "db-hour"                                        \
  --output-file "$(date --date='yesterday' '+%Y%m%d').csv.gz"  \
  --output-dir "db-day"                                        \
  --remove                                                     \
  --compress                                                   \
