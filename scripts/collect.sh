#!/bin/bash
python collect.py                               \
  --quiet                                       \
  --players 100000                              \
  --requests 13                                 \
  --database-dir "db-hour"                      \
  --compress                                    \
