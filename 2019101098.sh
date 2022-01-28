#!/bin/bash
for dir in "$@"
do
    python3 2019101098_sql.py "$dir"
done
