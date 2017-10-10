#!/usr/bin/env bash

for i in $(seq -w 1 24)
do
    echo "Sending to bass${i}..."
    scp $(dirname $0)/../target/dfs-1.0.jar bass${i}:.
done
