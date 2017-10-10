#!/usr/bin/env bash

for i in `seq -w 1 24`
do
echo bass${i}
ssh bass${i} './kill-storage-node.sh'
done
