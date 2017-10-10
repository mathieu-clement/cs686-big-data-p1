#!/usr/bin/env bash
for i in `seq -w 1 24`
do
ssh bass${i} 'bash -s' << 'ENDSSH'
rm -rf /home2/mclement2/dfs*
ENDSSH
done
