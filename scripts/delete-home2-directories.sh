#!/usr/bin/env bash
for i in `seq -w 1 24`
do
echo bass${i}
ssh bass${i} 'bash -s' << 'ENDSSH'
rm -rf /home2/mclement2/dfs*
ENDSSH
done
