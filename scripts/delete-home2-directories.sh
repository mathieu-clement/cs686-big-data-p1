#!/usr/bin/env bash
for node in `./remote-show-online-bass-machines.sh`
do
echo $node
ssh $node 'bash -s' << 'ENDSSH'
rm -rf /home2/mclement2/dfs*
ENDSSH
done
