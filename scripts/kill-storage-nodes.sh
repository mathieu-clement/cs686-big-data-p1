#!/usr/bin/env bash
for node in `./remote-show-online-bass-machines.sh` 
do
echo $node
ssh $node './kill-storage-node.sh'
done
