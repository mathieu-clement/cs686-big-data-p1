#!/usr/bin/env bash

nmap -sP 10.0.0.201-229 | grep bass | cut -f 5 -d' '