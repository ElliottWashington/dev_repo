#!/bin/bash

./msgstats_opra.sh >opra.log
ls -l opra.log
grep -P -A 1 -B 4 --no-group-separator ' "Messages drop percentage" :(?!"N/A").*' opra.log
