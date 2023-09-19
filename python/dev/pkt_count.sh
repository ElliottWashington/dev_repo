#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage: $0 <gmac log filename> <date in YYYY/MM/DD format>"
  exit 2
fi

echo line_number,packets_received
cat $1 | \
 grep "^$2" | \
 grep -A 1 --no-group-separator "Description" | \
 tr -s "[:space:]" | \
 cut -d'|' -f5- | \
 tr -d '\t' | \
 sed ':a;N;$!ba;s/\n RCVD_PACKETS//g' | \
 tr -s '=' ' ' | \
 cut -d' ' -f7,12 | \
 sort -t. -k3,3n -k4,4n | \
 grep  "[.][0-9][0-9]* [0-9]" | \
 cut -d. -f4- | \
 tr -s ' ' ',' | \
 sed "s/[.]00$//g"

