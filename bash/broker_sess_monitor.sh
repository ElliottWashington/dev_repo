#!/bin/bash

LOG_FILES=(
    '/home/elliott/Development/files/ordash_20230622.log'
    '/home/elliott/Development/files/orinet_20230622.log'
    '/home/elliott/Development/files/orvolant_20230622.log'
)

HOSTNAME=$(hostname)

for file in ${LOG_FILES[@]}; do
    cp $file $file.bak
done

while true; do
    for file in ${LOG_FILES[@]}; do
        diff -Naur $file.bak $file | grep '^+' | grep -v '+++' | cut -c 2- | awk -v filename="$file" -v hostname="$HOSTNAME" '{ print "File: " filename ", Host: " hostname ", Error: " $0 }' | python3 /home/elliott/Development/scripts/python/dev/send_email.py
        cp $file $file.bak
    done
    sleep 1
done