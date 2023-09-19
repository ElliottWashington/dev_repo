#!/bin/bash

LOG_FILE="/home/elliott/Development/files/test.log"

handle_interrupt() {
	echo "Interrupt received, stopping."
	exit 1
}

trap 'handle_interrupt' INT

# ASCII SOH (Start Of Heading) for delimiter
delimiter=$(printf '\001') #SOH Character

while true; do
	timestamp=$(date '+%Y%m%d-%H:%M:%S.%3N') # get the current timestamp with milliseconds
	log_entry="OUT: 8=FIX.4.2${delimiter}9=00081${delimiter}35=0${delimiter}49=RBTECHDROP${delimiter}56=EHUB${delimiter}34=171${delimiter}52=${timestamp}${delimiter}112=${timestamp}${delimiter}10=021${delimiter}"
	echo "$log_entry" >>"$LOG_FILE"
	sleep 1
done
