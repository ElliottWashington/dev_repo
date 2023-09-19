#!/bin/bash

LOG_FILES=(
	'/home/elliott/Development/files/test.log'
)

HOSTNAME=$(hostname)

handle_interrupt() {
	echo "Interrupt received, stopping."
	kill $(jobs -p)
	exit 1
}

trap 'handle_interrupt' INT

SOH=$(printf '\001')

for file in ${LOG_FILES[@]}; do
	tail -Fn0 $file | awk -v filename="$file" -v hostname="$HOSTNAME" -v FS="$SOH" '{
        if($0 ~ /35=0/) {
            for(i=1; i<=NF; i++) {
                split($i, pair, "=")
                if(pair[1] == 52) {
                    gsub(/-|:/, "", pair[2])  # remove hyphens and colons
                    log_time = mktime(substr(pair[2], 1, 4) " " substr(pair[2], 5, 2) " " substr(pair[2], 7, 2) " " substr(pair[2], 9, 2) " " substr(pair[2], 11, 2) " " substr(pair[2], 13, 2))
                    current_time = systime()
                    time_difference = current_time - log_time
                    print "File: " filename ", Host: " hostname ", Heartbeat detected. Current time: " current_time ", Log timestamp: " log_time ", Difference (seconds): " time_difference
                }
            }
        }
    }' &
done

# Wait for all background jobs to finish
wait
