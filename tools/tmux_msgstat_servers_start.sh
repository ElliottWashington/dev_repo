#!/bin/bash

# Variables
servers=("52" "100" "104" "105" "102" "103" "110" "112" "113" "114" "101")
ip_prefix="10.7.8"
username="scalp"
default_password="QAtr@de442"
special_password="Option5Tr@der988"

for server in "${servers[@]}"; do
	# Check if this is the special server
	if [ "$server" == "101" ]; then
		current_password=$special_password
	else
		current_password=$default_password
	fi

	tmux new -d -s $server
	tmux send-keys -t $server.0 "sshpass -p $current_password ssh -o StrictHostKeyChecking=no $username@$ip_prefix.$server " Enter \;
done
