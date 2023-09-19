#!/bin/bash

# Parse command-line arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <date>"
    exit 1
fi
date=$1

# Run the first Python script and store the CSV file
sudo python3 /home/scalp/latency_files/add_cancellatency_2db.py $date

# Run the second Python script and store the CSV file
sudo python3 /home/scalp/latency_files/add_equitylatency_2db.py $date

# Run the third Python script and store the CSV file
sudo python3 /home/scalp/latency_files/add_spreadlatency_2db.py $date

echo "Done."
