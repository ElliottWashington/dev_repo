#!/bin/bash

# Parse command-line arguments
if [ $# -ne 1 ]; then
    echo "Usage: $0 <date>"
    exit 1
fi
date=$1

# Run the first Python script and store the CSV file
sudo python3 /home/scalp/script/spread_nack_latencies.py $date

# Run the second Python script and store the CSV file
sudo python3 /home/scalp/script/equity_nack_latencies.py $date

# Run the third Python script and store the CSV file
sudo python3 /home/scalp/script/cancel_ack_latencies.py $date

echo "Wrote and emailed latencies csv's."