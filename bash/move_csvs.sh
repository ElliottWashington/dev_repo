#!/bin/bash

# Create the destination directory if it doesn't already exist
sudo mkdir -p ../latency_files/

# Move all CSV files in the current directory to the destination directory
sudo mv *.csv ../latency_files/