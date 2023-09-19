#!/bin/bash

# Install Apache and its dependencies
sudo yum install httpd

# Start and enable Apache to run on boot
sudo systemctl start httpd
sudo systemctl enable httpd

# Allow incoming HTTP traffic through firewall
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --reload

# Print success message
sudo systemctl status httpd
