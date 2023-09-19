#!/bin/bash

# Install Java 11
sudo yum install -y java-11-openjdk-devel

# Download and install Jenkins
sudo wget --no-check-certificate -O /etc/yum.repos.d/jenkins.repo https://pkg.jenkins.io/redhat-stable/jenkins.repo
sudo rpm --import https://pkg.jenkins.io/redhat-stable/jenkins.io.key
sudo yum upgrade
sudo yum install -y jenkins

# Start and enable Jenkins service
sudo systemctl start jenkins.service
sudo systemctl enable jenkins.service

sudo systemctl status jekins.service
