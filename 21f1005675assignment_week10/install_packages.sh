#!/bin/bash
# install_packages.sh

sudo apt-get update
# Install pip (if not already installed)
sudo apt-get install python3-pip -y

# Install required Python packages
pip3 install kafka-python pandas

