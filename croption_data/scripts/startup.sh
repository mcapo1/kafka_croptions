#!/bin/bash
apt-get update && apt-get install -y python3-pip screen
pip3 install -r /workspace/requirements.txt
tail -f /dev/null
