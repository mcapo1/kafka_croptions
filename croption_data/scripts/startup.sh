#!/bin/bash
#apt-get update && apt-get install -y python3-pip screen
#pip3 install -r /workspace/requirements.txt
#tail -f /dev/null

# Redirect all output to a log file
exec > /workspace/startup.log 2>&1

# Wait for any existing apt-get processes to complete
while fuser /var/lib/apt/lists/lock >/dev/null 2>&1; do
    echo "$(date) - Waiting for other apt-get process to finish..."
    sleep 1
done

echo "$(date) - Updating package lists..."
apt-get update

echo "$(date) - Installing python3-pip and screen..."
apt-get install -y python3-pip screen

echo "$(date) - Installing Python packages from requirements.txt..."
pip3 install -r /workspace/requirements.txt

echo "$(date) - Setup complete. Keeping the container running..."
# Keep the container running
tail -f /dev/null
