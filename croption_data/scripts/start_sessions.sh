#!/bin/bash

# Define your working directory where the Python scripts are located
WORKDIR="/workspace"

# Navigate to the working directory
cd $WORKDIR

# Start the 'prod' screen session and run producer_deribit.py
#screen -dmS prod bash -c "python3 producer_deribit.py; exec sh"

# Start the 'cons' screen session and run consumer_deribit.py
#screen -dmS cons bash -c "python3 consumer_deribit.py; exec sh"

echo "Started 'prod' and 'cons' screen sessions."
