#!/bin/bash
# This script runs the Iron Condor strategy in LIVE mode for SENSEX.

# Get the directory where the script is located to ensure it runs from the project root.
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Navigate to the project root directory (one level up from 'scripts').
cd "$DIR/.."

echo "Starting LIVE Iron Condor for SENSEX..."
python -m src.live.iron_condor_ws --live --index SENSEX

read -p "Script finished. Press Enter to exit..."