@echo off
REM This script runs the Iron Condor strategy in LIVE mode for NIFTY.

REM Navigate to the project root directory (one level up from 'scripts').
cd /d "%~dp0.."

echo Starting LIVE Iron Condor for NIFTY...
python -m src.live.iron_condor_ws --live --index NIFTY

pause