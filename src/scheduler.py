import schedule
import time
import subprocess
import logging
import sys
from datetime import datetime

# Setup logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("scheduler.log")
    ]
)
log = logging.getLogger(__name__)

def run_strategy(index_name):
    """Runs the trading bot for a specific index in a separate process."""
    log.info(f"🚀 Launching {index_name} Strategy...")
    
    # Command: python -m src.live.itm_momentum_breakdown --index NIFTY --live
    cmd = [
        sys.executable, "-m", "src.live.itm_momentum_breakdown",
        "--index", index_name,
        "--live"
    ]
    
    # Use Popen to run in background (non-blocking for the scheduler)
    try:
        process = subprocess.Popen(
            cmd,
            stdout=sys.stdout, # Redirect bot output to scheduler's stdout
            stderr=sys.stderr,
            text=True
        )
        log.info(f"✅ {index_name} process started with PID: {process.pid}")
    except Exception as e:
        log.error(f"❌ Failed to start {index_name}: {e}")

def job():
    """The scheduled job to run at 09:15 AM."""
    now = datetime.now()
    
    # 0=Monday, 4=Friday. Skip weekends (5=Sat, 6=Sun).
    if now.weekday() > 4:
        log.info("😴 Today is weekend. No trading.")
        return

    log.info("⏰ Market Open! Starting strategies...")
    
    # Launch NIFTY Strategy
    run_strategy("NIFTY")
    
    # Uncomment below to launch SENSEX as well
    # run_strategy("SENSEX")

def start_scheduler():
    log.info("⏳ Scheduler started. Waiting for 09:15 AM...")
    
    # Schedule the job every day at 09:15
    schedule.every().day.at("09:15").do(job)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("🛑 Scheduler stopped by user.")

if __name__ == "__main__":
    start_scheduler()