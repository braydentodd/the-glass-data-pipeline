"""
THE GLASS - Nightly ETL Job
Main orchestrator for the nightly NBA data pipeline.
Runs player updates and season stats updates in the correct order.
"""

import os
import sys
import subprocess
from datetime import datetime

# Load environment variables from .env file if it exists
if os.path.exists('.env'):
    with open('.env') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ.setdefault(key, value)

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [NIGHTLY-ETL] {message}")

def run_script(script_name, description):
    """Run a Python script and return success status"""
    log(f"Starting: {description}")
    
    try:
        # Run the script and capture output
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        # Print the output
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                print(line)
        
        if result.stderr:
            for line in result.stderr.strip().split('\n'):
                print(line)
        
        if result.returncode == 0:
            log(f"✓ Completed: {description}")
            return True
        else:
            log(f"✗ Failed: {description} (exit code: {result.returncode})")
            return False
            
    except Exception as e:
        log(f"✗ Error running {script_name}: {e}")
        return False

def nightly_etl_job():
    """Main nightly ETL pipeline"""
    
    log("=" * 60)
    log("THE GLASS - Nightly ETL Job Starting")
    log("=" * 60)
    
    overall_start = datetime.now()
    
    # Define the pipeline steps
    pipeline_steps = [
        ("nightly_player_roster_update.py", "Add new players and update team assignments"),
        ("nightly_stats_update.py", "Update player season statistics"),
        ("nightly_team_stats_update.py", "Update team season statistics")
    ]
    
    # Track results
    results = []
    
    # Execute each step
    for step_num, (script, description) in enumerate(pipeline_steps, 1):
        log(f"\n--- Step {step_num}/{len(pipeline_steps)}: {description} ---")
        
        step_start = datetime.now()
        success = run_script(script, description)
        step_end = datetime.now()
        step_duration = step_end - step_start
        
        results.append({
            'step': step_num,
            'script': script,
            'description': description,
            'success': success,
            'duration': step_duration
        })
        
        if not success:
            log(f"Pipeline failed at step {step_num}. Stopping execution.")
            break
        
        log(f"Step {step_num} completed in {step_duration}")
    
    # Generate summary report
    overall_end = datetime.now()
    total_duration = overall_end - overall_start
    
    log("\n" + "=" * 60)
    log("NIGHTLY ETL JOB SUMMARY")
    log("=" * 60)
    log(f"Start Time: {overall_start}")
    log(f"End Time: {overall_end}")
    log(f"Total Duration: {total_duration}")
    log("")
    log("Step Results:")
    
    all_successful = True
    
    for result in results:
        status = "✓ SUCCESS" if result['success'] else "✗ FAILED"
        log(f"  {result['step']}. {result['description']}")
        log(f"     Status: {status}")
        log(f"     Duration: {result['duration']}")
        
        if not result['success']:
            all_successful = False
    
    if all_successful:
        log("\n🎉 NIGHTLY ETL JOB COMPLETED SUCCESSFULLY!")
        log("All NBA data has been updated for tonight.")
    else:
        log("\n❌ NIGHTLY ETL JOB FAILED")
        log("Check the logs above for specific error details.")
    
    log("=" * 60)
    
    return all_successful

if __name__ == "__main__":
    # Check for required environment variables
    if not os.getenv('DB_PASSWORD'):
        log("ERROR: DB_PASSWORD environment variable must be set")
        log("Usage: DB_PASSWORD='your_password' python nightly_etl_job.py")
        sys.exit(1)
    
    # Run the nightly ETL job
    success = nightly_etl_job()
    
    # Exit with appropriate code for cron/automation systems
    sys.exit(0 if success else 1)