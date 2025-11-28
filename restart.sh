#!/bin/bash
# --- Configuration ---
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SERVICE_NAME=$(basename $SCRIPT_DIR)
SERVICE_PATH="/service/$SERVICE_NAME"
# Maximum time in seconds to wait for graceful shutdown
MAX_SHUTDOWN_WAIT=5
# Time in seconds to pause during shutdown checks
CHECK_INTERVAL=1 

echo
echo "Initiating **Simplified Restart** for $SERVICE_NAME..."

## 1. Stop Service Gracefully
echo "Attempting graceful shutdown with 'svc -d'..."

# Get the initial PID of the running application (not supervisor/multilog)
APP_PID=$(ps | grep 'python3.*'"$SERVICE_NAME" | grep -v 'grep' | awk '{print $1}')

if [ -z "$APP_PID" ]; then
    echo "Service application is not currently running. Proceeding to log reset."
else
    # Issue graceful shutdown command
    svc -d $SERVICE_PATH
    
    # Wait and check if the PID is gone
    ELAPSED_TIME=0
    while [ "$ELAPSED_TIME" -lt "$MAX_SHUTDOWN_WAIT" ]; do
        # Check if the original PID is still running
        if ! ps | grep -q "^[[:space:]]*$APP_PID[[:space:]]"; then
            echo "Service PID ($APP_PID) terminated gracefully."
            APP_PID="" # Clear PID since it's gone
            break
        fi
        sleep $CHECK_INTERVAL
        ELAPSED_TIME=$((ELAPSED_TIME + CHECK_INTERVAL))
    done

    # 2. Force Kill if Shutdown Failed, or Prompt Reboot
    if [ -n "$APP_PID" ]; then
        echo "⚠️ **Warning:** PID ($APP_PID) remains after graceful shutdown. Attempting **force kill**..."
        kill -9 $APP_PID 2>/dev/null
        # Pause to let the kill command execute
        sleep 1
        
        # Final check after force kill
        if ps | grep -q "^[[:space:]]*$APP_PID[[:space:]]"; then
            # --- CRITICAL FAILURE ---
            echo "❌ **CRITICAL ERROR: Failed to terminate service PID ($APP_PID) even with kill -9.**"
            echo "--------------------------------------------------------"
            echo "--- The system is likely in a deeply unstable state. ---"
            echo "--- **A system reboot is required to continue.** ---"
            echo "--------------------------------------------------------"
            exit 1 
        fi
        echo "Service PID force-killed. **Supervisor will attempt immediate restart.**"
    fi
fi

## 3. Reset Log Stream
echo "Resetting log stream..."

# Find the PID of the multilog process (new or old PID)
MULTILOG_PID=$(ps | grep 'multilog.*'"$SERVICE_NAME" | grep -v grep | awk '{print $1}')

if [ -n "$MULTILOG_PID" ]; then
    # Send SIGALRM to force rotation/truncation
    kill -ALRM $MULTILOG_PID
    echo "Log reset signal sent to PID ($MULTILOG_PID)."
else
    echo "❌ **Warning:** Could not find multilog PID. Log reset skipped."
fi

## 4. Restart Service
echo "Attempting service restart..."

# Check if the service is already running (due to automatic supervisor restart)
NEW_APP_PID=$(ps | grep 'python3.*'"$SERVICE_NAME" | grep -v 'grep' | awk '{print $1}')

if [ -z "$NEW_APP_PID" ]; then
    echo "Service is currently down. Issuing 'svc -u'..."
    svc -u $SERVICE_PATH
    
    # Wait briefly for startup
    sleep 2
else
    echo "Service is already running (PID $NEW_APP_PID) due to automatic supervisor restart."
fi

## 5. Verification
echo "--- Verification ---"

# Verify the service is running and no duplicates exist.
RUNNING_PIDS=$(ps | grep 'python3.*'"$SERVICE_NAME" | grep -v 'grep' | awk '{print $1}' | wc -l)
VERIFICATION_PID=$(ps | grep 'python3.*'"$SERVICE_NAME" | grep -v 'grep' | awk '{print $1}')

if [ "$RUNNING_PIDS" -eq 1 ]; then
    echo "✅ **Success:** Service is running cleanly (PID $VERIFICATION_PID)."
elif [ "$RUNNING_PIDS" -gt 1 ]; then
    echo "❌ **Failure:** Found $RUNNING_PIDS service PIDs. Duplicate/Zombie processes detected."
else
    echo "❌ **Failure:** Service is not running."
fi

echo "**Restart process complete.**"
