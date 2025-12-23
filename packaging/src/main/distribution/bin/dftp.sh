#!/bin/bash

# DFTP Control Script
# Filename: dftp.sh
# Usage: ./dftp.sh [start|stop|restart|status]

JAR_FILE="dftp-dist-0.5.0-20251201.jar"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PARENT_DIR/data"
PLUGINS_DIR="$PARENT_DIR/plugins"

# Define PID file path
PID_FILE="$PARENT_DIR/run/dftp.pid"
LOG_FILE="$PARENT_DIR/logs/dftp.log"

# Verify JAR file existence
if [ ! -f "$PARENT_DIR/lib/$JAR_FILE" ]; then
    echo "Error: Required JAR file not found $JAR_FILE"
    exit 1
fi

# Verify data directory existence
if [ ! -d "$DATA_DIR" ]; then
    echo "Warning: Data directory not found at $DATA_DIR"
    echo "Creating data directory..."
    mkdir -p "$DATA_DIR"
fi

# Collect plugin jars
PLUGIN_JARS=""
if [ -d "$PLUGINS_DIR" ]; then
    for jar in "$PLUGINS_DIR"/*.jar; do
        [ -f "$jar" ] && PLUGIN_JARS="$PLUGIN_JARS:$jar"
    done
fi

# Get PID from PID file
get_pid() {
    [ -f "$PID_FILE" ] && cat "$PID_FILE"
}

# Check if server is running by checking the PID file
is_running() {
    local pid
    pid=$(get_pid)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# Start DFTP server
start() {
    if is_running; then
        echo "DFTP server is already running (PID: $(get_pid))"
        return 1
    fi

    echo "Starting DFTP server..."
    nohup java \
      -Dlog4j.configurationFile="$PARENT_DIR/conf/log4j2.xml" \
      -cp "$PARENT_DIR/lib/$JAR_FILE$PLUGIN_JARS" \
      link.rdcn.server.DftpServerStart \
      "$PARENT_DIR" \
      > "$LOG_FILE" 2>&1 &

    # Save the PID of the started process to the PID file
    echo $! > "$PID_FILE"
    sleep 1

    if is_running; then
        echo "DFTP server started successfully (PID: $(get_pid))"
    else
        echo "Failed to start DFTP server. See $LOG_FILE"
        rm -f "$PID_FILE"
        return 1
    fi
}

# Stop DFTP server
stop() {
    if ! is_running; then
        echo "DFTP server is not currently running"
        return 1
    fi

    pid=$(get_pid)
    echo "Initiating DFTP server shutdown (PID: $pid)..."
    kill "$pid"  # Graceful shutdown

    sleep 2
    if is_running; then
        echo "Graceful shutdown unsuccessful, forcing termination (PID: $pid)..."
        kill -9 "$pid"  # Force termination if the process is still running
        sleep 1
    fi
    rm -f "$PID_FILE"  # Remove the PID file
    echo "DFTP service has stopped."
}

# Restart DFTP server
restart() {
    echo "Restarting DFTP server..."
    stop
    start
}

# Get the status of the DFTP server
status() {
    if is_running; then
        echo "DFTP server status: RUNNING (PID: $(get_pid))"
    else
        echo "DFTP server status: NOT RUNNING"
    fi
}

# Ensure the run directory exists
mkdir -p "$PARENT_DIR/run"

# Process command-line arguments
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
        ;;
esac

exit 0