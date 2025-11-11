#!/bin/bash

# DFTP Control Script
# Filename: dftp.sh
# Usage: ./dftp.sh [start|stop|restart|status]

JAR_FILE="dftp-dist-0.5.0-20251028.jar"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PARENT_DIR/data"

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

start() {
    if is_running; then
        echo "DFTP server is already running (PID: $(get_pid))"
        return 1
    fi

    echo "Starting DFTP server..."
    nohup java -jar "$PARENT_DIR/lib/$JAR_FILE" "$PARENT_DIR" > "$PARENT_DIR/logs/dftp.log" 2>&1 &
    echo "DFTP server started successfully"
}

stop() {
    if ! is_running; then
        echo "DFTP server is not currently running"
        return 1
    fi

    echo "Initiating DFTP server shutdown..."
    kill $(get_pid)
    sleep 2
    if is_running; then
        echo "Graceful shutdown unsuccessful, forcing termination..."
        kill -9 $(get_pid)
        sleep 1
    fi
    echo "DFTP service has stopped."
}

restart() {
  echo "Restarting DFTP server..."
    stop
    start
}

status() {
    if is_running; then
        echo "DFTP server status: RUNNING (PID: $(get_pid))"
    else
        echo "DFTP server status: NOT RUNNING"
    fi
}

is_running() {
    [ -n "$(get_pid)" ]
}

get_pid() {
    pgrep -f "java.*$JAR_FILE"
}

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