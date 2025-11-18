#!/bin/bash

# DACP Control Script
# Filename: dacp.sh
# Usage: ./dacp.sh [start|stop|restart|status]

JAR_FILE="dacp-dist-0.5.0-20251028.jar"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PARENT_DIR/data"
PLUGINS_DIR="$PARENT_DIR/plugins"

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

start() {
    if is_running; then
        echo "DACP server is already running (PID: $(get_pid))"
        return 1
    fi

    echo "Starting DACP server..."
    nohup java -cp "$PARENT_DIR/lib/$JAR_FILE$PLUGIN_JARS" link.rdcn.dacp.DacpServerStart "$PARENT_DIR" > "$PARENT_DIR/logs/dacp.log" 2>&1 &
    echo "DACP server started successfully"
}

stop() {
    if ! is_running; then
        echo "DACP server is not currently running"
        return 1
    fi

    echo "Initiating DACP server shutdown..."
    kill $(get_pid)
    sleep 2
    if is_running; then
        echo "Graceful shutdown unsuccessful, forcing termination..."
        kill -9 $(get_pid)
        sleep 1
    fi
    echo "DACP service has stopped."
}

restart() {
  echo "Restarting DACP server..."
    stop
    start
}

status() {
    if is_running; then
        echo "DACP server status: RUNNING (PID: $(get_pid))"
    else
        echo "DACP server status: NOT RUNNING"
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