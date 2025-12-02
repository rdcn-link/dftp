#!/bin/bash

JAR_FILE="dacp-proxy-dist-0.5.0-20251201.jar"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

# Verify JAR file existence
if [ ! -f "$PARENT_DIR/lib/$JAR_FILE" ]; then
    echo "Error: Required JAR file not found $JAR_FILE"
    exit 1
fi

start() {
    if is_running; then
        echo "dacp-proxy server is already running (PID: $(get_pid))"
        return 1
    fi

    echo "Starting dacp-proxy server..."
    nohup java -jar "$PARENT_DIR/lib/$JAR_FILE" "$PARENT_DIR" > "$PARENT_DIR/logs/proxy.log" 2>&1 &
    echo "dacp-proxy server started successfully"
}

stop() {
    if ! is_running; then
        echo "dacp-proxy server is not currently running"
        return 1
    fi

    echo "Initiating dacp-proxy server shutdown..."
    kill $(get_pid)
    sleep 2
    if is_running; then
        echo "Graceful shutdown unsuccessful, forcing termination..."
        kill -9 $(get_pid)
        sleep 1
    fi
    echo "dacp-proxy service has stopped."
}

restart() {
  echo "Restarting dacp-proxy server..."
    stop
    start
}

status() {
    if is_running; then
        echo "dacp-proxy server status: RUNNING (PID: $(get_pid))"
    else
        echo "dacp-proxy server status: NOT RUNNING"
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