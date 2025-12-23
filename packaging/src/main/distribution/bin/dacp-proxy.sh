#!/bin/bash

JAR_FILE="dacp-proxy-dist-0.5.0-20251201.jar"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

PID_FILE="$PARENT_DIR/run/dacp-proxy.pid"
LOG_FILE="$PARENT_DIR/logs/proxy.log"

if [ ! -f "$PARENT_DIR/lib/$JAR_FILE" ]; then
    echo "Error: Required JAR file not found: $JAR_FILE"
    exit 1
fi

get_pid() {
    [ -f "$PID_FILE" ] && cat "$PID_FILE"
}

is_running() {
    local pid
    pid=$(get_pid)
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

start() {
    if is_running; then
        echo "dacp-proxy server is already running (PID: $(get_pid))"
        return 1
    fi

    echo "Starting dacp-proxy server..."
    nohup java \
      -Dlog4j.configurationFile="$PARENT_DIR/conf/log4j2.xml" \
      -jar "$PARENT_DIR/lib/$JAR_FILE" \
      "$PARENT_DIR" \
      > "$LOG_FILE" 2>&1 &

    echo $! > "$PID_FILE"

    sleep 1
    if is_running; then
        echo "dacp-proxy server started successfully (PID: $(get_pid))"
    else
        echo "Failed to start dacp-proxy server. See $LOG_FILE"
        rm -f "$PID_FILE"
        return 1
    fi
}

stop() {
    if ! is_running; then
        echo "dacp-proxy server is not currently running"
        return 1
    fi

    pid=$(get_pid)
    echo "Initiating dacp-proxy server shutdown (PID: $pid)..."
    kill "$pid"

    sleep 2
    if is_running; then
        echo "Graceful shutdown unsuccessful, forcing termination (PID: $pid)..."
        kill -9 "$pid"
        sleep 1
    fi
    rm -f "$PID_FILE"
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