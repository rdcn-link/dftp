#!/bin/bash

# Usage:
#   ./dacp.sh {start|stop|restart|status} <instance-id>

JAR_FILE="dacp-dist-0.5.0-20251201.jar"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"

INSTANCE_ID="instance"
ACTION="$1"

if [[ -z "$ACTION" || -z "$INSTANCE_ID" ]]; then
    echo "Usage: $0 {start|stop|restart|status} <instance-id>"
    exit 1
fi

DATA_DIR="$PARENT_DIR/data"
PLUGINS_DIR="$PARENT_DIR/plugins"
LOG_DIR="$PARENT_DIR/logs"
RUN_DIR="$PARENT_DIR/run"

PID_FILE="$RUN_DIR/dacp-${INSTANCE_ID}.pid"
LOG_FILE="$LOG_DIR/dacp.log"

mkdir -p "$DATA_DIR" "$PLUGINS_DIR" "$LOG_DIR" "$RUN_DIR"

# Verify JAR
if [ ! -f "$PARENT_DIR/lib/$JAR_FILE" ]; then
    echo "Error: Required JAR file not found: $JAR_FILE"
    exit 1
fi

# Collect plugin jars
PLUGIN_JARS=""
if [ -d "$PLUGINS_DIR" ]; then
    for jar in "$PLUGINS_DIR"/*.jar; do
        [ -f "$jar" ] && PLUGIN_JARS="$PLUGIN_JARS:$jar"
    done
fi

get_pid() {
    [[ -f "$PID_FILE" ]] && cat "$PID_FILE"
}

is_running() {
    local pid
    pid="$(get_pid)"
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

start() {
    if is_running; then
        echo "DACP server is already running (PID: $(get_pid))"
        return 1
    fi

    echo "Starting DACP server..."

    nohup java \
        -cp "$PARENT_DIR/lib/$JAR_FILE$PLUGIN_JARS" \
        -Ddacp.instance.id="$INSTANCE_ID" \
        -Dlog4j.configurationFile="$PARENT_DIR/conf/log4j2.xml" \
        link.rdcn.server.ServerStart \
        "$PARENT_DIR" \
        > "$LOG_FILE" 2>&1 &

    echo $! > "$PID_FILE"

    sleep 1
    if is_running; then
        echo "DACP server started (PID: $(get_pid))"
    else
        echo "Failed to start DACP server. See $LOG_FILE"
        rm -f "$PID_FILE"
        return 1
    fi
}

stop() {
    if ! is_running; then
        echo "DACP server not running"
        rm -f "$PID_FILE"
        return 0
    fi

    pid="$(get_pid)"
    echo "Stopping DACP server (PID: $pid)..."
    kill "$pid"

    for _ in {1..5}; do
        if ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$PID_FILE"
            echo "DACP server stopped"
            return 0
        fi
        sleep 1
    done

    echo "Force killing DACP instance '$INSTANCE_ID'"
    kill -9 "$pid"
    rm -f "$PID_FILE"
}

status() {
    if is_running; then
        echo "DACP instance '$INSTANCE_ID' RUNNING (PID: $(get_pid))"
    else
        echo "DACP instance '$INSTANCE_ID' NOT RUNNING"
    fi
}

restart() {
    stop
    start
}

case "$ACTION" in
    start)   start ;;
    stop)    stop ;;
    restart) restart ;;
    status)  status ;;
    *)
        echo "Usage: $0 {start|stop|restart|status} <instance-id>"
        exit 1
        ;;
esac