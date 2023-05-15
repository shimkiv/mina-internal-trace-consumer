#!/bin/sh

case "$1" in
    consumer)
        shift
        exec /internal_trace_consumer "$@"
        ;;
    fetcher)
        shift
        exec /internal_log_fetcher "$@"
        ;;
    *)
        echo "Usage: <binary_name> [args]"
        echo "Available binaries: consumer, fetcher"
        exit 1
        ;;
esac