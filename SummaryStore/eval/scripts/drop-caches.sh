#!/bin/bash
# TODO: move into src/main/resources?
if [[ ! -e /proc/sys/vm/drop_caches ]]; then
    echo "ERROR: Linux only"
    exit 1
fi
if [[ $EUID -ne 0 ]]; then
    echo "ERROR: must be run as root"
    exit 1
fi
sync
echo 3 > /proc/sys/vm/drop_caches
