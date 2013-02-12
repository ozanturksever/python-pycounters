#!/bin/bash

case "$1" in
    remove)
        ensure_stop_service
        find /usr/local/lib/python2.7/dist-packages/pycounters/ -name *.pyc -exec rm {} \;
    ;;
esac
