#!/bin/sh
set -e
chmod +x /application/confd/bin/confd
/application/confd/bin/confd -confdir="/application/confd" -onetime -backend env

case "$1" in
    master)
        exec node --max-old-space-size=${MAX_OLD_SPACE_SIZE} server.js
        #exec node server.js -loglevel silent
        ;;
    worker)
        exec node --max-old-space-size=${MAX_OLD_SPACE_SIZE} worker.js
        #exec node worker.js -loglevel silent
        ;;
    bash)
        sh
        ;;
    *)
        echo $"Usage: {master|worker|bash}"
        exit 1
    ;;
esac
