#!/bin/sh
set -e

chmod +x /application/confd/bin/confd
/application/confd/bin/confd -confdir="/application/confd" -onetime -backend env

cd /usr/src/app/postprocessor
npm start
