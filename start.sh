#!/bin/bash
sed 's/^/export /g' cfg/build.ini > cfg/build.sh && source cfg/build.sh
docker-compose up -d --build
sed 's/^/unset /g' cfg/build.ini > cfg/build.sh && source cfg/build.sh