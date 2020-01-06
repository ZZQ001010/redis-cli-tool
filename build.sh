#!/usr/bin/env bash
PRJ=`pwd`
cd $PRJ/src
GOOS=darwin go build -o $PRJ/bin/redis_cli_mac
GOOS=linux go build -o $PRJ/bin/redis_cli_linux
GOOS=windows  go build -o $PRJ/bin/redis_cli.exe
