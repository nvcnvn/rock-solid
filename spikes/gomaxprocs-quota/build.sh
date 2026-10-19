#!/bin/sh
set -e
cd "$(dirname "$0")"
mkdir -p bin
cc -O2 -Wall -o bin/gen gen.c
go build -o bin/server ./server
