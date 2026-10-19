#!/bin/sh
set -e
cd "$(dirname "$0")"
mkdir -p bin
cc -O2 -Wall -o bin/uring uring.c
