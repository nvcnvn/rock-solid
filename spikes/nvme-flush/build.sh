#!/bin/sh
set -e
cd "$(dirname "$0")"
mkdir -p bin
cc -O2 -Wall -Wextra -o bin/flush flush.c -lpthread -lm
