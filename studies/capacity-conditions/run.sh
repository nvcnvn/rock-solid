#!/bin/sh
# Runs every test of the conditions model into results.txt, about 2 minutes. Needs python3 only.
cd "$(dirname "$0")"
{
	echo "# $(uname -sr), python $(python3 -c 'import sys; print(sys.version.split()[0])'), $(date -u +%Y-%m-%dT%H:%MZ)"
	python3 -I conditions.py "$@"
} | tee results.txt
