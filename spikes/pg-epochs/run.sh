#!/bin/sh
# Runs the model into results.txt, about 2 minutes. Needs python3, no root.
cd "$(dirname "$0")"
{
	echo "# $(uname -sr), python $(python3 -c 'import sys; print(sys.version.split()[0])'), rock-solid $(git -C ../.. rev-parse --short HEAD), $(date -u +%Y-%m-%dT%H:%MZ)"
	python3 model.py "$@"
} | tee results.txt
