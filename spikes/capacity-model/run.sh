#!/bin/sh
# Runs every model test into results.txt, about 40 s. Needs python3 and numpy, no root.
cd "$(dirname "$0")"
{
	echo "# $(uname -sr), $(python3 -c 'import sys, numpy; print("python", sys.version.split()[0], "numpy", numpy.__version__)'), $(date -u +%Y-%m-%dT%H:%MZ)"
	python3 model.py "$@"
} | tee results.txt
