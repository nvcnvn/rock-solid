# Source me: meson/ninja from vendor/pydeps (pip --target, no root).
V=$(cd "$(dirname "${BASH_SOURCE:-$0}")" && pwd)/vendor
export PATH=$V/pydeps/bin:$PATH
export PYTHONPATH=$V/pydeps
