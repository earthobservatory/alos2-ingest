#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)
source /opt/isce2/isce_env.sh

# export GDAL env variables (adapted from isce.sh)
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

source /opt/isce2/isce_env.sh && ${BASE_PATH}/$1 > $1.log 2>&1