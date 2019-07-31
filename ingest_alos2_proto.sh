#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# export GDAL env variables (adapted from isce.sh)
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

${BASE_PATH}/ingest_alos2_proto.py > ingest_alos2.log