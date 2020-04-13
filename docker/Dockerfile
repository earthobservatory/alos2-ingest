
FROM hysds/isce2:latest

MAINTAINER aria-ops "aria-ops@list.jpl.nasa.gov"
LABEL description="ALOS-2 PGE container"

USER ops
# copy alos2-ingest
COPY . /home/ops/verdi/ops/alos2-ingest

# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
