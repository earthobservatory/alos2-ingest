FROM hysds/pge-gdal

MAINTAINER stchin "stchin@ntu.edu.sg"
LABEL description="ALOS-2 PGE container"

ARG id
ARG gid

USER root
RUN set -ex && \
 sudo pip install scipy --upgrade

# create work directory
RUN set -ex \
 && mkdir -p /data/work \
 && chmod -R 755 /data \
 && chown -R ops:ops /data

USER ops
# copy alos2-ingest
COPY . /home/ops/verdi/ops/alos2-ingest


# set entrypoint
WORKDIR /home/ops
CMD ["/bin/bash", "--login"]
