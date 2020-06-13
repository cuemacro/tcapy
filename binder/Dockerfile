FROM tschm/jupyter as builder

# File Author / Maintainer
# MAINTAINER Thomas Schmelzer "thomas.schmelzer@gmail.com"

COPY --chown=jovyan:users  . /tmp/tcapy

COPY . ${HOME}
USER root
RUN chown -R ${NB_UID} ${HOME}

# Install snappy for pystore
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsnappy-dev

# Switch back to jovyan to avoid accidental container runs as root
USER $NB_UID

RUN pip install --no-cache-dir /tmp/tcapy && \
    rm -rf /tmp/tcapy