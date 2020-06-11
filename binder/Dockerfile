# Set the base image to Ubuntu, use a public image
FROM tschm/jupyter as builder

# File Author / Maintainer
MAINTAINER Thomas Schmelzer "thomas.schmelzer@gmail.com"

#COPY requirements.txt /tmp/antarctic/requirements.txt
COPY --chown=jovyan:users  . /tmp/tcapy

#RUN buildDeps='gcc g++' && \
#    apt-get update && apt-get install -y $buildDeps --no-install-recommends && \
RUN pip install --no-cache-dir /tmp/tcapy && \
    #pip install --no-cache-dir mongomock && \
    rm  -rf /tmp/tcapy