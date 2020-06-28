FROM tschm/jupyter as builder

# File Author / Maintainer
# MAINTAINER Thomas Schmelzer "thomas.schmelzer@gmail.com"
ENV HOME /home/${NB_USER}
ENV APP_ENV docker

COPY --chown=jovyan:users  . /tmp/tcapy

COPY . ${HOME}
USER root
RUN chown -R ${NB_UID} ${HOME}

# Install snappy for pystore and ODBC
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsnappy-dev gcc g++ unixodbc-dev

# Switch back to jovyan to avoid accidental container runs as root
USER $NB_UID

RUN pip install --no-cache-dir /tmp/tcapy && \
    rm -rf /tmp/tcapy

RUN ln -s /home/${NB_USER}/tcapy_notebooks /home/${NB_USER}/work/tcapy_notebooks