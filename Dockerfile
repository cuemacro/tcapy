# Set the base image to Ubuntu, use a public image
FROM python:3.7.7-slim-stretch as builder

# File Author / Maintainer
# MAINTAINER Thomas Schmelzer "thomas.schmelzer@gmail.com"

COPY requirements.txt /tmp/tcapy/requirements.txt

RUN buildDeps='gcc g++ libsnappy-dev unixodbc-dev' && \
    apt-get update && apt-get install -y $buildDeps --no-install-recommends && \
    pip install --no-cache-dir -r /tmp/tcapy/requirements.txt && \
    rm  /tmp/tcapy/requirements.txt
    # && \
    #apt-get purge -y --auto-remove $buildDeps


COPY ./tcapy /tcapy/tcapy

#### Here the test-configuration
FROM builder as test

# We install flask here to test some
RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx mongomock requests-mock

WORKDIR /tcapy

CMD py.test --cov=tcapy  --cov-report html:artifacts/html-coverage --cov-report term --html=artifacts/html-report/report.html test