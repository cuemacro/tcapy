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
COPY ./tcapygen /tcapy/tcapygen
COPY ./test /tcapy/test

# Make sure tcapy on the PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/tcapy"

#### Here's the test-configuration
FROM builder as test

# We install some extra libraries purely for testing
RUN pip install --no-cache-dir httpretty pytest pytest-cov pytest-html sphinx mongomock requests-mock

WORKDIR /tcapy

# For temp caching for the tests
RUN mkdir -p /tmp/csv
RUN mkdir -p /tmp/tcapy

# Run the pytest
# If CI is true, we're running on GitHub CI, so avoid multithreaded tests which runs out of memory
# We can run the multithreading tests locally
CMD if [ "${CI}" == "true" ]; \
    then py.test --cov=tcapy  --cov-report html:artifacts/html-coverage --cov-report term \
        --html=artifacts/html-report/report.html --ignore=test/test_tcapy/test_tca_multithreading.py test; \
    else py.test --cov=tcapy  --cov-report html:artifacts/html-coverage --cov-report term \
        --html=artifacts/html-report/report.html test; \
    fi

# Example to run a specific test script
#CMD py.test --cov=tcapy  --cov-report html:artifacts/html-coverage --cov-report term \
#    --html=artifacts/html-report/report.html test/test_tcapy/test_tca_multithreading.py

# Example to run an individual test function
# CMD py.test --cov=tcapy  --cov-report html:artifacts/html-coverage --cov-report term \
#    --html=artifacts/html-report/report.html test/test_tcapy/test_data_read_write.py::test_write_trade_data_sql

# For debugging to keep container going
# CMD tail -f /dev/null
