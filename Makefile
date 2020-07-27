#!make
PROJECT_VERSION := $(shell python setup.py --version)

SHELL := /bin/bash
PACKAGE := tcapy

.PHONY: help build jupyter test doc tag


.DEFAULT: help

help:
	@echo "make build"
	@echo "       Build the docker image."
	@echo "make test"
	@echo "       Build the docker image for testing and run them."
	@echo "make test_local"
	@echo "       Build the docker image for testing and run them on a local environment/settings."
	@echo "make doc"
	@echo "       Construct the documentation."
	@echo "make tag"
	@echo "       Make a tag on Github."



build:
	docker-compose build tcapy

jupyter:
	docker-compose build jupyter

test:
	docker-compose -f docker-compose.test.yml run sut

test_local:
	docker-compose -f docker-compose.local.test.yml run sut

doc:
	docker-compose -f docker-compose.test.yml run sut sphinx-build /source artifacts/build

tag:
	git tag -a ${PROJECT_VERSION} -m "new tag"
	git push --tags

