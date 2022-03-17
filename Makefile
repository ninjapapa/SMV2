SHELL := /bin/bash

SPARKS_DIR := $(shell pwd)/.sparks

SPARK_VERSIONS := $(shell cat admin/.spark_to_test)
DEFAULT_SPARK := $(shell tail -1 admin/.spark_to_test)

SPARK_HOMES := $(addprefix $(SPARKS_DIR)/, $(SPARK_VERSIONS))
DEFAULT_SPARK_HOME := $(addprefix $(SPARKS_DIR)/, $(DEFAULT_SPARK))

PYTHON_VERSIONS := $(shell cat admin/.python_to_test)
DEFAULT_PYTHON_PATCH := $(shell tail -1 admin/.python_to_test)
DEFAULT_PYTHON_MAJOR := $(shell cut -c 1-3 <<< $(DEFAULT_PYTHON_PATCH))

SMV_VERSION := v$(shell cat .smv_version)

SMV_ROOT := $(shell pwd)

clean:
	rm -rf .tox
	rm -f $(BUNDLE_NAME)
	sbt clean

install: install-basic

install-basic: install-spark-default assemble-fat-jar

install-full: install-spark-all assemble-fat-jar

assemble-fat-jar: xml-jar
	sbt assembly

xml-jar: 
	curl -OL --progress-bar --fail https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.13.0/spark-xml_2.12-0.13.0.jar > spark-xml_2.12-0.13.0.jar
	curl -OL --progress-bar --fail https://repo1.maven.org/maven2/com/databricks/spark-xml_2.11/0.13.0/spark-xml_2.11-0.13.0.jar > spark-xml_2.11-0.13.0.jar
	mv spark-xml*jar jars

BUNDLE_NAME = smv2_$(SMV_VERSION).tgz
BUNDLE_PATH = docker/smv/$(BUNDLE_NAME)
BUNDLE_INCLUDE = LICENSE README.md docs log4j.properties releases setup.cfg setup.py jars src/main/python tools .supported_spark

local_bundle: xml-jar
	# cleanup some unneeded binary files.
	# use the `find ... -exec` variant instead of xargs
	# because we don't want `rm` to execute if `find` returns nothing
	find src -name '*.pyc' -exec rm -f \{\} +
	find src -name '__pycache__' -exec rm -rf \{\} +

	tar zcvf $(SMV_ROOT)/../$(BUNDLE_NAME) -C $(SMV_ROOT)/.. $(addprefix $(shell basename $(SMV_ROOT))/, $(BUNDLE_INCLUDE))
	mv $(SMV_ROOT)/../$(BUNDLE_NAME) $(SMV_ROOT)/$(BUNDLE_NAME)

# This will upload the bundle as "latest.tgz" to the CI release if we built on master.
# Required env:
#   ROBOT_CREDS : must be set to the D2D robot credentials
#   SEMAPHORE_GIT_BRANCH : branch that semaphore is currently building (set by semaphore automatically)
ci-upload-bundle:
	@(\
		if [ "${SEMAPHORE_GIT_BRANCH}" = "master" ]; then \
			ln -s smv_v*.tgz latest.tgz; \
			./admin/ci-latest-mgr -upload "${ROBOT_CREDS}"; \
		fi; \
	)

DOCKER_BASE_NAME = local-smv-base-$(SMV_VERSION)
DOCKER_SMV_NAME = local-smv-$(SMV_VERSION)
DOCKERFILE = docker/smv/Dockerfile

docker_base:
	docker build --build-arg PYTHON_VERSION=$(DEFAULT_PYTHON_PATCH) --target smv-build \
		-t $(DOCKER_BASE_NAME) -f $(DOCKERFILE) .

release_bundle: docker_base
	docker run -v $(shell pwd):/mount $(DOCKER_BASE_NAME) bash -c "cp /usr/lib/SMV/$(BUNDLE_NAME) /mount"

docker_smv: docker_base
	docker build --build-arg PYTHON_VERSION=$(DEFAULT_PYTHON_PATCH) -t $(DOCKER_SMV_NAME) --target smv -f $(DOCKERFILE) .

docker: docker_smv


DOC_DIR = docs

PYDOC_DEST := $(DOC_DIR)/python
SMV_PY_DIR := $(SMV_ROOT)/src/main/python
SPARK_PY_DIR := $(DEFAULT_SPARK_HOME)/python
PY4J_LIBS = $(shell find $(SPARK_PY_DIR)/lib -maxdepth 1 -mindepth 1 -name 'py4j*-src.zip' -print | tr -d '\r')

py-doc: $(PYDOC_DEST)

$(PYDOC_DEST): install-spark-default
	env SMV_VERSION=$(SMV_VERSION) \
		PYTHONPATH="$(SMV_PY_DIR):$(SPARK_PY_DIR):$(PY4J_LIBS)" \
		sphinx-apidoc --full -o $(PYDOC_DEST) $(SMV_PY_DIR)/smv
	cp admin/conf/sphinx-conf.py $(PYDOC_DEST)/conf.py
	cd $(PYDOC_DEST)
	cd $(PYDOC_DEST) && env SMV_VERSION=$(SMV_VERSION) \
		PYTHONPATH="$(SMV_PY_DIR):$(SPARK_PY_DIR):$(PY4J_LIBS)" \
		make html

SCALADOC_DEST = $(DOC_DIR)/scala

# install-spark x.y.z
# Easier to remember than the .sparks/x.y.z that we define below
INSTALL_SPARK_RULES = $(addprefix install-spark-, $(SPARK_VERSIONS))

$(INSTALL_SPARK_RULES) : install-spark-% : $(SPARKS_DIR)/%

# .sparks/x.y.z
$(SPARK_HOMES) : $(SPARKS_DIR)/% :
	mkdir -p .sparks
	bash tools/spark-install --spark-version $* --target-dir $(SPARKS_DIR)/$*

install-spark-default: install-spark-$(DEFAULT_SPARK)

install-spark-all: $(INSTALL_SPARK_RULES)


# Quick tests are sufficient for day-to-day dev
test: test-quick

# Run all the basic tests tests with the default Python and Spark
test-quick: test-python test-integration

test-python: install-basic
	tox -e $(DEFAULT_PYTHON_MAJOR) -- bash tools/smv-pytest --spark-home $(DEFAULT_SPARK_HOME)

test-integration: install-basic
	tox -e $(DEFAULT_PYTHON_MAJOR) -- bash src/test/scripts/run-integration-test.sh --spark-home $(DEFAULT_SPARK_HOME)

test-ingration-pip:
	tox -e $(DEFAULT_PYTHON_MAJOR) -- bash src/test/scripts/run-integration-test.sh --pip-install
