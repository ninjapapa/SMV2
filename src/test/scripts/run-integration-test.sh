#!/bin/bash

set -e

TEST_DIR=target/test-sample
S_APP_NAME=SimpleApp

HASH_TEST_MOD="integration.test.hashtest.modules.M"

function parse_args() {
  local _spark_home="${SPARK_HOME:-}"

  for arg in "${@}"; do
    if [ "${arg}" == "--spark-home" ]; then
      shift
      _spark_home="${1}"
      shift
    fi
  done

  # In insatlling in pip, make sure eo emulate a user environment without a spark home
  export SPARK_HOME
  SPARK_HOME=$(cd "${_spark_home}"; pwd)
  echo "Using Spark installation at ${SPARK_HOME:? Expected SPARK_HOME to have been set}"
}

function verify_test_context() {
  echo "Using SMV_HOME of: ${SMV_HOME:-No SMV_HOME set}"

  if [ ! -z "${SMV_HOME}" ] && [ ! -d "${SMV_HOME}/src/test/scripts" ]; then
    echo "Must run this script from top level SMV directory"
    exit 1
  fi

  SMV_TOOLS="${SMV_HOME}/tools"
  SMV_INIT="${SMV_TOOLS}/smv-init"

  echo "Using SMV_INIT of ${SMV_INIT}"
  echo "Using SMV_RUN of ${SMV_RUN}"
}

function enter_clean_test_dir() {
  rm -rf ${TEST_DIR}
  mkdir -p ${TEST_DIR}
  cd ${TEST_DIR}
}

function execute_in_dir() {
  target_dir="${1}"
  func_to_exec="${2}"
  (
    cd "${target_dir}"
    ${func_to_exec}
  )
}

function count_output() {
  echo $(wc -l <<< "$(ls -d data/output/*.csv)")
}

function verify_hash_unchanged() {
  if [ $(count_output) -gt 1 ]; then
    echo "Unchanged module's hashOfHash changed"
    exit 1
  fi
}

function verify_hash_changed() {
  previous_num_outputs="$1"
  if [ $(count_output) -le "$previous_num_outputs" ]; then
    echo "Changed module's hashOfHash didn't change"
    exit 1
  fi
}

function test_simple_app() {
  echo "--------- GENERATE SIMPLE APP -------------"
  ${SMV_INIT} $S_APP_NAME

  echo "--------- RUN SIMPLE APP -------------"
  execute_in_dir "$S_APP_NAME" "${SPARK_HOME}/bin/spark-submit src/main/python/appdriver.py -- --run-app"
}

echo "--------- INTEGRATION TEST BEGIN -------------"

parse_args $@
verify_test_context
enter_clean_test_dir
test_simple_app

echo "--------- INTEGRATION TEST COMPLETE -------------"

