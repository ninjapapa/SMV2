#!/bin/bash
# setup common environment used by smv-run and smv-shell scripts
#
# The following vars will be set once this script is sourced:
# SMV_ARGS : command line args (other than the spark args which are extracted separately)
# SPARK_ARGS : spark specific command line args (everything after "--" on command line)
# APP_JAR : user specified --jar option or the discovered application fat jar.
# SMV_USER_SCRIPT : optional user-defined launch script

# This function is used to split the command line arguments into SMV / Spark
# arguments.  Everything before "--" are considered SMV arguments and everything
# after "--" are considered spark arguments.
function split_smv_spark_args()
{
    while [ $# -ne 0 ]; do
        if [ "$1" == "--" ]; then
            shift
            break
        fi

        if [ "$1" == "--script" ]; then
          SMV_USER_SCRIPT="$2"
        fi

        if [ "$1" == "--spark-home" ]; then
          shift
          SPARK_HOME="$1"
          shift
        else
          SMV_ARGS=("${SMV_ARGS[@]}" "$1")
          shift
        fi
    done

    # Need to extract the --jars option so we can concatenate those jars with
    # the APP_JAR when we run the spark-submit. Spark will not accept 2 separate
    # --jars options. Also need to handle the case when user uses equal signs (--jars=xyz.jar)
    # Same for --driver-class-path
    while [ $# -ne 0 ]; do
        if [ "$1" == "--jars" ]; then
            shift
            EXTRA_JARS="$1"
            shift
        # See http://wiki.bash-hackers.org/syntax/pe#search_and_replace for bash string parsing
        # tricks
        elif [ ${1%%=*} == "--jars" ]; then
            local ACTUAL_JARS_PORTION="${1#*=}"
            EXTRA_JARS="${ACTUAL_JARS_PORTION}"
            # Only need to shift once since we dont have a space separator
            shift
        elif [ "$1" == "--driver-class-path" ]; then
            EXTRA_DRIVER_CLASSPATHS="$1"
        elif [ ${1%%=*} == "--driver-class-path" ]; then
            local ACTUAL_CLASSPATHS_PORTION="${1#*=}"
            EXTRA_DRIVER_CLASSPATHS="${ACTUAL_CLASSPATHS_PORTION}"
            # Only need to shift once since we dont have a space separator
            shift
        else
          SPARK_ARGS=("${SPARK_ARGS[@]}" "$1")
          shift
        fi
    done

}

function find_file_in_dir()
{
  local file_candidate=""
  filepat=$1
  shift
  for dir in "${@}"; do
    if [ -d $dir ]; then
      file_candidate=$(find $dir -maxdepth 1 -name "${filepat}")
      if [ -n "$file_candidate" ]; then
        echo "${file_candidate}"
        break
      fi
    fi
  done
}

function show_run_usage_message() {
  echo "USAGE: $1 [-h] <smv_app_args> [-- spark_args]"
  echo "smv_app_args:"
  echo "    [--data-dir dir] ..."
  echo "    <-m mod1 [mod2 ...] | -s stage1 [stage2 ...] | --run-app> ..."
  echo "    ..."
  echo "spark_args:"
  echo "    [--master master]"
  echo "    [--driver-memory=driver-mem]"
  echo "    ..."
}

# We intercept --help (and -h) so that we can make a simple spark-submit for the
# help message without running pyspark and creating a SparkContext
function check_help_option() {
  for opt in $SMV_ARGS; do
    if [ $opt = "--help" ] || [ $opt = "-h" ]; then
      show_run_usage_message `basename $0`
      exit 0
    fi
  done
}

# Remove trailing alphanum characters in dot-separated version text.
function sanitize_version () {
  # match a digit, followed by a letter, "+" or "_," and anything up to a "."
  # keep just the digit -- essentially removing any trailing alphanum between dots
  echo $(sed -E 's/([0-9])[_+a-zA-Z][^.]*/\1/g' <<< "$1")
}

function installed_spark_major_version() {
  local installed_version=$(${SPARK_HOME}/bin/spark-submit --version 2>&1 | \
    grep -v "Spark Command" | grep version | head -1 | sed -e 's/.*version //')
  local sanitized_version=$(sanitize_version $installed_version)
  export SPARK_MAJOR_VERSION="${sanitized_version:0:1}"
}

function find_dependent_jars() {
  if [ "$SPARK_MAJOR_VERSION" == "2" ]; then 
    DEPENDENT_JARS="${SMV_HOME}/jars/spark-xml_2.11-0.13.0.jar"
  elif [ "$SPARK_MAJOR_VERSION" == "3" ]; then
    DEPENDENT_JARS="${SMV_HOME}/jars/spark-xml_2.12-0.13.0.jar"
  else
    echo "Spark $SPARK_MAJOR_VERSION detected. Need either Spark 2 or Spark 3."
    exit 1
  fi
  export DEPENDENT_JARS
}

function run_pyspark_with () {
  # Suppress creation of .pyc files. These cause complications with
  # reloading code and have led to discovering deleted modules (#612)
  local PYTHONDONTWRITEBYTECODE=1
  local SPARK_PRINT_LAUNCH_COMMAND=1
  local SMV_LAUNCH_SCRIPT="${SMV_LAUNCH_SCRIPT:-${SPARK_HOME}/bin/spark-submit}"

  ( export PYTHONDONTWRITEBYTECODE SPARK_PRINT_LAUNCH_COMMAND PYTHONPATH; \
    "${SMV_LAUNCH_SCRIPT}" "${SPARK_ARGS[@]}" \
    --jars "$DEPENDENT_JARS,$EXTRA_JARS" \
    --driver-class-path "$EXTRA_DRIVER_CLASSPATHS" \
    $1 "${SMV_ARGS[@]}"
  )
}
# --- MAIN ---
declare -a SMV_ARGS SPARK_ARGS

split_smv_spark_args "$@"
check_help_option
installed_spark_major_version
find_dependent_jars
