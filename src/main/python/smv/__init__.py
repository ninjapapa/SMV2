# flake8: noqa
# Smv DataSet Framework
from smv.smvmodule import *

from smv.smvapp import SmvApp

from smv.runconfig import SmvRunConfig
from smv.csv_attributes import CsvAttributes

import logging
logger = logging.getLogger(__name__)
# Default logging settings
# These may be overridden when SmvApp initializes
log_formatter = logging.Formatter(
    fmt='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S')
stderr_log_appender = logging.StreamHandler()
stderr_log_appender.setFormatter(log_formatter)
logger.addHandler(stderr_log_appender)
logger.setLevel("INFO")
