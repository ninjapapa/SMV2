#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Helper functions available in SMV's Python shell
"""
import sys
import re
import datetime

from smv import SmvApp
from smv.smvappinfo import SmvAppInfo
from smv.conn import SmvHdfsEmptyConn
from smv.iomod import SmvCsvInputFile
from smv.smviostrategy import SmvSchemaOnHdfsIoStrategy, SmvCsvOnHdfsIoStrategy
from smv.smvschema import SmvSchema
from smv.csv_attributes import CsvAttributes
from test_support.test_runner import SmvTestRunner
from test_support.testconfig import TestConfig


def _appInfo():
    return SmvAppInfo(SmvApp.getInstance())

def quickRun(name):
    """Run module and return result.
        No persist, but use existing persisted if possible.
        No DQM
    """
    return SmvApp.getInstance().runModuleByName(name, forceRun=False, quickRun=True)[0]

def fullRun(name):
    """Run module and return result.
        Persist and run DQM if given
    """
    return SmvApp.getInstance().runModuleByName(name, forceRun=False, quickRun=False)[0]

def df(name, forceRun=False, quickRun=True):
    """The DataFrame result of running the named module

        Args:
            name (str): The unique name of a module. Does not have to be the FQN.
            forceRun (bool): True if the module should be forced to run even if it has persisted output. False otherwise.
            quickRun (bool): skip computing dqm+metadata and persisting csv

        Returns:
            (DataFrame): The result of running the named module.
    """
    return SmvApp.getInstance().runModuleByName(name, forceRun, quickRun)[0]

def props():
    """The current app propertied used by SMV after the app, user, command-line
        and dynamic props are merged.

        Returns:
            (dict): The 'mergedProps' or final props used by SMV
    """
    return SmvApp.getInstance().getCurrentProperties()

def dshash(name):
    """The current hashOfHash for the named module as a hex string

        Args:
            name (str): The uniquen name of a module. Does not have to be the FQN.
            runConfig (dict): runConfig to apply when collecting info. If module
                              was run with a config, the same config needs to be
                              specified here to retrieve the correct hash.

        Returns:
            (int): The hashOfHash of the named module
    """
    return SmvApp.getInstance().getDsHash(name)

def getModel(name, forceRun = False):
    """Get the result of running the named SmvModel module

        Args:
            name (str): The name of a module. Does not have to be the FQN.
            forceRun (bool): True if the module should be forced to run even if it has persisted output. False otherwise.
            version (str): The name of the published version to load from

        Returns:
            (object): The result of running the named module
    """
    app = SmvApp.getInstance()
    fqn = app.dsm.inferFqn(name)
    return app.getModuleResult(fqn, forceRun)

def openCsv(path):
    """Read in a CSV file as a DataFrame

        Args:
            path (str): The path of the CSV file
            validate (bool): If true, validate the CSV before return DataFrame (raise error if malformatted)

        Returns:
            (DataFrame): The resulting DataFrame
    """
    app = SmvApp.getInstance()
    class TmpCsv(SmvCsvInputFile):
        def connectionName(self):
            return None

        def get_connection(self):
            return SmvHdfsEmptyConn

        def fileName(self):
            return path

        def csvReaderMode(self):
            return "DROPMALFORMED"

    return TmpCsv(app).doRun(None)

def saveCsv(df, path):
    """Save DF as a CSV file with header (single partition for easy sharing)
    
        Args:
            df (DataFrame): data to be saved
            path (string): CSV file path. A folder will be created with that path and a single CSV partition will be 
            stored in

        Return:
            (None)
    """
    schema = SmvSchema(df.schema).addCsvAttributes(CsvAttributes(hasHeader = "true"))
    data = df.coalesce(1)
    smvApp = SmvApp.getInstance()
    SmvCsvOnHdfsIoStrategy(smvApp, path, schema, "FAILFAST", "overwrite").write(data)


def lsStage():
    """List all the stages
    """
    print(_appInfo().ls_stage())

def ls(stageName = None):
    """List all datasets in a stage

        Args:
            stageName (str): The name of the stage. Defaults to None, in which ase all datasets in all stages will be listed.
    """
    print(_appInfo().ls(stageName))

def lsDead(stageName = None):
    """List dead datasets in a stage

        Args:
            stageName (str): The name of the stage. Defaults to None, in which ase all datasets in all stages will be listed.
    """
    print(_appInfo().ls_dead(stageName))

def ancestors(dsname):
    """List all ancestors of a dataset

        Ancestors of a dataset are the dataset it depends on, directly or
        in-directly, including datasets from other stages.

        Args:
            dsname (str): The name of an SmvGenericModule
    """
    print(_appInfo().ls_ancestors(dsname))

def descendants(dsname):
    """List all descendants of a dataset

        Descendants of a dataset are the datasets which depend on it directly or
        in-directly, including datasets from other stages

        Args:
            dsname (str): The name of an SmvGenericModule
    """
    print(_appInfo().ls_descendants(dsname))

def create_graph(dotfile=None):
    """Create dependency graph in dot format
    """
    dot_graph_str = _appInfo().create_graph_dot()
    if (dotfile is None):
        appName = SmvApp.getInstance().appName()
        path = "{}.dot".format(appName.replace(" ", "_"))
    else:
        path = dotfile
    with open(path, "w") as f:
        f.write(dot_graph_str)


def purge_persisted():
    """Remove current persisted data files, so when re-run will generate from input.

        Will only remove "current" version of persisted files. Historical persisted files (no more current) will
        not be removed. User need to do that from OS.
    """
    smvApp = SmvApp.getInstance()
    mods_to_run = smvApp.dsm.modulesToRun([], [], True)
    smvApp._purge_current_output_files(mods_to_run)


def now():
    """Print current time
    """
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def smvDiscoverSchema(path, attrs = {}):
    """Try best to discover Schema from raw Csv file

        Print infered Schema on console

        Args:
            path (str): Path to the CSV file
            ca (CsvAttributes): Defaults to CsvWithHeader

        Example:
            smvDiscoverSchema("data/input/user.csv", {"delimiter": "\\t"})
    """
    smvSchema = SmvApp.getInstance().discoverSchema(path, attrs)
    print(smvSchema.toStrForFile())

def smvDiscoverSchemaToFile(path, attrs = {}):
    """Try best to discover Schema from raw Csv file

        Will save a schema file with postfix ".toBeReviewed" in local directory.

        Args:
            path (str): Path to the CSV file
            ca (CsvAttributes): Defaults to CsvWithHeader

        Example:
            smvDiscoverSchemaToFile("data/input/user.csv", {"delimiter": "\\t"})
    """
    app = SmvApp.getInstance()
    schema_path = re.sub(r"(?i)csv", "schema.toBeReviewed", path)
    smvSchema = app.discoverSchema(path, attrs)
    SmvSchemaOnHdfsIoStrategy(app, schema_path).write(smvSchema)

def run_test(test_name):
    """Run a test with the given name without creating new Spark context

        First reloads SMV and the test from source, then runs the test.

        Args:
            test_name (str): Name of the test to run
    """
    # Ensure TestConfig has a canonical SmvApp (this will eventually be used
    # to restore the singleton SmvApp)
    TestConfig.setSmvApp(SmvApp.getInstance())

    first_dot = test_name.find(".")
    if first_dot == -1:
        test_root_name = test_name
    else:
        test_root_name = test_name[:first_dot]

    _clear_from_sys_modules(["smv", test_root_name])

    SmvTestRunner("src/test/python").run([test_name])

def _clear_from_sys_modules(names_to_clear):
    """Clear smv and the given names from sys.modules (don't clear this module)
    """
    all_mods = sys.modules.keys()
    for ntc in names_to_clear:
        for name in all_mods:
            if name != "smv.smvshell" and (name.startswith(ntc + ".") or name == ntc):
                sys.modules.pop(name)
                break

def show_run_info(collector):
    """Inspects the SmvRunInfoCollector object returned by smvApp.runModule"""
    collector.show_report()

def get_run_info(name, runConfig=None):
    """Get the SmvRunInfoCollector with full information about a module and its dependencies

        Args:
            name (str): name of the module whose information to collect
            runConfig (dict): runConfig to apply when collecting info. If module
                              was run with a config, the same config needs to be
                              specified here to retrieve the info.
    """
    return SmvApp.getInstance().getRunInfoByPartialName(name, runConfig)

__all__ = [
    'quickRun',
    'fullRun',
    'df',
    'dshash',
    'getModel',
    'openCsv',
    'saveCsv',
    'lsStage',
    'ls',
    'lsDead',
    'props',
    'ancestors',
    'descendants',
    'purge_persisted',
    'now',
    'smvDiscoverSchema',
    'smvDiscoverSchemaToFile',
    'run_test',
    'create_graph',
    'show_run_info',
    'get_run_info'
]
