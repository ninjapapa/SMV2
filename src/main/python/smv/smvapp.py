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

"""SmvApp entry class
This module provides the main SMV Python entry point ``SmvPy`` class and a singleton `smvApp`.
"""
import os
import sys
import re
import pkgutil
from collections import namedtuple
from smv.csv_attributes import CsvAttributes

from pyspark.java_gateway import launch_gateway

from smv.datasetmgr import DataSetMgr
from smv.smvappinfo import SmvAppInfo
from smv.datasetrepo import DataSetRepoFactory, DataSetRepo
from smv.error import SmvRuntimeError
import smv.helpers
from smv.modulesvisitor import ModulesVisitor
from smv.smvmodulerunner import SmvModuleRunner
from smv.smviostrategy import SmvJsonOnHdfsPersistenceStrategy
from smv.smvmetadata import SmvMetaHistory
from smv.smvhdfs import SmvHDFS
from smv.smvschema import SmvSchema
from smv.utils import is_string
from py4j.protocol import Py4JJavaError



class SmvApp(object):
    """The Python representation of SMV.

    Its singleton instance is created later in the containing module
    and is named smvApp

    Adds `java_imports` to the namespace in the JVM gateway in
    SparkContext (in pyspark).  It also creates an instance of
    SmvPyClient.

    """

    # Singleton instance of SmvApp
    _instance = None

    # default rel path for python sources from appDir
    SRC_PROJECT_PATH = "src/main/python"
    # default location for py UDL's in smv projects
    SRC_LIB_PATH = "library"
    # prefix which under SRC_LIB_PATH but should be excluded from userlib
    SEMI_PRIVATE_LIB_PREFIX = "dsalib"

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            raise SmvRuntimeError("An instance of SmvApp has not been created")
        else:
            return cls._instance

    @classmethod
    def createInstance(cls, smvconf, _sparkSession, py_module_hotload=True):
        """Create singleton instance. Also returns the instance.
        """
        cls._instance = cls(smvconf, _sparkSession, py_module_hotload)
        return cls._instance

    @classmethod
    def setInstance(cls, app):
        """Set the singleton instance.
        """
        cls._instance = app

    def __init__(self, smvconf, _sparkSession, py_module_hotload=True):
        self.smvHome = os.environ.get("SMV_HOME")
        if (self.smvHome is None):
            raise SmvRuntimeError("SMV_HOME env variable not set!")

        self.sparkSession = _sparkSession

        if (self.sparkSession is not None):
            sc = self.sparkSession.sparkContext
            sc.setLogLevel("ERROR")

            self.sc = sc
            # self.sqlContext = self.sparkSession._wrapped
            self._jvm = sc._jvm
        else:
            _gw = launch_gateway(None)
            self._jvm = _gw.jvm

        self.py_module_hotload = py_module_hotload
        self.py_smvconf = smvconf

        # CmdLine is static, so can be an attribute
        cl = self.py_smvconf.cmdline
        self.cmd_line = namedtuple("CmdLine", cl.keys())(*cl.values())

        # shortcut is meant for internal use only
        self.dsm = DataSetMgr(self.py_smvconf)

        # computed df cache, keyed by m.versioned_fqn
        self.data_cache = {}

        # AFTER app is available but BEFORE stages,
        # use the dynamically configured app dir to set the source path, library path
        self.prependDefaultDirs()

        self.repoFactory = DataSetRepoFactory(self)
        self.dsm.register(self.repoFactory)

        # provider cache, keyed by providers' fqn
        self.provider_cache = {}
        self.refresh_provider_cache()

        # Initialize DataFrame and Column with helper methods
        smv.helpers.init_helpers()

    def smvVersion(self):
        versionFile = self.smvHome + "/.smv_version"
        with open(versionFile, "r") as fp:
            line = fp.readline()
        return line.strip()

    def exception_handling(func):
        """ Decorator function to catch Py4JJavaError and raise.
            Otherwise just pass through the original exception
        """
        def func_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Py4JJavaError as e:
                raise e
        return func_wrapper

    def prependDefaultDirs(self):
        """ Ensure that mods in src/main/python and library/ are discoverable.
            If we add more default dirs, we'll make this a set
        """
        self.prepend_source(self.SRC_LIB_PATH)
        self.prepend_source(self.SRC_PROJECT_PATH)

    def removeDefaultDirs(self):
        """ The cleanup version of prependDefaultDirs
        """
        self.remove_source(self.SRC_PROJECT_PATH)
        self.remove_source(self.SRC_LIB_PATH)

    def all_data_dirs(self):
        """ All the config data dirs as an object.
            Could be dynamic, so calculate each time when use
        """
        dds = self.py_smvconf.all_data_dirs()
        return namedtuple("DataDirs", dds.keys())(*dds.values())

    def output_path_from_base(self, basename, postfix = None):
        """Add outputpather base file name"""
        output_dir = self.all_data_dirs().outputDir
        if postfix is None:
            file_path = "{}/{}".format(output_dir, basename)
        else:
            file_path = "{}/{}.{}".format(output_dir, basename, postfix)
        return file_path

    def appName(self):
        # get name from SmvConf, if none, get from sparksession
        if (self.py_smvconf is not None):
            name = self.py_smvconf.app_name()
        elif (self.sparkSession is not None):
            name = self.sc.getConf().get("spark.app.name")
        return name

    def appDir(self):
        return self.py_smvconf.app_dir

    def jdbcUrl(self):
        res = self.py_smvconf.merged_props().get('smv.jdbc.url')
        if (res is None):
            raise SmvRuntimeError("JDBC url not specified in SMV config")
        return res

    def jdbcDriver(self):
        res = self.py_smvconf.merged_props().get('smv.jdbc.driver')
        if (res is None):
            raise SmvRuntimeError("JDBC driver is not specified in SMV config")
        return res

    def getDynamicRunConf(self, key):
        return self.py_smvconf.get_run_config(key)

    def setAppDir(self, appDir):
        """ SMV's equivalent of 'cd' for app dirs. """
        self.removeDefaultDirs()

        # the app properties to be read from disk and reevaluated
        self.py_smvconf.set_app_dir(appDir)

        smv.logger.info("Set app dir to " + appDir)
        smv.logger.debug("Current SMV configuration: {}".format(self.py_smvconf.merged_props()))

        # this call will use the dynamic appDir that we just set ^
        # to change sys.path, allowing py modules, UDL's to be discovered by python
        self.prependDefaultDirs()

        # As switch app dir, need to re-discover providers
        self.refresh_provider_cache()

    def setDynamicRunConfig(self, runConfig):
        self.py_smvconf.set_dynamic_props(runConfig)

    def getCurrentProperties(self):
        """ Python dict of current megred props
            defaultProps ++ appConfProps ++ homeConfProps ++ usrConfProps ++ cmdLineProps ++ dynamicRunConfig
            Where right wins out in map merge.
        """
        return self.py_smvconf.merged_props()

    def stages(self):
        """Stages is a function as they can be set dynamically on an SmvApp instance"""
        return self.py_smvconf.stage_names()

    def _libsInDir(self, lib_dir, prefix):
        """Use introspection to determine list of availabe libs in a given directory."""
        lib_walker = pkgutil.walk_packages([lib_dir], prefix)
        lib_names = [name for (_, name, is_pkg) in lib_walker if not is_pkg]
        return lib_names

    def userLibs(self):
        """Use introspection to determine list of availabe user libs."""
        lib_dir = os.path.join(self.appDir(), self.SRC_LIB_PATH)
        return [i for i in self._libsInDir(lib_dir, "") if not i.startswith(self.SEMI_PRIVATE_LIB_PREFIX)]

    def semiLibs(self):
        """Use introspection to determine list of availabe semiprivate libs."""
        lib_dir = os.path.join(self.appDir(), self.SRC_LIB_PATH, self.SEMI_PRIVATE_LIB_PREFIX)
        return self._libsInDir(lib_dir, self.SEMI_PRIVATE_LIB_PREFIX + ".")

    def smvLibs(self):
        """Use introspection to determine list of availabe smv builtin libs."""
        # Note: since sys.path has the src/main/python, we need to prefix all discovered
        # objects by `smv.` so that they are importable.
        lib_dir = os.path.join(self.smvHome, "src/main/python/smv")
        return self._libsInDir(lib_dir, "smv.")

    def refresh_provider_cache(self):
        """Re-discover providers and set provider_cache"""
        self.provider_cache = DataSetRepo(self)._all_providers()

    def get_providers_by_prefix(self, fqn_prefix):
        """Discover all providers in user lib and smv lib that have an provider type fqn
           starting with the given prefix.
        """
        providers = self.provider_cache
        return {fqn:p for (fqn, p) in providers.items() if fqn.startswith(fqn_prefix)}

    def get_provider_by_fqn(self, fqn):
        """Return provider class from provider name fqn
        """
        providers = self.provider_cache
        return providers.get(fqn)

    def get_connection_by_name(self, name):
        """Get connection instance from name
        """
        props = self.py_smvconf.merged_props()
        type_name = "smv.conn.{}.type".format(name)

        if (type_name in props):
            con_type = props.get(type_name)
            provider_fqn = "conn.{}".format(con_type)
            ConnClass = self.get_provider_by_fqn(provider_fqn)
            return ConnClass(name, props)
        else:
            raise SmvRuntimeError("Connection name {} is not configured with a type".format(name))

    def get_all_connection_names(self):
        """Get all connetions defined in the app, return a list of names
        """
        props = self.py_smvconf.merged_props()
        res = []
        for k in props:
            s = re.search('smv\.conn\.(.*)\.type', k)
            if s:
                name = s.group(1)  # matched in '(.*)'
                res.append(name)
        return res

    def appId(self):
        return self.py_smvconf.app_id()

    def get_graph_json(self):
        """Generate a json string representing the dependency graph.
        """
        return SmvAppInfo(self).create_graph_json()

    def get_module_state_json(self, fqns):
        """Generate a json string for modules' needToRun state of the app

            Args:
                fqns (list(string)): module fqn list to get state for

            Return:
                (string): json string. E.g. {"stage.mymod": {"needsToRun" : True}}
        """
        return SmvAppInfo(self).create_module_state_json(fqns)

    def getModuleResult(self, fqn, forceRun=False):
        """Run module and get its result, which may not be a DataFrame
        """
        df, collector = self.runModule(fqn, forceRun)
        return df

    def load_single_ds(self, fqn):
        """Return ds from fqn"""
        return self.dsm.load(fqn)[0]

    def _to_single_run_res(self, res):
        """run and quick_run of SmvModuleRunner, returns (list(DF), SmvRunInfoCollector)
            for a single module, convert to (DF, SmvRunInfoCollector)"""
        (dfs, coll) = res
        return (dfs[0], coll)

    @exception_handling
    def runModule(self, fqn, forceRun=False, quickRun=False, runMonitorCallback=None):
        """Runs SmvModule by its Fully Qualified Name(fqn)

        Args:
            fqn (str): The FQN of a module
            forceRun (bool): True if the module should be forced to run even if it has persisted output. False otherwise.
            quickRun (bool): skip computing dqm+metadata and persisting csv
            runMonitorCallback: when is not quick run, runner will call this with ({fqn, statues})

        Example:
            To get just the dataframe of the module:
                dataframe = smvApp.runModule('package.module.SmvModuleClass')[0]
            To get both the dataframe and the run info collector:
                dataframe, collector = smvApp.runModule('package.module.SmvModuleClass')

        Returns:
            (DataFrame, SmvRunInfoCollector) tuple
            - DataFrame is the computed result of the module
            - SmvRunInfoCollector contains additional information
              about the run, such as validation results.
        """
        ds = self.dsm.load(fqn)[0]

        if (quickRun):
            return self._to_single_run_res(SmvModuleRunner([ds], self).quick_run(forceRun))
        else:
            return self._to_single_run_res(SmvModuleRunner([ds], self, runMonitorCallback).run(forceRun))

    @exception_handling
    def quickRunModule(self, fqn):
        return self.runModule(fqn, forceRun=False, quickRun=True)

    @exception_handling
    def runModuleByName(self, name, forceRun=False, quickRun=False, runMonitorCallback=None):
        """Runs a SmvModule by its name (can be partial FQN)

        See the `runModule` method above

        Args:
            name (str): The unique name of a module. Does not have to be the FQN.
            forceRun (bool): True if the module should be forced to run even if it has persisted output. False otherwise.
            quickRun (bool): skip computing dqm+metadata and persisting csv
            runMonitorCallback: if is not quick run, runner will call this with ({fqn, statues})

        Returns:
            (DataFrame, SmvRunInfoCollector) tuple
            - DataFrame is the computed result of the module
            - SmvRunInfoCollector contains additional information
              about the run, such as validation results.
        """
        fqn = self.dsm.inferFqn(name)
        return self.runModule(fqn, forceRun, quickRun, runMonitorCallback)

    def get_need_to_run(self, roots, keep_roots=False):
        """Given a list of target modules to run, return a list of modules which
            will run and be persisted in the order of how they should run. This
            is a sub-set of modules_needed_for_run, but only keep the
            non-ephemeral and not-persisted-yet modules.
            Please note that some of the roots may not be in this list, to include
            all roots, set `keep_roots` to True
        """
        visitor = ModulesVisitor(roots)
        return [m for m in visitor.modules_needed_for_run
            if ((not m._is_persisted() and not m.isEphemeral())
                or (keep_roots and m in roots))
        ]

    def getRunInfo(self, fqn):
        """Returns the run information of a module and all its dependencies
        from the last run.

        Unlike the runModule() method, which returns the run
        information just for that run, this method returns the run
        information from the last run.

        If no module was run (e.g. the code did not change, so the
        data is read from persistent storage), the SmRunInfoCollector
        returned from the runModule() method would be empty.  But the
        SmvRunInfoCollector returned from this method would contain
        all latest run information about all dependent modules.

        Args:
            fqn (str): fqn of target module
            runConfig (dict): runConfig to apply when collecting info. If module
                              was run with a config, the same config needs to be
                              specified here to retrieve the info.

        Returns:
            SmvRunInfoCollector

        """
        ds = self.dsm.load(fqn)[0]
        return SmvModuleRunner([ds], self).get_runinfo()

    def getRunInfoByPartialName(self, name):
        """Returns the run information of a module and all its dependencies
        from the last run.

        Unlike the runModule() method, which returns the run
        information just for that run, this method returns the run
        information from the last run.

        If no module was run (e.g. the code did not change, so the
        data is read from persistent storage), the SmRunInfoCollector
        returned from the runModule() method would be empty.  But the
        SmvRunInfoCollector returned from this method would contain
        all latest run information about all dependent modules.

        Args:
            name (str): unique suffix to fqn of target module

        Returns:
            SmvRunInfoCollector
        """
        fqn = self.dsm.inferFqn(name)
        return self.getRunInfo(fqn)

    def getMetadataJson(self, fqn):
        """Returns the metadata for a given fqn"""
        ds = self.load_single_ds(fqn)
        return ds._get_metadata().toJson()

    def getMetadataHistoryJson(self, fqn):
        """Returns the metadata history for a given fqn"""
        ds = self.load_single_ds(fqn)
        return self._read_meta_hist(ds).toJson()

    def getDsHash(self, name):
        """The current hashOfHash for the named module as a hex string

            Args:
                name (str): The uniquen name of a module. Does not have to be the FQN.
                runConfig (dict): runConfig to apply when collecting info. If module
                                  was run with a config, the same config needs to be
                                  specified here to retrieve the correct hash.

            Returns:
                (str): The hashOfHash of the named module
        """
        return self.dsm.inferDS(name)[0]._ver_hex()

    def copyToHdfs(self, fileobj, destination):
        """Copies the content of a file object to an HDFS location.

        Args:
            fileobj (file object): a file-like object whose content is to be copied,
                such as one returned by open(), or StringIO
            destination (str): specifies the destination path in the hadoop file system

        The file object is expected to have been opened in binary read mode.

        The file object is closed when this function completes.
        """
        SmvHDFS(self._jvm).writeToFile(fileobj, destination)

    def getStageFromModuleFqn(self, fqn):
        """Returns the stage name for a given fqn"""
        res = [s for s in self.stages() if fqn.startswith(s + ".")]
        if (len(res) == 0):
            raise SmvRuntimeError("Can't find {} from stages {}".format(fqn, ", ".join(self.stages())))
        return res[0]

    def outputDir(self):
        return self.all_data_dirs().outputDir

    def discoverSchema(self, path, attrs = CsvAttributes()):
        spark = self.sparkSession
        builder = spark.read\
            .option("mode", "DROPMALFORMED")\
            .option("inferSchema", "true")\
            .option("header", "true")\
            .option('ignoreLeadingWhiteSpace', 'true')

        # infer schema can't handle Date anyhow. If specify timestampFormat, it could 
        # infer to timestamp
        if attrs.get('timestampFormat'):
            builder = builder.option("timestampFormat", attrs.get('timestampFormat'))
        if attrs.get('delimiter'):
            builder = builder.option("sep", attrs.get('delimiter'))
        if attrs.get('escape'):
            builder = builder.option("escape", attrs.get('escape'))
        if attrs.get('quote-char'):
            builder = builder.option("quote", attrs.get('quote-char'))

        df = builder.csv(path)
        smvSchema = SmvSchema.dicoverFromInferedDF(df)
        return smvSchema.updateAttrs(attrs)
        
    def buildCsvIO(self, smvSchema, wr,  df = None, mode = "FAILFAST"):
        spark = self.sparkSession
        (s, attrs) = (smvSchema.schema, smvSchema.attributes)
        builder = spark.read if (wr == "r") else df.write
        builder = builder\
            .option("mode", mode)\
            .option('inferSchema', 'false')\
            .option('enforceSchema', 'true')\
            .option('ignoreLeadingWhiteSpace', 'true')\
            .option("encoding", "utf-8")

        # Could set nullValue when handle String[,_NULL_] type schema string
        if attrs.get('dateFormat'):
            builder = builder.option("dateFormat", attrs.get('dateFormat'))
        if attrs.get('timestampFormat'):
            builder = builder.option("timestampFormat", attrs.get('timestampFormat'))
        if attrs.get('has-header'):
            builder = builder.option("header", attrs.get('has-header'))
        if attrs.get('delimiter'):
            builder = builder.option("sep", attrs.get('delimiter'))
        if attrs.get('escape'):
            builder = builder.option("escape", attrs.get('escape'))
        if attrs.get('quote-char'):
            builder = builder.option("quote", attrs.get('quote-char'))

        # For PERMISSIVE mode add a column named "_corrupt_record" as string to hold 
        # corrupted records and be able to output to "data/output" 
        if (mode == "PERMISSIVE"):
            builder = builder.option("columnNameOfCorruptRecord", "_corrupt_record")

        return builder.schema(s) if (wr == 'r') else builder

    def createDF(self, schema, data = "", mode = "FAILFAST"):
        spark = self.sparkSession
        if (is_string(schema)):
            smvSchema = SmvSchema(schema)
        else:
            smvSchema = schema
        d = spark.sparkContext.parallelize(data.split(";"))
        reader_builder = self.buildCsvIO(smvSchema, "r", None, mode)
        dataframe = reader_builder.csv(d)
        return dataframe

    def abs_path_for_project_path(self, project_path):
        return os.path.abspath(os.path.join(self.appDir(), project_path))

    def prepend_source(self, project_path):
        abs_path = self.abs_path_for_project_path(project_path)
        # Source must be added to front of path to make sure it is found first
        sys.path.insert(1, abs_path)
        smv.logger.debug("Prepended {} to sys.path".format(abs_path))

    def remove_source(self, project_path):
        try:
            abs_path = self.abs_path_for_project_path(project_path)
            sys.path.remove(abs_path)
            smv.logger.debug("Removed {} from sys.path".format(abs_path))
        except ValueError:
            # ValueError will be raised if the project path was not previously
            # added to the sys.path
            pass

    def _hist_io_strategy(self, m):
        """Meta history is managed by smvapp and smvmodulerunner, module
            instances does not need to know it"""
        hist_dir = self.all_data_dirs().historyDir
        hist_path = "{}/{}.hist".format(hist_dir, m.fqn())
        return SmvJsonOnHdfsPersistenceStrategy(self, hist_path)

    def _read_meta_hist(self, m):
        io_strategy = self._hist_io_strategy(m)
        try:
            hist_json = io_strategy.read()
            hist = SmvMetaHistory().fromJson(hist_json)
        except:
            hist = SmvMetaHistory()
        return hist

    def _purge_current_output_files(self, mods):
        SmvModuleRunner(mods, self).purge_persisted()

    def _dry_run(self, mods):
        """Execute as dry-run if the dry-run flag is specified.
            This will show which modules are not yet persisted that need to run, without
            actually running the modules.
        """

        mods_need_to_run = self.get_need_to_run(mods, keep_roots=True)

        print("Dry run - modules not persisted:")
        print("----------------------")
        print("\n".join([m.fqn() for m in mods_need_to_run]))
        print("----------------------")

    def _modules_to_run(self):
        modPartialNames = self.py_smvconf.mods_to_run
        stageNames      = [self.py_smvconf.infer_stage_full_name(f) for f in self.py_smvconf.stages_to_run]

        mods = self.dsm.modulesToRun(modPartialNames, stageNames, self.cmd_line.runAllApp)

        # Default to runAllApp if no module specified
        if len(mods) == 0: 
            mods = self.dsm.modulesToRun(modPartialNames, stageNames, True)

        return mods

    def _publish_modules(self, mods):
        SmvModuleRunner(mods, self).publish()

    def _generate_output_modules(self, mods):
        SmvModuleRunner(mods, self).run()

    def run(self):
        mods = self._modules_to_run()

        if(self.cmd_line.forceRunAll):
            self._purge_current_output_files(mods)

        if (len(mods) > 0):
            print("Modules to run/publish")
            print("----------------------")
            print("\n".join([m.fqn() for m in mods]))
            print("----------------------")

        #either generate graphs, publish modules, or run output modules (only one will occur)
        if(self.cmd_line.dryRun):
            self._dry_run(mods)
        elif(self.cmd_line.publish):
            self._publish_modules(mods)
        else:
            self._generate_output_modules(mods)
