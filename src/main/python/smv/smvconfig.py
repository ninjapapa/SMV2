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
"""
Handle command line args and props files
"""
import os.path
import re
import argparse
import uuid
import smv.jprops as jprops
from smv.error import SmvRuntimeError
from smv.utils import infer_full_name_from_part

class SmvConfig(object):
    """Smv configurations
        Including:

            - command line parsing
            - read in property files
            - dynamic configuration handling
    """
    def __init__(self, arglist):
        self.cmdline, self.leftover = self._create_cmdline_conf(arglist)

        DEFAULT_SMV_APP_CONF_FILE  = "conf/smv-app-conf.props"
        DEFAULT_SMV_CONN_CONF_FILE  = "conf/connections.props"
        DEFAULT_SMV_USER_CONF_FILE = "conf/smv-user-conf.props"
        DEFAULT_SMV_HOME_CONF_FILE = os.getenv('HOME', '') + "/.smv/smv-user-conf.props"

        self.app_dir = self.cmdline.pop('smvAppDir')
        self.static_props = {}
        self.dynamic_props = {}

        self.app_conf_path = DEFAULT_SMV_APP_CONF_FILE
        self.conn_conf_path = DEFAULT_SMV_CONN_CONF_FILE
        self.user_conf_path = DEFAULT_SMV_USER_CONF_FILE
        self.home_conf_path = DEFAULT_SMV_HOME_CONF_FILE

        self.cmdline_props = dict(self.cmdline.pop('smvProps'))

        self.read_props_from_app_dir(self.app_dir)

        self.mods_to_run = self.cmdline.pop('modsToRun')
        self.stages_to_run = self.cmdline.pop('stagesToRun')

    def read_props_from_app_dir(self, _app_dir):
        """For a given app dir, read in the prop files
        """
        self.app_dir = _app_dir
        self.static_props = self._read_props()

    def merged_props(self):
        """All the props (static + dynamic)
        """
        res = self.static_props.copy()
        res.update(self.dynamic_props)
        return res

    def read_props_from_kernel_config_file(self):
        """Read props from the kernel config file
            The specfic props `smv.kernelConfigFile` defines the relative path of kernel config file
            against the current app dir
        """
        kernel_config_file = self.merged_props().get('smv.kernelConfigFile')
        if (kernel_config_file is not None):
            full_kernel_config_file_path = os.path.join(self.app_dir, kernel_config_file)
            return SmvConfig.load(full_kernel_config_file_path)
        return {}

    def set_dynamic_props(self, new_d_props):
        """Reset dynamic props
            Overwrite entire dynamic props fully each reset
            Ignore reset if new_d_props is None
        """
        if(new_d_props is not None):
            self.dynamic_props = new_d_props.copy()

    def set_app_dir(self, new_app_dir):
        """Dynamic reset of app dir, so that the location of app and user
            conf files. Re-read the props files
        """
        if(new_app_dir):
            self.app_dir = new_app_dir
            self.read_props_from_app_dir(self.app_dir)

    def all_data_dirs(self):
        """Create all the data dir configs
        """
        props = self.merged_props()
        if (self.cmdline.get('dataDir')):
            data_dir = self.cmdline.get('dataDir')
        elif (props.get('smv.dataDir')):
            data_dir = props.get('smv.dataDir')
        elif(os.getenv('DATA_DIR', None)):
            data_dir = os.getenv('DATA_DIR')
            print("WARNING: use of DATA_DIR environment variable is deprecated. use smv.dataDir instead!!!")
        else:
            raise SmvRuntimeError("Must specify a data-dir either on command line or in conf.")

        def get_sub_dir(name, default):
            res = "{}/{}".format(data_dir, default)
            if (props.get('smv.' + name)):
                res = props.get('smv.' + name)
            return res

        return {
            'dataDir': data_dir,
            'outputDir': get_sub_dir('outputDir', "output"),
            'lockDir': get_sub_dir('lockDir', "lock"),
            'historyDir': get_sub_dir('historyDir', "history"),
            'publishDir': get_sub_dir('publishDir', 'publish'),
            'publishVersion': self.cmdline.get('publish')
        }

    def app_id(self):
        return self.merged_props().get("smv.appId")

    def app_name(self):
        return self.merged_props().get("smv.appName")

    def stage_names(self):
        return self._split_prop("smv.stages")

    def use_lock(self):
        return self._get_prop_as_bool("smv.lock")

    def get_run_config(self, key):
        """Run config will be accessed within client modules. Return
            run-config value of the given key.

            2 possible sources of run-config:
                - dynamic_props (which passed in by client code)
                - props files/command-line parameters
        """
        if (key in self.dynamic_props):
            # when seting run-config in dynamic props, use the key directly
            return self.dynamic_props.get(key).strip()
        else:
            # when seting run-config in props, use smv.config.+key as key
            return self.merged_props().get("smv.config." + key, None)

    def get_run_config_keys(self):
        """Return all the run-config keys
        """
        pref = "smv.config."
        pref_len = len(pref)
        from_props = [k[pref_len:] for k in self.merged_props().keys() if k.startswith(pref)]
        from_dynamic = self.dynamic_props.keys()
        # dict_keys objects are not add-able, have to copy them to a new list
        res = []
        res.extend(from_props)
        res.extend(from_dynamic)
        return res

    def infer_stage_full_name(self, part_name):
        """For a given partial stage name, infer full stage name
        """
        return infer_full_name_from_part(self.stage_names(), part_name)

    def _get_prop_as_bool(self, prop):
        flag = self.merged_props().get(prop)
        if (flag and flag.lower() == "true"):
            return True
        else:
            return False

    def _split_prop(self, prop_name):
        """Split multi-value prop to a list
        """
        prop_val = self.merged_props().get(prop_name)
        return [f.strip() for f in re.split("[:,]", prop_val)]

    def _create_cmdline_conf(self, arglist):
        """Parse arglist to a config dictionary
        """
        parser = argparse.ArgumentParser(
            usage="spark-submit src/main/python/driver.py -- -m ModuleToRun\n    spark-submit src/main/python/driver.py -- --run-app",
            description="For additional usage information, please refer to the user guide and API docs at: \nhttp://tresamigossd.github.io/SMV"
        )


        # Where to find props files
        parser.add_argument('--smv-app-dir', dest='smvAppDir', default=".", help="SMV app directory")

        # Where to find/store data
        parser.add_argument('--data-dir', dest='dataDir', help="specify the top level data directory")

        # All app run flags
        parser.add_argument('--force-run-all', dest='forceRunAll', action="store_true", help="ignore persisted data and force all modules to run")
        parser.add_argument('--dry-run', dest='dryRun', action="store_true", help="determine which modules do not have persisted data and will need to be run")

        # Where to output CSVs
        parser.add_argument('--publish', dest='publish', help="publish the given modules/stage/app as given version")

        # What modules to run
        parser.add_argument('--run-app', dest='runAllApp', action='store_true', help="run all output modules in all stages in app")
        parser.add_argument('-m', '--run-module', dest='modsToRun', nargs='+', default=[], help="run specified list of module FQNs")
        parser.add_argument('-s', '--run-stage', dest='stagesToRun', nargs='+', default=[], help="run all output modules in specified stages")

        def parse_props(prop):
            # str.split([sep[, maxsplit]]): we just need to split the first "="
            return prop.split("=", 1)

        # command line props override
        parser.add_argument('--smv-props', dest="smvProps", nargs='+', type=parse_props, default=[], help="key=value command line props override")

        res_ns, leftover = parser.parse_known_args(arglist)
        res = vars(res_ns)
        return res, leftover

    @staticmethod
    def load(path):
        if os.path.exists(path):
            with open(path) as fp:
                return jprops.load_properties(fp)
        else:
            return {}

    def _read_props(self):
        """Read property files
        """
        # os.path.join has the following behavior:
        # ("/a/b", "c/d") -> "/a/b/c/d"
        # ("/a/b", "/c/d") -> "/c/d"
        full_app_conf_path = os.path.join(self.app_dir, self.app_conf_path)
        full_conn_conf_path = os.path.join(self.app_dir, self.conn_conf_path)
        full_user_conf_path = os.path.join(self.app_dir, self.user_conf_path)

        app_conf_props = SmvConfig.load(full_app_conf_path)
        # runtime_config_file is the relative path which defined in app_conf_props
        runtime_config_file = app_conf_props.get('smv.runtimeConfigFile')
        runtime_conf_props_from_file = {}
        if (runtime_config_file is not None):
            full_runtime_config_file_path = os.path.join(self.app_dir, runtime_config_file)
            runtime_conf_props_from_file = SmvConfig.load(full_runtime_config_file_path)
        conn_conf_props = SmvConfig.load(full_conn_conf_path)
        home_conf_props = SmvConfig.load(self.home_conf_path)
        user_conf_props = SmvConfig.load(full_user_conf_path)

        default_props = {
            "smv.appName"            : "Smv Application",
            "smv.appId"              : str(uuid.uuid4()),
            "smv.stages"             : "",
            "smv.config.keys"        : "",
            "smv.class_dir"          : "./target/classes"
        }

        # Priority: Low to High
        #   - default
        #   - conf/smv-app-conf.props
        #   - props defined in the runtime config file
        #   - conf/connections.props
        #   - ${HOME}/.smv/smv-user-conf.props
        #   - conf/smv-user-conf.props
        #   - command-line
        res = {}
        res.update(default_props)
        res.update(app_conf_props)
        res.update(runtime_conf_props_from_file)
        res.update(conn_conf_props)
        res.update(home_conf_props)
        res.update(user_conf_props)
        res.update(self.cmdline_props)
        return res
