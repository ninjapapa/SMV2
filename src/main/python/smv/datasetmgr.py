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

"""DataSetMgr entry class
This module provides the python entry point to DataSetMgr on scala side
"""

from smv.utils import smv_copy_array, scala_seq_to_list, list_distinct, infer_full_name_from_part
from smv.datasetresolver import DataSetResolver

class DataSetMgr(object):
    """The Python representation of DataSetMgr.
    """

    def __init__(self, sc, smvconfig):
        self.sc = sc
        self._jvm = sc._jvm

        self.smvconfig = smvconfig
        self.dsRepoFactories = []
        self.dsRepoFactories2 = []

        from py4j.java_gateway import java_import
        java_import(self._jvm, "org.tresamigos.smv.python.SmvPythonHelper")
        java_import(self._jvm, "org.tresamigos.smv.DataSetRepoFactoryPython")

        self.helper = self._jvm.SmvPythonHelper

    def stages(self):
        return self.smvconfig.stage_names()

    def tx(self):
        """Create a TXContext for multiple places, avoid the long TXContext line
        """
        return TXContext(self._jvm, self.dsRepoFactories, self.stages())

        # #1417 Scaffolding. To test on Python side resolver use the following line instead
        # return TXContext2(self._jvm, self.dsRepoFactories2, self.stages())

    def load(self, *urns):
        """Load SmvDataSets for specified URNs
        
        Args:
            *urns (str): list of URNs as strings

        Returns:
            list(SmvDataSet): list of Scala SmvDataSets (j_ds)
        """
        with self.tx() as tx:
            return tx.load(urns)

    def inferDS(self, *partial_names):
        """Return DSs from a list of partial names

        Args:
            *partial_names (str): list of partial names

        Returns:
            list(SmvDataSet): list of Scala SmvDataSets (j_ds)
        """
        with self.tx() as tx:
            return tx.inferDS(partial_names)

    def inferUrn(self, partial_name):
        """Return URN string from partial name
        """
        return self.inferDS(partial_name)[0].urn().toString()

    def register(self, repo_factory):
        """Register python repo factory
        """
        j_rfact = self._jvm.DataSetRepoFactoryPython(repo_factory)
        self.dsRepoFactories.append(j_rfact)
        self.dsRepoFactories2.append(repo_factory)

    def allDataSets(self):
        """Return all the SmvDataSets in the app
        """
        with self.tx() as tx:
            return tx.allDataSets()

    def modulesToRun(self, modPartialNames, stageNames, allMods):
        """Return a modules need to run
            Combine specified modules, (-m), stages, (-s) and if
            (--run-app) specified, all output modules
        """
        with self.tx() as tx:
            named_mods = tx.inferDS(modPartialNames)
            stage_mods = tx.outputModulesForStage(stageNames)
            app_mods = tx.allOutputModules() if allMods else []
            res = []
            res.extend(named_mods)
            res.extend(stage_mods)
            res.extend(app_mods)
            # Need to perserve the ordering
            return list_distinct(res)

class TXContext(object):
    """Create a TX context for "with tx() as tx" syntax
    """
    def __init__(self, _jvm, resourceFactories, stages):
        self._jvm = _jvm
        self.resourceFactories = resourceFactories
        self.stages = stages

    def __enter__(self):
        return TX(self._jvm, self.resourceFactories, self.stages)

    def __exit__(self, type, value, traceback):
        pass


class TXContext2(object):
    """Create a TX context for "with tx() as tx" syntax
    """
    def __init__(self, _jvm, resourceFactories, stages):
        self._jvm = _jvm
        self.resourceFactories = resourceFactories
        self.stages = stages

    def __enter__(self):
        return TX2(self._jvm, self.resourceFactories, self.stages)

    def __exit__(self, type, value, traceback):
        pass


class TX(object):
    """Abstraction of the transaction boundary for loading SmvDataSets. 
        A TX object

        * will instantiate a set of repos when itself instantiated and will
        * reuse the same repos for all queries. This means that each new TX object will
        * reload the SmvDataSet from source **once** during its lifetime.

        NOTE: Once a new TX is created, the well-formedness of the SmvDataSets provided
        by the previous TX is not guaranteed. Particularly it may become impossible
        to run modules from the previous TX.
    """
    def __init__(self, _jvm, resourceFactories, stages):
        self._jvm = _jvm
        self.repos = [rf.createRepo() for rf in resourceFactories]
        self.stages = stages
        self.resolver = _jvm.org.tresamigos.smv.python.SmvPythonHelper.createDsResolver(self.repos)
        self.log = _jvm.org.apache.log4j.LogManager.getLogger("smv")

    def load(self, urn_strs):
        return self.resolver.loadDataSet(urn_strs)

    def inferDS(self, partial_names):
        return self.load(self._inferUrn(partial_names))

    def allDataSets(self): 
        return self.load(self._allUrns())

    def allOutputModules(self):
        return self._filterOutput(self.allDataSets())

    def outputModulesForStage(self, stageNames):
        return self._filterOutput(self._dsForStage(stageNames))

    def _dsForStage(self, stageNames):
        return self.load(self._urnsForStage(stageNames))

    def _urnsForStage(self, stageNames):
        return [u.toString() 
            for repo in self.repos 
            for s in stageNames 
            for u in scala_seq_to_list(self._jvm, repo.urnsForStage(s))
        ]

    def _allUrns(self):
        if (len(self.stages) == 0):
            log.warn("No stage names configured. Unable to discovr any modules.")
        return self._urnsForStage(self.stages)
   
    def _inferUrn(self, partial_names):
        def urn_str(pn):
            return infer_full_name_from_part(
                self._allUrns(), 
                pn
            )

        return [urn_str(pn) for pn in partial_names]

    def _filterOutput(self, dss):
        return [ds for ds in dss if ds.dsType() == "Output"]

class TX2(object):
    """Abstraction of the transaction boundary for loading SmvDataSets. 
        A TX object

        * will instantiate a set of repos when itself instantiated and will
        * reuse the same repos for all queries. This means that each new TX object will
        * reload the SmvDataSet from source **once** during its lifetime.

        NOTE: Once a new TX is created, the well-formedness of the SmvDataSets provided
        by the previous TX is not guaranteed. Particularly it may become impossible
        to run modules from the previous TX.
    """
    def __init__(self, _jvm, resourceFactories, stages):
        self.repos = [rf.createRepo() for rf in resourceFactories]
        self.stages = stages
        self.resolver = DataSetResolver(self.repos[0])
        self.log = _jvm.org.apache.log4j.LogManager.getLogger("smv")

    def load(self, urns):
        fqns = [u[4:] for u in urns]
        return self.resolver.loadDataSet(fqns)

    def inferDS(self, partial_names):
        return self.load(self._inferUrn(partial_names))

    def allDataSets(self): 
        return self.load(self._allUrns())

    def allOutputModules(self):
        return self._filterOutput(self.allDataSets())

    def outputModulesForStage(self, stageNames):
        return self._filterOutput(self._dsForStage(stageNames))

    def _dsForStage(self, stageNames):
        return self.load(self._urnsForStage(stageNames))

    def _urnsForStage(self, stageNames):
        return [u
            for repo in self.repos 
            for s in stageNames 
            for u in repo._dataSetsForStage(s)
        ]

    def _allUrns(self):
        if (len(self.stages) == 0):
            log.warn("No stage names configured. Unable to discover any modules.")
        return self._urnsForStage(self.stages)
   
    def _inferUrn(self, partial_names):
        def urn_str(pn):
            return infer_full_name_from_part(
                self._allUrns(), 
                pn
            )

        return [urn_str(pn) for pn in partial_names]

    def _filterOutput(self, dss):
        return [ds for ds in dss if ds.dsType() == "Output"]