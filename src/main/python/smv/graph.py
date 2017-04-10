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

"""Provides dependency graphing of SMV modules.
"""
from utils import smv_copy_array
from graphviz import Source

class SmvDependencyGraph(Source):
    def __init__(self, smvApp, stageNames = None):
        self.smvApp = smvApp
        if stageNames is None:
            self.stageNames = smvApp.j_smvApp.stages()
        elif (isinstance(stageNames, list)):
            self.stageNames = smvApp._jvm.PythonUtils.toSeq(stageNames)
        else:
            self.stageNames = smvApp._jvm.PythonUtils.toSeq([stageNames])
        self.dotstring = smvApp.j_smvApp.dependencyGraphDotString(self.stageNames)
        super(SmvDependencyGraph, self).__init__(self.dotstring)
