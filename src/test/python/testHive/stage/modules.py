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

from smv import SmvApp, SmvModule, SmvOutput 
from smv.iomod import SmvHiveInputTable, SmvHiveOutputTable

class M(SmvModule):
    def requiresDS(self): return []
    def run(self, i):
        return self.smvApp.createDF("k:String;v:Integer", "a,;b,2")

class NewHiveInput(SmvHiveInputTable):
    def tableName(self): return "M"
    def connectionName(self): return "my_hive"

class NewHiveOutput(SmvHiveOutputTable):
    def requiresDS(self):
        return [M]
    def tableName(self): return "M"
    def connectionName(self): return "my_hive"
