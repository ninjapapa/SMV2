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

from smv import *
from smv.iomod import SmvCsvStringInputData
import stage.modules

class CsvStr2(SmvCsvStringInputData):
    def schemaStr(self):
        return "name:String;id:integer"
    def dataStr(self):
        return "bob,1"

M1Link = SmvModuleLink(stage.modules.M1)

class M2(SmvModule):
    def requiresDS(self):
        return[CsvStr2, M1Link]
    def run(self, i):
        return i[CsvStr2]
