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
import smv


class R0(smv.iomod.SmvCsvStringInputData):
    def schemaStr(self):
        return "a:String;b:Integer"


class R1(R0):
    def dataStr(self):
        return "x,1;y,2"


class R2(smv.SmvModule, smv.SmvOutput):
    def requiresDS(self):
        return [R1]

    def run(self, i):
        return i[R1]


class R3(R0):
    def requiresDS(self):
        return [R1]

    def dataStr(self):
        return "l,2;m,3;n,5;"


class R4(R0):
    def requiresDS(self):
        return [R2, R3]

    def dataStr(self):
        return "l,2;m,3;n,5;"
