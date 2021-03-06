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
from smv.iomod import SmvCsvInputFile, SmvMultiCsvInputFiles, SmvXmlInputFile, SmvCsvStringInputData

import pyspark.sql.functions as F

class D1(SmvCsvStringInputData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "x,10;y,1"

class D1WithDate(SmvCsvStringInputData):
    def schemaStr(self):
        return "@dateFormat = MM/dd/yyyy;a:String;b:Integer;c:Date"
    def dataStr(self):
        return "x,10,02/28/2021;y,1,03/12/2011"

class D1WithError(SmvCsvStringInputData):
    def csvReaderMode(self):
        return "PERMISSIVE"

    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return "a1,10;a2,x;a3,2;a4,;a5,y"

class D1WithEscape(SmvCsvStringInputData):
    def schemaStr(self):
        return "a:String;b:Integer"
    def dataStr(self):
        return 'a1,10;""a3"",2;a4,'

class SqlMod(SmvSqlModule):
    def tables(self):
        return {
            "A": SqlInputA,
            "B": SqlInputB
        }

    def query(self):
        return "select a, b from A inner join B on A.ida = B.idb"

class SqlInputA(SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return self.smvApp.createDF("ida: Integer; a: String", "1,def;2,ghi")

class SqlInputB(SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return self.smvApp.createDF("idb: Integer; b: String", "2,jkl;1,mno")


class Xml1(SmvXmlInputFile):
    def connectionName(self):
        return "my_xml"
    def fileName(self):
        return "f1.xml"
    def rowTag(self):
        return 'ROW'

class Xml2(SmvXmlInputFile):
    def connectionName(self):
        return "my_xml"
    def fileName(self):
        return "f1.xml"
    def schemaFileName(self):
        return "f1.xml.json"
    def rowTag(self):
        return 'ROW'


class NewCsvFile1(SmvCsvInputFile):
    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "csvtest/csv1.csv"

class NewCsvFileWithError(SmvCsvInputFile):
    def csvReaderMode(self):
        return "PERMISSIVE"

    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "csvtest/csv2.csv"

class NewCsvFileWithErrorIgnore(SmvCsvInputFile):
    def csvReaderMode(self):
        return "PERMISSIVE"

    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "csvtest/csv2.csv"

class NewCsvFileWithErrorFail(SmvCsvInputFile):
    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "csvtest/csv2.csv"

class NewCsvFile2(SmvCsvInputFile):
    def connectionName(self):
        return "my_hdfs"

    def schemaConnectionName(self):
        return "my_hdfs_2"

    def fileName(self):
        return "csvtest/csv1.csv"

class NewCsvFile3(SmvCsvInputFile):
    def connectionName(self):
        return "my_hdfs"

    def schemaConnectionName(self):
        return "my_hdfs_2"

    def fileName(self):
        return "csvtest/csv1.csv"

    def schemaFileName(self):
        return "csv1.csv.schema"

class NewCsvFile4(SmvCsvInputFile):
    def connectionName(self):
        return "my_hdfs"

    def fileName(self):
        return "csvtest/csv1.csv"

    def csvAttr(self):
        return CsvAttributes(",", '"', "true")

    def userSchema(self):
        return "name2:String;id2:Integer"

class NewMultiCsvFiles1(SmvMultiCsvInputFiles):
    def connectionName(self):
        return "my_hdfs"

    def dirName(self):
        return "multi_csv"
