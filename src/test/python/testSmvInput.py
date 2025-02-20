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

import unittest
import json
import os

from test_support.smvbasetest import SmvBaseTest
from smv import *
from smv.datasetrepo import DataSetRepo
from smv.conn import SmvHdfsConnectionInfo
from py4j.protocol import Py4JJavaError

class SmvInputTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        xml_path = cls.tmpInputDir() + "/xmltest"
        return [
            '--smv-props',
            'smv.stages=stage',
            'smv.conn.my_xml.type=hdfs',
            'smv.conn.my_xml.path=' + xml_path,
        ]

    class Resource(object):
        def __init__(self, smvApp, fqn):
            self.dsr = DataSetRepo(smvApp)
            self.fqn = fqn

        def __enter__(self):
            return self.dsr.loadDataSet(self.fqn)

        def __exit__(self, type, value, traceback):
            pass

    def _create_xml_file(self, name):
        self.createTempInputFile(name,
        """<?xml version="1.0"?>
<ROWSET>
    <ROW>
        <year>2012<!--A comment within tags--></year>
        <make>Tesla</make>
        <model>S</model>
        <comment>No comment</comment>
    </ROW>
    <ROW>
        <year>1997</year><!--A comment within elements-->
        <make>Ford</make>
        <model>E350</model>
        <comment><!--A comment before a value-->Go get one now they are going fast</comment>
    </ROW>
</ROWSET>""")

    def _create_xml_schema(self, name):
        self.createTempInputFile(name,
        """{
  "fields": [
    {
      "metadata": {},
      "name": "comment",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "make",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "model",
      "nullable": true,
      "type": "string"
    },
    {
      "metadata": {},
      "name": "year",
      "nullable": true,
      "type": "long"
    }
  ],
  "type": "struct"
}""")

    def _create_csv_file(self, name):
        self.createTempInputFile(name,
            """"Name","ID"
Bob,1
Fred,2"""
        )

    def _create_csv_schema(self, name):
        self.createTempInputFile(name,
        """@has-header = true
name:string
id:integer"""
        )

    def _schema_json_str(self, schema):
        import json
        sch_str = json.dumps(schema.jsonValue(), indent=2, separators=(',', ': '), sort_keys=True)
        return sch_str

    def _dump_schema(self, schema, jsonfile):
        o = file(jsonfile, "w+")
        o.write(self._schema_json_str(schema))
        o.close()

    def test_SmvCsvStringData(self):
        fqn = "stage.modules.D1"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "x,10;y,1")
        self.should_be_same(expect, df)

    def test_SmvCsvStringDataWithDate(self):
        fqn = "stage.modules.D1WithDate"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer;c:Date[MM/dd/yyyy]", "x,10,02/28/2021;y,1,03/12/2011")
        self.should_be_same(expect, df)

    def test_SmvCsvStringData_with_error(self):
        fqn = "stage.modules.D1WithError"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", "a1,10;a3,2;a4,")
        self.should_be_same(expect, df)
        corrupted_path = self.smvApp.output_path_from_base("{}_corrupted".format(fqn), "csv")
        self.assertTrue(os.path.exists(corrupted_path))

    def test_SmvCsvStringData_with_double_quote_escape(self):
        fqn = "stage.modules.D1WithEscape"
        df = self.df(fqn)
        expect = self.createDF("a:String;b:Integer", 'a1,10;""a3"",2;a4,')
        self.should_be_same(expect, df)

    def test_SmvSqlModule(self):
        fqn = "stage.modules.SqlMod"
        exp = self.createDF("a: String; b: String", "def,mno;ghi,jkl")
        df = self.df(fqn)
        self.should_be_same(df, exp)

        # verify that the tables have been dropped
        tablesDF = self.smvApp.sparkSession.sql('show tables from default')
        tableNames = [r.tableName for r in tablesDF.collect()]
        self.assertNotIn("a", tableNames)
        self.assertNotIn("b", tableNames)

    def test_SmvXmvFile_infer_schema(self):
        fqn = "stage.modules.Xml1"
        self._create_xml_file('xmltest/f1.xml')
        self._create_xml_schema('xmltest/f1.xml.json')
        df = self.df(fqn)
        expect = self.createDF("comment: String;make: String;model: String;year: Long",
            """No comment,Tesla,S,2012;
                Go get one now they are going fast,Ford,E350,1997""")
        self.should_be_same(expect, df)

    def test_SmvXmvFile_given_schema(self):
        fqn = "stage.modules.Xml2"
        self._create_xml_file('xmltest/f1.xml')
        self._create_xml_schema('xmltest/f1.xml.json')
        df = self.df(fqn)
        expect = self.createDF("comment: String;make: String;model: String;year: Long",
            """No comment,Tesla,S,2012;
                Go get one now they are going fast,Ford,E350,1997""")
        self.should_be_same(expect, df)


class SmvNewInputTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        data_path = cls.tmpInputDir()
        return [
            '--smv-props',
            'smv.stages=stage',
            'smv.conn.my_hdfs.type=hdfs',
            'smv.conn.my_hdfs.path=' + data_path,
            'smv.conn.my_hdfs_2.type=hdfs',
            'smv.conn.my_hdfs_2.path=' + data_path + "/conn2",
        ]

    def _create_csv_file(self, name, withError = False):
        data = """"Name","ID"
Bob,1
Fred,2"""
        if (withError):
            data = data + "\nKarl,badid"
        self.createTempInputFile(name, data)

    def _create_csv_schema(self, name):
        self.createTempInputFile(name,
        """@has-header = true
name:string
id:integer"""
        )


    def test_basic_csv_input(self):
        self._create_csv_file("csvtest/csv1.csv")
        self._create_csv_schema("csvtest/csv1.schema")
        res = self.df("stage.modules.NewCsvFile1")
        exp = self.createDF("name:String;id:Integer", "Bob,1;Fred,2")
        self.should_be_same(res, exp)

    def test_basic_csv_input_with_error(self):
        self._create_csv_file("csvtest/csv2.csv", True)
        self._create_csv_schema("csvtest/csv2.schema")
        fqn = "stage.modules.NewCsvFileWithError"
        df = self.df(fqn)
        exp = self.createDF("name:String;id:Integer", "Bob,1;Fred,2")
        self.should_be_same(exp, df)
        corrupted_path = self.smvApp.output_path_from_base("{}_corrupted".format(fqn), "csv")
        self.assertTrue(os.path.exists(corrupted_path))

    def test_basic_csv_input_with_error_failfast(self):
        self._create_csv_file("csvtest/csv2.csv", True)
        self._create_csv_schema("csvtest/csv2.schema")
        fqn = "stage.modules.NewCsvFileWithErrorFail"
        with self.assertRaisesRegex(Exception, r"Parse Mode: FAILFAST"):
            df = self.df(fqn)
            df.count()
 
    def test_basic_csv_input_with_error(self):
        self._create_csv_file("csvtest/csv2.csv", True)
        self._create_csv_schema("csvtest/csv2.schema")
        fqn = "stage.modules.NewCsvFileWithErrorIgnore"
        df = self.df(fqn)
        exp = self.createDF("name:String;id:Integer", "Bob,1;Fred,2")
        self.should_be_same(exp, df)

    def test_csv_diff_schema_conn(self):
        self._create_csv_file("csvtest/csv1.csv")
        self._create_csv_schema("conn2/csvtest/csv1.schema")
        res = self.df("stage.modules.NewCsvFile2")
        exp = self.createDF("name:String;id:Integer", "Bob,1;Fred,2")
        self.should_be_same(res, exp)

    def test_csv_diff_schema_file_name(self):
        self._create_csv_file("csvtest/csv1.csv")
        self._create_csv_schema("conn2/csv1.csv.schema")
        res = self.df("stage.modules.NewCsvFile3")
        exp = self.createDF("name:String;id:Integer", "Bob,1;Fred,2")
        self.should_be_same(res, exp)

    def test_csv_user_schema(self):
        self._create_csv_file("csvtest/csv1.csv")
        res = self.df("stage.modules.NewCsvFile4")
        exp = self.createDF("name2:String;id2:Integer", "Bob,1;Fred,2")
        self.should_be_same(res, exp)

    def test_multi_csv_basic(self):
        self.createTempInputFile("multi_csv/f1", "col1\na\n")
        self.createTempInputFile("multi_csv/f2", "col1\nb\n")
        self.createTempInputFile("multi_csv.schema", "@has-header = true\ncol1: String\n")

        res = self.df("stage.modules.NewMultiCsvFiles1")
        exp = self.createDF("col1:String", "a;b")
        self.should_be_same(res, exp)

    def test_conn_hash(self):
        conn1 = SmvHdfsConnectionInfo("testconn", {'smv.conn.testconn.path': '/dummy1'})
        conn2 = SmvHdfsConnectionInfo("testconn2", {'smv.conn.testconn2.path': '/dummy2'})

        self.assertNotEqual(conn1.conn_hash(), conn2.conn_hash())

    def test_get_connections(self):
        res = self.smvApp.get_all_connection_names()
        self.assertTrue('my_hdfs' in res)
        self.assertTrue('my_hdfs_2' in res)

    def test_get_contents(self):
        self.createTempInputFile("f1.csv", "col1\na\n")
        self.createTempInputFile("f1.schema", "a:String")
        conn = self.smvApp.get_connection_by_name('my_hdfs')
        res = conn.get_contents(self.smvApp)
        self.assertTrue('f1.csv' in res)
