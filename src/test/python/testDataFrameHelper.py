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
import os

from test_support.smvbasetest import SmvBaseTest
from smv import SmvApp

from smv.helpers import DataFrameHelper as dfhelper

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import col, struct, count
from py4j.protocol import Py4JJavaError
from smv.error import SmvRuntimeError

class DfHelperTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def test_smvHist(self):
        df = self.createDF("a:Integer; b:String", """1,a;2,s;3,a;4,s;5,a;6,s;7,a;8,y;9,t;10,x""")
        res = df.smvHist("a")
        expect = self.createDF("a:Integer;count:Long", """1,1;2,1;3,1;4,1;5,1;6,1;7,1;8,1;9,1;10,1""")
        self.should_be_same(expect, res)
        res = df.smvHist("b")
        expect = self.createDF("b:String;count:Long", """a,4;s,3;y,1;t,1;x,1""")
        self.should_be_same(expect, res)

    def test_smvJoinByKey(self):
        df1 = self.createDF(
            "a:Integer; b:Double; c:String",
            """1,2.0,hello;
            1,3.0,hello;
            2,10.0,hello2;
            2,11.0,hello3"""
        )
        df2 = self.createDF("a:Integer; c:String", """1,asdf;2,asdfg""")
        res = df1.smvJoinByKey(df2, ['a'], "inner")
        expect = self.createDF(
            "a:Integer;b:Double;c:String;_c:String",
            "1,2.0,hello,asdf;1,3.0,hello,asdf;2,10.0,hello2,asdfg;2,11.0,hello3,asdfg"
        )
        self.should_be_same(expect, res)

    def test_smvJoinByKey_nullSafe(self):
        df1 = self.createDF(
            "a:String; b:String; i:Integer",
            """a,,1;
            a,b,2;
            ,,3"""
        )
        df2 = self.createDF(
            "a:String; b:String; j:String",
            """a,,x;
            ,,y;
            c,d,z"""
        )
        res = df1.smvJoinByKey(df2, ['a', 'b'], 'inner', isNullSafe=True)
        expect = self.createDF(
            "a: String;b: String;i: Integer;j: String",
            """,,3,y;
            a,,1,x"""
        )
        self.should_be_same(expect, res)
 
    def test_smvUnion(self):
        schema       = "a:Integer; b:Double; c:String"
        schema2      = "c:String; a:Integer; d:Double"
        schemaExpect = "a:Integer; b:Double; c:String; d:Double"

        df = self.createDF(
            schema,
            """1,2.0,hello;
               2,3.0,hello2"""
        )
        df2 = self.createDF(
            schema2,
            """hello5,5,21.0;
               hello6,6,22.0"""
        )
        result = df.smvUnion(df2)
        expect = self.createDF(
            schemaExpect,
            """1,2.0,hello,;
            2,3.0,hello2,;
            5,,hello5,21.0;
            6,,hello6,22.0;"""
        )
        self.should_be_same(expect, result)
