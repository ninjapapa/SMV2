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
