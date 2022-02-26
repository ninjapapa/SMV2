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
import sys

from test_support.smvbasetest import SmvBaseTest

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SQLContext, HiveContext
from pyspark.sql.functions import array, col
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import smv.functions as SF

class ColumnHelperTest(SmvBaseTest):
    def test_smvGetColName(self):
        df = self.createDF("k:String; v:String;", "a,b;c,d;,")
        self.assertEqual(df.k.smvGetColName(), 'k')
        self.assertEqual(array(df.k, df.v).smvGetColName(), 'array(k, v)')

    def test_smvDayMonth70(self):
        df = self.createDF("t:Timestamp[yyyyMMdd]", "19760131;20120229")
        r1 = df.select(col("t").smvDay70().alias("t_day70"))
        r2 = df.select(col("t").smvMonth70().alias("t_month70"))

        e1 = self.createDF("t_day70: Integer", "2221;15399")
        e2 = self.createDF("t_month70: Integer", "72;505")

        self.should_be_same(e1, r1)
        self.should_be_same(e2, r2)
