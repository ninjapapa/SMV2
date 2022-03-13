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

from test_support.smvbasetest import SmvBaseTest
from smv.smvmodulerunner import SmvModuleRunner
import smv.smvshell

class HiveTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    @classmethod
    def setUpClass(cls):
        super(HiveTest, cls).setUpClass()
        import tempfile
        import getpass


class NewHiveTableTest(HiveTest):
    @classmethod
    def smvAppInitArgs(cls):
        return super(NewHiveTableTest, cls).smvAppInitArgs()\
            + [
                'smv.conn.my_hive.type=hive',
                '-m',
                "stage.modules.NewHiveOutput"
            ]

    @classmethod
    def setUpClass(cls):
        super(NewHiveTableTest, cls).setUpClass()
        cls.smvApp.run()

    def test_new_hive_output(self):
        res = self.df("stage.modules.M")
        readBack = self.smvApp.sqlContext.sql("select * from M")
        self.should_be_same(res, readBack)

    def test_new_hive_input(self):
        res = self.df("stage.modules.NewHiveInput")
        exp = self.smvApp.sqlContext.sql("select * from M")
        self.should_be_same(res, exp)

    def test_get_conn_contents(self):
        conn = self.smvApp.get_connection_by_name('my_hive')
        tablenames = conn.get_contents(self.smvApp)
        self.assertTrue('m' in tablenames)

