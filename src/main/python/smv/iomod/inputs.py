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

from smv.iomod.base import SmvInput, AsTable

from smv.smviostrategy import SmvJdbcIoStrategy, SmvHiveIoStrategy


class SmvJdbcInputTable(SmvInput, AsTable):
    """
        User need to implement

            - connectionName
            - tableName
    """

    def doRun(self, known):
        conn = self.get_connection()
        return SmvJdbcIoStrategy(self.smvApp, conn, self.tableName()).read()


class SmvHiveInputTable(SmvInput, AsTable):
    """
        User need to implement:

            - connectionName
            - tableName
    """

    def doRun(self, known):
        conn = self.get_connection()
        return SmvHiveIoStrategy(self.smvApp, conn, self.tableName()).read()

__all__ = [
    'SmvJdbcInputTable',
    'SmvHiveInputTable',
]
