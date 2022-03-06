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

import sys
import abc
import os
import re
import json

from pyspark.sql.types import StructType, StringType
from py4j.protocol import Py4JJavaError
import pyspark.sql.functions as F

import smv
from smv.iomod.base import SmvInput, AsTable, AsFile
from smv.smvmodule import SparkDfGenMod
from smv.smviostrategy import SmvJdbcIoStrategy, SmvHiveIoStrategy, \
    SmvCsvOnHdfsIoStrategy, SmvTextOnHdfsIoStrategy,\
    SmvXmlOnHdfsIoStrategy
from smv.smvschema import SmvSchema
from smv.utils import smvhash
from smv.error import SmvRuntimeError
from smv.smvhdfs import SmvHDFS


if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})

class SmvJdbcInputTable(SparkDfGenMod, SmvInput, AsTable):
    """
        User need to implement

            - connectionName
            - tableName
    """

    def connectionType(self):
        return 'jdbc'

    def instanceValHash(self):
        """Jdbc input hash depends on connection and table name
        """

        _conn_hash = self.connectionHash()
        smv.logger.debug("{} connectionHash: {}".format(self.fqn(), _conn_hash))

        _table_hash = self.tableNameHash()
        smv.logger.debug("{} tableNameHash: {}".format(self.fqn(), _table_hash))

        res = _conn_hash + _table_hash
        return res

    def _get_input_data(self):
        conn = self.get_connection()
        return SmvJdbcIoStrategy(self.smvApp, conn, self.tableName()).read()


class SmvHiveInputTable(SparkDfGenMod, SmvInput, AsTable):
    """
        User need to implement:

            - connectionName
            - tableName
    """

    def connectionType(self):
        return 'hive'

    def _get_input_data(self):
        conn = self.get_connection()
        return SmvHiveIoStrategy(self.smvApp, conn, self.tableName()).read()

    def instanceValHash(self):
        """Hive input hash depends on connection and table name
        """

        _conn_hash = self.connectionHash()
        smv.logger.debug("{} connectionHash: {}".format(self.fqn(), _conn_hash))

        _table_hash = self.tableNameHash()
        smv.logger.debug("{} tableNameHash: {}".format(self.fqn(), _table_hash))

        res = _conn_hash + _table_hash
        return res


class SchemaString(ABC):
    """Abstract class to provide a schema string (any form, could be SmvSchema string for CSV or as JSON string)
    """
    @abc.abstractmethod
    def schemaString(self):
        """Return the schema string"""

    def userSchema(self):
        """User-defined schema
            Optional method. Override this method to define your own schema string

            Returns:
                (string):
        """
        return None

class WithSchemaFile(SchemaString, SmvInput, AsFile):
    """Implementation of SchemaString from input schema file"""

    def schemaConnectionName(self):
        """Optional method to specify a schema connection"""
        return None

    def schemaFileName(self):
        """Optional name of the schema file relative to the
            schema connection path
        """
        return None

    def _get_schema_connection(self):
        """Return a schema connection with the following priority:

            - User specified in current module through schemaConnectionName method
            - Configured in the global props files with prop key "smv.schemaConn"
            - Connection for data (user specified through connectionName method)

            Since in some cases user may not have write access to the data folder,
            need to provide more flexibility on where the schema files can come from.
        """
        name = self.schemaConnectionName()
        props = self.smvApp.py_smvconf.merged_props()
        global_schema_conn = props.get('smv.schemaConn')
        if (name is not None):
            return self.smvApp.get_connection_by_name(name)
        elif (global_schema_conn is not None):
            return self.smvApp.get_connection_by_name(global_schema_conn)
        else:
            return self.get_connection()

    def _get_schema_file_name(self):
        """The schema_file_name is determined by the following logic

                - schemaFileName
                - fileName replace the post-fix to schema
        """
        if (self.schemaFileName() is not None):
            return self.schemaFileName()
        else:
            return self.fileName().rsplit(".", 1)[0] + ".schema"

    def _full_path(self):
        return os.path.join(self.get_connection().path, self.fileName())

    def _full_schema_path(self):
        return os.path.join(self._get_schema_connection().path,
            self._get_schema_file_name())

    def schemaString(self):
        if (self.userSchema() is not None):
            return self.userSchema()
        else:
            s_path = self._full_schema_path()
            return SmvTextOnHdfsIoStrategy(self.smvApp, s_path).read()

    def _file_hash(self, path, msg):
        _file_path_hash = smvhash(path)
        smv.logger.debug("{} {} file path hash: {}".format(self.fqn(), msg, _file_path_hash))

        # It is possible that the file doesn't exist
        try:
            _m_time = SmvHDFS(self.smvApp._jvm).modificationTime(path)
        except Py4JJavaError:
            _m_time = 0

        smv.logger.debug("{} {} file mtime: {}".format(self.fqn(), msg, _m_time))

        res = _file_path_hash + _m_time
        return res

    def instanceValHash(self):
        """Hash of file with schema include data file hash (path and mtime),
            and schema hash (userSchema or schema file)
        """

        _data_file_hash = self._file_hash(self._full_path(), "data")
        smv.logger.debug("{} data file hash: {}".format(self.fqn(), _data_file_hash))

        if (self.userSchema() is not None):
            _schema_hash = smvhash(self.userSchema())
        else:
            _schema_hash = self._file_hash(self._full_schema_path(), "schema")
        smv.logger.debug("{} schema hash: {}".format(self.fqn(), _schema_hash))

        res = _data_file_hash + _schema_hash
        return res


class SmvXmlInputFile(SparkDfGenMod, WithSchemaFile):
    """Input from file in XML format
        User need to implement:

            - rowTag: required
            - connectionName: required
            - fileName: required
            - schemaConnectionName: optional
            - schemaFileName: optional
            - userSchema: optional
    """

    @abc.abstractmethod
    def rowTag(self):
        """XML tag for identifying a record (row)"""
        pass

    def _schema(self):
        """load schema from userSchema (as a json string) or a json file"""
        try:
            s = self.schemaString()
            return StructType.fromJson(json.loads(s))
        except:
            return None

    def _get_input_data(self):
        """readin xml data"""
        file_path = os.path.join(self.get_connection().path, self.fileName())
        return SmvXmlOnHdfsIoStrategy(
            self.smvApp,
            file_path,
            self.rowTag(),
            self._schema()
        ).read()

class WithSmvSchema(SchemaString):
    def csvAttr(self):
        """Specifies the csv file format.  Corresponds to the CsvAttributes case class in Scala.
            Derive from smvSchema if not specified by user.

            Override this method if user want to specify CsvAttributes which is different from
            the one can be derived from smvSchema
        """
        return None

    def smvSchema(self):
        """Return the schema specified by user either through
            userSchema method, or through a schema file. The priority is the following:

                - userSchema
                - schema_file_name under schema_connection

        """
        one_str = re.sub(r"[\r\n]+", ";", self.schemaString().encode("utf-8"))
        smv_schema = SmvSchema(one_str)

        if (self.csvAttr() is not None):
            return smv_schema.addCsvAttributes(self.csvAttr())
        else:
            return smv_schema


class WithCsvParser(WithSmvSchema, SmvInput):
    """Mixin for input modules to parse csv data"""

    def csvReaderMode(self):
        """When set, any parsing error will throw an exception to make sure we can stop early.
            To tolerant some parsing error, user can

            - Override csvReadermode 
                "DROPMALFORMED"
                "PERMISSIVE"
        """
        return "FAILFAST"

    def smvSchema(self):
        orig_smvSchema = super(WithCsvParser, self).smvSchema()
        if (self.csvReaderMode() == "PERMISSIVE"):
            s = orig_smvSchema.schema       # StructType
            s = s.add("_corrupt_record", StringType(), True)
            return SmvSchema(s).updateAttrs(orig_smvSchema.attributes)
        else:
            return orig_smvSchema

    def doRun(self, know):
        raw_csv = super(WithCsvParser, self).doRun(know)
        if (self.csvReaderMode() == "PERMISSIVE"):
            # When using PERMISSive mode, corrupted records are kept in a new column
            # The following will extract the clean records and return, but output the
            # corrupted records to a file
            # column name "_corrupt_record" is defined in buildCsvIO() of SmvApp
            raw_csv.cache()
            clean_df = raw_csv.where(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
            corrupted = raw_csv.where(F.col("_corrupt_record").isNotNull()).select("_corrupt_record")
            path = self.smvApp.output_path_from_base("{}_corrupted".format(self.fqn()), "csv")
            SmvCsvOnHdfsIoStrategy(self.smvApp, path).write(corrupted)
            raw_csv.unpersist()
            return clean_df
        else:
            return raw_csv


class SmvCsvInputFile(SparkDfGenMod, WithCsvParser, WithSchemaFile):
    """Csv file input
        User need to implement:

            - connectionName: required
            - fileName: required
            - schemaConnectionName: optional
            - schemaFileName: optional
            - userSchema: optional
            - csvAttr: optional
            - csvReaderMode: optional, default True
    """

    def _get_input_data(self):
        self._assert_file_postfix(".csv")

        file_path = os.path.join(self.get_connection().path, self.fileName())

        return SmvCsvOnHdfsIoStrategy(
            self.smvApp,
            file_path,
            self.smvSchema(),
            self.csvReaderMode()
        ).read()


class SmvMultiCsvInputFiles(SparkDfGenMod, WithCsvParser, WithSchemaFile):
    """Multiple Csv files under the same dir input
        User need to implement:

            - connectionName: required
            - dirName: required
            - schemaConnectionName: optional
            - schemaFileName: optional
            - userSchema: optional
            - csvAttr: optional
            - csvReaderMode: optional, default True
    """

    @abc.abstractmethod
    def dirName(self):
        """Path to the directory containing the csv files
            relative to the path defined in the connection

            Returns:
                (str)
        """

    # Override schema_file_name logic to depend on dir name instead of file name
    def _get_schema_file_name(self):
        """The schema_file_name is determined by the following logic

                - schemaFileName
                - dirName with post-fix schema
        """
        if (self.schemaFileName() is not None):
            return self.schemaFileName()
        else:
            return self.dirName() + ".schema"

    def fileName(self):
        return self.dirName()

    def _get_input_data(self):
        dir_path = os.path.join(self.get_connection().path, self.dirName())
        smv_schema = self.smvSchema()

        flist = SmvHDFS(self.smvApp._jvm).dirList(dir_path)
        # ignore all hidden files in the data dir
        filesInDir = [os.path.join(dir_path, n) for n in flist if not n.startswith(".")]

        if (not filesInDir):
            raise SmvRuntimeError("There are no data files in {}".format(dir_path))

        combinedDf = None
        for filePath in filesInDir:
            df = SmvCsvOnHdfsIoStrategy(
                self.smvApp,
                filePath,
                smv_schema,
                self.csvReaderMode()
            ).read()
            combinedDf = df if (combinedDf is None) else combinedDf.unionAll(df)

        return combinedDf


class SmvCsvStringInputData(SparkDfGenMod, WithCsvParser):
    """Input data defined by a schema string and data string

        User need to implement:

            - schemaStr(): required
            - dataStr(): required
            - csvReaderMode(): optional
    """

    def _get_input_data(self):
        return self.smvApp.createDF(self.smvSchema(), self.dataStr(), self.csvReaderMode())

    @abc.abstractmethod
    def schemaStr(self):
        """Smv Schema string.

            E.g. "id:String; dt:Timestamp"

            Returns:
                (str): schema
        """

    def schemaString(self):
        return self.schemaStr()

    @abc.abstractmethod
    def dataStr(self):
        """Smv data string.

            E.g. "212,2016-10-03;119,2015-01-07"

            Returns:
                (str): data
        """

    def connectionType(self):
        return None

    def connectionName(self):
        return None


__all__ = [
    'SmvJdbcInputTable',
    'SmvHiveInputTable',
    'SmvXmlInputFile',
    'SmvCsvInputFile',
    'SmvMultiCsvInputFiles',
    'SmvCsvStringInputData',
]
