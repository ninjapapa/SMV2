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

import re
import json
import pyspark.sql.types as T
from smv.error import SmvRuntimeError
from smv.utils import is_string
from smv.csv_attributes import CsvAttributes

# make it as a class with spark-schema, attrs (consider date, time formats as attr)
class SmvSchema(object):
    """
    """
    def __init__(self, _schema):
        if is_string(_schema):
            (s, a) = self._fullStrToSchema(_schema)
        elif isinstance(_schema, T.StructType):
            (s, a) = (
                _schema,
                CsvAttributes(),        # Default csv attributes
            )
        else:
            raise SmvRuntimeError("Unsupported schema type: {}".format(type(_schema)))

        self.schema = s 
        self.attributes = a
        
    def updateAttrs(self, attrs):
        self.attributes.update(attrs)
        return self

    def _strToStructField(self, fieldStr):
        # *? is for non-greedy match
        pattern = re.compile(r"""\s*(?P<name>[^:]*?)\s*:    # Col Name part
                                \s*(?P<dtype>[^@]*?)\s*    # Type part
                                (@metadata=(?P<meta>.*))?  # Meta if any
                                \Z                         #end of string""", re.VERBOSE)
        match = pattern.match(fieldStr)
        name = match.group('name')
        dtype = match.group('dtype')
        meta = match.group('meta') or "{}"

        # Timestamp, date, decimal
        dfmtStr = None
        tfmtStr = None
        if (re.match(r"[Dd]ecimal", dtype)):
            dpat = re.compile(r"""[Dd]ecimal(\[ *(?P<precision>\d+) *(, *(?P<scale>\d+) *)?\])?""")
            dmatch = dpat.match(dtype)
            precision = dmatch.group('precision') or 10
            scale = dmatch.group('scale') or 0
            dtypeStr = "decimal({},{})".format(precision, scale)
        elif (re.match(r"[Dd]ate", dtype)):
            dmatch = re.match(r"[Dd]ate(\[(?P<fmt>.+)\])?", dtype)
            dfmtStr = dmatch.group('fmt')
            dtypeStr = "date"
        elif (re.match(r"[Tt]imestamp", dtype)):
            dmatch = re.match(r"[Tt]imestamp(\[(?P<fmt>.+)\])?", dtype)
            tfmtStr = dmatch.group('fmt')
            dtypeStr = "timestamp"
        elif (re.match(r"[Ss]tring", dtype)):
            # smv allow String[,_SmvStrNull_] type of value. Ignor here
            dtypeStr = "string"
        else:
            dtypeStr = dtype.lower()

        fieldJson = {
            "name": name,
            "type": dtypeStr,
            "nullable": True,
            "metadata": json.loads(meta)
        }

        field = T.StructField.fromJson(fieldJson)
        return (field, dfmtStr, tfmtStr)

    def _strToAttr(self, attrStr):
        pattern = re.compile(r"@\s*(?P<name>\S*)\s*=\s*(?P<value>\S*)\s*")
        match = pattern.match(attrStr)
        name = match.group('name')
        value = match.group('value')
        return (name, value)

    def _strListToSchema(self, smvStrs):
        no_comm = [re.sub(';[ \t]*$', '', r).strip() for r in smvStrs if not (re.match(r"^(//|#).*$", r) or re.match(r"^[ \t]*$", r))]
        attrStrs = [s for s in no_comm if s.startswith("@")]
        fieldStrs = [s for s in no_comm if not s.startswith("@")]

        attrs = dict([self._strToAttr(a) for a in attrStrs])

        fieldlist = []
        dfmtlist = []
        tfmtlist = []
        for s in fieldStrs:
            (field, dfmt, tfmt) = self._strToStructField(s)
            fieldlist.append(field)
            if dfmt:
                dfmtlist.append(dfmt)
            if tfmt:
                tfmtlist.append(tfmt)

        if len(set(dfmtlist)) > 1:
            raise SmvRuntimeError("Date type has multiple formats: {}".format(set(dfmtlist)))
        elif len(set(dfmtlist)) == 1:
            dateFormat = dfmtlist[0]
        else:
            dateFormat = None

        if len(set(tfmtlist)) > 1:
            raise SmvRuntimeError("TimeStamp type has multiple formats: {}".format(set(tfmtlist)))
        elif len(set(tfmtlist)) == 1:
            timestampFormat = tfmtlist[0]
        else:
            timestampFormat = None

        if dateFormat:
            attrs.update({"dateFormat": dateFormat})
        if timestampFormat:
            attrs.update({"timestampFormat": timestampFormat})

        schema = T.StructType(fieldlist)
        return (schema, attrs)


    def _fullStrToSchema(self, smvStr):
        (s, a) = self._strListToSchema(smvStr.split(";"))
        return (s, a)

    def toStrForFile(self):
        # For delimiter, use \t for print
        attr_for_print = {k: r"\t" if k == "delimiter" and v == "\t" else v for k, v in self.attributes.items()}
        attrStr = "\n".join(["@{} = {}".format(k, v) for (k, v) in attr_for_print.items()])
        s = self.schema
        fmtStr = "\n".join([
            "{}: {} @metadata={}".format(name, s[name].dataType.typeName(), json.dumps(s[name].metadata))
            for name in s.fieldNames()
        ])
        return attrStr + "\n\n" + fmtStr

    def addCsvAttributes(self, attr):
        self.attributes.update(attr)
        return self

    @classmethod
    def dicoverFromInferedDF(cls, df):
        raw_schema = df.schema
        first_row = df.limit(1).collect()[0]

        new_schema = T.StructType([])
        for n in raw_schema.fieldNames():
            name_norm = re.sub(r"\W+", "_", n.strip())
            dtype = raw_schema[n].dataType
            meta = {"smvDesc": str(first_row[n])}
            new_schema.add(name_norm, dtype, True, meta)

        return cls(new_schema).addCsvAttributes({"has-header": "true"})
