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
"""SMV DataFrame Helpers and Column Helpers

    This module provides the helper functions on DataFrame objects and Column objects
"""
import sys
import inspect

import decorator
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
import pyspark.sql.functions as F
from pyspark.sql.types import DataType

from smv.utils import smv_copy_array
from smv.error import SmvRuntimeError
from smv.utils import is_string

if sys.version_info >= (3, 0):
    from functools import reduce

# Modified from Spark column.py
def _to_seq(cols, converter=None):
    """
    Convert a list of Column (or names) into a JVM Seq of Column.

    An optional `converter` could be used to convert items in `cols`
    into JVM Column objects.
    """
    if converter:
        cols = [converter(c) for c in cols]
    return _sparkContext()._jvm.PythonUtils.toSeq(cols)

def _sparkContext():
    return SparkContext._active_spark_context

def _getUnboundMethod(helperCls, oldMethod):
    def newMethod(_oldMethod, self, *args, **kwargs):
        return _oldMethod(helperCls(self), *args, **kwargs)

    return decorator.decorate(oldMethod, newMethod)


def _helpCls(receiverCls, helperCls):
    iscallable = lambda f: hasattr(f, "__call__")
    for name, oldMethod in inspect.getmembers(helperCls, predicate=iscallable):
        # We will use decorator.decorate to ensure that attributes of oldMethod, like
        # docstring and signature, are inherited by newMethod. decorator.decorate
        # won't accept an unbound method, so for Python 2 we extract oldMethod's
        # implementing function __func__. In Python 3, inspect.getmembers will return
        # the implementing functions insead of unbound method - this is due to
        # Python 3's data model.
        try:
            impl = oldMethod.__func__
        except:
            impl = oldMethod
        if not name.startswith("_"):
            newMethod = _getUnboundMethod(helperCls, impl)
            setattr(receiverCls, name, newMethod)

def _mkUniq(collection, candidate, ignorcase = False, postfix = None):
    """
    Repeatedly changes `candidate` so that it is not found in the `collection`.
    Useful when choosing a unique column name to add to a data frame.
    """
    if ignorcase:
        col_cmp = [i.lower() for i in collection]
        can_cmp = candidate.lower()
    else:
        col_cmp = collection
        can_cmp = candidate
    
    can_to = "_" + can_cmp if postfix is None else can_cmp + postfix
    if can_to in col_cmp:
        res = _mkUniq(col_cmp, can_to, ignorcase, postfix)
    else:
        res = can_to

    return res

class DataFrameHelper(object):
    def __init__(self, df):
        self.df = df
       
    def smvJoinByKey(self, other, keys, joinType, isNullSafe=False):
        """joins two DataFrames on a key

            Check none key columns for duplicate and rename right DF duplicate column before join.

            Same as Spark native join funcation if not NullSafe.
            For NullSafe specified, replicate Spark native join behaviour on NullSafe case
            Post join, only the left side keys will remain. In case of outer-join, the
            `coalesce(leftkey, rightkey)` will replace the left key to be kept.

            Args:
                other (DataFrame): the DataFrame to join with
                keys (list(string)): a list of column names on which to apply the join
                joinType (string): choose one of ['inner', 'outer', 'leftouter', 'rightouter', 'leftsemi']
                isNullSafe (boolean): if true matches null keys between left and right tables and keep in output. Default False. 

            Example:
                >>> df1.smvJoinByKey(df2, ["k"], "inner")
                >>> df_with_null_key.smvJoinByKey(df2, ["k"], "inner", True)

            Returns:
                (DataFrame): result of the join operation
        """
        
        # Check right DF non-key col names for duplicates``
        left_nonkey_cols = [i for i in self.df.columns if i not in keys]
        right_need_change = [i for i in other.columns if i not in keys and i in left_nonkey_cols]

        # Rename right DF duplicated column with prefix "_"
        other_name_changed = other
        for i in right_need_change:
            other_name_changed = other_name_changed.withColumnRenamed(i, _mkUniq(left_nonkey_cols, i, ignorcase=True))

        if isNullSafe:
            keys_rn = [(k, _mkUniq(other.columns, k)) for k in keys]
            key_name_changed = other_name_changed
            for (k, nk) in keys_rn:
                key_name_changed = key_name_changed.withColumnRenamed(k, nk)
            joinOpt = reduce(lambda a, b: a & b, [self.df[k].eqNullSafe(key_name_changed[nk]) for (k, nk) in keys_rn])
            joinOut = self.df.join(key_name_changed, joinOpt, joinType)
            res = joinOut
            # for each key used in NullSafe, coalesce key value from left to right (replicate spark "join" on key behavior)
            # Only impact outer join result, since inner join will have both key the same
            for (k, nk) in keys_rn:
                res = res.withColumn(k, F.coalesce(k, nk)).drop(nk)
        else:
            # native join on key can't do null safe
            res = self.df.join(other_name_changed, keys, joinType)

        return res

    def smvUnion(self, dfother):
        """Unions DataFrames with different number of columns by column name and schema

            Spark unionAll ignores column names & schema, and can only be performed on tables with the same number of columns.

            Args:
                dfOther (DataFrame): the dataframe to union with

            Example:
                >>> df.smvUnion(df2)

            Returns:
                (DataFrame): the union of all specified DataFrames
        """
        # val leftNeed   = dfother.columns diff df.columns
        left_need = list(set(dfother.columns) - set(self.df.columns))
        left_name_type = [(i, dfother.schema[i].dataType) for i in left_need]
        leftFull = self.df.select("*", *[F.lit(None).cast(t).alias(n) for (n, t) in left_name_type])

        right_need = list(set(self.df.columns) - set(dfother.columns))
        right_name_type = [(i, self.df.schema[i].dataType) for i in right_need]
        rightFull = dfother.select("*", *[F.lit(None).cast(t).alias(n) for (n, t) in right_name_type])

        overlap = list(set(self.df.columns).intersection(set(dfother.columns)))
        cols_diff_struct = [i for i in overlap if self.df.schema[i].dataType != dfother.schema[i].dataType]

        if bool(cols_diff_struct):
            raise SmvRuntimeError("fail to union columns with same name but different StructTypes:" + ", ".join(cols_diff_struct))

        return leftFull.union(rightFull.select(*(leftFull.columns)))

    def peek(self, pos = 1):
        """Display a DataFrame row in transposed view

            Args:
                pos (integer): the n-th row to display, default as 1
                colRegex (string): show the columns with name matching the regex, default as ".*"

            Returns:
                (None)
        """
        rows = self.df.take(pos)
        if (bool(rows)):
            rec = rows[-1]  # last row
            labels = [ (s.name, "{} : {}".format(s.name, str(s.dataType).replace("Type", ""))) for s in self.df.schema ]
            width = max([len(l) for (n, l) in labels])
            for (n, l) in labels:
                print("{} = {}".format(l, rec[n]))


class ColumnHelper(object):
    def __init__(self, col):
        self.col = col
        self._jc = col._jc
        self._jvm = _sparkContext()._jvm
        self._jPythonHelper = self._jvm.SmvPythonHelper
        self._jColumnHelper = self._jvm.ColumnHelper(self._jc)

    def smvGetColName(self):
        """Returns the name of a Column as a sting

            Example:
            >>> df.a.smvGetColName()

            Returns:
                (str)
        """
        return self._jColumnHelper.getName()

    def smvIsAllIn(self, *vals):
        """Returns true if ALL of the Array columns' elements are in the given parameter sequence

            Args:
                vals (\*any): vals must be of the same type as the Array content

            Example:
                input DF:

                    +---+---+
                    | k | v |
                    +===+===+
                    | a | b |
                    +---+---+
                    | c | d |
                    +---+---+
                    |   |   |
                    +---+---+

                >>> df.select(array(col("k"), col("v")).smvIsAllIn("a", "b", "c").alias("isFound"))

                output DF:

                    +---------+
                    | isFound |
                    +=========+
                    |  true   |
                    +---------+
                    |  false  |
                    +---------+
                    |  false  |
                    +---------+

            Returns:
                (Column): BooleanType
        """
        jc = self._jPythonHelper.smvIsAllIn(self._jc, _to_seq(vals))
        return Column(jc)

    def smvIsAnyIn(self, *vals):
        """Returns true if ANY one of the Array columns' elements are in the given parameter sequence

            Args:
                vals (\*any): vals must be of the same type as the Array content

            Example:
                input DF:

                    +---+---+
                    | k | v |
                    +===+===+
                    | a | b |
                    +---+---+
                    | c | d |
                    +---+---+
                    |   |   |
                    +---+---+

                >>> df.select(array(col("k"), col("v")).smvIsAnyIn("a", "b", "c").alias("isFound"))

                output DF:

                    +---------+
                    | isFound |
                    +=========+
                    |  true   |
                    +---------+
                    |  true   |
                    +---------+
                    |  false  |
                    +---------+

            Returns:
                (Column): BooleanType
        """
        jc = self._jPythonHelper.smvIsAnyIn(self._jc, _to_seq(vals))
        return Column(jc)

    def smvMonth(self):
        """Extract month component from a timestamp

            Example:
                >>> df.select(col("dob").smvMonth())

            Returns:
                (Column): IntegerType. Month component as integer, or null if input column is null
        """
        jc = self._jColumnHelper.smvMonth()
        return Column(jc)

    def smvYear(self):
        """Extract year component from a timestamp

            Example:
                >>> df.select(col("dob").smvYear())

            Returns:
                (Column): IntegerType. Year component as integer, or null if input column is null
        """
        jc = self._jColumnHelper.smvYear()
        return Column(jc)

    def smvQuarter(self):
        """Extract quarter component from a timestamp

            Example:
                >>> df.select(col("dob").smvQuarter())

            Returns:
                (Column): IntegerType. Quarter component as integer (1-based), or null if input column is null
        """
        jc = self._jColumnHelper.smvQuarter()
        return Column(jc)

    def smvDayOfMonth(self):
        """Extract day of month component from a timestamp

            Example:
                >>> df.select(col("dob").smvDayOfMonth())

            Returns:
                (Column): IntegerType. Day of month component as integer (range 1-31), or null if input column is null
        """
        jc = self._jColumnHelper.smvDayOfMonth()
        return Column(jc)

    def smvDayOfWeek(self):
        """Extract day of week component from a timestamp

            Example:
                >>> df.select(col("dob").smvDayOfWeek())

            Returns:
                (Column): IntegerType. Day of week component as integer (range 1-7, 1 being Monday), or null if input column is null
        """
        jc = self._jColumnHelper.smvDayOfWeek()
        return Column(jc)

    def smvHour(self):
        """Extract hour component from a timestamp

            Example:
                >>> df.select(col("dob").smvHour())

            Returns:
                (Column): IntegerType. Hour component as integer, or null if input column is null
        """
        jc = self._jColumnHelper.smvHour()
        return Column(jc)

    def smvPlusDays(self, delta):
        """Add N days to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of days to add

            Example:
                >>> df.select(col("dob").smvPlusDays(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType

            Please note that although Spark's `date_add` function does the similar
            thing, they are actually different.

            - Both can act on both `Timestamp` and `Date` types
            - `smvPlusDays` always returns `Timestamp`, while `F.date_add` always returns
              `Date`
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusDays(jdelta)
        return Column(jc)

    def smvPlusWeeks(self, delta):
        """Add N weeks to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of weeks to add

            Example:
                >>> df.select(col("dob").smvPlusWeeks(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusWeeks(jdelta)
        return Column(jc)

    def smvPlusMonths(self, delta):
        """Add N months to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of months to add

            Note:
                The calculation will do its best to only change the month field retaining the same day of month. However, in certain circumstances, it may be necessary to alter smaller fields. For example, 2007-03-31 plus one month cannot result in 2007-04-31, so the day of month is adjusted to 2007-04-30.

            Example:
                >>> df.select(col("dob").smvPlusMonths(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType

            Please note that although Spark's `add_months` function does the similar
            thing, they are actually different.

            - Both can act on both `Timestamp` and `Date` types
            - `smvPlusMonths` always returns `Timestamp`, while `F.add_months` always returns
              `Date`
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusMonths(jdelta)
        return Column(jc)

    def smvPlusYears(self, delta):
        """Add N years to `Timestamp` or `Date` column

            Args:
                delta (int or Column): the number of years to add

            Example:
                >>> df.select(col("dob").smvPlusYears(3))

            Returns:
                (Column): TimestampType. The incremented Timestamp, or null if input is null.
                    **Note** even if the input is DateType, the output is TimestampType
        """
        if (isinstance(delta, int)):
            jdelta = delta
        elif (isinstance(delta, Column)):
            jdelta = delta._jc
        else:
            raise RuntimeError("delta parameter must be either an int or a Column")
        jc = self._jColumnHelper.smvPlusYears(jdelta)
        return Column(jc)

    def smvStrToTimestamp(self, fmt):
        """Build a timestamp from a string

            Args:
                fmt (string): the format is the same as the Java `Date` format

            Example:
                >>> df.select(col("dob").smvStrToTimestamp("yyyy-MM-dd"))

            Returns:
                (Column): TimestampType. The converted Timestamp
        """
        jc = self._jColumnHelper.smvStrToTimestamp(fmt)
        return Column(jc)

    def smvTimestampToStr(self, timezone, fmt):
        """Build a string from a timestamp and timezone

            Args:
                timezone (string or Column): the timezone follows the rules in 
                    https://www.joda.org/joda-time/apidocs/org/joda/time/DateTimeZone.html#forID-java.lang.String-
                    It can be a string like "America/Los_Angeles" or "+1000". If it is null, use current system time zone.
                fmt (string): the format is the same as the Java `Date` format

            Example:
                >>> df.select(col("ts").smvTimestampToStr("America/Los_Angeles","yyyy-MM-dd HH:mm:ss"))

            Returns:
                (Column): StringType. The converted String with given format
        """
        if is_string(timezone):
            jtimezone = timezone
        elif isinstance(timezone, Column):
            jtimezone = timezone._jc
        else:
            raise RuntimeError("timezone parameter must be either an string or a Column")
        jc = self._jColumnHelper.smvTimestampToStr(jtimezone, fmt)
        return Column(jc)

    def smvDay70(self):
        """Convert a Timestamp to the number of days from 1970-01-01

            Example:
                >>> df.select(col("dob").smvDay70())

            Returns:
                (Column): IntegerType. Number of days from 1970-01-01 (start from 0)
        """
        jc = self._jColumnHelper.smvDay70()
        return Column(jc)

    def smvMonth70(self):
        """Convert a Timestamp to the number of months from 1970-01-01

            Example:
                >>> df.select(col("dob").smvMonth70())

            Returns:
                (Column): IntegerType. Number of months from 1970-01-01 (start from 0)
        """
        jc = self._jColumnHelper.smvMonth70()
        return Column(jc)

    def smvTimeToType(self):
        """smvTime helper to convert `smvTime` column to time type string

            Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
            Example output type "quarter", "month", "day"
        """
        jc = self._jColumnHelper.smvTimeToType()
        return Column(jc)

    def smvTimeToIndex(self):
        """smvTime helper to convert `smvTime` column to time index integer

            Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
            Example output 172, 551, 16405 (# of quarters, months, and days from 19700101)
        """
        jc = self._jColumnHelper.smvTimeToIndex()
        return Column(jc)

    def smvTimeToLabel(self):
        """smvTime helper to convert `smvTime` column to human readable form

             Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
             Example output "2013-Q1", "2015-12", "2014-12-01"
        """
        jc = self._jColumnHelper.smvTimeToLabel()
        return Column(jc)

    def smvTimeToTimestamp(self):
        """smvTime helper to convert `smvTime` column to a timestamp at the beginning of
            the given time pireod.

             Example `smvTime` values (as String): "Q201301", "M201512", "D20141201"
             Example output "2013-01-01 00:00:00.0", "2015-12-01 00:00:00.0", "2014-12-01 00:00:00.0"
        """
        jc = self._jColumnHelper.smvTimeToTimestamp()
        return Column(jc)

    def smvArrayFlatten(self, elemType):
        """smvArrayFlatten helper applies flatten operation on an Array of Array
            column.

            Example:
                >>> df.select(col('arrayOfArrayOfStr').smvArrayFlatten(StringType()))

            Args:
                elemType (DataType or DataFram): array element's data type,
                    in object form or the DataFrame to infer the
                    element data type
        """
        if(isinstance(elemType, DataType)):
            elemTypeJson = elemType.json()
        elif(isinstance(elemType, DataFrame)):
            elemTypeJson = elemType.select(self.col)\
                .schema.fields[0].dataType.elementType.elementType.json()
        else:
            raise SmvRuntimeError("smvArrayFlatten does not support type: {}".format(type(elemType)))

        jc = self._jColumnHelper.smvArrayFlatten(elemTypeJson)
        return Column(jc)



# Initialize DataFrame and Column with helper methods. Called by SmvApp.
def init_helpers():
    _helpCls(Column, ColumnHelper)
    _helpCls(DataFrame, DataFrameHelper)
