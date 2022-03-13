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
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
import pyspark.sql.functions as F

from smv.error import SmvRuntimeError

if sys.version_info >= (3, 0):
    from functools import reduce

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
            labels = [ (n, "{} : {}".format(n, t)) for (n, t) in self.df.dtypes ]
            width = max([len(l) for (n, l) in labels])
            for (n, l) in labels:
                print("{} = {}".format(l, rec[n]))


class ColumnHelper(object):
    def __init__(self, col):
        self.col = col

        fullstr = str(self.col._jc.toString())
        self.name = fullstr.split(" AS ")[1].strip('`') if " AS " in fullstr else fullstr


    def smvGetColName(self):
        """Returns the name of a Column as a sting

            Example:
            >>> df.a.smvGetColName()

            Returns:
                (str)
        """
        return self.name

    def smvAsUTC(self):
        """Take the string part of a timestamp column and make it in UTC TZ.
        Note: this actually changed the timestamp if the original TZ is not UTC
        """
        return F.to_timestamp(
            F.concat(F.date_format(self.col, 'yyyy-MM-dd HH:mm:SS'), F.lit('UTC')), 
            'yyyy-MM-dd HH:mm:SSz'
        ).alias("smvAsUTC({})".format(self.name))

    def smvDay70(self):
        """Convert a Timestamp to the number of days from 1970-01-01
            d70 = 0 is a Thursday
            If consider Sunday as the start of a week and define 
            week70=0 is the week where 1970-01-01 is in 

            week70 = (d70 - 3) / 7 + 1

            Example:
                >>> df.select(col("dob").smvDay70())

            Returns:
                (Column): IntegerType. Number of days from 1970-01-01 (start from 0)
        """

        return (F.unix_timestamp(self.smvAsUTC()) / 24 / 3600).cast('int').alias("smvDay70({})".format(self.name))

    def smvMonth70(self):
        """Convert a Timestamp to the number of months from 1970-01-01

            Example:
                >>> df.select(col("dob").smvMonth70())

            Returns:
                (Column): IntegerType. Number of months from 1970-01-01 (start from 0)
        """
        year = F.year(self.col)
        month = F.month(self.col)
        return ((year - 1970) * 12 + month - 1).alias("smvMonth70({})".format(self.name))


# Initialize DataFrame and Column with helper methods. Called by SmvApp.
def init_helpers():
    _helpCls(Column, ColumnHelper)
    _helpCls(DataFrame, DataFrameHelper)
