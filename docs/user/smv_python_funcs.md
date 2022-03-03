# SMV Python Functions

SMV provides both the module framework and some additional helper functions. Users
and developers can pickup the framework just by following the template project (cretaed by `smv-init`)
code styles.

For the helper functions, the full Python API documentation should be generated from code. However it will
take some time for us to determine which Python document framework we should take. In the mean time we do
need to have some API document to make sure users can use the powerful Python interface. This doc plays that role.

#### smvJoinByKey
```python
smvJoinByKey(otherPlan, keys, joinType)
```

The Spark `DataFrame` join operation does not handle duplicate key names. If both left and
right side of the join operation contain the same key, the result `DataFrame` is unusable.

The `joinByKey` method will allow the user to join two `DataFrames` using the same join key.
Post join, only the left side keys will remain. In case of outer-join, the
`coalesce(leftkey, rightkey)` will replace the left key to be kept.

```python
df1.smvJoinByKey(df2, Seq("k"), "inner")
```

If, in addition to the duplicate keys, both df1 and df2 have column with name "v",
both will be kept in the result, but the df2 version will be prefix with "\_".

#### smvUnion
```python
smvUnion(*dfothers)
```

`smvUnion` unions DataFrames with different numbers of columns by column name & schema.
Spark unionAll ignores column names & schema, and can only be performed on tables with the same number of columns.