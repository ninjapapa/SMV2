# SmvModule

An SMV Module is a collection of transformation operations and validation rules.  Each module depends on one or more `SmvDataSet`s and defines a set of transformation on its inputs that define the module output.

## Module Dependency Definition
Each module **must** define its input dependency by overriding the `requiresDS` method. The `requiresDS` method should return a sequence of `SmvDataSet`s required as input for this module.

```Python
class MyModule(smv.SmvModule):
  def requiresDS(self):
    return [Mod1,Mod2]
```

Note that `requiresDS` returns a sequence of the actual `SmvDataSet` objects that the module depends on, **not** the name. The dependency can be on any combination of `SmvDataSet`s which may be from smv-input modules (files, Hive tables) or modules, etc. It is not limited to other `SmvModules`.

```python
class MyModule(smv.SmvModule):
  def requiresDS(self):
    return [File1,Hive2,Mod3]
```

## Module Transformation Definition (run)
The module **must** also provide a `run()` method that performs the transformations on the inputs to produce the module output.  The `run` method will be provided with the results (DataFrame) of running the dependent input modules as a map keyed by the dependent module.

```Python
class MyModule(smv.SmvModule):
  def requiresDS(self):
    return [Mod1,Mod2]
  def run(self, i):
    m1df = i[Mod1]
    m2df = i[Mod2]
    return m1df.join(m2df, ...).select("col1", "col2", ...)
```

The `run` method should return the result of the transformations on the input as a `DataFrame`.

The parameter `i` of the `run` method maps `SmvDataSet` to its resulting `DataFrame`. The driver (Smv Framework) will run the dependencies of the `SmvDataSet` to provide this map.

## Module Persistence
To aid in development and debugging, the output of each module is persisted by default.  Subsequent requests for the module output will result in reading the persisted state rather than in re-running the module.
The persisted file is versioned.  The version is computed from the CRC of this module and all dependent modules.  Therefore, if this module code or any of the dependent module code changes, then the module will be re-run.
On a large development team, this makes it very easy to "pull" the latest code changes and regenerate the output by only running the modules that changed.

However, for trivial modules (e.g. filter), it might be too expensive to persist/read the module output.  In these cases, the module may override the default persist behaviour by setting the `isEphemeral` flag to true.  In that case, the module output will not be persisted (unless the module was run explicitly).

### Python
```python
class MyModule(smv.SmvModule):
  def isEphemeral(self): return False
  ....    
```

## Simple Publish To Hive
If you would like to publish the module to a Hive table, include a `tableName`, and use `--publish-hive` command line parameter to publish/export the output to the specified Hive table (For advanced use case, see section below).  For example:

```python
class MyModule(smv.SmvModule, smv.SmvOutput):
  def tableName(self):
    return "hiveschema.hivetable"
  ...
class MyFile(smv.SmvCsvFile, smv.SmvOutput):
  ...
```
