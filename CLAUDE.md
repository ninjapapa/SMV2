# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

**SMV2 (Spark Modularized View 2)** is a pure Python framework for building enterprise-scale ETL applications on Apache Spark. It provides a dependency-based module system with automatic caching, persistence, and metadata tracking. Key differentiators from its predecessor SMV: no Scala/Java dependency, no EDD component, and relies on modern PySpark instead of custom DataFrame helpers.

## Environment Setup

```sh
export SMV_HOME="${HOME}/SMV2"
export PATH="${SMV_HOME}/tools:${PATH}"
export PYTHONPATH="$SMV_HOME/src/main/python:$PYTHONPATH"
```

Install Python dependencies:
```sh
pip install -r tools/requirements.txt
```

Spark must be installed separately and `SPARK_HOME` set. The framework is tested against Spark versions listed in `admin/.spark_to_test`.

## Build & Install Commands

```sh
make install          # Download default Spark version and install
make install-full     # Install all tested Spark/Python combinations
make clean            # Remove .tox and bundles
make local_bundle     # Create release tarball (smv2_vX.Y.Z.tgz)
make py-doc           # Generate Sphinx API docs
```

## Testing

```sh
# Run all tests (unit + integration) with default Spark/Python
make test

# Unit tests only
make test-python

# Integration tests only (creates a sample app and runs spark-submit)
make test-integration

# Full matrix testing across all Spark/Python combinations
make test-full
```

### Running a Single Test File or Test Class

Tests use Python `unittest` and are run via the `smv-pytest` tool. On macOS, `SPARK_LOCAL_IP=127.0.0.1` is required to fix an IPv6 binding issue:

```sh
# Run all unit tests (macOS) — JAVA_HOME, SMV_HOME, PYTHONPATH, and SPARK_LOCAL_IP must be set explicitly
JAVA_HOME="/Users/bo.i.zhang/jdk-17.0.18+8/Contents/Home" \
  SPARK_LOCAL_IP=127.0.0.1 SMV_HOME=$(pwd) PYTHONPATH=$(pwd)/src/main/python \
  bash tools/smv-pytest --spark-home .sparks/4.0.2

# Run a specific test file (by filename without .py)
JAVA_HOME="/Users/bo.i.zhang/jdk-17.0.18+8/Contents/Home" \
  SPARK_LOCAL_IP=127.0.0.1 SMV_HOME=$(pwd) PYTHONPATH=$(pwd)/src/main/python \
  bash tools/smv-pytest --spark-home .sparks/4.0.2 -t testDataSetMgr
```

**Java requirement**: Spark 4.0.x requires Java 17 or 21. Java 23+ is incompatible due to removed Hadoop security APIs. Recommended: [Eclipse Temurin JDK 17](https://adoptium.net/temurin/releases/?version=17). Set `JAVA_HOME` to the JDK's `Contents/Home` directory.

Test files live in `src/test/python/` and follow the naming pattern `testXxx.py`.

## Architecture

### Module System (Core Abstraction)

The central concept is the **SmvModule**: a named, self-describing unit of Spark transformation that declares its dependencies. The framework handles caching, re-execution, and metadata automatically.

```python
class MyModule(SmvModule, SmvOutput):
    def requiresDS(self):
        return [UpstreamModule]

    def run(self, i):
        df = i[UpstreamModule]
        return df.filter(...)
```

Key module classes in `src/main/python/smv/`:
- `SmvGenericModule` (`smvgenericmodule.py`) — abstract base; defines hashing, metadata, persistence interface
- `SmvModule` (`smvmodule.py`) — concrete base for user ETL modules
- `SmvOutput` mixin — marks a module as a stage output (appears in CLI `-s` listings)
- `SmvInput` subclasses (`iomod/inputs.py`) — CSV, Parquet, JDBC, Hive, XML readers
- `SmvModuleRunner` (`smvmodulerunner.py`) — executes a single module (resolve deps → load or run → persist → metadata)

### Execution Flow

```
SmvApp.runModule(fqn)
  → DataSetMgr resolves FQN to class
  → SmvModuleRunner executes:
      1. Resolve dependencies (requiresDS)
      2. Check persisted hash — skip re-run if unchanged
      3. Call module.run(i) with dependency DataFrames
      4. Compute metadata (schema, DQM, user-defined)
      5. Persist to disk (parquet/pickle/json)
      6. Cache result in memory
```

Hash-based cache invalidation: `SmvGenericModule` computes a hash from source code (comments stripped). Changing module code automatically invalidates the cache.

### Key Files

| File | Role |
|---|---|
| `src/main/python/smv/smvapp.py` | Singleton orchestrator; holds SparkSession, module registry, provider cache |
| `src/main/python/smv/smvgenericmodule.py` | Abstract base for all modules; hashing, metadata, persistence |
| `src/main/python/smv/smvmodulerunner.py` | Execution engine for a single module |
| `src/main/python/smv/datasetmgr.py` | Module discovery and dependency resolution |
| `src/main/python/smv/datasetrepo.py` | Dynamic Python module loading (hot-reload) |
| `src/main/python/smv/smvconfig.py` | Configuration from CLI args, props files, and env |
| `src/main/python/smv/iomod/` | I/O strategy implementations (CSV, Parquet, JDBC, Hive) |
| `src/main/python/smv/helpers.py` | DataFrame/Column extension methods |
| `src/main/python/smv/smvmetadata.py` | Metadata structure and history |
| `src/main/python/test_support/smvbasetest.py` | Base test class with SparkSession setup and helpers |

### Configuration Precedence (highest to lowest)

1. CLI arguments
2. Kernel config (dynamic, runtime)
3. `conf/smv-app-conf.props`
4. `conf/connections.props`
5. `conf/smv-user-conf.props`
6. `~/.smv/smv-user-conf.props`

### Running a User App

```sh
# Run entire app
spark-submit src/main/python/appdriver.py

# Run a specific module by class name
spark-submit src/main/python/appdriver.py -- -m EmploymentByStateOut
```

Note the `--` before SMV args to prevent `spark-submit` from parsing them.

### Interactive Shell

```sh
cd MyApp
smv-shell          # PySpark shell with SMV extensions

# Inside the shell:
ls()               # List available modules
df("ModuleName")   # Run module and return DataFrame
fullRun("ModuleName")  # Run and persist intermediate results
```

### Testing Conventions

- Test classes extend `SmvBaseTest` from `test_support/smvbasetest.py`
- `SmvBaseTest` provides: `createDF(schema, data)`, `df(fqn)`, `load(*fqn)`, `should_be_same(expected, result)`
- Each test class gets an isolated temp data directory under `target/pytest/<ClassName>/`
- Resource modules for a test `testFoo.py` go in `src/test/python/testFoo/`
