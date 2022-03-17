
![SMV2](docs/images/smv2-logo-100px.png)

# Spark Modularized View 2 (SMV2)

SMV2 is based on [SMV](https://github.com/TresAmigosSD/SMV) with the following changes:

* Totally removed the SCALA/JAVA dependency, pure Python
* Removed most SMV specific DataFrame functions and helpers (additional DF and Column methods) since current PySpark functions are pretty comprehensive, no need and should not encourage using SMV functions
* Removed [EDD](https://github.com/TresAmigosSD/SMV/blob/master/docs/user/edd.md). Although EDD has an elegant design and implementation, we barely use it in real world projects. Could bring back when see need.
  
Spark Modularized View enables users to build enterprise scale applications on Apache Spark platform.

* [User Guide](docs/user/0_user_toc.md)
* [Python API Doc](https://ninjapapa.github.io/SMV2/python_api/)

# SMV2 Quickstart

## Installation

Current SMV2 is tested on 

* Spark 2.4.8 (Should work on any Spark 2 distribution)
* Spark 3.2.1
* Python 2.7.* (for better interactive experience, install IPython for Python2)
* Python 3.8.* (for better interactive experience, install IPython)

SMV2 can easily setup on Linux and MACOS (Intel silicon, M1 chip MAC book does not have full Python2.7 support) env with or without Hadoop. 

### Python 
There are a lot tutorial online for how to setup different python versions on your system. Need to make sure you have `pip` command also set up 
on the correct Python version so that you can install python packages.

Install python packages:
```
$ pip install -r tools/requirements.txt
```

There are some challenges of using Python 2.7 on M1 chip Mac books. Although Python 2.7 is supported, no easy way to setup `pip`. 


### Setup Spark
Download a Spark bin tar release, such as: https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz or other versions as in
'.supported_spark'. 

```sh
$ cd ~
$ tar zx spark-2.4.8-bin-hadoop2.7.tgz
```

Need to setup `SPARK_HOME` env var in `bashrc` or other profile file for the shell you use:
```
export SPARK_HOME="${HOME}/park-2.4.8-bin-hadoop2.7"
export PATH="${SPARK_HOME}/bin:${PATH}"
```
Then reload shell to have the env vars take effect.

You can test spark by:

```sh
$ pyspark
```

You should see something like

```
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 2.3.3
      /_/

Using Python version 2.7.18 (default, Mar  8 2021 13:02:45)
SparkSession available as 'spark'.
```

To enhance the interactive experience in `pyspark` shell, you can set the pyshell to IPython by an env var:
```
export PYSPARK_DRIVER_PYTHON=ipython2
```
where the value of the var is thc command of IPython on your env.

### Install SMV2 

Just download the lasted release from https://github.com/ninjapapa/SMV2/releases. And unarchive to `${HOME}` dir. It typically under `SMV2` folder.

Need to setup env vars for SMV_HOME, PATH and PYTHONPATH:
```sh
export SMV_HOME="${HOME}/SMV2"
export PATH="${SMV_HOME}/tools:${PATH}"
export PYTHONPATH="$SMV_HOME/src/main/python:$PYTHONPATH"
```

## Create Example App

SMV provides a shell script to easily create template applications. We will use a simple example app to explore SMV.

```shell
$ SMV2/tools/smv-init -s MyApp
```

## Run Example App

Run the entire application with

```shell
$ cd MyAPP
$ ../SMV2/tools/smv-run --run-app
```

This command must be run from the root of the project.

The output csv file and schema can be found in the `data/output` directory. 

```shell
$ cat data/output/employment_by_state.csv/part-* | head -5
"50",245058
"51",2933665
"53",2310426
"54",531834
"55",2325877

$ cat data/output/employment_by_state.schema
@has-header = false
@delimiter = ,
@quote-char = "
@timestampFormat = yyyy-MM-dd HH:mm:ss
@dateFormat = yyyy-MM-dd

ST: string @metadata={}
EMP: long @metadata={}
```

Or you can run it SMV interactive shell (using PySpark shell):
```shell
$ cd MyApp
$ ../SMV2/tools/smv-shell
```

You can run some command in the shell like below:
```
In [1]: ls()

stage1:
  (I) employment.Employment
  (O) employment.EmploymentByState

In [2]: data1 = df("EmploymentByState")

In [3]: data1.peek()
ST : string = 51
EMP : bigint = 2933665
```

Basically you can access the output of an `SmvModule` (e.g. "EmploymentByState") by calling `df` function and get the `DataFrame` to play with.

## Edit Example App

The `EmploymentByState` module is defined in `src/python/stage1/employment.py`:

```shell
class EmploymentByState(SmvModule, SmvOutput):
    """Python ETL Example: employment by state"""

    def requiresDS(self):
        return [inputdata.Employment]

    def run(self, i):
        df = i[inputdata.Employment]
        return df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))
```

The `run` method of a module defines the operations needed to get the output based on the input. We would like to create a new column to indicate 
whether a state has more than 1M employees:

```shell
  def run(self, i):
      df = i[inputdata.Employment]
      df1 = df.groupBy(col("ST")).agg(sum(col("EMP")).alias("EMP"))
      df2 = df1.withColumn('is_large', F.col("EMP") > 1000000)
      return df
```

The best practice is to open an Smv-shell and an editor in a different window (either VS-Code or just Vim) of the code editing. 
After finished editing and save the file, you can access the new module results from the shell.

For example continuing on the previous smv-shell example, we can call `df` method again

```
In [4]: data2 = df("EmploymentByState")

In [5]: data2.peek()
ST : string = 51
EMP : bigint = 2933665
is_large : boolean = True

In [6]: data1.peek()
ST : string = 51
EMP : bigint = 2933665
```

Please note, at this point you can still access the old DataFrame `data1`, so you can even do a comparison check between the new one 
and old one.

See the [user guide](docs/user/0_user_toc.md) for further examples and documentation.
