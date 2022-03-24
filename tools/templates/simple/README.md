# Example SMV2 project

Full details on how to build/run/use this application can be found in [SMV2](https://github.com/ninjapapa/SMV2) docs.

## Run App in batch
```shell
$ cd project_dir
$ spark-submit src/main/python/appdriver.py -- --run-app
```

## Run App in Spark Shell
```shell
$ cd project_dir
$ ../SMV2/tools/smv-shell
```

# Start working on your own project

## Change connection and data-dir

**Data Dir** is a place to hold temporary persisted log and data. Typically on high performance HDFS (if on cloud), or local HD (on single machine). Might grow big and need to clean occasionally.

Under `conf/smv-user-conf.props`
```
smv.dataDir = file://_temp_data_dir
```
or 
```
smv.dataDir = hdfs://_temp_data_dir
```

**Connections** will be referred from `SmvInput` and `SmvOutput` modules by name. Typically defined in 
`conf/connections.props`:
```
smv.conn.myinput.type = hdfs
smv.conn.myinput.path = file://_input_data_location_
```

## Load data
Typically start with reading in CSV files from input connection. Need to copy/move/link csv files to the input dir, and create schema file either manually or through the schema discovery tool from smv-shell.

## Create input module to read in CSV
Using `Employment` as an example in employment.py to create your own input module. Test on smv-shell.

## Change stage name for your project
Change `smv.stages = emp` in `conf/smv-app-conf.props` to your desired project folder name, and also change the folder name under `src/main/python` accordingly. 

## Init Git and checkin to a Git server
