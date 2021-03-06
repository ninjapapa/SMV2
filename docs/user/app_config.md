# SMV Application Configuration

SMV provides multiple ways to set the runtime configuration.  Multiple config files are utilized to
facilitate the ability to define static application level configuration and easily allow the user to
provide their own configuration.  This also allows the static application level configuration to be
"checked-in" with the individual apps.  In addition to the configuration files, the user is able
to override any of the options using the command line interface.

## Command line override

For any given option X, the user may override the value specified in any of the configuration files as follows:

```
spark-submit src/main/driver.py -- --smv-props X=55 ... -m module_to_run
```

multiple properties may be specified at the same time:

```
spark-submit src/main/driver.py -- --smv-props smv.appName="myApp" smv.stages="s1,s2" ... -m module_to_run
```

Some configuration parameters have a shorthand direct command line override (e.g. --data-dir)

## Name conflict resolution

If a configuration parameter appears in multiple places, then the conflict is resolved by picking the value from the higher priority source.
The sources are ordered from high to low priority as follows:

* command line options
* project user level config file (default: conf/smv-user-conf.props)
* global user level config file (~/.smv/smv-user-conf.props)
* app level config file (default: conf/smv-app-conf.props)
* smv default options

Since SMV support both `smv-user-conf.props` and `smv-app-conf.props`, a typical practice is to
put project level shared (accross team members) config into `smv-app-conf.props` and user
specific config into `smv-user-conf`. Also config the version control system to ignore
`conf/smv-user-conf.props`.

The property files `smv-app-conf.props` and `smv-user-conf.props` are standard Jave property files.
Here is an `smv-app-conf.props` example:
```
# application name
smv.appName = My Sample App

# stage definitions
smv.stages = etl, \
             account, \
             customer

# spark sql properties
spark.sql.shuffle.partitions = 256

# Runtime config
smv.config.keys=sample
smv.config.sample=full
```

Here is an `smv-user-conf.props` example:
```
# data dir
smv.dataDir = hdfs:///user/myunixname/data

smv.config.sample=1pct
```

## Connection Config 

Connections are ways for SMV to specify input or output connections to DB, HDFS or other storages. Please refer 
[SMV Input Doc](smv_input.md#Connections) for details. 

To config a connection, one need to specify connection type and other connection attributes for a given type. For example:
```
smv.conn.myinput.type = hdfs
smv.conn.myinput.path = file://project_path/data/input

smv.conn.myoutput.type = hdfs
smv.conn.myoutput.path = file://project_path/data/output
```

One can put it in either `smv-app-conf.props` or `smv-user-conf.props`. Or for best practice, in a separate file:
```
connections.props
```

## SMV Config Parameters

The table below describes all available SMV configuration parameters.
Note that for sequence/list type parameters (e.g. smv.stages), a "," or ":" can be used to separate out the items in the list.
**For command line arguments, only the ":" can be used as a separator.**
<table>
<tr>
<th>Property Name</th>
<th>Default</th>
<th>Required/<br>Optional</th>
<th>Description</th>
</tr>

<tr>
<td>smv.appName</td>
<td>appName from SparkSession</td>
<td>Optional</td>
<td>The application name. When specify SparkSession in an app driver file, the appName of SparkSession with be used.</td>
</tr>

<tr>
<td>smv.stages</td>
<td>empty</td>
<td>Required</td>
<td>List of stage names in application.<br>Example: "etl, model, ui"</td>
</tr>

<tr>
<td>smv.lock</td>
<td>False</td>
<td>Optional</td>
<td>When set to "true" or "True", data persisting and metadata persisting will be combined in an atom with file base lock. The lock files will be under <code>smv.lockDir</code></td>
</tr>

<tr>
<th colspan="4">Data Directories Parameters</th>
</tr>

<tr>
<td>smv.dataDir</td>
<td>N/A</td>
<td>Required</td>
<td>The top level data directory. Mainly for tmp persistence
Can be overridden using <code>--data-dir</code> command line option</td>
</tr>

<tr>
<td>smv.lockDir</td>
<td>dataDir<code>/lock</code></td>
<td>Optional</td>
<td>If <code>smv.lock</code> specified, dir for lock files</td>
</tr>

<tr>
<td>smv.publishDir</td>
<td>dataDir<code>/publish</code></td>
<td>Optional</td>
<td>Data publish directory
</tr>

<tr>
<th colspan="4">Jdbc Parameter</th>
</tr>

<tr>
<td>smv.jdbc.url</td>
<td>None</td>
<td>Required for use with JDBC, otherwise optional</td>
<td>JDBC url to use for publishing and reading tables</td>
</tr>

<tr>
<td>smv.jdbc.driver</td>
<td>None</td>
<td>Required for use with JDBC, otherwise optional</td>
<td>JDBC driver to use for publishing and reading tables</td>
</tr>

<tr>
<td>smv.runtimeConfigFile</td>
<td>None</td>
<td>Optional</td>
<td>Relative path to the file which defines runtime props </td>
</tr>

<tr>
<td>smv.kernelConfigFile</td>
<td>None</td>
<td>Optional</td>
<td>Relative path to the file which defines spark conf props to create Spark session</td>
</tr>

</table>

Please refer [Runtime User Configuration](run_config.md) for details and examples
of how to use runtime user specified configuration.

## Spark SQL configuration parameters.

For small projects, user may also provide spark SQL configuration properties along with the SMV properties.
This reduces the number of configuration files to maintain and also allows the application to specify a better default
spark configuration parameter depending on its needs.  All properties starting with "spark.sql" will be added to the
runtime SqlContext configuration.
