# SMV with JDBC

SMV supports both reading and writing data over a JDBC connection. Note that this has not been tested across all connection types and drivers.

## Configuration

Reading or writing over a JDBC connection requires
- a JDBC driver
- a JDBC url

### JDBC driver

You will need make the correct JDBC driver available in the classpath. JDBC drivers are specific to the type of database you want to connect to. Once you have identified the correct driver, you can include it in the class path with the Spark `--jars` option.
The driver needs to be specified with the SMV property `smv.jdbc.driver`

### JDBC url

You will need to provide the JDBC url needed to connect to the database. JDBC url format varies by connection type. Once you have identified the correct url, you can include it in the class path with SMV property `smv.jdbc.url`. This can be set in the app or user config, or alternatively at the commandline.

# Read data over JDBC with SmvJdbcTable

Data can be read over JDBC using `SmvJdbcTable`. Read more [here](smv_input.md#jdbc-inputs).
