import smv
import pyspark.sql.functions as F

class Employment(smv.iomod.SmvCsvInputFile):
    def connectionName(self):
        return "myinput"

    def fileName(self):
        return "employment/CB1200CZ11.csv"

    def csvReaderMode(self):
        return "DROPMALFORMED"

class EmploymentByState(smv.SmvModule):
    """Python ETL Example: employ by state"""

    def requiresDS(self):
        return [Employment]

    def run(self, i):
        df = i[Employment]
        return df.groupBy(F.col("ST")).agg(F.sum(F.col("EMP")).alias("EMP"))

class EmploymentByStateOut(smv.iomod.SmvCsvOutputFile):
    def requiresDS(self):
        return [EmploymentByState]

    def connectionName(self):
        return "myoutput"

    def fileName(self):
        return "employment_by_state.csv"
