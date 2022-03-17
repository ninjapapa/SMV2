from pyspark.sql import SparkSession
from smv.smvdriver import SmvDriver

class AppDriver(SmvDriver):
    def main(self, smvapp, args):

        # Example logging and debuging:
        #
        # spark = smvapp.sparkSession
        # print(dict(spark.sparkContext.getConf().getAll()))

        # Run app
        smvapp.run()

    def createSpark(self):
        builder = SparkSession.builder.enableHiveSupport()
        builder = builder.master('local[*]')\
            .appName("SampleApp")\
            .config("spark.driver.cores", "2")
        return builder.getOrCreate()

if __name__ == "__main__":
    AppDriver().run()