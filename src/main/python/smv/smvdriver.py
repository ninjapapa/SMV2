import sys

from pyspark.sql import SparkSession
from smv import SmvApp, logger
from smv.smvconfig import SmvConfig

class SmvDriver(object):
    """Driver for an SMV application

        SmvDriver handles the boiler plate around parsing driver args, constructing an SmvApp, and running an application.

        Extend SmvDriver as the entry point to a project app override the following methods to customize:

            - createSpark (optional) - return a SparkSession
            - main - what to do with the smvapp (typically call smvapp.run())

        Project driver python script could create the customize SmvDriver class and call run() method in __main__:

            if __name__ == "__main__":
                AppDriver().run()
    """

    def createSpark(self, smvconf):
        """Override this to define SparkSession with customize parameters
            
            If not defined, will use parameters in kernel_config_file, which specified by "smv.kernelConfigFile" 
            keyword in smv opts files. If kernel_config not specified, will create default spark session.
        """
        return None

    def _create_smv_app(self, smvconf):
        """Creeate SmvApp
        """
        spark = self.createSpark(smvconf)

        # if not specified, use default
        if (spark is None):
            spark_builder = SparkSession.builder.enableHiveSupport()
            spark_builder = spark_builder.appName(smvconf.app_name())
            # read the props from kernel config file and use them as spark conf
            kernel_conf = smvconf.read_props_from_kernel_config_file()
            for key in kernel_conf:
                # use the master setting in the config file if exists
                if key == 'master':
                    spark_builder = spark_builder.master(kernel_conf.get(key))
                else:
                    spark_builder = spark_builder.config(key, kernel_conf.get(key))

            spark= spark_builder.getOrCreate()

        # When SmvDriver is in use, user will call spark-submit
        # through command-line, so no need to do py module hotload
        return SmvApp.createInstance(smvconf, spark, py_module_hotload=False)

    def main(self, smvapp, driver_args):
        """Override this to define the driver logic

            Default is to just call `run` on the `SmvApp`.

            Args:
                smvapp (SmvApp): app which was constructed
                driver_args (list(str)): CLI args for the driver
        """
        logger.debug("Running SmvApp with driver_args: {}".format(driver_args))
        smvapp.run()

    def run(self):
        """Run the driver
        """
        args = sys.argv[1:]

        smvconf = SmvConfig(args)
        driver_args = smvconf.leftover

        smvapp = self._create_smv_app(smvconf)
        self.main(smvapp, driver_args)
