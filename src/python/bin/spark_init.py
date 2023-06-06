from pyspark.sql import SparkSession
import Variable as gav

class SparkObject:
    def __init__(self, envn, appName):
        self.envn = envn
        self.appName = appName

    def get_spark_object(self):
        try:
            master = "local" if self.envn == "TEST" else "Yarn"
            spark = SparkSession.builder.master(master).appName(self.appName).getOrCreate()
            return spark
        except Exception as exp:
            print("NameError in the method - get_spark_object(). Please check the Stack Trace. " + str(exp))
            raise

#spark = SparkObject(gav.envn, gav.appName)
#spark = spark.get_spark_object()
#spark.stop()
#print(sparsparkk)