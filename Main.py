from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
from DataManipulation.DemographicInfo import DemographicInfo
import sys
import os
from importlib import reload
from DataManipulation.Utils.Path import Path

spark_context= SparkContext(
        master = 'local',
        appName = 'LSC_PROJECT', 
        sparkHome = None, 
        pyFiles = None, 
        environment = None, 
        batchSize = 0, 
        conf = None, 
        gateway = None, 
        jsc = None
)


demographic_info = DemographicInfo(spark_context)

rdd_demographic_info = demographic_info.get_rdd()
#...