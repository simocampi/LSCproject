from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
#import org.apache.spark.SparkContext
import sys
import os
from importlib import reload
from Utils.Path import Path

reload(sys)

# path management
file_path= __file__
file_path = file_path.replace('Spark.py','')


PATIENT_DIAGNOSIS_PATH = file_path+'Database/patient_diagnosis.csv'
FILENAME_DIFFERENCES_PATH = file_path+'Database/filename_differences.txt'
FILENAME_FORMAT_PATH = file_path+'Database/filename_format.txt'


sc = SparkContext(
        master = 'local',
        appName = 'LSC_PRPJECT', 
        sparkHome = None, 
        pyFiles = None, 
        environment = None, 
        batchSize = 0, 
        conf = None, 
        gateway = None, 
        jsc = None
)

#create rdd of tables-
demographic_info_rdd = sc.textFile(DEMOGRAPHIC_INFO_PATH)
patient_diagnosis_rdd = sc.textFile(PATIENT_DIAGNOSIS_PATH)
filename_diagnosis_rdd = sc.textFile(FILENAME_DIFFERENCES_PATH)
filename_format_rdd = sc.textFile(FILENAME_FORMAT_PATH)


c = Path.get_database_path()
print(c)