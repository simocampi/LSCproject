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

reload(sys)

# path management
file_path= __file__
file_path = file_path.replace('Spark.py','')

DEMOGRAPHIC_INFO_PATH = file_path+'Database/demographic_info.txt'
PATIENT_DIAGNOSIS_PATH = file_path+'Database/patient_diagnosis.csv'
FILENAME_DIFFERENCES_PATH = file_path+'Database/filename_differences.txt'
FILENAME_FORMAT_PATH = file_path+'Database/filename_format.txt'


sc = SparkContext()

#create rdd of tables
demographic_info_rdd = sc.textFile(DEMOGRAPHIC_INFO_PATH)
patient_diagnosis_rdd = sc.textFile(PATIENT_DIAGNOSIS_PATH)
filename_diagnosis_rdd = sc.textFile(FILENAME_DIFFERENCES_PATH)
filename_format_rdd = sc.textFile(FILENAME_FORMAT_PATH)

