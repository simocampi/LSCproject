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

#dataset
demographic_info = sc.textFile("file:///C:/Users/simoc/Documents\GitHub/LSCproject/Database/filename_differences.txt")


di= demographic_info.collect()
for l in di:
        print(l) 