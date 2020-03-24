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

<<<<<<< HEAD
print('\n\n', DEMOGRAPHIC_INFO_PATH,'\n', PATIENT_DIAGNOSIS_PATH,'\n',FILENAME_DIFFERENCES_PATH,'\n',FILENAME_FORMAT_PATH,'\n')

sc = SparkContext()

#dataset
demographic_info = sc.textFile("file:///C:/Users/simoc/Documents\GitHub/LSCproject/Database/filename_differences.txt")

=======
>>>>>>> c54604498b9d341bf9a56dd49d8dd11a372de7fa

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

r = patient_diagnosis_rdd.collect()