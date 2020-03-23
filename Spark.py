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

sc = SparkContext()

#dataset
demographic_info = sc.textFile("file:///C:/Users/SimoneCampisi/Documents/GitHub/LSCproject/Database/audio_and_txt_files")
di= demographic_info.collect()

for l in di:
        print(l)    #dshdh