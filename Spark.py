from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
import sys

spark = SparkSession\
        .builder\
        .appName("LSC PROJECT")\
        .getOrCreate()
#dataset