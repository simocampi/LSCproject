import sys
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()