from os import *
from os.path import *
from Utils.Path import *
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("Python Spark Exercise 5") \
    .getOrCreate()

class WAV:
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    

    @staticmethod
    def wav_files():
        wav_files = [[f] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))] 

        cSchema = StructType([StructField("FileName", StringType())])
        wav_DF = spark.createDataFrame(wav_files, StructType([StructField("FileName", StringType())]))

        wav_DF = wav_DF.withColumn('Patient_ID', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Recording_idx', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Chest_location', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Acquisition_mode', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Recording_equipment', lit(None).cast(StringType()))

        wav_DF.printSchema()
        wav_DF.show()
        
        #data_schema = [StructField('Patient_Number',IntegerType(),True)]
        #    column = wav_files[0].split('_')
      
    

