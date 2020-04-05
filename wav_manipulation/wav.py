from os import *
from os.path import *
from Utils.Path import *
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, substring, col, regexp_replace

class WAV(object):
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    
    def __init__(self,spark_session):
        self.spark_session = spark_session

    def wav_files(self):
        wav_files = [[f[:-4]] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))] 

        wav_DF = self.spark_session.createDataFrame(wav_files, StructType([StructField("FileName", StringType(), False)]))

        split_col = split(wav_DF['FileName'], '_')
        wav_DF = wav_DF.withColumn("Patient_ID", split_col.getItem(0))
        wav_DF = wav_DF.withColumn("Recording_idx", split_col.getItem(1))
        wav_DF = wav_DF.withColumn("Chest_Location", split_col.getItem(2))
        wav_DF = wav_DF.withColumn("Acquisition_Mode", split_col.getItem(3))
        wav_DF = wav_DF.withColumn("Recording_Equipement", split_col.getItem(4))

        wav_DF.printSchema()
        wav_DF.show()