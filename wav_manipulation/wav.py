from os import *
import io
from os.path import *
from Utils.Path import *
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType,FloatType)
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, substring, col, regexp_replace, reverse
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from time import time


class WAV(object):
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    
    def __init__(self,spark_session):
        self.spark_session = spark_session

    def recording_info(self):
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

    def recording_annotation(self):
        filenames = [[f[:-4] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.txt'))]]

        original_schema = [ StructField("Start", FloatType(), True),
                            StructField("End",  FloatType(), True),
                            StructField("Crackels", IntegerType(), True),
                            StructField("Wheezes", IntegerType(), True)]

        data_structure = StructType(original_schema)

<<<<<<< HEAD
        
        df = self.spark_session.read.csv(path=WAV.PATH_FILES_WAV+'\\*.txt',header=False, schema= data_structure, sep='\t').withColumn("Filename", input_file_name() )
        #df = df.withColumn("Filename", split_col.getItem(0))
        rdd= df.rdd.map(lambda x: x[4].replace(WAV.PATH_FILES_WAV,''))
        
        df2 = rdd.toDF()
        
        #df = rdd.toDF()
        #df = df.select(regexp_replace('Filename',WAV.PATH_FILES_WAV,''))
        #df= self.spark_session.read.csv(path=WAV.PATH_FILES_WAV+'/*.txt').option('delimiter','\\s+')
        df.show()
        print(df.count())
=======
        df = self.spark_session.read.\
            csv(path=WAV.PATH_FILES_WAV+'\\*.txt', header=False, schema= data_structure, sep='\t').\
            withColumn("Filename", reverse(split(input_file_name(), "/")).getItem(0) ).\
            withColumn("duration", col("End") - col("Start"))

        

        df.show(20, False)
>>>>>>> b32e1ef8eee2fcab0a60edeeb83dc9eeec6fa623
        df.printSchema()
        

