from os import *
import io
from os.path import *
from DataManipulation.Utils.Path import Path
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType,FloatType)
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, substring, col, regexp_replace, reverse
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from time import time
import pandas as pd


class WAV(object):
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    
    def __init__(self,spark_session):
        super().__init__()
        self.spark_session = spark_session

    def recording_info(self):
        
        filenames = pd.read_csv('Filenames.csv',header=0, index_col=False)
        #wav_files = [[f[:-4]] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))] 
        wav_files = [[f[:-4]] for f in filenames.values[:,1] ] 

        wav_DF = self.spark_session.createDataFrame(wav_files, StructType([StructField("FileName", StringType(), False)]))

        split_col = split(wav_DF['FileName'], '_')
        wav_DF = wav_DF.withColumn("Patient_ID", split_col.getItem(0))
        wav_DF = wav_DF.withColumn("Recording_idx", split_col.getItem(1))
        wav_DF = wav_DF.withColumn("Chest_Location", split_col.getItem(2))
        wav_DF = wav_DF.withColumn("Acquisition_Mode", split_col.getItem(3))
        wav_DF = wav_DF.withColumn("Recording_Equipement", split_col.getItem(4))

        wav_DF.printSchema()
        wav_DF.show(5)

    def recording_annotation(self):
        idx_fileName = len(WAV.PATH_FILES_WAV.split(Path.path_separator))


        original_schema = [ StructField("Start", FloatType(), True),
                            StructField("End",  FloatType(), True),
                            StructField("Crackels", IntegerType(), True),
                            StructField("Wheezes", IntegerType(), True)]

        data_structure = StructType(original_schema)

        df = self.spark_session.read.\
            csv(path=WAV.PATH_FILES_WAV+'*.txt', header=False, schema= data_structure, sep='\t').\
            withColumn("Filename", split(input_file_name(), "/").getItem(idx_fileName) ).\
            withColumn("duration", col("End") - col("Start"))

        

        df.show(5, False)
        df.printSchema()








    
    def get_fileNames_test(self):
        path = Path.get_wav_file_path()

        if Path.RunningOnLocal:
            list_of_fileName = [f[:-4] for f in listdir(path) if (isfile(join(path, f)) and f.endswith('.txt'))]
        else:
            args = "hdfs dfs -ls "+path+" | awk '{print $8}'"
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

            s_output, s_err = proc.communicate()
            list_of_fileName = s_output.split()
        
        print("\n#################################################################")
        print(len(list_of_fileName))
        print(list_of_fileName)
        print("\n#################################################################")
        

