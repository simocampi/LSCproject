from os import *
from os.path import *
from Utils.Path import *
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, split

class WAV(object):
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    
    def __init__(self,spark_session):
        self.spark_session = spark_session

    def wav_files(self):
        wav_files = [[f] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))] 

        #cSchema = StructType([StructField("FileName", StringType())])
        data_schema = [ StructField('Patient_ID',IntegerType(),True),
                        StructField('Recording_idx',StringType(),True),
                        StructField('Chest_Location',StringType(),True),
                        StructField('Acquisition_Mode',StringType(),True),
                        StructField('Recording_Equipement',StringType(),True)
                        ]
        
        struct_dataschema = StructType(fields=data_schema)
        
        wav_DF = self.spark_session.createDataFrame(wav_files, StructType([StructField("FileName", StringType())]))

        split_col = split(wav_DF['FileName'], '_')
        wav_DF = wav_DF.withColumn("Patient_ID", split_col.getItem(0))
        wav_DF = wav_DF.withColumn("Recording_idx", split_col.getItem(1))
        wav_DF = wav_DF.withColumn("Chest_Location", split_col.getItem(2))
        wav_DF = wav_DF.withColumn("Acquisition_Mode", split_col.getItem(3))
        wav_DF = wav_DF.withColumn("Recording_Equipement", split_col.getItem(4))

        wav_DF.printSchema()
        wav_DF.show()
    

    def wav_dataframe(self):  
        wav_files = [ f for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))]   
        data_schema = [ StructField('Patient_ID',IntegerType(),True),
                        StructField('Recording_idx',StringType(),True),
                        StructField('Chest_Location',StringType(),True),
                        StructField('Acquisition_Mode',StringType(),True),
                        StructField('Recording_Equipement',StringType(),True)
                        ]
        struct_dataschema = StructType(fields=data_schema)
        df = self.spark_session.createDataFrame([],struct_dataschema)
        columns = ['Patien_ID','Recording_idx','Chest_Location','Acquisition_Mode','Recording_Equipement']
        for filename in wav_files:
            data = filename.split('_')
            data[len(data)-1]=data[len(data)-1].replace('.wav','')
            data[0]=int(data[0])
            newRow = self.spark_session.createDataFrame([tuple(data)],columns)
            df = df.union(newRow)
        
        df.printSchema()
        df.show(100)
      
    ##sgshgsh

