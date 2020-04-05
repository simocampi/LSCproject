from os import *
from os.path import *
from Utils.Path import *
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

class WAV(object):
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    
    def __init__(self,spark_session):
        self.spark_session = spark_session

    def wav_files(self):
        wav_files = [[f] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))] 

        #cSchema = StructType([StructField("FileName", StringType())])
        wav_DF = self.spark_session.createDataFrame(wav_files, StructType([StructField("FileName", StringType())]))

        wav_DF = wav_DF.withColumn('Patient_ID', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Recording_idx', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Chest_location', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Acquisition_mode', lit(None).cast(StringType()))
        wav_DF = wav_DF.withColumn('Recording_equipment', lit(None).cast(StringType()))

        wav_DF.printSchema()
        wav_DF.show()
    
    def wav_dataframe(self):  
        wav_files = [[f] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))]   
        data_schema = [ StructField('Patient_ID',IntegerType(),True),
                        StructField('Recording_idx',StringType(),True),
                        StructField('Chest_Location',StringType(),True),
                        StructField('Acquisition_Mode',StringType(),True),
                        StructField('Recording_Equipement',StringType(),True)
                        ]
        struct_dataschema = StructType(fields=data_schema)
        df = self.spark_session.createDataFrame([],struct_dataschema)
        df.printSchema()
        df.show()
      
    

