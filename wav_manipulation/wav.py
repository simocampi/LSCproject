from os import *
from os.path import *
from Utils.Path import *
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType)

class WAV:
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    

    @staticmethod
    def wav_files():
        wav_files = [f[:-4] for f in listdir(WAV.PATH_FILES_WAV) if (isfile(join(WAV.PATH_FILES_WAV, f)) and f.endswith('.wav'))] 
        for f in wav_files:
            print(f)
        #data_schema = [StructField('Patient_Number',IntegerType(),True)]
        #    column = wav_files[0].split('_')
      
    

