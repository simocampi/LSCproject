from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import format_number 
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
from pydub import AudioSegment
import pickle
import sparkpickle

import sys,os
#from importlib import reload
from Utils.BMI import replace_bmi_child
from wav_manipulation.wav import *
import pandas as pd
from wav_manipulation.Utils_WAV import Wav_Preprocessing
import wave
#import audiosegment
import zlib
from io import BytesIO
import numpy as np


conf = SparkConf().setAppName('LSC_Project')
spark_context = SparkContext(conf=conf)


spark_session = SparkSession(sparkContext=spark_context).builder \
                .getOrCreate()


'''
# ----the dataframe containing the informations about patients is created
demographic_info = DemographicInfo(spark_session)

# ----the diagnosis dataframe is created
patient_diagnosis = PatientDiagnosis(spark_session)

# ----visualize first 5 rows and the schema 
df_patient_diagnosis=patient_diagnosis.get_DataFrame()
df_patient_diagnosis.show(5)
df_patient_diagnosis.printSchema()

# ----visualize first 5 rows and the schema
df_demographic_info = demographic_info.get_DataFrame()
df_demographic_info.show(5) 
df_demographic_info.printSchema()

# get rid of the Child's informations => now BMI column contains the BMI for both Adult and Children
rdd_demographic_info=demographic_info.get_Rdd()
rdd_demographic_info_shrank= rdd_demographic_info.map(lambda p: replace_bmi_child(p)).toDF(demographic_info.shrank_schema) # new schema DemographicInfo
'''



def read_was_as_binary(sc):
        #list_filename = self.spark_context.parallelize([Path.get_wav_file_path()+filename[0]+'.wav' for filename in self.wav_fileName])

        bytesRdd= sc.binaryFiles(Path.get_wav_file_path()+'*.wav')
        # cosi' dovrebbe tornare un rdd (nome file, Wave_read Object)
        #.map(lambda file: (file[0], file[1])) # cosi' dobbiamo sperare che funzioni altrimenti non potremo usare le librerie di python e rip lo abbiamo nel culo forte (non ricordo se la sintassi e' giusta)
        return bytesRdd
     
def deserialize(bstr):
    """
     Attempts to deserialize a bytestring into an audiosegment.

    :param bstr: The bytestring serialized via an audiosegment's serialize() method.
    :returns: An AudioSegment object deserialized from `bstr`.
     """
    d = pickle.loads(bstr)
    seg = pickle.loads(d['seg'])
    return AudioSegment(seg, d['name'])


wav = WAV(spark_session, spark_context)

wav_Preprocessing = Wav_Preprocessing(spark_context)
binary_wave_rdd = read_was_as_binary(spark_context)

binary_wave_rdd = binary_wave_rdd.map(lambda x : (x[0], deserialize(np.array(x[1]))))

frame_rate = binary_wave_rdd.map(lambda x : x[1].frame_rate())

print(frame_rate.collect())


#def decompress(val):
#    try:
#        s = zlib.decompress(val, 16 + zlib.MAX_WBITS)
#    except:
#        return val
#    return s

#binary_files = binary_wave_rdd.map(lambda x : x[1])

#print(type(binary_files.mapValues(wav.deserialize).take(1)))







#print(Path.get_wav_file_path() +"225_1b1_Pl_sc_Meditron.wav")
#stecosandro= spark_session.read.format("binaryFile").load(Path.get_wav_file_path() +"*.wav")
#stecosandro = stecosandro.map(lambda x : (x[0], deserialize(x[1])))
#frame_rate = stecosandro.map(lambda x : x[1].frame_rate())

#print(frame_rate.collect())


#tecosandro.show()

#print(stecosandro.collect())
#binary_wave_rdd.toDF().show(5)
#gigi = WAV()
#rdd = wav.rddWAV(spark_context)
#binary_wave_rdd = wav.read_was_as_binary(spark_context)


#spectogram_rdd = binary_wave_rdd.map(lambda x: (x[0] , x[1].spectogram(window_length_s=0.03, overlap=0.5)))
#print(rdd.collect())
#spectogram_df = spectogram_rdd.toDF()
#spectogram_rdd.show()



#wav.recording_info()
#wav.recording_annotation()