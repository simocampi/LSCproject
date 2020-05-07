from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.sql.functions import format_number 

from wav_manipulation.wav import *
from wav_manipulation.Utils_WAV import *
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
from Utils.BMI import replace_bmi_child

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
rdd_demographic_info_shrank= rdd_demographic_info.map(lambda p: replace_bmi_child(p)).toDF(demographic_info.) # new schema DemographicInfo
'''



     
wav = WAV(spark_session, spark_context)

audio_rdd = wav.get_Rdd()
print(audio_rdd.collect())
#frame_rate = binary_wave_rdd.map(lambda x : x[1].getframerate())
#frame_rate = binary_wave_rdd.map(lambda x : x[1][1])

#print('Count Frame rate in rdd: ', frame_rate.collect())


#spect = binary_wave_rdd.map(lambda x: x[1])
#spect = binary_wave_rdd.map( lambda x: (WAV.audio_to_melspectogram_rdd(x[1][0], x[1][1])))
#print(spect.collect())


#librosa.display.specshow()#ciaoooooooooooo, come va?? io ho un problema, mi dice 'DataManipulation.PatientDiagnosis' has no attribute 'get_DataFrame'

#rint('DAPPU MANGIA I GATTI: ', y,'   ', sr)
#wav.wav_to_melspectogram_rdd(y,sr)

wav.recording_info()
recording_annotation = wav.recording_annotation()
recording_annotation.show(5)