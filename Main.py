from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import format_number 
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
import sys,os
#from importlib import reload
from Utils.BMI import replace_bmi_child
from wav_manipulation.wav import *
import pandas as pd

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


wav = WAV(spark_session, spark_context)

binary_wave = wav.read_was_as_binary(spark_context)
#print(binary_wave.toDF().printSchema())
#print(binary_wave[1])

wav.get_fileNames_test()
wav.recording_info()
#wav.recording_annotation()