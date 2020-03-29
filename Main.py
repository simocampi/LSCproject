from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
from DataManipulation.Utils.Path import Path
import sys,os
from importlib import reload


spark_session = SparkSession.builder \
                .master('local') \
                .appName('LSC_PROJECT') \
                .getOrCreate()

demographic_info = DemographicInfo(spark_session)
patient_diagnosis = PatientDiagnosis(spark_session)

rdd_demographic_info = demographic_info.get_rdd()
rdd_patient_diagnosis = patient_diagnosis.get_rdd()

for n in rdd_patient_diagnosis.collect():
        print(n)