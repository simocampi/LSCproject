from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import format_number 
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
from DataManipulation.Utils.Path import Path
from pyspark.sql import functions as F
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

# map function
#def bmi(x):
#       if x['Age']<18:
#              x['Adult_BMI']=x['Child_weight'] / (x['Child_height']/100)**2
#       return x
#
#rdd_demographic_info=rdd_demographic_info.map(bmi)

rdd_demographic_info=rdd_demographic_info.toDF()
rdd_demographic_info_adult = rdd_demographic_info.select('*').where('Age >= 18')
rdd_demographic_info_child= rdd_demographic_info.withColumn("Adult_BMI", col("Child_weight")/(col("Child_height")/100)**2).where("Age < 18").union(rdd_demographic_info_adult)

rdd_demographic_info_child.show(100)


