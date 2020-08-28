from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


from wav_manipulation.wav import WAV
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
from Utils.BMI import replace_bmi_child
import detector


print("-------------------------------------------------------------------------------")
print("START")

conf = SparkConf().setAppName('LSC_Project')
spark_context = SparkContext(conf=conf)

spark_session = SparkSession(sparkContext=spark_context).builder \
                .config("spark.driver.memory", "15g") \
                .getOrCreate()
print("spark context & spark session created")
                
wav = WAV(spark_session, spark_context)
print("loaded all data")

data_set = wav.get_DataFrame()
data_set.printSchema()

detector.test(data_set)