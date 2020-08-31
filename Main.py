from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import time


from wav_manipulation.wav import WAV
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
from datetime import datetime
import MultiLayerPerceptron

from Classifier import RandomForest




print("\n\n-------------------------------------------------------------------------------\n")
print("START...", datetime.now())

start = time.time()

conf = SparkConf().setAppName('LSC_Project')
spark_context = SparkContext(conf=conf)

spark_session = SparkSession(sparkContext=spark_context).builder \
                .getOrCreate()

print("spark context & spark session created\t", datetime.now())
                
wav = WAV(spark_session, spark_context)
demInfo = DemographicInfo(spark_session)
demInfo.get_DataFrame().show()
print("loaded all data\t", datetime.now())

data_labeled = wav.get_data_labeled_df()
print('\n\n----------Schema Dataset After VectorAssembler------------------------------\n')
data_labeled.printSchema()
print('\n------------------------------------------------------------------------------\n\n')
MultiLayerPerceptron.fit_and_test(data_labeled)

end= time.time()
print("\n----------------------------------------------------------------------------------\n\n")
print("END...", datetime.now())
hours, rem = divmod(end-start, 3600)
minutes, seconds = divmod(rem, 60)
print("\n\nTotal Execution Time : {:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds),end="\n\n")