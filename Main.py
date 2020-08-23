from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


from wav_manipulation.wav import WAV
from DataManipulation.DemographicInfo import DemographicInfo
from DataManipulation.PatientDiagnosis import PatientDiagnosis
from Utils.BMI import replace_bmi_child

from Utils.miscellaneous import test
from Classifier import RandomForest

conf = SparkConf().setAppName('LSC_Project')
spark_context = SparkContext(conf=conf)

spark_session = SparkSession(sparkContext=spark_context).builder \
                .getOrCreate() \
                
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

#wav.get_DataFrame().show(5)
#wav.get_data_labeled_df().show(4)
#test(wav.get_data_labeled_df())
#test(wav.get_Rdd().toDF(['Data','Wheezes','Crackels', 'Diagnosis']))
#print(audio_rdd.printSchema())
#print('\n\n---------------------------------------------------------------------\n\n', audio_rdd.take(1))

random_forest = RandomForest(spark_session, spark_context) 
predictions, model = random_forest.train()
random_forest.model_evalation(predictions=predictions,model=model)


#spect = binary_wave_rdd.map(lambda x: x[1])
#spect = binary_wave_rdd.map( lambda x: (WAV.audio_to_melspectogram_rdd(x[1][0], x[1][1])))
#print(spect.collect())
#wav.wav_to_melspectogram_rdd(y,sr)

#wav.recording_info()#
#wav.recording_annotation()


#os.rename(r'C:\\Users\\jacop\\OneDrive\\Documents\\GitHub\\LSCproject\\Database\\audio_and_txt_files\\* 1.wav',r'C:\\Users\\jacop\\OneDrive\\Documents\\GitHub\\LSCproject\\Database\\audio_and_txt_files\\*_16.wav')


#rename all the files
#for filename in os.listdir(Path.get_wav_file_path()):#
    #if filename[-3:] =='txt' or filename == 'index_fileName':
        #continue
    #dst = filename[:-5] + ".wav"
    
    #os.rename(Path.get_wav_file_path()+filename, Path.get_wav_file_path()+dst)