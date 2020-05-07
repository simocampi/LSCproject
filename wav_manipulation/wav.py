#from os import listdir
from os.path import *
import io, subprocess, wave, librosa


from pyspark.sql.types import (StructField,StringType,IntegerType,StructType,FloatType)
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, substring, col, regexp_replace, reverse, lit, input_file_name
from wav_manipulation.Utils_WAV import *
from DataManipulation.Utils.Path import Path


class WAV(object):
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    
    def __init__(self,spark_session,spark_context):
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.wav_fileName = self.get_fileNames_test() 

        # parameters in order to have an equivalent representations for each Wav file
        self.target_sample_rate = 22000 
        self.sample_length_seconds = 6 # 5 o 6 xdlolololol



        # MI AMMAZZ' CAZzO HAAAAAAAA ZIO MEEERDA
        binary_to_librosa_rdd()
        split_and_pad()
        # audio_to_melspectogram_rdd

    def get_DataFrame(self):
        return self.rdd.toDF()
        
    def get_Rdd(self):
        return self.rdd
        
    # return an rdd with librosa objects and corresponding path
    def binary_to_librosa_rdd(self):
        binary_wave_rdd= self.spark_context.binaryFiles(Path.get_wav_file_path()+'*.wav')
        self.rdd = binary_wave_rdd.map(lambda x : (x[0], librosa.load(io.BytesIO(x[1]))))

    def split_and_pad(self):
        data = self.rdd.map(lambda audio: slice_with_annotation(audio[1], self.attonationDataframe.where("Filename=={}".format(audio[0])), self.sample_length_seconds))
    
    # y_s : splitted signal
    # sr  : sample_rate splitted
    def audio_to_melspectogram_rdd(self, y_s, sr_s):

        mel_spec = librosa.feature.melspectrogram(y=y_s, sr=sr_s)
        

    def recording_info(self):
        wav_files = self.get_fileNames_test()

        wav_DF = self.spark_session.createDataFrame(wav_files, StructType([StructField("FileName", StringType(), False)]))

        split_col = split(wav_DF['FileName'], '_')
        wav_DF = wav_DF.withColumn("Patient_ID", split_col.getItem(0))
        wav_DF = wav_DF.withColumn("Recording_idx", split_col.getItem(1))
        wav_DF = wav_DF.withColumn("Chest_Location", split_col.getItem(2))
        wav_DF = wav_DF.withColumn("Acquisition_Mode", split_col.getItem(3))
        wav_DF = wav_DF.withColumn("Recording_Equipement", split_col.getItem(4))

        #wav_DF.printSchema()
        #wav_DF.show(2, False)

    def recording_annotation(self):
        idx_fileName = len(WAV.PATH_FILES_WAV.split(Path.path_separator))

        original_schema = [ StructField("Start", FloatType(), False),
                            StructField("End",  FloatType(), False),
                            StructField("Crackels", IntegerType(), False),
                            StructField("Wheezes", IntegerType(), False)]

        df = self.spark_session.read.\
            csv(path=WAV.PATH_FILES_WAV+'*.txt', header=False, schema= StructType(original_schema), sep='\t').\
            withColumn("Filename", split(input_file_name(), "/").getItem(idx_fileName) ).\
            withColumn("Duration", col("End") - col("Start"))

        self.attonationDataframe = df

        #df.printSchema()
        #df.show(2, False)
   
    def get_fileNames_test(self):
        path = Path.get_wav_file_path()
        list_of_fileName = []

        try:
            #IF THE FILE ALREDY EXIST
            indexingFiles = self.openIndexingFiles(folder_path=path)
            for line in indexingFiles:
                list_of_fileName.append([line[:-1]])
        except IOError:
            print("\nIndexing file for path \'{}\' not present, creating it...".format(path))
            list_of_fileName = self.createIndexingFile_andGetContent(folder_path=path)

        return list_of_fileName


    def openIndexingFiles(self, folder_path):
        if Path.RunningOnLocal:
            #WINDOWS LOCAL MACHINE
            f = open(folder_path+'index_fileName', 'r')
            return f
        else:
            #UNIGE CLUSTER SERVER
            args = "hdfs dfs -cat "+folder_path+"index_fileName"
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = proc.communicate()

            if proc.returncode != 0:
                raise IOError('Indexing file not found.')
            return stdout.split()
    

    def createIndexingFile_andGetContent(self, folder_path):
        list_of_fileName = []
        if Path.RunningOnLocal:
            #WINDOWS LOCAL MACHINE
            list_of_fileName = [[f[:-4]] for f in listdir(folder_path) if (isfile(join(folder_path, f)) and f.endswith('.txt'))]

            indexingFiles = open(folder_path+'index_fileName','w')
            for fileName in list_of_fileName:
                indexingFiles.write(fileName[0])
                indexingFiles.write("\n")
            indexingFiles.close()
            
        else:
            #UNIGE CLUSTER SERVER
            args = "hdfs dfs -ls "+folder_path+"*.txt | awk '{print $8}'"
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)

            s_output, s_err = proc.communicate()
            tmp_list = s_output.split()
            for line in tmp_list:
                fileName = line.split("/")[-1]
                list_of_fileName.append([fileName[:-4]])

            #save file in hadoop file system
            tmpFile = open('tmp','w')
            for fileName in list_of_fileName:
                tmpFile.write(fileName[0]+"\n")
            tmpFile.close()

            args = "hdfs dfs -put tmp "+folder_path+"index_fileName"
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            #os.remove('tmp')
            
        return list_of_fileName
