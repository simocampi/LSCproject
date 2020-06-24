from os import listdir
from os.path import *
import io
import subprocess
from scipy.io import wavfile
from pyspark.sql.types import (StructField,StringType,IntegerType,StructType,FloatType, ArrayType, ByteType, BinaryType)
from pyspark.sql.functions import split, substring, col, regexp_replace, reverse, lit, input_file_name, udf
import numpy as np
from Utils.utils_wav import *
from DataManipulation.Utils.Path import Path
import librosa as lb

def round_half_up(number):
    return int(decimal.Decimal(number).quantize(decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP))
class WAV():
    
    PATH_FILES_WAV = Path.get_wav_file_path()
    
    def __init__(self,spark_session,spark_context):
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.wav_fileName = self.get_fileNames_test() 

        self.binary_wav_schema = [StructField('Filename', StringType(), False),
                                    StructField('Sample_rate', IntegerType(), False),
                                    StructField('Data', ArrayType(FloatType()), False)]

        # parameters in order to have an equivalent representations for each Wav file
        self.sample_length_seconds = 6 # 5 o 6 xdlolololol

        # info about recording
        self.recording_info()
        # nrecording annotation
        self.recording_annotation()

        self.read_wav()
        self.split_and_pad()
        self.audio_to_mfcc()

    def get_DataFrame(self):
        return self.rdd
        
    def get_Rdd(self):
        return self.rdd

    # return an rdd with data and corresponding path
    def read_wav(self):

        binary_wav_rdd = self.spark_context.binaryFiles(Path.get_wav_file_path()+'*.wav')
        open_file_rdd = binary_wav_rdd.map(lambda x: (x[0].split("/")[-1][:-3] +'txt', wavfile.read(io.BytesIO(x[1])))).map(lambda x: (x[0],x[1][0], np.array(x[1][1], dtype=float).tolist()))
        self.dataFrame = open_file_rdd.toDF(StructType(self.binary_wav_schema))

    # select the portion of the file of interest and add zero if not long enough
    def split_and_pad(self):
        audioDataFrame = self.dataFrame
        annotationRdd = self.annotationDataframe

        # join annotation and file content
        jointDataframe = annotationRdd.join(audioDataFrame, on=['Filename'], how='inner').rdd

        max_len = self.sample_length_seconds

        # slicing the data
        slice_data = jointDataframe.map(lambda x: (x[7][min(int(x[1] * x[6]), len(x[7])):min(int((x[1] + max_len) * x[6]), len(x[7]))], x[6], x[3], x[4]) if max_len < x[2] - x[1] \
                                                                                                                   else (x[7][min(int(x[1] * x[6]), len(x[7])):min(int(x[2] * x[6]), len(x[7]))], x[6], x[3], x[4]))
        # padding if not long enough
        self.rdd = slice_data.map(lambda x: (x[0] + [0 for _ in range(max_len - len(x[0]))], x[1], x[2], x[3])) # data, sample rate, Crackels, Wheezes

    # function inspired by https://github.com/jameslyons/python_speech_features/blob/master/python_speech_features/base.py
    def audio_to_mfcc(self,winlen=0.025,winstep=0.01,numcep=13, nfilt=26,lowfreq=0,highfreq=None,preemph=0.97,ceplifter=22,appendEnergy=True,winfunc=lambda x:np.ones((x,))):
        """Compute MFCC features from an audio signal.
        :param signal: the audio signal from which to compute features. Should be an N*1 array
        :param samplerate: the sample rate of the signal we are working with, in Hz.
        :param winlen: the length of the analysis window in seconds. Default is 0.025s (25 milliseconds)
        :param winstep: the step between successive windows in seconds. Default is 0.01s (10 milliseconds)
        :param numcep: the number of cepstrum to return, default 13
        :param nfilt: the number of filters in the filterbank, default 26.
        :param nfft: the FFT size. Default is None, which uses the calculate_nfft function to choose the smallest size that does not drop sample data.
        :param lowfreq: lowest band edge of mel filters. In Hz, default is 0.
        :param highfreq: highest band edge of mel filters. In Hz, default is samplerate/2
        :param preemph: apply preemphasis filter with preemph as coefficient. 0 is no filter. Default is 0.97.
        :param ceplifter: apply a lifter to final cepstral coefficients. 0 is no lifter. Default is 22.
        :param appendEnergy: if this is true, the zeroth cepstral coefficient is replaced with the log of the total frame energy.
        :param winfunc: the analysis window to apply to each frame. By default no window is applied. You can use numpy window functions here e.g. winfunc=numpy.hamming
        :returns: A list of shape (NUMFRAMES by numcep) containing features. Each row holds 1 feature vector.
        """
        coeff=0.95
        nfft=2048
        # compute points evenly spaced in mels
        lowmel = hz2mel(lowfreq)
        #highmel = hz2mel(highfreq) # depending on the sample rate
        #melpoints = np.linspace(lowmel,highmel,nfilt+2)
        

        data_idx = 0
        sample_rate_idx = 1
        crackels_idx = 2
        wheezes_idx = 3

        preprocessing_map = self.rdd.map(lambda x: (np.array(x[data_idx]), x[sample_rate_idx], x[crackels_idx], x[wheezes_idx]))

        preemphasis_map = preprocessing_map.map(lambda x: (np.append(x[data_idx][0], x[data_idx][1:] - x[data_idx][:-1] * coeff), \
                                                                x[sample_rate_idx], x[crackels_idx], x[wheezes_idx])) #perform preemphasis on the input signal.

        frame_info_map = preemphasis_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], int(decimal.Decimal(winlen * x[sample_rate_idx]).quantize(decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP)), int(decimal.Decimal(winstep * x[sample_rate_idx]).quantize(decimal.Decimal('1'), rounding=decimal.ROUND_HALF_UP)))) # data, sample rate, Crackels, Wheezes, frame_length, frame_step
        
        num_frame_map = frame_info_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], x[4], x[5], 1 if len(x[0]) < x[4] \
                                                        else 1 + int(math.ceil((1.0 * len(x[0]) - x[4]) / x[5])))) # data, sample rate, Crackels, Wheezes, frame_length, frame_step, num_frames

        framesig_map = num_frame_map.map(lambda x: ( np.lib.stride_tricks.as_strided(np.concatenate((x[data_idx], np.zeros((int((x[6] - 1) * x[5] + x[4]) - len(x[data_idx]))))), shape=x[data_idx].shape[:-1] + (x[data_idx].shape[-1] - x[4] + 1, x[4]), strides=x[data_idx].strides + (x[data_idx].strides[-1],))[::x[5]], \
                                                                x[sample_rate_idx], x[crackels_idx], x[wheezes_idx])) # create A list of shape (NUMFRAMES by frame_length)
        
        # the windowing of the frame is not applied since is the identity (forse faccio la map dopo)
        
        # flatting in order to have for each element of the rdd a frame with its sample rate, Crackels and Wheezes
        add_info_to_frame_map = framesig_map.flatMap(lambda x: np.array([f.tolist() + [x[sample_rate_idx]] + [x[crackels_idx]] + [x[wheezes_idx]] for f in x[data_idx]]))
        flat_frames_map = add_info_to_frame_map.map(lambda x: (x[:-4], int(x[-3]), int(x[-2]), int(x[-1])))

        powspec_map = flat_frames_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx],\
                                             1.0 / nfft * np.square(np.absolute(np.fft.rfft(x[data_idx], nfft))))) # data, sample rate, Crackels and Wheezes, power spectrum

        energy_map = powspec_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], x[4],np.sum(x[4]))) # data, sample rate, Crackels and Wheezes, power spectrum, energy
        energy_map = energy_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], x[4],np.finfo(float).esp if x[5]== 0 else x[5])) # if energy is zero, we get problems with log


        # ***get_filterbanks***

        # compute points evenly spaced in mels, Convert a value in Hertz to Mels
        melpoint_map = energy_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], x[4], x[5], np.linspace(lowmel, 2595 * np.log10(1+(x[sample_rate_idx]/2)/700.),nfilt+2))) # ... power spect, energy, melpoints   

        # Convert a value in Mels to Hertz: our points are in Hz, but we use fft bins, so we have to convert
        #  from Hz to fft bin number
        bin_map = melpoint_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], x[4], x[5], np.floor( (nfft + 1) * (700*(10**(x[6]/2595.0)-1)) /x[sample_rate_idx]) )) # ...power spect, energy, bin

        """Compute a Mel-filterbank. The filters are stored in the rows, the columns correspond
        to fft bins. The filters are returned as an array of size nfilt * (nfft/2 + 1)"""
        bin_idx = 6
        filter_banks_map = bin_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], x[4], x[5], [ (i - x[bin_idx][j]) / (x[bin_idx][j+1]-x[bin_idx][j]) if i in range(int(x[bin_idx][j]), int(x[bin_idx][j+1])) \
                                                                                                                                    else (x[bin_idx][j+2]-i) / (x[bin_idx][j+2]-x[bin_idx][j+1]) if i in range(int(x[bin_idx][j+1]), int(x[bin_idx][j+2])) \
                                                                                                                                    else 0. \
                                                                                                                                    for j in range(nfilt) for i in range(nfft//2) ] ))
        filter_banks_map = filter_banks_map.map(lambda x: (x[data_idx], x[sample_rate_idx], x[crackels_idx], x[wheezes_idx], x[4], x[5], np.array(x[6]).reshape([nfilt,nfft//2+1])))
        
        #filterbank_map = filtered_signal_map.map(lambda x: (fbank(x[0],x[1], winlen,winstep,nfilt, calculate_nfft(x[1], winlen),lowfreq,highfreq,preemph,winfunc), x[1], x[2], x[3]))  [ x[7][j, i]=((i - bin[j]) / (bin[j+1]-bin[j])) for j in range(0,nfilt) for i in range(int(x[6][j]), int(x[6][j+1])) ]
        self.rdd=filter_banks_map

    #def audio_to_melspectogram_rdd(self, rdd_split_and_pad_rdd):
        
        #split and pad:
        # 1 elem -> nome file
        # 2 elem -> audio spezzettato
         
        #rdd_spect = self.spark_context.emptyRDD


        #spectrogram_rdd = splitted_signal.map(lambda sliced_data : sliced_data_to_spectrogram(self.spark_context,rdd_spect, slice_data))
        
        #return spectrogram_rdd
    
        

    def recording_info(self):
        wav_files = self.get_fileNames_test()

        wav_DF = self.spark_session.createDataFrame(wav_files, StructType([StructField("FileName", StringType(), False)]))

        split_col = split(wav_DF['FileName'], '_')
        wav_DF = wav_DF.withColumn("Patient_ID", split_col.getItem(0))
        wav_DF = wav_DF.withColumn("Recording_idx", split_col.getItem(1))
        wav_DF = wav_DF.withColumn("Chest_Location", split_col.getItem(2))
        wav_DF = wav_DF.withColumn("Acquisition_Mode", split_col.getItem(3))
        wav_DF = wav_DF.withColumn("Recording_Equipement", split_col.getItem(4))

        self.recordingInfo = wav_DF # the class variable the Dataframe containing the recording info
        #wav_DF.printSchema()
        #wav_DF.show(2, False)

    def recording_annotation(self):
        idx_fileName = len(WAV.PATH_FILES_WAV.split("/"))

        original_schema = [ StructField("Start", FloatType(), False),
                            StructField("End",  FloatType(), False),
                            StructField("Crackels", IntegerType(), False),
                            StructField("Wheezes", IntegerType(), False)]

        self.annotationDataframe = self.spark_session.read.\
            csv(path=WAV.PATH_FILES_WAV+'*.txt', header=False, schema= StructType(original_schema), sep='\t').\
            withColumn("Filename", split(input_file_name(), "/").getItem(idx_fileName - 1)).\
            withColumn("Duration", col("End") - col("Start"))

        #print(self.annotationDataframe.select("Filename").count())
        #self.annotationDataframe.where("Filename=='101_1b1_Pr_sc_Meditron.txt'").show()
        # the class variable the Dataframe containing the recording annotation
        #self.annotationDataframe.printSchema()
        #self.annotationDataframe.show(2, False)
   
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
