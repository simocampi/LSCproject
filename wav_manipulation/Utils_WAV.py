import math
import numpy as np
#import scipy.io.wavfile as wf
from DataManipulation.Utils.Path import Path
from scipy.signal import spectrogram


def slice_with_annotation(audio, annotations, max_len, sample_rate):
    
    start, end, duration = annotations['Start'].collect(), annotations['End'].collect(), annotations['Duration'].collect()
    
    if max_len < end - start:
        end = start + max_len

    splitted_data = slice_data(start, end, audio[1],  sample_rate)
        
    return (audio[0], splitted_data, annotations["Crackels"], annotations["Wheezes"])
    

def slice_data(start, end, raw_data,  sample_rate):
    max_ind = len(raw_data) 
    start_ind = min(int(start * sample_rate), max_ind)
    end_ind = min(int(end * sample_rate), max_ind)
    return raw_data[start_ind: end_ind]

#def sliced_data_to_spectrogram(spark_context, rdd_f, sliced_data, fs = 44100):
        #rdd = spark_context.emptyRDD
        #for s in slice_data:
            #rdd_spect = spark_context.parallelize(spectrogram(s,fs))
            #spark_context.union([rdd, rdd_spect])
        #return spark_context.union( [rdd_f, rdd] )

#def compute_len(samp_rate=22050, time=6, acquisition_mode=0):
'''Computes the supposed length of sliced data
        samp_size = sample size from the data
        samp_rate = sampling rate. by default since we're working on 24-bit files, we'll use 96kHz
        time = length of time for the audio file. by default we'll use the max we have which is 5.48
        acquisition_mode = either mono or stereo. 0 for mono, 1 for stereo
'''
    #comp_len = 0
    #if acquisition_mode == 1: #ac mode is single channel which means it's 'mono'
        #comp_len = samp_rate * time
    #else: #stereo
        #comp_len = (samp_rate * time) * 2

    #return comp_len

























'''
    #-----------------------KAGGLE FUNC-----------------------------------------------------------------
    
    def read_wav(self, path_wav):
        fs , data = wf.read(path_wav)

    #wave file reader
    #Will resample all files to the target sample rate and produce a 32bit float array
    def read_wav_file(self, str_filename, target_rate):
        wav = wave.open(str_filenaame, mode = 'r')
        sample_rate, data = extract2FloatArr(wav,str_filename)
        if (sample_rate != target_rate):
            ( _ , data) = resample(sample_rate, data, target_rate)
        
        wav.close()
        return (target_rate, data.astype(np.float32))

    def resample(self, current_rate, data, target_rate):
        x_original = np.linspace(0,100,len(data))
        x_resampled = np.linspace(0,100, int(len(data) * (target_rate / current_rate)))
        resampled = np.interp(x_resampled, x_original, data)
        return (target_rate, resampled.astype(np.float32))

    # -> (sample_rate, data)
    def extract2FloatArr(self, lp_wave, str_filename):
        (bps, channels) = bitrate_channels(lp_wave)
    
        if bps in [1,2,4]:
            (rate, data) = wf.read(str_filename)
            divisor_dict = {1:255, 2:32768}
            if bps in [1,2]:
                divisor = divisor_dict[bps]
                data = np.divide(data, float(divisor)) #clamp to [0.0,1.0]        
            return (rate, data)
    
        elif bps == 3: 
            #24bpp wave
            return read24bitwave(lp_wave)
    
        else:
            raise Exception('Unrecognized wave format: {} bytes per sample'.format(bps))
        
#Note: This function truncates the 24 bit samples to 16 bits of precision
#Reads a wave object returned by the wave.read() method
#Returns the sample rate, as well as the audio in the form of a 32 bit float numpy array
#(sample_rate:float, audio_data: float[])
    def read24bitwave(self, lp_wave):
        nFrames = lp_wave.getnframes()
        buf = lp_wave.readframes(nFrames)
        reshaped = np.frombuffer(buf, np.int8).reshape(nFrames,-1)
        short_output = np.empty((nFrames, 2), dtype = np.int8)
        short_output[:,:] = reshaped[:, -2:]
        short_output = short_output.view(np.int16)
        return (lp_wave.getframerate(), np.divide(short_output, 32768).reshape(-1))  #return numpy array to save memory via array slicing

    def bitrate_channels(self, lp_wave):
        bps = (lp_wave.getsampwidth() / lp_wave.getnchannels()) #bytes per sample
        return (bps, lp_wave.getnchannels())

    def slice_data(self, start, end, raw_data,  sample_rate):
        max_ind = len(raw_data) 
        start_ind = min(int(start * sample_rate), max_ind)
        end_ind = min(int(end * sample_rate), max_ind)
        return raw_data[start_ind: end_ind]

    # da aggiungere altre funzioni...forse da utilizzare pure le annotation.txtxt xddd

    # return the Dataframe(rdd) of the wav file ready to be used in the pipeline
    def processing(self):

        #Dataframe: each row represents a piece of a wav file (spectogram? boh? da vedere lolol)
        pass
'''