import math
import numpy as np
from scipy.signal import spectrogram
import io
import decimal


def slice_with_annotation(filename, start, end, crackels, wheezes, duration, sample_rate, data , max_len):

    if max_len < end - start:
        end = start + max_len

    max_ind = len(data)
    start_ind = min(int(start * sample_rate), max_ind)
    end_ind = min(int(end * sample_rate), max_ind)
    return data[start_ind: end_ind], crackels, wheezes, sample_rate
    
def calculate_nfft(samplerate, winlen):
    """Calculates the FFT size as a power of two greater than or equal to
    the number of samples in a single window length.
    
    Having an FFT less than the window length loses precision by dropping
    many of the samples; a longer FFT than the window allows zero-padding
    of the FFT buffer which is neutral in terms of frequency domain conversion.
    :param samplerate: The sample rate of the signal we are working with, in Hz.
    :param winlen: The length of the analysis window in seconds.
    """
    window_length_samples = winlen * samplerate
    nfft = 1
    while nfft < window_length_samples:
        nfft *= 2
    return nfft

def hz2mel(hz):
    """Convert a value in Hertz to Mels
    :param hz: a value in Hz. This can also be a numpy array, conversion proceeds element-wise.
    :returns: a value in Mels. If an array was passed in, an identical sized array is returned.
    """
    return 2595 * np.log10(1+hz/700.)

def mel2hz(mel):
    """Convert a value in Mels to Hertz
    :param mel: a value in Mels. This can also be a numpy array, conversion proceeds element-wise.
    :returns: a value in Hertz. If an array was passed in, an identical sized array is returned.
    """
    return 700*(10**(mel/2595.0)-1)


def get_shape_frame(a, win_len):
    return a.shape[:-1] + (a.shape[-1] - win_len + 1, win_len)

def get_strides_frame(a):
    return a.strides + (a.strides[-1],)

def fbank(signal,samplerate=16000,winlen=0.025,winstep=0.01, nfilt=26,nfft=512,lowfreq=0,highfreq=None,preemph=0.97, winfunc=lambda x:np.ones((x,))):
    """Compute Mel-filterbank energy features from an audio signal.
    :param signal: the audio signal from which to compute features. Should be an N*1 array
    :param samplerate: the sample rate of the signal we are working with, in Hz.
    :param winlen: the length of the analysis window in seconds. Default is 0.025s (25 milliseconds)
    :param winstep: the step between successive windows in seconds. Default is 0.01s (10 milliseconds)
    :param nfilt: the number of filters in the filterbank, default 26.
    :param nfft: the FFT size. Default is 512.
    :param lowfreq: lowest band edge of mel filters. In Hz, default is 0.
    :param highfreq: highest band edge of mel filters. In Hz, default is samplerate/2
    :param preemph: apply preemphasis filter with preemph as coefficient. 0 is no filter. Default is 0.97.
    :param winfunc: the analysis window to apply to each frame. By default no window is applied. You can use numpy window functions here e.g. winfunc=numpy.hamming
    :returns: 2 values. The first is a numpy array of size (NUMFRAMES by nfilt) containing features. Each row holds 1 feature vector. The
        second return value is the energy in each frame (total energy, unwindowed)
    """

    highfreq= highfreq or samplerate/2

def prova():
    return np.zeros(2)




















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