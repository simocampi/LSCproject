from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import rand
from pyspark.mllib.evaluation import MulticlassMetrics


from Utils.miscellaneous import split_data_label, split_train_test
from wav_manipulation.wav import *
from datetime import datetime

# Keras / Deep Learning
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras import optimizers, regularizers
from keras.optimizers import Adam

# Elephas for Deep Learning on Spark
from elephas.ml_model import ElephasEstimator



class NN():
    
    def __init__(self, spark_session, spark_context):        
        #will contain each step that the data pipeline needs to to complete all transformations within our pipeline
        self.stages = []
        self.model = None
        self.spark_session = spark_session
        self.spark_context = spark_context
    
    def get_train_test(self):
        wav = WAV(self.spark_session, self.spark_context)
        data_labeled = wav.get_data_labeled_df()
        assembler,data=split_data_label(data_labeled,label='indexedDiagnosis', features=['Data','Wheezes','Crackels'])

        # Split the data into training and test sets
        print('split_train_test...', datetime.now())
        training_data, test_data = split_train_test(data)

        return training_data, test_data

    def create_model(self, input_dim = 15, num_classes=8):
        pass
    
