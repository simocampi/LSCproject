from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import rand
from pyspark.mllib.evaluation import MulticlassMetrics


from Utils.miscellaneous import split_data_label, split_train_test
from wav_manipulation.wav import *
from datetime import datetime

import os
#os.environ['TF_KERAS'] = '1'
import tensorflow as tf
# sess = tf.compat.v1.Session(graph=tf.import_graph_def(), config=session_conf)
#tf.compat.v1.disable_eager_execution()

# Keras / Deep Learning
from keras.models import Sequential
from keras.layers.core import Dense, Dropout, Activation
from keras import optimizers, regularizers
from keras.optimizers import Adam

# Elephas for Deep Learning on Spark
from elephas.ml_model import ElephasEstimator
from IPython.core.display import display

import tensorflow as tf


class NN():
    
    def __init__(self, spark_session, spark_context, input_dim=15, num_classes=8):        
        #will contain each step that the data pipeline needs to to complete all transformations within our pipeline
        #physical_devices = tf.config.list_physical_devices('GPU')
        #print("Num GPUs:", len(physical_devices))
        self.stages = []
        self.model = Sequential()
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.input_dim = input_dim
        self.num_classes = num_classes
        self.estimator=None

        self.create_model()
        self.spark_ml_estimator()
    
    def get_train_test(self):
        
        wav = WAV(self.spark_session, self.spark_context)
        data_labeled = wav.get_data_labeled_df()
        assembler,data=split_data_label(data_labeled,label='indexedDiagnosis', features=['Data','Wheezes','Crackels'])
        data = assembler.transform(data) 
        data = data.select(col('indexedDiagnosis').alias('labels'),col('features'))
        #data.show(5)
        # Split the data into training and test sets
        print('split_train_test...', datetime.now())
        training_data, test_data = split_train_test(data)
        return training_data, test_data

    def create_model(self ):
        self.model.add(Dense(256, input_shape=(self.input_dim,)))
        self.model.add(Activation('relu'))
        self.model.add(Dense(128))
        self.model.add(Activation('relu'))
        self.model.add(Dense(64))
        self.model.add(Activation('relu'))
        self.model.add(Dense(self.num_classes))
        self.model.add(Activation('softmax'))
        self.model.compile(loss='categorical_crossentropy', optimizer=Adam(lr=0.01))
        self.model.summary()

    def spark_ml_estimator(self, epoch = 25, batch_size=64):
        
        optimizer_conf = optimizers.Adam(lr=0.01)
        opt_conf = optimizers.serialize(optimizer_conf)
        # Initialize SparkML Estimator and Get Settings
        self.estimator = ElephasEstimator()
        #self.estimator = ElephasEstimator(self.model, epochs = epoch, batch_size = batch_size, mode = 'asynchronous', nb_classes=self.num_classes, categorical=True, num_workers = 1, validation_split = 0.3, verbose = 1,metrics = ['acc'], labelCol = "indexedDiagnosis", featuresCol = "features")
        #self.estimator = ElephasEstimator(self.model,  epochs=epoch, batch_size=batch_size, frequency='batch', mode='asynchronous',categorical=True, nb_classes=self.num_classes)
        
        self.estimator.setFeaturesCol("features")
        self.estimator.setLabelCol("labels")
        self.estimator.set_keras_model_config(self.model.to_yaml())
        self.estimator.set_categorical_labels(True)
        self.estimator.set_nb_classes(self.num_classes)
        self.estimator.set_num_workers(1)
        self.estimator.set_epochs(epoch) 
        self.estimator.set_batch_size(batch_size)
        self.estimator.set_verbosity(1)
        self.estimator.set_validation_split(30)
        self.estimator.set_optimizer_config(opt_conf)
        self.estimator.set_mode("synchronous")
        self.estimator.set_loss("categorical_crossentropy")
        self.estimator.set_metrics(['acc'])  
    
    def fit(self, label="labels"):

        training_data, test_data = self.get_train_test()
        
        dl_pipeline = Pipeline(stages=[self.estimator])
        fit_dl_pipeline = dl_pipeline.fit(training_data)
        #fit_dl_pipeline = self.estimator.fit(training_data)

        pred_train = fit_dl_pipeline.transform(training_data)
        pred_test = fit_dl_pipeline.transform(test_data)

        pnl_train = pred_train.select(label, "prediction")
        pnl_test = pred_test.select(label, "prediction")

        pred_and_label_train = pnl_train.rdd.map(lambda row: (row[label], row['prediction']))
        pred_and_label_test = pnl_test.rdd.map(lambda row: (row[label], row['prediction']))

        metrics_train = MulticlassMetrics(pred_and_label_train)
        metrics_test = MulticlassMetrics(pred_and_label_test)

        print("Training Data Accuracy: {}".format(round(metrics_train.precision(),4)))
        print("Training Data Confusion Matrix")
        display(pnl_train.crosstab('labels', 'prediction').toPandas())
        
        print("\nTest Data Accuracy: {}".format(round(metrics_test.precision(),4)))
        print("Test Data Confusion Matrix")
        display(pnl_test.crosstab('labels', 'prediction').toPandas())


    def OneHot(self, df):
        oneHot_dict = {0:[0,0,0,0,0,0,0,1],1:[0,0,0,0,0,0,1,0],
                       2:[0,0,0,0,0,1,0,0],3:[0,0,0,0,1,0,0,0],
                       4:[0,0,0,1,0,0,0,0],5:[0,0,1,0,0,0,0,0],
                       6:[0,1,0,0,0,0,0,0],7:[1,0,0,0,0,0,0,0]}

        df = df.rdd.map(lambda x:(oneHot_dict.get(x[0]), x[1]))
        df = df.toDF(['labels','features'])
        pass
            
