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
from tensorflow.python.keras.models import Sequential
from tensorflow.python.keras.layers import Dense, Dropout, Activation
from keras import optimizers, regularizers
from tensorflow.python.keras.optimizers import Adam

# Elephas for Deep Learning on Spark
from elephas.ml_model import ElephasEstimator
from IPython.core.display import display



class NN():
    
    def __init__(self, spark_session, spark_context, input_dim=15, num_classes=8):        
        #will contain each step that the data pipeline needs to to complete all transformations within our pipeline
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

        # Split the data into training and test sets
        print('split_train_test...', datetime.now())
        training_data, test_data = split_train_test(data)

        return training_data, test_data, assembler

    def create_model(self ):
        self.model.add(Dense(256, input_shape=(self.input_dim,)))
        self.model.add(Activation('relu'))
        self.model.add(Dense(128))
        self.model.add(Activation('relu'))
        self.model.add(Dense(64))
        self.model.add(Activation('relu'))
        self.model.add(Dense(self.num_classes))
        self.model.add(Activation('softmax'))
        self.model.compile(loss='categorical_crossentropy', optimizer='adam')
        self.model.summary()

    def spark_ml_estimator(self, epoch = 25, batch_size=64):
        
        optimizer_conf = optimizers.Adam(lr=0.01)
        opt_conf = optimizers.serialize(optimizer_conf)
        
        # Initialize SparkML Estimator and Get Settings
        self.estimator = ElephasEstimator()
        self.estimator.setFeaturesCol("features")
        self.estimator.setLabelCol("indexedDiagnosis")
        self.estimator.set_keras_model_config(self.model.to_yaml())
        self.estimator.set_categorical_labels(True)
        self.estimator.set_nb_classes(self.num_classes)
        self.estimator.set_num_workers(1)
        self.estimator.set_epochs(epoch) 
        self.estimator.set_batch_size(batch_size)
        self.estimator.set_verbosity(1)
        self.estimator.set_validation_split(0.10)
        self.estimator.set_optimizer_config(opt_conf)
        self.estimator.set_mode("synchronous")
        self.estimator.set_loss("categorical_crossentropy")
        self.estimator.set_metrics(['acc'])  
    
    def fit(self, label="indexedDiagnosis"):

        training_data, test_data, assembler = self.get_train_test()
        
        dl_pipeline = Pipeline(stages=[assembler, self.estimator])
        fit_dl_pipeline = dl_pipeline.fit(training_data)
        
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
        display(pnl_train.crosstab('indexedDiagnosis', 'prediction').toPandas())
        
        print("\nTest Data Accuracy: {}".format(round(metrics_test.precision(),4)))
        print("Test Data Confusion Matrix")
        display(pnl_test.crosstab('indexedDiagnosis', 'prediction').toPandas())
            
