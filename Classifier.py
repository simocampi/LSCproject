from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from wav_manipulation.wav import *
from Utils.miscellaneous import *
from wav_manipulation.wav import *
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

class RandomForest():

    def __init__(self, spark_session, spark_context):
        self.spark_session = spark_session
        self.spark_context = spark_context

        #test
        self.data_preprocessing()
            
    
    def data_preprocessing(self, training_data_ratio=0.7, random_seeds=13579):
        wav = WAV(self.spark_session, self.spark_context)
        data_labeled = wav.get_data_labeled_df()
        data=get_data_label(df,label='Diagnosis', features=['Data','Wheezes','Crackels'])

        # Index labels, adding metadata to the label column.
        # Fit on whole dataset to include all labels in index.
        labelIndexer = StringIndexer(inputCol="Diagnosis", outputCol="indexedLabel").fit(data)
        # Automatically identify categorical features, and index them.
        # Set maxCategories so features with > 4 distinct values are treated as continuous.
        featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)
        splits= [training_data_ratio, 1.0 - training_data_ratio]
        training_data, test_data = data.randomSplit(splits, random_seeds)
        training_data.show(5)

        return training_data, test_data, labelIndexer, featureIndexer
    
    
        