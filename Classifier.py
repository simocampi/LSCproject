from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from wav_manipulation.wav import *
from Utils.miscellaneous import split_data_label, split_train_test
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


class RandomForest():

    def __init__(self, spark_session, spark_context):
        self.spark_session = spark_session
        self.spark_context = spark_context

        #test
        self.train()
            
    
    def train(self):
        wav = WAV(self.spark_session, self.spark_context)
        data_labeled = wav.get_data_labeled_df()
        data=split_data_label(data_labeled,label='indexedDiagnosis', features=['Data','Wheezes','Crackels'])
        
        # Index labels, adding metadata to the label column.
        # Fit on whole dataset to include all labels in index.
        print('StringIndexer')
        #labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

        # Automatically identify categorical features, and index them.
        # Set maxCategories so features with > 4 distinct values are treated as continuous.
        print('VectorIndexer')
        featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=6).fit(data)
        
        # Split the data into training and test sets
        print('split_train_test')
        training_data, test_data = split_train_test(labeled_point)
        # Train a RandomForest model.
        print('RandomForestClassifier')
        rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

        # Convert indexed labels back to original labels.
        labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=labelIndexer.labels)       
        
        # Chain indexers and forest in a Pipeline
        pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

        # Train model.  This also runs the indexers.
        model = pipeline.fit(trainingData)

        # Make predictions.
        predictions = model.transform(testData)

        # Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5)

        return predictions
    
    def model_evalation(self,predictions):
        # Select (prediction, true label) and compute test error
        evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print("Test Error = %g" % (1.0 - accuracy))

        rfModel = model.stages[2]
        print(rfModel)  # summary only
    
        