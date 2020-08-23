from pyspark.mllib.util import MLUtils
from pyspark.mllib.regression import LabeledPoint
from wav_manipulation.wav import *
from Utils.miscellaneous import split_data_label, split_train_test
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



class RandomForest():

    def __init__(self, spark_session, spark_context):
        self.spark_session = spark_session
        self.spark_context = spark_context

            
    
    def train(self):
        wav = WAV(self.spark_session, self.spark_context)
        data_labeled = wav.get_data_labeled_df()
        assembler,data=split_data_label(data_labeled,label='indexedDiagnosis', features=['Data','Wheezes','Crackels'])
        
        # Index labels, adding metadata to the label column.
        # Fit on whole dataset to include all labels in index.
        
        #labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)

        # Automatically identify categorical features, and index them.
        # Set maxCategories so features with > 4 distinct values are treated as continuous.
        #print('VectorIndexer')
        #featureIndexer = VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=15).fit(data)
        
        # Split the data into training and test sets
        print('split_train_test')
        training_data, test_data = split_train_test(data)
        # Train a RandomForest model.
        print('RandomForestClassifier')
        rf = RandomForestClassifier(labelCol="indexedDiagnosis", featuresCol="features", numTrees=10)

        # Convert indexed labels back to original labels.
        #labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=data.labels)       
        
        # Chain indexers and forest in a Pipeline
        pipeline = Pipeline(stages=[assembler, rf])

        # Train model.  This also runs the indexers.
        model = pipeline.fit(training_data)

        model.save("/home/user24/LSCproject")
        sameModel = PipelineModel.load("target/tmp/myRandomForestClassificationModel")

        # Make predictions.
        predictions = model.transform(test_data)

        # Select example rows to display.
        predictions.select("predictedLabel", "label", "features").show(5)

        return predictions, model
    
    def model_evalation(self,predictions,model):
        # Select (prediction, true label) and compute test error
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print("Test Error = %g" % (1.0 - accuracy))

        rfModel = model.stages[2]
        print(rfModel)  # summary only
    
        