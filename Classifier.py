from pyspark.mllib.util import MLUtils
from wav_manipulation.wav import *
from Utils.miscellaneous import split_data_label, split_train_test
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.feature import IndexToString
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from datetime import datetime
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator
import numpy as np
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import size



class RandomForest():

    def __init__(self, spark_session, spark_context):
        self.spark_session = spark_session
        self.spark_context = spark_context
        self.model = None

              
    def train(self):
        wav = WAV(self.spark_session, self.spark_context)
        data_labeled = wav.get_data_labeled_df()
        assembler,data=split_data_label(data_labeled,label='indexedDiagnosis', features=['Data','Wheezes','Crackels'])

        print('select count')
        #data.select("*").count().show()

        # Split the data into training and test sets
        print('split_train_test...', datetime.now())
        training_data, test_data = split_train_test(data)
        # Train a RandomForest model.
        '''
        model = None
        try:
            print('Load model..')
            model = PipelineModel.load("/home/user24/LSCproject/model")

        except (IOError, FileNotFoundError, ValueError, Py4JJavaError):
            print('RandomForestClassifier...', datetime.now())
            rf = RandomForestClassifier(labelCol="indexedDiagnosis", featuresCol="features", numTrees=10)       
            # Convert indexed labels back to original labels.
            #labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel", labels=data.labels)       

            # Chain indexers and forest in a Pipeline
            print('Pipeline...\       ',datetime.now())
            pipeline = Pipeline(stages=[assembler, rf])     
            # Train model.  This also runs the indexers.
            print('Fit...', datetime.now())
            model = pipeline.fit(training_data) 

            
            #---------------SE FUNZIONA SOSTISTURE CON LA RIGA SOPRA CON QUELLO COMMENTATO SOTTO ----------------
            #crossval = self.crossvalidation(rf=rf, pipeline=pipeline)
            #model = crossval.fit(training_data)
            #model.bestModel.write().save(hdfs://master:9000/user/user24/model)
            #----------------------------------------------------------------------------------------------------

            print('Save model..', datetime.now())
            model.write().save("/home/user24/LSCproject/model")
        '''
        print('Load model...')
        self.model = PipelineModel.load("/home/user24/LSCproject/model")

        # Make predictions.
        print('Prediction...',datetime.now())
        predictions = self.model.transform(test_data)
    
        # Select example rows to display.
        print('Show prediction...',datetime.now())
        predictions.select("prediction", "indexedDiagnosis", "features").show(5)

        print('End Prediction...',datetime.now())
        return predictions
    
    def model_evalation(self,predictions):
        # Select (prediction, true label) and compute test error
        print('evaluation')
        evaluator = MulticlassClassificationEvaluator(labelCol="indexedDiagnosis", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print("Test Error = %g" % (1.0 - accuracy))

        rfModel = self.model.stages[2]
        print(rfModel)  # summary only
    
    def crossvalidation(self,rf, pipeline):
        
        paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [int(x) for x in np.linspace(start = 10, stop = 50, num = 3)]) \
        .addGrid(rf.maxDepth, [int(x) for x in np.linspace(start = 5, stop = 25, num = 3)]) \
        .build()

        crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(),
                          numFolds=3)

        return crossval


    

    

    
        