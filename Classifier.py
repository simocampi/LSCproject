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
        assembler,data=split_data_label(data_labeled,label='label', features=['Data','Wheezes','Crackels'])
        #data = assembler.transform(data) 
        #data = data.select(col('indexedDiagnosis').alias('label'),col('features'))

        #data.show(15)

        print('select count')
        #data.select("*").count().show()

        # Split the data into training and test set
        print('split_train_test...', datetime.now())
        training_data, test_data = split_train_test(data)
        # Train a RandomForest model.
     
        #try:
         #   print('Load model..')
          #  self.model = PipelineModel.load("/user/user24/model_crossval")

        #except: #(IOError, FileNotFoundError, ValueError, Py4JJavaError):
        print('RandomForestClassifier...', datetime.now())
        rf = RandomForestClassifier(labelCol="label", featuresCol="features")       
      
        print('Pipeline...\       ',datetime.now())
        pipeline = Pipeline(stages=[assembler, rf])     
        
        # Train model.  This also runs the indexers.
        #print('Fit...', datetime.now())
        #self.model = pipeline.fit(training_data)   

        #---------------SE FUNZIONA SOSTISTURE CON LA RIGA SOPRA CON QUELLO COMMENTATO SOTTO ----------------
        print('crossvalidation... ', datetime.now())
        crossval = self.crossvalidation(rf=rf, pipeline=pipeline)
        print('fit after crossvalidation... ', datetime.now())
        self.model = crossval.fit(training_data)
        #---------------------------------------------------------------------------------------------------    
        print('Save model..', datetime.now())
        self.model.bestModel.write().overwrite().save("/user/user24/model_crossval")
        print('Load model...')
        self.model = PipelineModel.load("/user/user24/model_crossval")
        
        # Make predictions
        print('Prediction...',datetime.now())
        predictions = self.model.transform(test_data)

        print('End Prediction...',datetime.now())
        return predictions
    
    def model_evalation(self,predictions):
        # Select (prediction, true label) and compute test error
        print('evaluation...', datetime.now())
        evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
        accuracy = evaluator.evaluate(predictions)
        print("Accuracy on Test = %g" % (accuracy))
        print(rfModel)  # summary only
    
    def crossvalidation(self,rf, pipeline):
        
        paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [int(x) for x in np.linspace(start = 10, stop = 50, num = 3)]) \
        .addGrid(rf.maxDepth, [int(x) for x in np.linspace(start = 5, stop = 25, num = 3)]) \
        .build()

        crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=MulticlassClassificationEvaluator(),
                          numFolds=3,
                          parallelism = 4)

        return crossval


    

    

    
        