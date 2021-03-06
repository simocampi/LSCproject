from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT, Vector

from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorIndexer

from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel
from py4j.protocol import Py4JJavaError


from Utils.miscellaneous import list_to_vector
from Utils.miscellaneous import split_train_test, split_data_label

from datetime import datetime


def drop_unecessaryColumns(data, columns=[]):
    data = data.drop(*columns)
    return data

def train(trainingData):
    #layers = [13, 8, 2]    accuracy = 0.650186, Test Error = 0.349814
    layers = [15, 8, 5, 8]

    FNN = MultilayerPerceptronClassifier(labelCol="label", \
                                         featuresCol="features",\
                                         maxIter=50, layers=layers, \
                                         blockSize=128, seed=1234)
    pipeline = Pipeline(stages=[FNN])
    
    # train the model
    model = pipeline.fit(trainingData)
    return model

def evaluate(model, eval_data):
    # Make predictions.
    predictions = model.transform(eval_data)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print('evaluation finish', datetime.now())
    print("Predictions accuracy = {0}%, Test Error = {1}".format( accuracy*100, (1.0 - accuracy)))
    return accuracy

#con 40% dataset: train 42 min, evaluate 1.30 h, accuracy: 88 %, 
#con 60% dataset: train 47 min, evaluate 1.32 h, accuracy: 88.03853398828596%
#con 100% dataset: train 43 min, evaluate 1.25 h, accuracy: Predictions accuracy = 87.57565176908753%, Test Error = 0.12424348230912474
# usando la persist su dataset la evaluate ci mette solo 3 min


def fit_and_test(data_labeled):
    data=split_data_label(data_labeled,label='label', features=['Data','Wheezes','Crackels'])

    print('split_train_test...', datetime.now())
    training_data, test_data = split_train_test(data)
    
    #print('Load model...')
    #per testare la load
    #try:
     #   model = PipelineModel.load("/home/user24/LSCproject_2/multiperceptron_model")
    
    #except Py4JJavaError:
    print('Train... ', datetime.now())
    model = train(training_data)
    #/home/user24/LSCproject_2/multiperceptron_model
    print('save model... ')
    model.write().overwrite().save("/home/user24/LSCproject_2/multiperceptron_model")

    print('evaluating....', datetime.now(),"\n")
    acc = evaluate(model, test_data)