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

from Utils.miscellaneous import list_to_vector
from Utils.miscellaneous import split_train_test, split_data_label

from datetime import datetime




'''def split_train_test(labeled_point_rdd, training_data_ratio=0.7, random_seeds=42):
    splits = [training_data_ratio, 1.0 - training_data_ratio]
    training_data, test_data = labeled_point_rdd.randomSplit(splits, random_seeds)
    return training_data, test_data
'''

#divide the data into features and labels 
def transform_data_label(data, label, features):    
    data = list_to_vector(data, 'Data')

    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features")
    data = assembler.transform(data) 

    input_data = data.select(col(label).alias('label'), data['features'])

    return input_data

def drop_unecessaryColumns(data, columns=[]):
    data = data.drop(*columns)
    return data

def train(trainingData):
    #layers = [13, 8, 2]    accuracy = 0.650186, Test Error = 0.349814
    layers = [13, 8, 5, 2]

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
    print("Predictions accuracy = %g, Test Error = %g" % (accuracy, (1.0 - accuracy)))
    return accuracy


'''def test(my_data):
    
    print("drop...", datetime.now())
    to_drop = ['Crackels', 'Patient_number']
    my_data = drop_unecessaryColumns(my_data, to_drop)
    my_data.printSchema()

    print('cast...', datetime.now())
    my_data = my_data.withColumn("WheezesTemp", my_data['Wheezes'].cast(IntegerType()))\
                     .drop("Wheezes")\
                     .withColumnRenamed("WheezesTemp", "Wheezes")
    my_data.printSchema()

    print('rename...', datetime.now())
    my_data = transform_data_label(my_data, label='Wheezes', features=['Data'])
    my_data.printSchema()
    my_data.groupBy('label').count().show()

    print('splitting...', datetime.now(),"\n")
    training_data, validation_data = split_train_test(my_data, training_data_ratio=0.3)
    training_data, test_data = split_train_test(training_data)
    
    print('training data count...', datetime.now())
    training_data.groupBy('label').count().show()

    print('training...', datetime.now(),"\n")
    my_mode = train(training_data)

    print('evaluating....', datetime.now(),"\n")
    acc = evaluate(my_mode, test_data)
'''

def fit_and_test(data_labeled):
    data=split_data_label(data_labeled,label='label', features=['Data','Wheezes','Crackels'])
    print('split_train_test...', datetime.now())
    training_data, test_data = split_train_test(data)
    print('Train... ', datetime.now())
    model = train(training_data)
    #/home/user24/LSCproject_2/multiperceptron_model
    model.write().save("/home/user24/LSCproject_2/multiperceptron_model")
    
    model = PipelineModel.load("/home/user24/LSCproject_2/multiperceptron_model")
    print('evaluating....', datetime.now(),"\n")
    acc = evaluate(model, test_data)