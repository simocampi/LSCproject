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

from Utils.miscellaneous import list_to_vector



def split_train_test(labeled_point_rdd, training_data_ratio=0.7, random_seeds=42):
    splits = [training_data_ratio, 1.0 - training_data_ratio]
    training_data, test_data = labeled_point_rdd.randomSplit(splits, random_seeds)
    return training_data, test_data

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
    layers = [13, 8, 1]

    FNN = MultilayerPerceptronClassifier(labelCol="label", \
                                         featuresCol="features",\
                                         maxIter=50, layers=layers, \
                                         blockSize=128, seed=1234)
    
    # train the model
    model = FNN.fit(trainingData)
    return model

def evaluate(model, eval_data):
    # Make predictions.
    predictions = model.transform(eval_data)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Predictions accuracy = %g, Test Error = %g" % (accuracy,(1.0 - accuracy)))

    return accuracy


def test(my_data):
    print("drop...")
    to_drop = ['Crackels', 'Patient_number']
    my_data = drop_unecessaryColumns(my_data, to_drop)
    my_data.printSchema()

    print('cast...')
    my_data = my_data.withColumn("WheezesTemp", my_data['Wheezes'].cast(IntegerType()))\
                     .drop("Wheezes")\
                     .withColumnRenamed("WheezesTemp", "Wheezes")
    my_data.printSchema()

    print('rename...')
    my_data = transform_data_label(my_data, label='Wheezes', features=['Data'])
    my_data.printSchema()
    #my_data.groupBy('label').count().show()

    print('splitting...\n')
    training_data, test_data = split_train_test(my_data)

    print('training...\n')
    my_mode = train(training_data)

    print('evaluating....')
    acc = evaluate(my_mode, test_data)




