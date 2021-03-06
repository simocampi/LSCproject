from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT, Vector
from pyspark.sql.functions import udf
from pyspark.ml import linalg
from pyspark.sql import Row
from py4j.protocol import Py4JJavaError

def split_train_test(labeled_point_rdd, training_data_ratio=0.7, random_seeds=13579):
    splits = [training_data_ratio, 1.0 - training_data_ratio]
    training_data, test_data = labeled_point_rdd.randomSplit(splits, random_seeds)
    return training_data, test_data

#divide the data into features and labels 
def split_data_label(data, label, features):    
    data = list_to_vector(data, 'Data')
    
    print('\n\n-------Schema Dataset Before VectorAssembler--------\n')
    data.printSchema()
    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features")
    print('\n---------------------------------------------\n\n')
    data = assembler.transform(data)  
      
    return data

def list_to_vector(df, col_name):
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

    df = df.withColumn('vector', list_to_vector_udf(df[col_name]))

    df = df.drop(df[col_name])
    df = df.withColumnRenamed('vector', col_name)

    return df