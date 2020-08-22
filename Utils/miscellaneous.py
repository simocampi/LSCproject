from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT, Vector
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.sql.functions import udf
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml import linalg
from pyspark.ml import linalg as ml_linalg
from pyspark.sql import Row
from pyspark.ml.linalg import Vector as MLVector, Vectors as MLVectors
from pyspark.mllib.linalg import Vector as MLLibVector, Vectors as MLLibVectors
from pyspark.mllib.regression import  LabeledPoint

def split_train_test(labeled_point_rdd, training_data_ratio=0.7, random_seeds=13579):
    splits = [training_data_ratio, 1.0 - training_data_ratio]
    training_data, test_data = labeled_point_rdd.randomSplit(splits, random_seeds)
    return training_data, test_data

#divide the data into features and labels and return an RDD 
def split_data_label(data, label, features):    
    data = list_to_vector(data, 'Data')

    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features")
    data = assembler.transform(data)
    
    input_data = data.select(col(label).alias('label'), data['features'])
    return input_data
    
    '''
    # dictionary to associate a number to each 
    dict = {'Bronchiectasis':0, 'Bronchiolitis':1, 'COPD':2, 'Healthy':3, 'Pneumonia':4, 'URTI':5}
   
    input_data_rdd = input_data.rdd.map(lambda x: ( float( dict.get(x[0])), x[1] ))
    input_data_rdd.toDF(['l','f']).printSchema()

    transformed_df = input_data_rdd.map(lambda x: LabeledPoint(x[0], Vectors.dense(as_mllib(x[1]))))
    print(type(transformed_df))
    print("PASSATO!!!!!!!!!!!!!!")

    return transformed_df
    '''

def as_mllib(v):
    if isinstance(v, ml_linalg.SparseVector):
        return MLLibVectors.sparse(v.size, v.indices, v.values)
    elif isinstance(v, ml_linalg.DenseVector):
        return MLLibVectors.dense(v.toArray())
    else:
        raise TypeError("Unsupported type: {0}".format(type(v)))


def list_to_vector(df, col_name):
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

    df = df.withColumn('vector', list_to_vector_udf(df[col_name]))

    df = df.drop(df[col_name])
    df = df.withColumnRenamed('vector', col_name)

    return df


def OneHotEncoder(df):
    dict = {'Bronchiectasis':0, 'Bronchiolitis':1, 'COPD':2, 'Healthy':3, 'Pneumonia':4, 'URTI':5}
    df = df.rdd.map(lambda x: dict.get(x[0]))
    print(df.take(1))
    

def test(df):
    pass
    #OneHotEncoder(df)
    #get_labeled_point(df,label='Diagnosis', features=['Data','Wheezes','Crackels'])