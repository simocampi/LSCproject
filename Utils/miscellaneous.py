from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT, Vector
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.sql.functions import udf
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml import linalg
from pyspark.sql import Row

def split_train_test(labeled_point_rdd, training_data_ratio=0.7):
    train_data, test_data = scaled_df.randomSplit([.8,.2],seed=1234)
    #splits = [training_data_ratio, 1.0 - training_data_ratio]
    
    #return train_data, test_data

#divide the data into features and labels and return an RDD 
def get_data_label(data, label, features):    
    data = list_to_vector(data, 'Data')

    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features")
    data = assembler.transform(data)
    #data.show(6)
    
    
    #data.show(8)
    input_data = data.select(col(label).alias('label'), data['features'])
    #print(input_data.rdd.map(lambda x: x[1]).take(1))
    # dictionary to associate a number to each 
    dict = {'Bronchiectasis':0, 'Bronchiolitis':1, 'COPD':2, 'Healthy':3, 'Pneumonia':4, 'URTI':5}
   
    input_data_rdd = input_data.rdd.map(lambda x: ( float( dict.get(x[0])), x[1] ))
    input_data_rdd.toDF(['l','f']).printSchema()
    print("PASSATO!!!!!!!!!!!!!!")

    transformed_df = input_data_rdd.rdd.map(lambda x: LabeledPoint(x[0], Vectors.dense(x[1])))
    transformed_df.toDF().printSchema()
    print("PASSATO!!!!!!!!!!!!!!")

    #print( labeled_point_rdd.take(1))
    #return labeled_point_rdd
    #return data
    


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
    #OneHotEncoder(df)
    get_data_label(df,label='Diagnosis', features=['Data','Wheezes','Crackels'])