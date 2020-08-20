from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf

def split_train_test(all_data):
    train_data, test_data = scaled_df.randomSplit([.8,.2],seed=1234)

    return train_data, test_data


def divide_in_label_and_feature(data, label, features):    
    data = list_to_vector(data, 'Data')

    assembler = VectorAssembler(
        inputCols=features,
        outputCol="features")
    data = assembler.transform(data)

    input_data = data.select(col(label).alias('label'), data['features'])
    input_data.show(2)

def list_to_vector(df, col_name):
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

    df = df.withColumn('vector', list_to_vector_udf(df[col_name]))

    df = df.drop(df[col_name])
    df = df.withColumnRenamed('vector', col_name)

    return df


def OneHotEncoder(df):
    dict = {'Bronchiectasis':0, 'Bronchiolitis':1, 'COPD':2, 'Healthy':3, 'Pneumonia':4, 'URTI':5}
    df = df.select('Diagnosis').map(lambda x: dict.get(x))
    df.show()
    pass

def test(df):
    divide_in_label_and_feature(df,label='Diagnosis', features=['Data','Wheezes','Crackels'])