from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors, VectorUDT, Vector
from pyspark.sql.functions import udf
from pyspark.ml import linalg
from pyspark.sql import Row


def list_to_vector(df, col_name):
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())

    df = df.withColumn('vector', list_to_vector_udf(df[col_name]))

    df = df.drop(df[col_name])
    df = df.withColumnRenamed('vector', col_name)

    return df