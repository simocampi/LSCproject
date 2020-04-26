from DataManipulation.Utils.Path import Path
import pandas as pd
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField

class DemographicInfo(object):
    
    def __init__(self, spark_session):

        self.original_schema = [StructField('Patient_number', IntegerType(), True), 
                                StructField('Age', FloatType(), True),
                                StructField('Sex', StringType(), True),
                                StructField('Adult_BMI', FloatType(), True),
                                StructField('Child_weight', FloatType(), True),
                                StructField('Child_height', FloatType(), True)]

        self.shrank_schema = StructType(fields=[StructField('Patient_number', IntegerType(), True), 
                                StructField('Age', FloatType(), True),
                                StructField('Sex', StringType(), True),
                                StructField('BMI', FloatType(), True)]) # this column is filled with child Bmi calculated with Child_weight and Child_height

        self.data_structure = StructType(self.original_schema)

        self.DEMOGRAPHIC_INFO_FILE = 'demographic_info.csv'
        self.DEMOGRAPHIC_INFO_PATH = Path.get_database_path() + self.DEMOGRAPHIC_INFO_FILE #'hdfs://master:9000/user/user24/LSCproject/Database/demographic_info.csv' 

        self.spark_session= spark_session

        # tapullata increddibbile START
        df = pd.read_csv( self.DEMOGRAPHIC_INFO_PATH, sep=',', header=0) 
        self.dataFrame = spark_session.createDataFrame(df,schema=self.data_structure)
        # tapullata increddibbile END
 

    def get_DataFrame(self):
        return self.dataFrame

    def get_Rdd(self):
        return self.dataFrame.rdd

    # method fillna to replace nan values with   a certain value
    # .fillna("No College", inplace = True) read reference