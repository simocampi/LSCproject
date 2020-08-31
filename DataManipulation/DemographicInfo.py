from DataManipulation.Utils.Path import Path
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField
from Utils.BMI import replace_bmi_child as replace_bmi_child_stronza

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
        self.DEMOGRAPHIC_INFO_PATH = Path.get_database_path() + self.DEMOGRAPHIC_INFO_FILE

        self.spark_session= spark_session

        self.dataFrame = self.spark_session.read \
            .csv(path=self.DEMOGRAPHIC_INFO_PATH, header=True, schema= self.data_structure, sep=',', nullValue='NA')

        # get rid of the Child's informations => now BMI column contains the BMI for both Adult and Children
        temp_rdd = self.dataFrame.rdd
        temp_rdd = temp_rdd.map(lambda p: replace_bmi_child(p))
        temp_rdd.take(1)
        self.dataFrame = temp_rdd.toDF()#self.shrank_schema) #['Patient_number', 'Age', 'Sex', 'BMI']

    def get_DataFrame(self):
        return self.dataFrame

    def get_Rdd(self):
        return self.dataFrame.rdd

    # the BMI for children is calculated whenever is possible 
def replace_bmi_child(self, p):
    return p
    if p['Age'] is  None or p['Child_weight'] is None or p['Child_height'] is None:
        return (p['Patient_number'], p['Age'], p['Sex'], None)

    if p['Age']<18:
        return (p['Patient_number'], p['Age'], p['Sex'], p['Child_weight'] / (p['Child_height']/100)**2)
    else:
        return (p['Patient_number'], p['Age'], p['Sex'], p['Adult_BMI'])