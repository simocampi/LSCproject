from DataManipulation.Utils.Path import Path
from pyspark.sql.types import StringType, IntegerType, FloatType, StructType, StructField

class PatientDiagnosis(object):
    
    def __init__(self, spark_session):

        self.schema = [StructField('Patient_number', IntegerType(), False), 
                        StructField('Diagnosis', StringType(), False)]

        self.data_structure = StructType(self.schema)

        self.PATIENT_DIAGNOSIS_FILE = 'patient_diagnosis.csv'
        self.PATIENT_DIAGNOSIS_PATH= Path.get_database_path()+ self.PATIENT_DIAGNOSIS_FILE #'hdfs://master:9000/user/user24/LSCproject/Database/patient_diagnosis.csv' 

        self.spark_session= spark_session
        self.dataFrame = spark_session.read.csv(self.PATIENT_DIAGNOSIS_PATH, sep=',', schema = self.data_structure)


    def get_DataFrame(self):
        return self.dataFrame 

    def get_Rdd(self):
        return self.dataFrame.rdd
