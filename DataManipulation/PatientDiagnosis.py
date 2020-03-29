from DataManipulation.Utils.Path import Path

class PatientDiagnosis(object):
    
    def __init__(self, spark_session):
        
        PATIENT_DIAGNOSIS_FILE = 'patient_diagnosis.csv'
        PATIENT_DIAGNOSIS_PATH= Path.get_database_path()+ PATIENT_DIAGNOSIS_FILE

        self.spark_session= spark_session
        self.rdd = spark_session.read.format('csv').option("sep", ",") \
                .option("inferSchema", "true") \
                .option("header", "false") \
                .load(PATIENT_DIAGNOSIS_PATH)

    def get_rdd(self):
        return self.rdd

#..