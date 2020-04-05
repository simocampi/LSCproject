from Utils.Path import *

class DemographicInfo(object):
    
    def __init__(self, spark_session):
        
        DEMOGRAPHIC_INFO_FILE = 'demographic_info.csv'
        DEMOGRAPHIC_INFO_PATH = Path.get_database_path()+DEMOGRAPHIC_INFO_FILE

        self.spark_session= spark_session
        self.rdd = spark_session.read.format('csv').option("sep", " ") \
                .option("inferSchema", "true") \
                .option("header", "true") \
                .load(DEMOGRAPHIC_INFO_PATH).rdd

    def get_rdd(self):
        return self.rdd

    # method fillna to replace nan values with   a certain value
    # .fillna("No College", inplace = True) read reference