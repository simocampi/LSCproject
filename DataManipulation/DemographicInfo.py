from DataManipulation.Utils.Path import Path

class DemographicInfo(object):
    
    def __init__(self, spark_context):
        
        DEMOGRAPHIC_INFO_FILE = 'demographic_info.txt'
        DEMOGRAPHIC_INFO_PATH = Path.get_database_path()+DEMOGRAPHIC_INFO_FILE

        self.spark_context = spark_context
        self.rdd = spark_context.textFile(DEMOGRAPHIC_INFO_PATH)

    def get_rdd(self):
        return self.rdd