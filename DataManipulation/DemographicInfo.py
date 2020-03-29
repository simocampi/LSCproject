import sys,os

from Utils.Path import Path

class DemographicInfo(object):
    DEMOGRAPHIC_INFO_FILE = 'demographic_info.txt'
    DEMOGRAPHIC_INFO_PATH = Path.get_database_path()+DEMOGRAPHIC_INFO_FILE
