from os.path import *
from Utils.Path import *

class WAV():
    a=0


    @staticmethod
    def printw():
        file_path= __file__      
        head, sep, tail = file_path.partition('LSCproject\\')
        project_path = head+sep
        print('h-',head,'\n\n\n','s-',sep,'t-',tail)
        Path.get_database_path()
       