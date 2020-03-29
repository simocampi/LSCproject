class Path():
    
    @staticmethod
    def  get_database_path():
        file_path= __file__
        
        head, sep, tail = file_path.partition('LSCproject\\')
        print('\n'+head+sep+'\n')

        return head+sep+'Database\\'
