class Path():
    
    #Get Path of Project directory
    @staticmethod
    def  get_database_path():
        file_path= __file__
        
        head, sep, tail = file_path.partition('LSCproject\\')

        project_path = head+sep
        return project_path+'Database\\'
