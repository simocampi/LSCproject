import os
class Path():

    RunningOnLocal = bool(os.name == 'nt')
    ClusterDatabasePath = 'hdfs://master:9000/user/user24/'
    path_separator = '\\'
    if not RunningOnLocal:
        path_sepatator = '/'
        
    #Get Path of Project directory
    @staticmethod
    def  get_database_path():
        if not Path.RunningOnLocal:
            return Path.ClusterDatabasePath+'Database/'
        
        file_path= __file__

        head, sep, tail = file_path.partition('LSCproject\\')

        project_path = head+sep
        return project_path+'Database\\'
    
    @staticmethod
    def  get_wav_file_path():
        if not Path.RunningOnLocal:
            return Path.ClusterDatabasePath+'Database/audio_and_txt_files/'
        file_path= __file__
        
        head, sep, tail = file_path.partition('LSCproject\\')
        project_path = head+sep
        return project_path+'Database\\audio_and_txt_files\\'