
1- connect to cluster: ssh user24@192.168.20.157

2- to upload files: scp /mnt/c/Users/simoc/Documents/GitHub/LSCproject/Spark.py user24@192.168.20.157:Project

3- to download files: scp user24@192.168.20.157:Project /mnt/c/Users/simoc/Documents/GitHub/LSCproject/Spark.py

4- to exute a spark program: spark-submit --master yarn Spark.py hdfs://master:9000/user/user24/Project
