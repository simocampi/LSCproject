
Intruction of Spark execution:

1- connect to cluster: ssh user24@192.168.20.157

2- to upload files: scp /mnt/c/Users/simoc/Documents/GitHub/LSCproject/Spark.py user24@192.168.20.157:Project

3- to download files: scp user24@192.168.20.157:Project /mnt/c/Users/simoc/Documents/GitHub/LSCproject/Spark.py

4- to exute a spark program: spark-submit --master yarn Spark.py hdfs://master:9000/user/user24/Project


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Project delivery:

Document your project with a Powerpoint presentation, pointing out:

-group members
-the problem you have considered
-the dataset you have chosen
-the libraries you have used
-details about the design of the proposed solution
-details about the implementation of the proposed solution
-performance evaluation for local and cluster-based executions
-a rough estimate of the effort (in terms, e.g., of number of hours) devoted to the project