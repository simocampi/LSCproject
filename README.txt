
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


local Spark execution:

.\spark-2.4.5-bin-hadoop2.7\bin\spark-submit C:\Users\simoc\Documents\GitHub\LSCproject\Spark.py

C:\Users\carot\Desktop\Ale\LSC_spark\spark-2.4.5-bin-hadoop2.7\bin\spark-submit .\Main.py

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

demographic_info.csv:

the original file has 6 columns:

  - Patient number
  - Age
  - Sex
  - Adult BMI (kg/m2)
  - Child Weight (kg)
  - Child Height (cm) must be converted in m

In the original file the BMI was available only for patient with age >= 18; BMI for children is calculated using  Child Weight and Child Height using the following formula:

Child weight / (Child height / 100)**2 


Steps:

1) Get rid of Child Weight and Child Height: now the schema has 4 columns:
	- Patient number
  	- Age
  	- Sex
  	- BMI (kg/m2)


