'''
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from DataManipulation.DemographicInfo import DemographicInfo

import time

conf = SparkConf().setAppName("app_name")
sc = SparkContext(conf=conf)
spark_session = SparkSession(sparkContext=sc).builder \
                .getOrCreate() \

log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger("dbg_et")

print('defaultParallelism={0}, and size of executorMemoryStatus={1}'.format(sc.defaultParallelism,
           sc._jsc.sc().getExecutorMemoryStatus().size()))
time.sleep(15)
print('After 15 seconds: defaultParallelism={0}, and size of executorMemoryStatus={1}'
          .format(sc.defaultParallelism, 
                  sc._jsc.sc().getExecutorMemoryStatus().size()))



dem_inf = DemographicInfo(spark_session)
rdd = dem_inf.get_Rdd()
rdd = rdd.repartition(5000)
print(rdd.getNumPartitions())
print(sc._jsc.sc().getExecutorMemoryStatus().size())

print('And after rdd operations: defaultParallelism={0}, and size of executorMemoryStatus={1}'
          .format(sc.defaultParallelism,
                  sc._jsc.sc().getExecutorMemoryStatus().size()))
rdd = rdd.collect()

print('And after rdd operations: defaultParallelism={0}, and size of executorMemoryStatus={1}'
          .format(sc.defaultParallelism,
                  sc._jsc.sc().getExecutorMemoryStatus().size()))
'''

import socket

import pyspark
from pip._internal.operations import freeze

NUMBER_OF_OPERATIONS = 100_000
NUMBER_OF_PARTITIONS = 10_000

def get_environment_and_hostname(_):
    hostname = socket.gethostname()
    package_versions = list(freeze.freeze())
    return tuple(package_versions), hostname


def to_set(a):
    return {a}


def add_to_set(current_set, new_value):
    current_set.add(new_value)
    return current_set


def combine_sets(first_set, second_set):
    return first_set.union(second_set)


spark_configuration = pyspark.SparkConf().setAppName('Debug worker node Python environment')
spark_context = pyspark.SparkContext.getOrCreate(conf=spark_configuration)
environment_hostnames = spark_context.parallelize([1] * NUMBER_OF_OPERATIONS). \
    repartition(NUMBER_OF_PARTITIONS). \
    map(get_environment_and_hostname). \
    combineByKey(to_set, add_to_set, combine_sets). \
    collect()
environment_hostnames = {environment: hostnames for environment, hostnames in environment_hostnames}
environment_to_number = {environment: i for i, environment in enumerate(environment_hostnames.keys())}

for environment in environment_hostnames.keys():
    print('---------- Environment {} --------------'.format(environment_to_number[environment]))
    for package in environment:
        print('    {}'.format(package))
    print()
print()

for environment, hostnames in environment_hostnames.items():
    for hostname in sorted(hostnames):
        print('Node {}: Environment {}'.format(hostname, environment_to_number[environment]))