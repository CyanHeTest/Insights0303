# coding: utf-8

# In[1]:

import os
import pandas as pd
import numpy as np
import csv as csv
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configure Spark Settings
spark = SparkSession.builder                    .appName("testSpark")                    .config("spark.dynamicAllocation.enabled", "true")                    .config("spark.shuffle.service.enabled", "true")                    .config("spark.executor.cores", "5")                    .config("spark.executor.memory", "25G")                    .config("spark.driver.memory", "12G")                    .config("spark.dynamicAllocation.maxExecutors", "5")                    .config("spark.yarn.quene", "default")                    .config("spark.sql.catalogImplementation","hive")                    .enableHiveSupport()                    .getOrCreate()
#spark = SparkSession.builder.appName("CARE").config().enableHiveSupport().getOrCreate()
sc = spark.sparkContext


# In[2]:

import argparse

parser2 = argparse.ArgumentParser()
parser2.add_argument('--input_data_path', nargs='+')
parser2.add_argument('--inputfile', nargs='+')

args = parser2.parse_args() 

def load_params():
    try:
        input_data_path = args.input_data_path
        inputfile = args.inputfile
        logging.info(f'''
                    Parameters:
                    input_data_path: {input_data_path}
                    inputfile: {inputfile}
                ''')
            #raise EmptyParam(f'Params: HDFS_URL empty. Please input HDFS_URL.')
        if not input_data_path:
            raise EmptyParam(f'Params: input_data_path empty. Please input input_data_path.')
        if not inputfile:
            raise EmptyParam(f'Params: inputfile empty. Please input inputfile.')

        return  input_data_path,  inputfile

    except EmptyParam as e:
        logging.error(f'{e}', exc_info=1)
        sys.exit(1)
    except InvalidParam as e:
        logging.error(f'{e}', exc_info=1)
        sys.exit(1)

input_data_path, inputfile = load_params() 

readFile = sc.textFile(input_data_path[0] + inputfile[0])
readFileSplit = readFile.map(lambda k: k.split(","))
readFileSplitDf = readFileSplit.toDF()
readFileSplitPdDf = readFileSplitDf.toPandas()
print(readFileSplitPdDf.head())

