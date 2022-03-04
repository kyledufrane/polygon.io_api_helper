import findspark
findspark.init('/opt/spark/')

from Technical_Analysis.technical_analysis import technical_analysis
from DataCollection.DataCollection import MyRESTClient
import utils
import pyspark
from delta import *

if __name__ == '__main__':
        
    t_a = technical_analysis()
    
    builder = pyspark.sql.SparkSession.builder.appName("technical_indicators") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    multipliers = [1,3,5,15,30,45]
    
    timespan = 'minute'
    
    for symbol in spark.read.format('delta').load('/home/kyle/crypto_db/tickers/').select('ticker').collect():
        for multiplier in multipliers:
            t_a.apply_all_indicators(ticker=symbol[0], multiplier=multiplier, timespan=timespan)
    
    t_a.spark.stop()
