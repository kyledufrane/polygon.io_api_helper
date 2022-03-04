import findspark
findspark.init('/opt/spark/')

from polygon import RESTClient
import json
import os
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import date, datetime, timedelta
import pyspark
from pyspark.sql.types import *
from pyspark.sql import functions as f
from delta import *

class MyRESTClient(RESTClient):
    """ This is a custom generated class """
    DEFAULT_HOST = "api.polygon.io"
[{"ev":"XA","pair":"ETH-BTC","v":0.52725026,"vw":0.0712,"z":0,"o":0.07119,"c":0.07114,"h":0.07119,"l":0.07114,"s":1645140000000,"e"

    def __init__(self, auth_key: str, timeout: int=None):
        super().__init__(auth_key)
        retry_strategy = Retry(total=10,
                               backoff_factor=10,
                              status_forcelist=[429, 500, 502, 503, 504])
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount('https://', adapter)
        self.builder = pyspark.sql.SparkSession.builder.appName("kline_data_collection") \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                        .config("spark.driver.memory", "8g")
        self.spark = configure_spark_with_delta_pip(self.builder).getOrCreate()
        self.markets = ['crypto', 'stocks']
    
    def get_tickers(self, market: str=None):
                
        if not market in self.markets:
            raise Exception(f'Market must be one of {self.markets}')

        resp = self.reference_tickers_v3(market='crypto')

        if hasattr(resp, 'results'):
            df = self.spark.createDataFrame(resp.results)

            while hasattr(resp, 'next_url'):
                resp = self.reference_tickers_v3(next_url=resp.next_url)
                df = df.union(self.spark.createDataFrame(resp.results))

            df = df.drop_duplicates(subset=['ticker'])
            df.write.format("delta").mode("overwrite").save('/home/kyle/crypto_db/tickers')
            
            return df
        
        return None 
    
    def get_bars(self, market:str='crypto', ticker:str=None, multiplier:int=1, timespan:str='minute', from_:date=None, to:date=None):
        
        if not market in self.markets:
            raise Exception(f'Market must be one of {self.markets}')
        
        if ticker is None:
            raise Exception(f'Ticker must not be None')
            
        to = to if to else datetime.now()
        
        path = f'/home/kyle/crypto_db/{ticker}_{multiplier}_{timespan}'
        if os.path.isdir(path):
            files = os.listdir(path)
            files = max([datetime.strptime(file.lstrip('from_='),'%Y-%m-%d')  for file in files if file != '_delta_log'])
            dir_path = 'from_=' + "'" + datetime.strftime(files, '%Y-%m-%d') + "'"
            df = self.spark.read.format('delta').load(path).where(dir_path)
            from_ = datetime.fromtimestamp(int(df.sort('t').select('t').collect()[-1][0])/1000)
            days = abs(datetime.now() - from_).days
            del df
            print(f'Downloading {days} days of data for {ticker}')
        else:
            from_ = from_ if from_ else date(2000,1,1)
            print(f'Downloading all data for {ticker}')
            
        if market == 'crypto':
            resp = self.crypto_aggregates(ticker, multiplier, timespan, from_.strftime('%Y-%m-%d'), to.strftime('%Y-%m-%d'), limit=50000)
        
        if hasattr(resp, 'results'):

            myschema = StructType([StructField('v', StringType(), True),
                                  StructField('vw', StringType(), True),
                                  StructField('o', StringType(), True),
                                  StructField('c', StringType(), True),
                                  StructField('h', StringType(), True),
                                  StructField('l', StringType(), True),
                                  StructField('t', StringType(), True),
                                  StructField('n', StringType(), True)])

            df = self.spark.createDataFrame(resp.results, schema=myschema)
            df = df.withColumn('from_', f.lit(from_.strftime('%Y-%m-%d')))
            df.write.partitionBy('from_').format("delta").mode("append").save(f'/home/kyle/crypto_db/{ticker}_{multiplier}_{timespan}')

            last_minute = 0
            while resp.results[-1]['t'] > last_minute:
                last_minute = resp.results[-1]['t']
                last_minute_date = datetime.fromtimestamp(last_minute/1000)
                resp = self.crypto_aggregates(ticker, multiplier, timespan, last_minute, to.strftime('%Y-%m-%d'), limit=50000)
                
                new_bars = self.spark.createDataFrame(resp.results, schema=myschema)
                new_bars = new_bars.withColumn('from_', f.lit(last_minute_date.strftime('%Y-%m-%d')))
                new_bars = new_bars[new_bars['t'] > last_minute]
                new_bars.write.partitionBy('from_').format('delta').mode('append').save(f'/home/kyle/crypto_db/{ticker}_{multiplier}_{timespan}')
            
            print(f'Completed {ticker}_{multiplier} --> sent to Database')
        else:
            print(f'No data for {ticker}_{multiplier}_{timespan}')
            
#         try:
#             df = self.spark.read.format('delta').load(f'/home/kyle/crypto_db/{ticker}_{multiplier}_{timespan}/')

#             floats = ['v', 'vw', 'o', 'c', 'h', 'l']
#             ints = ['n']

#             for float_ in floats:
#                 df = df.withColumn(float_, df[float_].cast(FloatType()))
#             for int_ in ints:
#                 df = df.withColumn(int_, df[int_].cast(IntegerType()))

#             coin = ticker.lstrip('X:')

#             new_columns = []

#             for col in df.columns:
#                 if col == 'v':
#                     new_columns.append('volume')
#                 elif col == 'vw':
#                     new_columns.append('vwap')
#                 elif col == 'o':
#                     new_columns.append('open')
#                 elif col == 'c':
#                     new_columns.append('close')
#                 elif col == 'h':
#                     new_columns.append('high')
#                 elif col == 'l':
#                     new_columns.append('low')
#                 elif col == 't':
#                     new_columns.append('unix_time')
#                 elif col == 'n':
#                     new_columns.append('transactions')

#             for c,n in zip(df.columns, new_columns):
#                 df = df.withColumnRenamed(c,n)

#             df = df.withColumn("date_time",f.concat_ws(".",\
#                           f.from_unixtime(f.substring(f.col("unix_time"),0,10),\
#                           "yyyy-MM-dd HH:mm:ss"),f.substring(f.col("unix_time"),-3,3)))\
#                     .withColumn('asset', f.lit(f'{coin}'))\
#                     .drop('unix_time')
#             df = df.withColumn('date_time', df['date_time'].cast(TimestampType()))
#             df.write.partitionBy('from_').format("delta").option('mergeSchema', 'true').mode("overwrite").save(f'/home/kyle/crypto_db/{ticker}_{multiplier}_{timespan}')
        
#         except:
#             print(f'***************************** No data for {ticker}_{multiplier}_{timespan} *****************************')
        

