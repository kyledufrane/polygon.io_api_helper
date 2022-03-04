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

    def __init__(self, auth_key: str, timeout: int=None):
        super().__init__(auth_key)
        retry_strategy = Retry(total=10,
                               backoff_factor=10,
                              status_forcelist=[429, 500, 502, 503, 504])
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount('https://', adapter)
        self.builder = pyspark.sql.SparkSession.builder.appName("trade_data_collection") \
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
        
        
    def get_trades(self, from_:str=None, to:str=None, start_date=datetime.today(), days:int=365*10):
        '''
        from_: coin (Ex. BTC)
        to: currency (Ex. USD)
        start_date: date 
        '''
        new_db = False
        path = f'/home/kyle/crypto_db/trade_history_{from_}{to}'
        print(f'Starting to collect data for {from_}{to}')
        if os.path.isdir(path):
            files = os.listdir(path)
            files = max([datetime.strptime(file.lstrip('day='),'%Y-%m-%d')  for file in files if file != '_delta_log'])
            dir_path = 'day=' + "'" + datetime.strftime(files, '%Y-%m-%d') + "'"
            df = self.spark.read.format('delta').load(path).where(dir_path)
            latest_timestamp = int(df.sort('t').select('t').collect()[-1][0])
            begin_date = datetime.fromtimestamp(latest_timestamp/1000)
            days = abs(datetime.now() - begin_date).days
            if days == 0:
                days = 1
            date_list = [start_date - timedelta(days=x) for x in range(days)]
            dates = [date.strftime('%Y-%m-%d') for date in date_list]
            dates.sort()
            del df
            print(f'Uploaded DataBase -> Downloading {days} days of data')
        else:
            date_list = [start_date - timedelta(days=x) for x in range(days)]
            dates = [date.strftime('%Y-%m-%d') for date in date_list]
            dates.sort()
            print('Creating new database -> Default history set to 10 years')
            new_db = True

        myschema = StructType([StructField('x', StringType(), True),
                              StructField('p', StringType(), True),
                              StructField('s', StringType(), True),
                              StructField('c', StringType(), True),
                              StructField('i', StringType(), True),
                              StructField('t', StringType(), True)])

        for day in dates:
            print(f'Starting new day {day}')
            resp = self.crypto_historic_crypto_trades(from_, to, day, limit=50000)
            
            if resp.ticks != None:
                print(f'resp.ticks != None')
                df = self.spark.createDataFrame(resp.ticks, schema=myschema)
                df = df.withColumn('day', f.lit(day))
                if new_db:
                    df.write.partitionBy('day').format("delta").mode("append").save(f'/home/kyle/crypto_db/trade_history_{from_}{to}')
                else:
                    df = df[df['t'] > latest_timestamp]
                    df.write.partitionBy('day').format("delta").mode("append").save(f'/home/kyle/crypto_db/trade_history_{from_}{to}')
                    print(f'Writing to existing database for {from_}{to}')

                offset = 0
                    
                while resp.ticks[-1]['t'] > offset:
                    offset = resp.ticks[-1]['t']
                    resp = self.crypto_historic_crypto_trades(from_, to, day, limit=50000, offset=offset)

                    if resp.ticks != None:
                        new_bars = self.spark.createDataFrame(resp.ticks, schema=myschema)
                        new_bars = new_bars.withColumn('day', f.lit(day))
                        new_bars = new_bars[new_bars['t'] > offset]
                        new_columns = ['exchange_code', 'price_of_trade', 'size_of_trade', 'condition_codes', 'unknown', 'unix_time']

#                         for c,n in zip(new_bars.columns, new_columns):
#                             new_bars = new_bars.withColumnRenamed(c,n)

#                         new_bars = new_bars.withColumn('conditions',
#                                         f.when(f.col('condition_codes') == '[0]', 'normal_trade')\
#                                         .when(f.col('condition_codes') == '[1]', 'sell_side')\
#                                         .when(f.col('condition_codes') == '[2]', 'buy_side'))\
#                             .withColumn('exchanges',
#                                        f.when(f.col('exchange_code') == '1', 'Coinbase')\
#                                        .when(f.col('exchange_code') == '2', 'Bitfinex')\
#                                        .when(f.col('exchange_code') == '6', 'Bitstamp')\
#                                        .when(f.col('exchange_code') == '10', 'HitBTC')\
#                                        .when(f.col('exchange_code') == '23', 'Kraken'))\
#                             .withColumn("date_time",f.concat_ws(".",\
#                                       f.from_unixtime(f.substring(f.col("unix_time"),0,10),\
#                                       "yyyy-MM-dd HH:mm:ss"),f.substring(f.col("unix_time"),-3,3)))\
#                             .withColumn('asset', f.lit(f"{from_}{to}"))\
#                             .drop('exchange_code', 'condition_codes', 'unix_time')
#                         new_bars.write.partitionBy('day').format('delta').option('mergeSchema', 'true').mode('append').save(f'/home/kyle/crypto_db/trade_history_{from_}{to}')

                    else:
                        print(f'End of data for {from_} on {day}')
                        break
                
            else:
                print(f'No data for {from_} on {day}')
                pass