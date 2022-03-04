import pyspark
from pyspark.sql.types import *
from pyspark.sql import functions as f
from delta import *

class DataCleaning:

    def __init__(self):
        self.builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                .config("spark.driver.memory", "8g")
        self.spark = configure_spark_with_delta_pip(self.builder).getOrCreate()

    def clean_bars(self, ticker, multiplier, timespan='minute'):
        
        try:
            df = self.spark.read.format('delta').load(f'/home/kyle/crypto_db/{ticker}_{multiplier}_{timespan}/')

            floats = ['v', 'vw', 'o', 'c', 'h', 'l']
            ints = ['n']

            for float_ in floats:
                df = df.withColumn(float_, df[float_].cast(FloatType()))
            for int_ in ints:
                df = df.withColumn(int_, df[int_].cast(IntegerType()))

            coin = ticker.lstrip('X:')

            new_columns = []

            for col in df.columns:
                if col == 'v':
                    new_columns.append('volume')
                elif col == 'vw':
                    new_columns.append('vwap')
                elif col == 'o':
                    new_columns.append('open')
                elif col == 'c':
                    new_columns.append('close')
                elif col == 'h':
                    new_columns.append('high')
                elif col == 'l':
                    new_columns.append('low')
                elif col == 't':
                    new_columns.append('unix_time')
                elif col == 'n':
                    new_columns.append('transactions')

            for c,n in zip(df.columns, new_columns):
                df = df.withColumnRenamed(c,n)

            df = df.withColumn("date_time",f.concat_ws(".",\
                          f.from_unixtime(f.substring(f.col("unix_time"),0,10),\
                          "yyyy-MM-dd HH:mm:ss"),f.substring(f.col("unix_time"),-3,3)))\
                    .withColumn('asset', f.lit(f'{coin}'))\
                    .drop('unix_time')
            df = df.withColumn('date_time', df['date_time'].cast(TimestampType()))
            df.write.partitionBy('from_').format("delta").mode("overwrite").save(f'/home/kyle/crypto_db/cleaned_{ticker}_{multiplier}_{timespan}')
        
        except:
            print(f'***************************** No data for {ticker}_{multiplier}_{timespan} *****************************')
        

        