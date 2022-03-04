    def get_exchanges(self, market: str=None):
        
        if not market in self.markets:
            raise Exception(f'Market must be one of {self.markets}')
            
        resp = self.crypto_crypto_exchanges().cryptoexchange
        temp_dict = {}
        for i in range(0, len(resp)):
            temp_dict[i] = [resp[i].name,
                            resp[i].i_d_of_the_exchange,
                            resp[i].market,
                            resp[i].locale,
                            resp[i].url,
                            resp[i].tier]

        df = pd.DataFrame(temp_dict).T
        df.columns = ['name', 'id', 'market', 'locale', 'url','tier']
        df_spark = self.spark.createDataFrame(df)
        df_spark.write.format('delta').mode('overwrite').save('/home/kyle/crypto_db/exchanges')
        
        return df_spark

    
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
        