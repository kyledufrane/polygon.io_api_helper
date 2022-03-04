from DataCollection.DataCollection_Klines import MyRESTClient
import utils
        
if __name__ == "__main__": 
            
    api_key = utils.get_keys('/home/kyle/.secret/polygon_key')['key']
    
    client = MyRESTClient(api_key)
        
    multipliers = [1,3,5,15,30,45]
    
    timespan = 'minute'
    
    for symbol in client.get_tickers(market='crypto').select('ticker').collect():
        for multiplier in multipliers:
            client.get_bars(market='crypto', ticker=symbol[0], multiplier=multiplier, timespan=timespan)