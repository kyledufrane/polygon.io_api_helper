from DataCollection.DataCollection_Trades import MyRESTClient
import utils
    
if __name__ == "__main__": 
            
    api_key = utils.get_keys('/home/kyle/.secret/polygon_key')['key']
    client = MyRESTClient(api_key)
    
    for symbol in client.get_tickers(market='crypto').select('base_currency_symbol').collect():
        client.get_trades(from_=symbol[0], to='USD')
    
        
        