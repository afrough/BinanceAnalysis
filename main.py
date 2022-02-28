from datetime import datetime, timedelta
import time
import pandas as pd
import os
import sys
script_dir = os.path.dirname( __file__ )
mymodule_dir = os.path.join( '/home/orkideh/usr/Ori/Kafka_binance/' )
sys.path.append( mymodule_dir )
import binance_api.log_api as ba
import message_bus.kafka_producer as kp



'''
- this message receive a symbol. 
- call the function of get the id of the trade in 10 second before current time. 
- then start to call the trades by ids. 
- and send every resultset to the kafak topic 

  {
    "a": 26129,         // Aggregate tradeId
    "p": "0.01633102",  // Price
    "q": "4.70443515",  // Quantity
    "f": 27781,         // First tradeId
    "l": 27781,         // Last tradeId
    "T": 1498793709153, // Timestamp
    "m": true,          // Was the buyer the maker?
    "M": true           // Was the trade the best price match?
  }
'''

def create_data_from_binance(symbol):
    
    #1 get the id of the trade for the ten second before current time 
    from_id = ba.get_first_trade_id_from_date_time(symbol,10)
    current_time = 0
    df = pd.DataFrame()
    columns = ['a','p', 'q', 'f', 'l', 'T', 'm', 'M']

    #2 start to read the data by trade id
    while True:
        try:
            trades = ba.get_trades(symbol, from_id)        
            df = pd.DataFrame(trades)            
        
            for index, row in df.iterrows(): 
                #3 start to read the data by trade id
                kp.send_message('binance_messages',[row[x] for x in columns])        
        
            from_id = trades[-1]['a'] 
        except Exception as e:
            print(e)
            print('somethings wrong....... sleeping for 15s')
            time.sleep(15)
        

if __name__ == "__main__":    
    symbol = 'BTCUSDT' 
    create_data_from_binance(symbol)
    