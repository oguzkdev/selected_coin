import pandas as pd
from binance.client import Client
import ta
import numpy as np
import time
from datetime import datetime, timedelta
import sqlalchemy

class Signals:
    
    def __init__(self,symbol,interval,lookback,lags):
        self.symbol = symbol
        self.interval = interval
        self.lookback = lookback
        self.lags = lags
    
    def getminutedata(self):
        
        frame = pd.DataFrame(client.get_historical_klines(self.symbol, self.interval, self.lookback + ' hour ago UTC')) #'min ago UTC' 'day' ya da 'hour' olarakta giriliyor
        frame = frame.iloc[:,:6]
        frame.columns = ['Time','Open','High','Low','Close','Volume']
        frame = frame.set_index('Time')
        frame.index = pd.to_datetime(frame.index, unit='ms')
        frame = frame.astype(float)
        return frame
    
    def applytechnicals(self):
        df = self.getminutedata()
        df['%K'] = ta.momentum.stoch(df.High, df.Low,df.Close, window=14, smooth_window=3)
        df['%D'] = df['%K'].rolling(3).mean()
        df['rsi'] = ta.momentum.rsi(df.Close, window=14)
        df['macd'] = ta.trend.macd_diff(df.Close)
        df['sma_s'] = ta.trend.sma_indicator(df.Close, window=50) #MA(50) for short position
        df['sma_l'] = ta.trend.sma_indicator(df.Close, window=200) #MA(200) for long position
        df.dropna(inplace=True)
        return df
        
    def gettrigger(self):
        df = self.applytechnicals()
        dfx = pd.DataFrame()
        for i in range(self.lags + 1):
            mask = (df['%K'].shift(i) < 20) & (df['%D'].shift(i < 20))
            dfx = pd.concat([dfx,mask], axis=1, ignore_index=True)
        return dfx.sum(axis=1)
    def decide(self):
        df = self.applytechnicals()
        df['trigger'] = np.where(self.gettrigger(), 1, 0)
        df['Buy'] = np.where((df.trigger) & (df['%K'].between(20,80)) & (df['%D'].between(20,80)) & (df.rsi > 50) & (df.macd > 0) & (df.sma_s > df.sma_l) & (df.Close > df.sma_s) & (df.Close > df.sma_l) , 1, 0)
        df['Coin'] = self.symbol
        return df[df['Buy'] == 1]

def get_top_symbol_sql():
    engine = sqlalchemy.create_engine('sqlite:///CryptoDB.db')
    all_pairs = pd.DataFrame(client.get_ticker())
    relev = all_pairs[all_pairs.symbol.str.contains('USDT')]
    non_lev = relev[~((relev.symbol.str.contains('UP')) | (relev.symbol.str.contains('DOWN')))]
    top_symbol = non_lev.sort_values(by='priceChangePercent', ascending=False)
    top_symbol.to_sql('crypto',engine, if_exists='replace',index=False)
    get_top_symbol_sql = pd.read_sql('crypto',engine)
    return get_top_symbol_sql


if __name__ == "__main__": 

    api_key = "api_key" # you can write your own binance api_key
    secret_key = "secret_key" # you can write your own binance secret_key

    client = Client(api_key = api_key, api_secret = secret_key)   
    
    symbols = get_top_symbol_sql().symbol #Binance all symbols
    interval = '1m'
    lookback = '25'
    lags = 25
    
    for symbol in symbols:
        buy_signal = Signals(symbol = symbol, interval = interval, lookback = lookback, lags = lags)
        print(buy_signal.decide())



