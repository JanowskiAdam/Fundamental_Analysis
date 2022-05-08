from FS_SQL import *
import yahoo_fin.stock_info as si
import numpy as np
import pandas as pd
import datetime
from sqlalchemy import create_engine
import sqlite3
import datetime
import csv

pd.options.mode.chained_assignment = None
pd.set_option('display.max_columns', None)

def getColumns(data):
    list1 = list(data.columns)
    return (list1)

class Ticker(object):
    def __init__(self, ticker):
        self.ticker = ticker
        self.FS = self.getFinancialStatement()
        self.Positions = self.getPositionsList()

    def getFinancialStatement(self):
        df = FS_SQL(self.ticker).load_table()
        for x in list(df.columns):
            df[x] = df[x].mask(df[x]==0,np.NaN)
            df[x] = df[x].interpolate()
        return df

    def getPositionsList(self):
        return list(self.FS.columns)

    def dependentVar(self, binary=True):
        # Binary = 1 If asset beats the market 0 if not
        # NonBinary = diff (log asset return) - (log index return)

        ################
        #TEMORARY LINES
        self.FS = self.FS.drop(columns=['adjClose','futureAdjClose','indexAdjClose','futureIndexAdjClose'])
        ################

        def addTickerValues():
            data = YahooFinancials(self.ticker).get_historical_price_data('2017-09-29', datetime.datetime.today(), "daily")
            prices = data[self.ticker]['prices']
            price_df = pd.DataFrame.from_dict(prices)
            price_df = price_df.rename(columns={'formatted_date': 'Date'})
            price_df = price_df.set_index('Date')
            price_df = price_df['adjclose']
            #TODO
            #idx = pd.date_range('2017-09-29', datetime.datetime.today())
            #price_df = price_df.reindex(idx).fillna(method='ffill')
            self.FS = pd.merge(self.FS, price_df, left_index=True, right_index=True, how='left')
            self.FS = self.FS.rename(columns={'adjclose': 'adjClose'})
            self.FS['futureAdjClose'] = self.FS['adjClose'].shift(-1)
        def addIndexValues():
            if self.ticker in get_SP500_tickers_list() or self.ticker == 'DISCA':
                data = YahooFinancials('^GSPC').get_historical_price_data('2017-09-29', datetime.datetime.today(),"daily")
                prices = data['^GSPC']['prices']
                price_df = pd.DataFrame.from_dict(prices)
                price_df = price_df.rename(columns={'formatted_date': 'Date'})
                price_df = price_df.set_index('Date')
                price_df = price_df['adjclose']
                #TODO
                #idx = pd.date_range('2017-09-29', datetime.datetime.today())
                #price_df = price_df.reindex(idx).fillna(method='ffill')
                self.FS = pd.merge(self.FS, price_df, left_index=True, right_index=True, how='left')
                self.FS = self.FS.rename(columns={'adjclose': 'indexAdjClose'})
                self.FS['futureIndexAdjClose'] = self.FS['indexAdjClose'].shift(-1)
            else:
                print('Unknown Index')

        def LastFuturePrice():
            def lastDay():
                lastBusDay = datetime.datetime.today()
                if datetime.date.weekday(lastBusDay) == 5:  # if it's Saturday
                    lastBusDay = lastBusDay - datetime.timedelta(days=1)  # then make it Friday
                elif datetime.date.weekday(lastBusDay) == 6:  # if it's Sunday
                    lastBusDay = lastBusDay - datetime.timedelta(days=2)
                elif datetime.date.weekday(lastBusDay) == 0:  # if it's Monday
                    lastBusDay = lastBusDay - datetime.timedelta(days=3)  # then make it Friday
                return lastBusDay
            today = lastDay().date().strftime("%Y-%m-%d")
            yesterday = (lastDay()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            #TODO
            actualPrice = si.get_data(ticker=self.ticker, start_date = yesterday, end_date = today, index_as_date = True, interval = '1d')['adjclose'].item()
            actualIndexPrice = si.get_data(ticker='^GSPC', start_date = yesterday, end_date = today, index_as_date = True, interval = '1d')['adjclose'].item()
            self.FS['futureAdjClose'] = self.FS['futureAdjClose'].fillna(actualPrice)
            self.FS['futureIndexAdjClose'] = self.FS['futureIndexAdjClose'].fillna(actualIndexPrice)
        def binaryBeatMarket(price,futurePrice,index,futureIndex):
            if (futurePrice / price) - (futureIndex / index)> 0:
                return 1
            else:
                return 0
        def continousBeatMarket(price,futurePrice,index,futureIndex):
             return np.log(futurePrice / price) - np.log(futureIndex / index)
        addTickerValues()
        addIndexValues()
        LastFuturePrice()
        if binary == True:
            self.FS['Y'] = self.FS.apply(lambda x: binaryBeatMarket(x['adjClose'],
                                                                    x['futureAdjClose'],
                                                                    x['indexAdjClose'],
                                                                    x['futureIndexAdjClose']),
                                         axis=1)
        else:
            self.FS['Y'] = self.FS.apply(lambda x: continousBeatMarket(x['adjClose'],
                                                                    x['futureAdjClose'],
                                                                    x['indexAdjClose'],
                                                                    x['futureIndexAdjClose']),
                                         axis=1)
        # Cleaning ticker/index values
        self.FS = self.FS.drop(columns=['adjClose','futureAdjClose','indexAdjClose','futureIndexAdjClose'])
    def CurrentRatio(self):
        self.FS['CurrentRatio'] = self.FS['totalCurrentAssets'] / self.FS['totalCurrentLiabilities']
        self.FS['CurrentRatio_Change'] = (self.FS['CurrentRatio'] - self.FS['CurrentRatio'].shift(1)) / self.FS['CurrentRatio'].shift(1)
    def EBITMargin(self):
        self.FS['EBITMargin'] = self.FS['ebit'] / self.FS['totalRevenue']
        self.FS['EBITMargin_Change'] = (self.FS['EBITMargin'] - self.FS['EBITMargin'].shift(1)) / self.FS['EBITMargin'].shift(1)
    def GrossMargin(self):
        self.FS['GrossMargin'] = self.FS['grossProfit'] / self.FS['totalRevenue']
        self.FS['GrossMargin_Change'] = (self.FS['GrossMargin'] - self.FS['GrossMargin'].shift(1)) / self.FS['GrossMargin'].shift(1)
    def OperatingMargin(self):
        self.FS['OperatingMargin'] = self.FS['operatingIncome'] / self.FS['totalRevenue']
        self.FS['OperatingMargin_Change'] = (self.FS['OperatingMargin'] - self.FS['OperatingMargin'].shift(1)) / self.FS['OperatingMargin'].shift(1)
    def NetMargin(self):
        self.FS['NetMargin'] = self.FS['netIncome'] / self.FS['totalRevenue']
        self.FS['NetMargin_Change'] = (self.FS['NetMargin'] - self.FS['NetMargin'].shift(1)) / self.FS['NetMargin'].shift(1)
    def AssetTurnover(self):
        self.FS['AssetTurnover'] = self.FS['totalRevenue'] / self.FS['totalAssets']
        self.FS['AssetTurnover_Change'] = (self.FS['AssetTurnover'] - self.FS['AssetTurnover'].shift(1)) / self.FS['AssetTurnover'].shift(1)
    def DSO(self):
        #Day Sales Outstanding
        self.FS['DSO'] = (self.FS['netReceivables'] * 365) / self.FS['totalRevenue']
        self.FS['DSO_Change'] = (self.FS['DSO'] - self.FS['DSO'].shift(1)) / self.FS['DSO'].shift(1)
    def DE(self):
        #Debt to Equity
        self.FS['DE'] = self.FS['totalLiab'] / self.FS['totalStockholderEquity']
        self.FS['DE_Change'] = (self.FS['DE'] - self.FS['DE'].shift(1)) / self.FS['DE'].shift(1)
    def ROA(self):
        #Return on Assets
        self.FS['ROA'] = self.FS['netIncome'] / self.FS['totalAssets']
        self.FS['ROA_Change'] = (self.FS['ROA'] - self.FS['ROA'].shift(1)) / self.FS['ROA'].shift(1)
    def ROE(self):
        #Return on Equity
        if 'totalEquity' in self.Positions:
            self.FS['ROE'] = self.FS['netIncome'] / self.FS['totalEquity']
        elif 'totalAssets' and 'totalLiab' in self.Positions:
            self.FS['ROE'] = self.FS['netIncome'] / (self.FS['totalAssets'] - self.FS['totalLiab'])
        self.FS['ROE_Change'] = (self.FS['ROE'] - self.FS['ROE'].shift(1)) / self.FS['ROE'].shift(1)
    def ROI(self):
        #Return on Investment
        self.FS['ROI'] = self.FS['netIncome'] / abs(self.FS['capitalExpenditures'])
        self.FS['ROI_Change'] = (self.FS['ROI'] - self.FS['ROI'].shift(1)) / self.FS['ROI'].shift(1)
    def EPS(self):
        #Earnings per Share
        if 'dividendsPaid' in self.Positions:
            self.FS['EPS'] = (self.FS['netIncome']-self.FS['dividendsPaid']) / self.FS['shares']
        else:
            self.FS['EPS'] = (self.FS['netIncome']) / self.FS['shares']
        self.FS['EPS_Change'] = (self.FS['EPS'] - self.FS['EPS'].shift(1)) / self.FS['EPS'].shift(1)
    def PE(self):
        #Price to Earnings
        price_df = si.get_data(self.ticker, '2017-09-29', datetime.datetime.today(), index_as_date=True, interval='1d')['adjclose']
        idx = pd.date_range('2017-09-29', datetime.datetime.today())
        price_df = price_df.reindex(idx).fillna(method='ffill')
        self.FS = pd.merge(self.FS, price_df, left_index=True, right_index=True, how='left')
        self.FS['PE'] = self.FS['adjclose'] / self.FS['EPS']
        self.FS['PE_Change'] = (self.FS['PE'] - self.FS['PE'].shift(1)) / self.FS['PE'].shift(1)
        self.FS = self.FS.drop(columns=['adjclose'])
    def FCF(self):
        #Future Cash Flow
        self.FS['FCF'] = self.FS['netIncome'] - self.FS['capitalExpenditures']
        self.FS['FCF_Change'] = (self.FS['FCF'] - self.FS['FCF'].shift(1)) / self.FS['FCF'].shift(1)
    def BVPS(self):
        #Book Value per Share
        self.FS['BVPS'] = self.FS['totalStockholderEquity'] / self.FS['shares']
        self.FS['BVPS_Change'] = (self.FS['BVPS'] - self.FS['BVPS'].shift(1)) / self.FS['BVPS'].shift(1)
    def FinancialRatios(self):
        if 'capitalExpenditures' not in self.Positions:
            if 'propertyPlantEquipment' not in self.Positions:
                self.FS['propertyPlantEquipment'] = self.FS['netTangibleAssets']-self.FS['cash']
            self.FS['PP&E'] = self.FS['propertyPlantEquipment'] - self.FS['propertyPlantEquipment'].shift(1)
            self.FS['capitalExpenditures'] = self.FS['depreciation'] + self.FS['PP&E']

        self.CurrentRatio()
        self.EBITMargin()
        self.GrossMargin()
        self.OperatingMargin()
        self.NetMargin()
        self.AssetTurnover()
        self.DSO()
        self.DE()
        self.ROA()
        self.ROE()
        self.ROI()
        self.EPS()
        self.PE()
        self.FCF()
        self.BVPS()

if __name__ == '__main__':
    #example1 = FS_Concated().append_tables()
    #print(get_tickers_list_in_db())
    df = pd.DataFrame()
    main_loop = 1
    runtime_loop = 1
    ticker_loop = 1
    error_counter = 0
    error_list = []
    tickers_list = get_tickers_list_in_db()
    list_length = len(tickers_list)
    remaining_tickers_list = tickers_list.copy()
    while tickers_list and main_loop < 6:
        print(f'loop {main_loop}')
        for tkr in tickers_list:
            try:
                with timeout(200):
                    asset = Ticker(tkr)
                    asset.dependentVar(binary=False)
                    asset.FinancialRatios()
                    df = df.append(asset.FS.reset_index())
                    print(datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S"))
                    print(f'{"_" * 50} Loading of {str(tkr)} is done. {"_" * 50}')
                    print(f'{"_" * 50} {ticker_loop} of {list_length} has been updated. {"_" * 50}')
                    remaining_tickers_list.remove(tkr)
                    ticker_loop += 1
            except RuntimeError:
                runtime_loop += 1
                error_counter += 1
                print(f'{"#" * 41} Runtime error, proceeding to next loop: {runtime_loop}. {"#" * 41}')
                break
            except:
                error_counter += 1
                print(f'{"#" * 41} Error occure on {tkr}. {len(remaining_tickers_list)} tickers have left. {"#" * 30}')
                error_list.append(tkr)
        main_loop += 1
        tickers_list = remaining_tickers_list.copy()
    if error_list:
        print(f'Errors occurred on : {set(error_list)}')
    else:
        print('No errors occurred')

    df.to_csv('Financial_Statements_dataset.csv', index=False)

    print('Done!!!')

