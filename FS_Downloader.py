from yahoofinancials import YahooFinancials
import yahoo_fin.stock_info as si
import datetime
import pandas as pd

def get_SP500_tickers_list():
    payload = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = payload[0]
    tickers = df['Symbol'].values.tolist()
    tickers = [w.replace('.', '-') for w in tickers]
    return tickers

def get_SP500_sectors_list():
    payload=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = payload[0]
    sectors = set(df['GICS Sector'].values.tolist())
    return(sectors)

def get_SP500_tickers_sectors_df():
    payload=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = payload[0]
    return df[['Symbol','GICS Sector']]

class FS_Downloader(object):
    ''' FA - Financial Analysis of chosen company.
        Attributes
        ==========
        ticker: str
            TR RIC (financial instrument) to be used
        Methods
        =======
        getIncomeStatement:
            Downloading Income Statement from Yahoo Finance into DataFrame
        getCashFlow:
            Downloading Cash Flow from Yahoo Finance into DataFrame
        getBalanceSheet:
            Downloading Income Statement from Yahoo Finance into DataFrame
        getFullfinancialStatement:
            Concate downloaded Financial Statement and add prices.
    '''
    def __init__(self, ticker):
        self.ticker = ticker

    def getBalanceSheet(self):
        #transformation of JSON file to DataFrame
        def dropnested(alist):
            outputdict = {}
            dates = []
            for dic in alist:
                for key, value in dic.items():
                    dates.append(key.replace('-', ''))
                    if isinstance(value, dict):
                        for k2, v2, in value.items():
                            outputdict[k2] = outputdict.get(k2, []) + [v2]
                    else:
                        outputdict[key] = outputdict.get(key, []) + [value]

            return outputdict, dates

        #remove missing rows
        def removerows(alist):
            missed = []
            for key, value in alist.items():
                if len([item for item in value if item]) != 4 :
                    missed.append(key)
            for item in missed:
                outputdict.pop(item)
            return outputdict

        #Annual
        yahoo_financials = YahooFinancials(self.ticker)
        balanceSheet = yahoo_financials.get_financial_stmts('annual', 'balance', reformat=True)
        data = balanceSheet['balanceSheetHistory']

        df1 = pd.DataFrame()
        t_data = data[self.ticker]
        outputdict, dates = dropnested(t_data)

        removerows(outputdict)

        _df = pd.DataFrame.from_dict(outputdict).apply(pd.to_numeric)
        _df.insert(0, 'Ticker', self.ticker)
        end_date = pd.Series(dates, name='Date')
        norm_df = pd.concat([end_date, _df], axis=1)
        df1 = df1.append(norm_df)
        df1['Date'] = pd.to_datetime(df1['Date']).dt.date
        df1 = df1.set_index('Date')
        df1['Type'] = "Annual"

        # Quarterly
        yahoo_financials = YahooFinancials(self.ticker)
        balanceSheet = yahoo_financials.get_financial_stmts('quarterly', 'balance', reformat=True)
        data = balanceSheet['balanceSheetHistoryQuarterly']

        df2 = pd.DataFrame()
        t_data = data[self.ticker]
        outputdict, dates = dropnested(t_data)

        removerows(outputdict)

        _df = pd.DataFrame.from_dict(outputdict).apply(pd.to_numeric)
        _df.insert(0, 'Ticker', self.ticker)
        end_date = pd.Series(dates, name='Date')
        norm_df = pd.concat([end_date, _df], axis=1)
        df2 = df2.append(norm_df)
        df2['Date'] = pd.to_datetime(df2['Date']).dt.date
        df2 = df2.set_index('Date')
        df2['Type'] = "TTM"

        #Concat
        final_df = pd.concat([df1, df2], axis=0)
        final_df.sort_index(ascending=True, inplace=True)

        duplicates = final_df.index.duplicated()
        keep = duplicates == False
        final_df = final_df.loc[keep, :]

        return final_df

    def getCashFlow(self):
        #transformation of JSON file to DataFrame
        def dropnested(alist):
            outputdict = {}
            dates = []
            for dic in alist:
                for key, value in dic.items():
                    dates.append(key.replace('-', ''))
                    if isinstance(value, dict):
                        for k2, v2, in value.items():
                            outputdict[k2] = outputdict.get(k2, []) + [v2]
                    else:
                        outputdict[key] = outputdict.get(key, []) + [value]
            return outputdict, dates

        #remove missing rows
        def removerows(alist):
            missed = []
            for key, value in alist.items():
                if len([item for item in value if item]) != 4 :
                    missed.append(key)
            for item in missed:
                outputdict.pop(item)
            return outputdict

        #Annual
        yahoo_financials = YahooFinancials(self.ticker)
        cashFlow = yahoo_financials.get_financial_stmts('annual', 'cash', reformat=True)
        data = cashFlow['cashflowStatementHistory']

        df1 = pd.DataFrame()
        t_data = data[self.ticker]
        outputdict, dates = dropnested(t_data)

        removerows(outputdict)

        _df = pd.DataFrame.from_dict(outputdict).apply(pd.to_numeric)
        _df.insert(0, 'Ticker', self.ticker)
        end_date = pd.Series(dates, name='Date')
        norm_df = pd.concat([end_date, _df], axis=1)
        df1 = df1.append(norm_df)
        df1['Date'] = pd.to_datetime(df1['Date'], format='%Y%m%d')
        df1 = df1.set_index('Date')
        df1.insert(1, 'Type', 'Annual')

        #Quarterly
        yahoo_financials = YahooFinancials(self.ticker)
        cashFlow = yahoo_financials.get_financial_stmts('quarterly', 'cash', reformat=True)
        data = cashFlow['cashflowStatementHistoryQuarterly']

        df2 = pd.DataFrame()
        t_data = data[self.ticker]
        outputdict, dates = dropnested(t_data)

        removerows(outputdict)

        _df = pd.DataFrame.from_dict(outputdict).apply(pd.to_numeric)
        _df.insert(0, 'Ticker', self.ticker)
        end_date = pd.Series(dates, name='Date')
        norm_df = pd.concat([end_date, _df], axis=1)
        df2 = df2.append(norm_df)
        df2['Date'] = pd.to_datetime(df2['Date'], format='%Y%m%d')
        df2 = df2.set_index('Date')

        df2.sort_index(ascending=True, inplace=True)
        df2 = df2.loc[:, df2.columns != 'Ticker'].cumsum()
        df2 = df2[-1:]
        df2.insert(1, 'Type', 'TTM')


        #Concat
        final_df = pd.concat([df1, df2], axis=0)
        final_df.sort_index(ascending=True, inplace=True)
        final_df['Ticker'] = final_df['Ticker'].fillna(method='ffill')
        duplicates = final_df.index.duplicated()
        keep = duplicates == False
        final_df = final_df.loc[keep, :]

        return final_df

    def getIncomeStatement(self):
        #transformation of JSON file to DataFrame
        def dropnested(alist):
            outputdict = {}
            dates = []
            for dic in alist:
                for key, value in dic.items():
                    dates.append(key.replace('-', ''))
                    if isinstance(value, dict):
                        for k2, v2, in value.items():
                            outputdict[k2] = outputdict.get(k2, []) + [v2]
                    else:
                        outputdict[key] = outputdict.get(key, []) + [value]

            return outputdict, dates
        #remove missing rows
        def removerows(alist):
            missed = []
            for key, value in alist.items():
                if len([item for item in value if item]) != 4 :
                    missed.append(key)
            for item in missed:
                outputdict.pop(item)
                #print(missed)
            return outputdict

        #Annual
        yahoo_financials = YahooFinancials(self.ticker)
        income = yahoo_financials.get_financial_stmts('annual', 'income', reformat=True)
        data = income['incomeStatementHistory']

        df1 = pd.DataFrame()
        t_data = data[self.ticker]
        outputdict, dates = dropnested(t_data)

        removerows(outputdict)

        _df = pd.DataFrame.from_dict(outputdict).apply(pd.to_numeric)
        _df.insert(0, 'Ticker', self.ticker)
        end_date = pd.Series(dates, name='Date')
        norm_df = pd.concat([end_date, _df], axis=1)
        df1 = df1.append(norm_df)
        df1['Date'] = pd.to_datetime(df1['Date'], format='%Y%m%d')
        df1 = df1.set_index('Date')
        df1.insert(1, 'Type', 'Annual')

        # Quarterly
        yahoo_financials = YahooFinancials(self.ticker)
        income = yahoo_financials.get_financial_stmts('quarterly', 'income', reformat=True)
        data = income['incomeStatementHistoryQuarterly']

        df2 = pd.DataFrame()
        t_data = data[self.ticker]
        outputdict, dates = dropnested(t_data)

        removerows(outputdict)

        _df = pd.DataFrame.from_dict(outputdict).apply(pd.to_numeric)
        _df.insert(0, 'Ticker', self.ticker)
        end_date = pd.Series(dates, name='Date')
        norm_df = pd.concat([end_date, _df], axis=1)
        df2 = df2.append(norm_df)
        df2['Date'] = pd.to_datetime(df2['Date'], format='%Y%m%d')
        df2 = df2.set_index('Date')

        df2.sort_index(ascending=True, inplace=True)
        df2 = df2.loc[:, df2.columns != 'Ticker'].cumsum()
        df2 = df2[-1:]
        df2.insert(1, 'Type', 'TTM')

        # Concat
        final_df = pd.concat([df1, df2], axis=0)
        final_df.sort_index(ascending=True, inplace=True)
        final_df['Ticker'] = final_df['Ticker'].fillna(method='ffill')
        duplicates = final_df.index.duplicated()
        keep = duplicates == False
        final_df = final_df.loc[keep, :]

        return final_df

    def getFullFinancialStatement(self):
        # get financial statements
        bs = self.getBalanceSheet()
        cf = self.getCashFlow()
        ist = self.getIncomeStatement()
        # get positions of financial statements
        bs_col = list(bs.columns)[1:]
        cf_col = list(cf.columns)[2:]
        ist_col = list(ist.columns)[2:]
        # concat Balance Sheet, Cash Flow, Income Statement
        df = pd.concat([bs, cf, ist], axis=1)
        # drop duplicated Ticker and Type columns
        df = df.loc[:, ~df.columns.duplicated()]
        #df['Type'] = df['Type'].fillna('TTM')
        df = df.fillna(0)

        # Adding number of shares
        yahoo_financials = YahooFinancials(self.ticker)
        shares = yahoo_financials.get_num_shares_outstanding(price_type='average')
        df['shares'] = shares

        '''
        # Add ticker values
        price_df = si.get_data(self.ticker, '2017-09-29', datetime.datetime.today(), index_as_date=True, interval='1d')['adjclose']
        idx = pd.date_range('2017-09-29', datetime.datetime.today())
        price_df = price_df.reindex(idx).fillna(method='ffill')
        final_df = pd.merge(df, price_df, left_index=True, right_index=True, how='left')
        final_df = final_df.rename(columns={'adjclose': 'adjClose'})
        final_df['futureAdjClose'] = final_df['adjClose'].shift(-1)
        
        # Add index values
        if self.ticker in get_SP500_tickers_list():
            price_df = si.get_data('^GSPC', '2017-09-29', datetime.datetime.today(), index_as_date=True, interval='1d')['adjclose']
            idx = pd.date_range('2017-09-29', datetime.datetime.today())
            price_df = price_df.reindex(idx).fillna(method='ffill')
            final_df = pd.merge(final_df, price_df, left_index=True, right_index=True, how='left')
            final_df = final_df.rename(columns={'adjclose': 'indexAdjClose'})
            final_df['futureIndexAdjClose'] = final_df['indexAdjClose'].shift(-1)
        else:
            print('Unknown Index')
        '''

        return final_df, bs_col, cf_col, ist_col

#if __name__ == '__main__':
#    ex1 = FS_Download('AAPL').getFullFinancialStatement()
#    print(ex1)
#    print(ex1.getBalanceSheet())
#    print('class done')
print(FS_Downloader('MMM').getFullFinancialStatement()[0].loc[:,['adjClose']])
#print(FS_Downloader('MMM').getBalanceSheet().loc[:,['adjClose']])
#print(bs)
#print(cf)
#print(ist)