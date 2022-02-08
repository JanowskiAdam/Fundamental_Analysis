from FS_SQL import *
from yahoofinancials import YahooFinancials
import pandas as pd
import numpy as np

from plotly.subplots import make_subplots
import plotly.graph_objects as go
import yahoo_fin.stock_info as si
import datetime
pd.options.mode.chained_assignment = None
import seaborn as sns
import matplotlib.pyplot as plt

class FA(object):
    ''' FA - Financial Analysis of chosen company.
        Attributes
        ==========
        ticker: str
            TR RIC (financial instrument) to be used

        Methods
        =======
        getIncomeStatement:
            Loading Income Statement from Financial_Statements_SQL" to DataFrame

        getCashFlow:
            Downloading Cash Flow from Financial_Statements_SQL" to DataFrame

        getBalanceSheet:
            Downloading Income Statement from Financial_Statements_SQL" DataFrame
    '''
    def __init__(self, ticker):
        self.ticker = ticker
        self.BalanceSheet = self.getBalanceSheet()
        self.CashFlow = self.getCashFlow()
        self.IncomeStatement = self.getIncomeStatement()

        self.OHLC = self.getOHLC()
        self.shares = self.getSharesNum()
        self.BookValue = self.getBookValue()
        self.MarketValue = self.getMarketValue()

        self.BVPS = self.getBVPS()
        #self.getFCF()
        self.getEPS()

    def getIncomeStatement(self):
        return FS_SQL(self.ticker, 'IncomeStatement').load_table()

    def getCashFlow(self):
        return FS_SQL(self.ticker, 'CashFlow').load_table()

    def getBalanceSheet(self):
        return FS_SQL(self.ticker, 'BalanceSheet').load_table()

    def getFA_Analysis(self):
        #Price
        price = pd.DataFrame()
        price['Price'] = self.OHLC['adjclose']
        #BookValuePerShare
        df = pd.DataFrame()
        df['BookValuePerShare'] = self.BookValue / self.shares

        df = pd.concat([price['Price'], df], axis=1)
        df['Price'] = df['Price'].fillna(method='ffill')
        df.dropna(inplace=True)
        '''
        #CAPEX
        df['CAPEX'] = self.getCapex()['Capex']
        '''
        #ROA
        df['ROA'] = self.getROA()['roa']
        #ROE
        df['ROE'] = self.getROE()['roe']
        #EPS
        df['EPS'] = self.getEPS()['epsactual']
        #PE
        df['PE'] = self.getPE()['pe']

        return df

    def getOHLC(self):

        def yearsAgo(year):
            yearsBack = datetime.datetime.today() - datetime.timedelta(365 * year)
            if datetime.date.weekday(yearsBack) == 5:  # if it's Saturday
                lastBusDay = yearsBack - datetime.timedelta(days=1)  # then make it Friday
            elif datetime.date.weekday(yearsBack) == 6:  # if it's Sunday
                lastBusDay = yearsBack - datetime.timedelta(days=2)
            return yearsBack.date().strftime("%Y-%m-%d")

        final_df = si.get_data(self.ticker, '2017-09-29', datetime.datetime.today(), index_as_date = True, interval = '1d')

        return final_df

    def getSharesNum(self):

        yahoo_financials = YahooFinancials(self.ticker)
        shares = yahoo_financials.get_num_shares_outstanding(price_type='current')

        return(shares)

    def getBookValue(self):

        df = pd.DataFrame()
        df['BookValue'] = self.BalanceSheet['totalStockholderEquity']

        return(df)

    def getMarketValue(self):

        final_df = pd.DataFrame()
        final_df['MarketValue'] = self.OHLC['adjclose'] * self.shares

        return final_df

    def getCapex(self):

        final_df = pd.DataFrame()
        final_df['Depreciation'] = self.CashFlow['depreciation']
        final_df['DepreciationChange'] = self.CashFlow['depreciation'] - self.CashFlow['depreciation'].shift(1)
        final_df['PP&E'] = self.BalanceSheet['propertyPlantEquipment'] - self.BalanceSheet['propertyPlantEquipment'].shift(1)
        final_df['Capex'] = final_df['Depreciation'] + final_df['PP&E']

        return final_df

    def getBVPS(self):

        final_df = pd.DataFrame()
        final_df['BVPS'] = self.getBookValue() / self.shares

        return final_df
    '''
    def getFCF(self):

        final_df = self.CashFlow[['totalCashFromOperatingActivities', 'capitalExpenditures']]
        final_df['FCF'] = final_df['totalCashFromOperatingActivities']+final_df['capitalExpenditures']
        final_df['FCFperShare'] = final_df['FCF'] / self.shares

        price = self.OHLC['adjclose']

        final_df = pd.concat([final_df, price], axis=1)
        final_df['adjclose'] = final_df['adjclose'].fillna(method='ffill')
        final_df.dropna(inplace=True)
        final_df['PFCF'] = final_df['adjclose'] / final_df['FCFperShare']

        return final_df
    
    def getFCFE(self):

        final_df = pd.DataFrame()
        final_df['TaxRate'] = self.IncomeStatement['incomeTaxExpense'] / self.IncomeStatement['netIncome']

        #Exception/Error handling
        final_df.loc[final_df['TaxRate'] > 0.4 , 'TaxRate'] = 0.4
        final_df.loc[final_df['TaxRate'] < 0.2, 'TaxRate'] = 0.2

        final_df['FCFE'] = self.getFCF()['FCF'] + \
                           self.IncomeStatement['interestExpense'] * \
                           (1 - final_df['TaxRate']) + \
                           self.CashFlow['netBorrowings']

        price = self.OHLC['adjclose']
        final_df = pd.concat([final_df, price], axis=1)

        final_df['adjclose'] = final_df['adjclose'].fillna(method='ffill')
        final_df.dropna(inplace=True)
        final_df['PFCFE'] = final_df['adjclose'] / \
                            (final_df['FCFE'] / self.shares)

        return final_df
    '''
    def getDYR(self):

        final_df = pd.DataFrame()
        final_df['DividendsPerShare'] = self.CashFlow['dividendsPaid'].abs() / self.shares
        final_df['Price'] = self.OHLC['adjclose']
        final_df.dropna(inplace=True)
        final_df['DYR'] = final_df['DividendsPerShare'] / final_df['Price']

        return final_df

    def getROA(self):

        final_df = pd.DataFrame()
        final_df['netIncome'] = self.CashFlow['netIncome']
        final_df['totalAssets'] = self.BalanceSheet['totalAssets']

        final_df.dropna(inplace=True)
        final_df['roa'] = final_df['netIncome'] / final_df['totalAssets']
        final_df.sort_index(ascending=True, inplace=True)

        return final_df

    def getROE(self):

        final_df = pd.DataFrame()
        final_df['netIncome'] = self.CashFlow['netIncome']
        final_df['totalEquity'] = self.BalanceSheet['totalStockholderEquity']

        final_df.dropna(inplace=True)
        final_df['roe'] = final_df['netIncome'] / final_df['totalEquity']
        final_df.sort_index(ascending=True, inplace=True)

        return final_df

    def getEPS(self):

        earnings_hist = si.get_earnings_history(self.ticker)

        frame = pd.DataFrame.from_dict(earnings_hist)[['epsactual', 'epsestimate', 'startdatetime']]
        frame['startdatetime'] = pd.to_datetime(frame['startdatetime']).dt.date
        frame.set_index('startdatetime', inplace = True)
        frame.sort_index(ascending=True, inplace=True)

        frame['epsdiff'] = frame['epsactual'] - frame['epsestimate']
        frame['RollDiff'] = frame['epsdiff'].rolling(4).mean()
        frame['TTM'] = frame['epsactual'].rolling(4).sum()
        frame['estTTM'] = frame['epsestimate'].rolling(4).sum()

        final_df = frame[-20:-3]

        return(final_df)

    def getPE(self):
        #EPS
        eps = self.getEPS()[['epsactual','TTM']]

        #Price
        price = self.OHLC['adjclose']

        #FinalData
        final_df = pd.concat([eps, price], axis=1)
        final_df.dropna(inplace=True)
        final_df['pe'] = final_df['adjclose'] / final_df['TTM']

        return(final_df)

    def getPEG(self):
        pe = self.getPE()['pe']
        eps = self.getEPS()['estTTM']

        final_df = pd.concat([pe, eps], axis=1)
        final_df['estGrowthTTM'] = ((final_df['estTTM'] / final_df['estTTM'].shift(1)) - 1) * 100
        final_df['peg'] = final_df['pe'] / final_df['estGrowthTTM']

        return(final_df)

    def getPB(self):
        #Book Value
        bvps = self.BVPS

        #Price
        price = self.OHLC['adjclose']

        #FinalData
        final_df = pd.concat([bvps, price], axis=1)

        final_df['adjclose'] = final_df['adjclose'].fillna(method='ffill')
        final_df.dropna(inplace=True)
        final_df['PB'] = final_df['adjclose'] / final_df['BVPS']

        return(final_df)

    def showLiabRatio(self):

        final_df = self.BalanceSheet[['totalLiab', 'totalAssets']]
        final_df['LiabRatio'] = final_df['totalLiab'] / final_df['totalAssets']

        fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04 ,subplot_titles=('Liabilities Ratio', 'Balance Sheet Items'),
                            row_width=[0.5, 0.5])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['LiabRatio'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['totalAssets'],
                                 name='totalAssets', line_shape='spline',
                                 line=dict(color='goldenrod', width=2)),
                      row=2, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['totalLiab'],
                                 name='totalLiab', line_shape='spline',
                                 line=dict(color='magenta', width=2)),
                      row=2, col=1)

        fig.update_layout(
            title=str(self.ticker)+" - Assets/Liabilities")

        fig.show()

    def showROAROE(self):
        final_df = self.getROA()
        final_df[['roe','totalEquity']] = self.getROE()[['roe','totalEquity']]

        # Plot
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04,
                            subplot_titles=('ROA & ROA', 'Revenue', 'Assets & Equity'),
                            row_width=[0.25, 0.25, 0.5])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['roe'],
                                 name='ROE', line_shape='spline',
                                 line=dict(color='magenta', width=2)),
                      row=1, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['roa'],
                                 name='ROA', line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['netIncome'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='goldenrod', width=2)),
                      row=2, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['totalEquity'],
                                 name='Equity', line_shape='spline',
                                 line=dict(color='magenta', width=2, dash='dot')),
                      row=3, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['totalAssets'],
                                 name='Assets', line_shape='spline',
                                 line=dict(color='blue', width=2, dash='dot')),
                      row=3, col=1)

        fig.update_layout(title=str(self.ticker) + " - Return on Assets & Return on Equity")

        fig.show()
    '''
    def showFCF(self):

        final_df = self.getFCF()
        final_df['capitalExpenditures'] = final_df['capitalExpenditures'].abs()

        # Plot
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04,
                            subplot_titles=('P/FCF', 'FCF per Share', 'Operating Cash Flow & Capital Expenditures'),
                            row_width=[0.25, 0.25, 0.5])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['PFCF'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['FCFperShare'],
                                 showlegend=False, line_shape='spline',
                                 line_color='rgb(0,176,246)',
                                 line=dict(width=2)),
                      row=2, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['totalCashFromOperatingActivities'],
                                 name = 'Operating Cash Flow', line_shape='spline',
                                 line=dict(color='goldenrod', width=2)),
                      row=3, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['capitalExpenditures'],
                                 name = 'Capital Expenditure', line_shape='spline',
                                 line=dict(color='magenta', width=2)),
                      row=3, col=1)

        fig.update_layout(title=str(self.ticker) + " - Price to Free Cash Flow")

        fig.show()
    
    def showFCFE(self):
        final_df = self.getFCFE()
        # Plot
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04,
                            subplot_titles=('P/FCFE', 'Price', 'FCFE'),
                            row_width=[0.25, 0.25, 0.5])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['PFCFE'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['adjclose'],
                                 showlegend=False, line_shape='spline',
                                 line_color='goldenrod',
                                 line=dict(width=2)),
                      row=2, col=1)

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['FCFE'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='magenta', width=2)),
                      row=3, col=1)

        fig.update_layout(title=str(self.ticker) + " - Price per Free Cash Flow to Equity")

        fig.show()
    '''
    def showEPS(self):

        final_df = self.getEPS()

        #Plot
        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04, subplot_titles=('EPS-Trailing Twelve Months(TTM)', 'Surplus','EPS-Quarterly'),
                            row_width=[0.25,0.25, 0.5])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['TTM'],
                                 name='Actual_TTM', line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['estTTM'],
                                 name='Estimate_TTM', line_shape='spline',
                                 line=dict(color='blue', width=2, dash='dot')),
                      row=1, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['epsdiff'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='goldenrod', width=2)),
                      row=2, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['RollDiff'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='goldenrod', width=2, dash='dot')),
                      row=2, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['epsactual'],
                                 name='Actual',line_shape='spline',
                                 line=dict(color='magenta', width=2)),row=3, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['epsestimate'],
                                 name='Estimated', line_shape='spline',
                                 line=dict(color='magenta', width=2, dash='dot')),
                      row=3, col=1)

        fig.update_layout(title=str(self.ticker)+" - Earnings per Share")

        fig.show()

    def showPE(self):

        final_df = FA.getPE(self)

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04, subplot_titles=('P/E', 'Price', 'EPS'),
                            row_width=[0.25,0.25, 0.5])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['pe'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['adjclose'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='goldenrod', width=2)),
                      row=2, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['epsactual'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='magenta', width=2)),
                      row=3, col=1)

        fig.update_layout(title=str(self.ticker)+" - Price/Earnings")

        fig.show()

    def showPEG(self):

        final_df = FA.getPEG(self)

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04, subplot_titles=('PEG', 'P/E', 'Earnings Growth'),
                            row_width=[0.25,0.25, 0.5])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['peg'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['pe'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='goldenrod', width=2)),
                      row=2, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['estGrowthTTM'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='magenta', width=2)),
                      row=3, col=1)

        fig.update_layout(title=str(self.ticker)+" - Price to Earnings / Growth")

        fig.show()

    def showPB(self):

        final_df = self.getPB()

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04, subplot_titles=('P/B', 'Price','Book Value per Share'),
                            row_width=[0.25, 0.25, 0.8])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['PB'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)
        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['adjclose'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='goldenrod', width=2)),
                      row=2, col=1)


        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['BVPS'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='magenta', width=2)),
                      row=3, col=1)


        fig.update_layout(title=str(self.ticker)+" - Price/Book Value Per Share")

        fig.show()

    def showDYR(self):
        final_df = self.getDYR()

        fig = make_subplots(rows=1, cols=1, shared_xaxes=True,
                            vertical_spacing=0.04, subplot_titles=('Dividend Yield Ratio'),
                            row_width=[0.8])

        fig.add_trace(go.Scatter(x=final_df.index, y=final_df['DYR'],
                                 showlegend=False, line_shape='spline',
                                 line=dict(color='blue', width=2)),
                      row=1, col=1)

        fig.update_layout(title=str(self.ticker)+" - Dividend Yeald Ratio")

        fig.show()

if __name__ == '__main__':
    ex1 = FA(ticker='AAPL')
    pd.set_option('display.max_columns', None)
    print(ex1.getBalanceSheet())
    #df = ex1.getCapex()
    #df['CFFromInvestingAct'] = ex1.CashFlow['totalCashflowsFromInvestingActivities']
    #df['Correct_CapEx'] = ex1.CashFlow['capitalExpenditures'].abs()
    #print(df)
    #print(ex1.getEPS())

    #print(ex1.getFA_Analysis())
    #print(ex1.CashFlow)
    #print(ex1.IncomeStatement)
    #print(si.tickers_dow())
    #print(ex1.getFA_Analysis())
    '''
    ex1.showLiabRatio()
    ex1.showROAROE()
    ex1.showFCF()
    ex1.showFCFE()
    ex1.showEPS()
    ex1.showPE()
    ex1.showPEG()
    ex1.showPB()
    ex1.showDYR()
    '''