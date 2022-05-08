import sqlite3
from FS_Downloader import *
from sqlalchemy import create_engine
import datetime
from interruptingcow import timeout
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def create_db(name):
    try:
        sqliteConnection = sqlite3.connect(name+'.db')
        cursor = sqliteConnection.cursor()
        print("Database "+"'"+name+".db'"+" created and Successfully Connected to SQLite")

        sqlite_select_Query = "select sqlite_version();"
        cursor.execute(sqlite_select_Query)
        record = cursor.fetchall()
        print("SQLite Database Version is: ", record)
        cursor.close()

    except sqlite3.Error as error:
        print("Error while connecting to sqlite", error)
    finally:
        if sqliteConnection:
            sqliteConnection.close()
            print("The SQLite connection is closed")

def get_tickers_list_in_db(name='Full_Financial_Statements.db'):
    # connecting to database
    cursor = sqlite3.connect(name).cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    statements_list = cursor.fetchall()
    cursor.close()
    # creating list of tickers
    tickers_list = []
    for statement in statements_list:
        tickers_list.append(statement[0].split("_", 1)[0])
    tickers_list = sorted(list(set(tickers_list)))
    return tickers_list

def download_statements(tickers_list=get_tickers_list_in_db):
    ''' Function downloads financial statements of given tickers(by default it updates already existing ones)
        from yahoo finance to Financial_Statements_SQL.
        Downloading is done through "save_table" inside "FA_SQL" class
        If loop takes more than 5 min, through exception loop is restarted from the beginning of remaining list
        If error occurs during processing financial statement of a ticker,
    '''
    list_length = len(tickers_list)
    print(f'List of {list_length} tickers to update:')
    print(tickers_list)
    # create databases iterating through the list of companies
    main_loop = 0
    runtime_loop = 1
    ticker_loop = 1
    error_counter=0
    error_list = []
    remaining_tickers_list = tickers_list.copy()

    while tickers_list or main_loop < 3:
        main_loop +=1
        #print(main_loop)
        for ticker in tickers_list:
            try:
                with timeout(200):
                    statements = ['BalanceSheet', 'CashFlow', 'IncomeStatement']
                    for j in statements:
                        FS_SQL(ticker=ticker, tb=j).save_table()
                    print(f'{"_"*50} Updating of {str(ticker)} is done. {"_"*50}')
                    print(f'{"_"*50} {ticker_loop} of {list_length} has been updated. {"_"*50}')
                    remaining_tickers_list.remove(ticker)
                    ticker_loop += 1
            except sqlite3.Error as error:
                print("Error while connecting to sqlite", error)
            except RuntimeError:
                runtime_loop += 1
                error_counter += 1
                print(f'{"#"*41} Runtime error, proceeding to next loop: {runtime_loop}. {"#"*41}')
                tickers_list = remaining_tickers_list.copy() #function copy for testing purpose
                break
            except:
                error_counter += 1
                print(f'{"#"*41} Error occure on {ticker}/{j}. {len(remaining_tickers_list)} tickers have left. {"#"*30}')
                error_list.append(ticker)
            finally:
                pass

    print(f'{"#"*50} All saving is over. {"#"*50}')
    print(f'{"#"*45} On process {error_counter} errors  occured. {"#"*45}')
    if error_list:
        print(f'Error list = {error_list}')

class FS_SQL(object):
    ''' FA - Financial Analysis of chosen company.
    '''
    def __init__(self):
        #self.ticker = ticker
        #self.table = str(ticker)+"_FFS"
        self.db = sqlite3.connect('Full_Financial_Statements.db')
        self.engine = create_engine('sqlite:///Full_Financial_Statements.db', echo=False)

    def getList(self):
        '''
        :return: whole list of statements inside "Full_Financial_Statements" in format "Ticker_FFS"
        '''
        cursor = self.db.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        list = cursor.fetchall()
        cursor.close()
        return list

    def load_table(self,ticker):
        '''
        :return: df=DataFrame with Financial Statement from "Full_Financial_Statement.db"
        '''
        if (str(ticker)+'_FFS',) in self.getList():
            df = pd.read_sql_query("SELECT * FROM "+(str(ticker)+'_FFS'), self.db)
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            df = df.set_index('Date')
        else:
            print('No table '+(str(ticker)+'_FFS')+" in database yet")
            df = pd.DataFrame()
        return df

    def save_table(self, ticker):
        '''
        Saving Financial Statement to "Full_Financial_Statement.db"
        It loads to DataFrame already existing table from "Full_Financial_Statement.db" with function "load_table",
        then downloads actual statement from Yahoo Finance in DataFrame with "FA_Stat" class from "FA_Statment" file,
        next it concat two DataFrames and remove duplicates.
        Lastly it replace already existing table in "Full_Financial_Statement.db"
        '''
        #connecting to database
        sqlite_connection = self.engine.connect()
        #loading already existing table
        df1 = self.load_table()
        #downloading actual table
        df2 = FS_Downloader(ticker=ticker).getFullFinancialStatement()
        #concating two tables
        df = df1.append(df2)
        df.sort_index(ascending=True, inplace=True)
        #removing duplicated rows
        duplicates = df.index.duplicated()
        keep = duplicates == False
        df = df.loc[keep, :]
        #replacing table in database
        df.to_sql((str(ticker)+'_FFS'), sqlite_connection, index_label='Date', if_exists='replace')
        sqlite_connection.close()
        print(datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S")+" "+str(ticker,"_FFS")+" saved")

    def drop_table(self,ticker):
        cursor = self.db.cursor()
        cursor.execute("DROP TABLE "+str(ticker,"_FFS"))
        print("Table dropped... ")

    def save_many_tables(self, tickers_list):
        #connecting to database
        sqlite_connection = self.engine.connect()
        #tickers_list = get_tickers_list_in_db()[0:10]
        # initializing variab les
        final_df = pd.DataFrame()
        list_length = len(tickers_list)
        main_loop = 1
        runtime_loop = 1
        ticker_loop = 1
        error_counter = 0
        error_list = []
        remaining_tickers_list = tickers_list.copy()
        #loop
        while tickers_list and main_loop <4:
            print(f'loop {main_loop}')
            for x in tickers_list:
                try:
                    with timeout(200):
                        df1 = self.load_table(x)
                        df2 = FS_Downloader(x).getFullFinancialStatement()[0]
                        final_df = df1.append(df2)
                        cols_at_start = ['Ticker', 'Type']
                        final_df = final_df[[c for c in final_df if c in cols_at_start]
                                + [c for c in final_df if c not in cols_at_start]]
                        final_df.sort_index(ascending=True, inplace=True)
                        # replacing table in database
                        final_df.to_sql((str(x)+'_FFS'), sqlite_connection, index_label='Date', if_exists='replace')
                        #sqlite_connection.close()

                        # print if succeeded
                        print(datetime.datetime.now().strftime("%d.%b %Y %H:%M:%S"))
                        print(f'{"_" * 50} Loading of {str(x)} is done. {"_" * 50}')
                        print(f'{"_" * 50} {ticker_loop} of {list_length} has been updated. {"_" * 50}')
                        remaining_tickers_list.remove(x)
                        ticker_loop += 1
                except RuntimeError:
                    runtime_loop += 1
                    error_counter += 1
                    print(f'{"#" * 41} Runtime error, proceeding to next loop: {runtime_loop}. {"#" * 41}')
                    #tickers_list = remaining_tickers_list.copy()  # function copy for testing purpose
                    break
                except:
                    error_counter += 1
                    print(f'{"#" * 41} Error occure on {x}. {len(remaining_tickers_list)} tickers have left. {"#" * 30}')
                    error_list.append(x)
            main_loop += 1
            tickers_list = remaining_tickers_list.copy()
        #final print
        if error_list:
            print(f'Errors occurred on : {set(error_list)}')
        else:
            print('No errors occurred')
        #return(final_df)

#if __name__ == '__main__':
#    ex1 = FS_SQL('AAPL', 'BalanceSheet')
#    print(ex1.getList())
#FS_SQL().save_many_tables(get_SP500_tickers_list()[:2])
#FS_SQL().load_concated_table_from_FS_SQL()
#FS_SQL().save_concated_table_from_FS_SQL()

