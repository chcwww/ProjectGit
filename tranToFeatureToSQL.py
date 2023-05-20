import sys
sys.path.append(r'C:\\vs_code_python\\Project')
from datetime import datetime
from function import *
# from function import *
# import dask.dataframe as dd
import pandas as pd
import pymssql
import time
import gc

dirThis = 'C:\\Users\\chcww\\Downloads\\'

offers = pd.read_csv(dirThis + 'offers.csv')
# transactions = pd.read_csv(dirThis + 'newdata.csv', dtype = tranDtype, header = 0, nrows = 100000)
trainHistory = pd.read_csv(dirThis + 'trainHistory.csv')
testHistory = pd.read_csv(dirThis + 'testHistory.csv')


#===============
data = offers
#===============
tranDtype = typeGenerator(data)
tableCols, tableType, colNames = getSQLtype(tranDtype)
tableName = 'trainFULL'
creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')
insertSQLtable(data, tableName, colNames, tableType, '127.0.0.1', 'sa', '123123', 'Project')





import sys
sys.path.append(r'C:\\vs_code_python\\Project')
from datetime import datetime
from function import *
# from function import *
# import dask.dataframe as dd
import pandas as pd
import pymssql
import time
import gc
dirThis = 'C:\\Users\\chcww\\Downloads\\'
offers = pd.read_csv(dirThis + 'offers.csv')
trainHistory = pd.read_csv(dirThis + 'trainHistory.csv')
testHistory = pd.read_csv(dirThis + 'testHistory.csv')
transactions = dirThis + 'transactions.csv'
file1 = dirThis + 'outputSQL/'

big_size = 20000000
k = 0
start = datetime.now()

tranCols = list(getTransactionsType().keys())
df = pd.read_csv(transactions, dtype = getTransactionsType(), 
                 header = None, skiprows = 1, nrows = 100000,
                #  parse_dates = ['date'], infer_datetime_format = True,
                 engine = 'c')
df.columns = tranCols
use = generateFeature(offers, df, trainHistory, testHistory)

tranDtype = typeGenerator(use)
tableCols, tableType, colNames = getSQLtype(tranDtype)
tableName = 'trainFULL'
creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')

with pd.read_csv(transactions, chunksize = big_size, dtype = getTransactionsType(), 
                 header = None, skiprows = 1,
                #  parse_dates = ['date'], infer_datetime_format = True,
                 engine = 'c') as reader:
  for df in reader:
    k += 1
    df.columns = tranCols
    use = generateFeature(offers, df, trainHistory, testHistory, k)
    path = dirThis + 'outputSQL\\sqlout' + str(k) + '.csv'
    use.to_csv(path)
    insertSQLtable(use, tableName, colNames, tableType, '127.0.0.1', 'sa', '123123', 'Project')
    del use
    gc.collect()
    print('Export : ', k, len(df), datetime.now() - start)
















import sys
sys.path.append(r'C:\\vs_code_python\\Project')
from datetime import datetime
from function import *
# from function import *
# import dask.dataframe as dd
import pandas as pd
import pymssql
import time
import gc
dirThis = 'C://Users//chcww//Downloads//'
use = pd.read_csv(dirThis + 'use.csv')
offers = pd.read_csv(dirThis + 'offers.csv')
trainHistory = pd.read_csv(dirThis + 'trainHistory.csv')
testHistory = pd.read_csv(dirThis + 'testHistory.csv')
transactions = dirThis + 'transactions.csv'
file1 = dirThis + 'outputSQL//'

big_size = 20000000
k = 0
start = datetime.now()
tranCols = list(getTransactionsType().keys())

with pd.read_csv(transactions, chunksize = big_size, dtype = getTransactionsType(), 
                 header = None, skiprows = 1,
                #  parse_dates = ['date'], infer_datetime_format = True,
                 engine = 'c') as reader:
  for df in reader:
    k += 1
    df.columns = tranCols
    use = generateFeature(offers, df, trainHistory, testHistory, k)
    path = file1 + f'sqlout{str(k)}.csv'
    use.to_csv(path, index = False)
    if(k == 1) :
        tranDtype = typeGenerator(use)
        tableCols, tableType, colNames = getSQLtype(tranDtype)
        tableName = 'trainFULL'
        creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')

    bulkSQLtable(path, tableName, '127.0.0.1', 'Project')
    
    del use
    gc.collect()
    print('Export : ', k, len(df), datetime.now() - start)






conn = pymssql.connect(server = '192.168.0.223', user = 'sa', password = '123123', database = 'Project', port = 1433)

tranDtype = typeGenerator(use)
tableCols, tableType, colNames = getSQLtype(tranDtype)
tableName = 'myUse'
creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')

bulkSQLtable(dirThis + 'use.csv', tableName, '127.0.0.1', 'Project')




tranCols = list(getTransactionsType().keys())
df = pd.read_csv(transactions, dtype = getTransactionsType(), 
                 header = None, skiprows = 1, nrows = 5000000,
                #  parse_dates = ['date'], infer_datetime_format = True,
                 engine = 'c')
df.columns = tranCols


def a(path) :
   print(rf'{path}')
   k = 'asdf' + path + 'asdf'
   return k

c = a(rf'{path}')
b = a(path)

pathlist = path.split('\\')
bulkSQLtable1(path, tableName, '127.0.0.1', 'Project')

path = 'C:\\Users\\chcww\\Downloads\\outhist.csv'
trainHistory = pd.read_csv(dirThis + 'trainHistory.csv')
trainHistory.to_csv('C:\\Users\\chcww\\Downloads\\outhist.csv')

tranDtype = typeGenerator(trainHistory)
tableCols, tableType, colNames = getSQLtype(tranDtype)
tableName = 'trainHistory'
creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')

t1 = time.time()
SQLCon = pymssql.connect(server='127.0.0.1', 
                    #    user='sa', password='123123', 
                    database='Project', port = 1433)
Cursor = SQLCon.cursor()

BulkInsert = """BULK INSERT [dbo].[{0}]
FROM '{1}'
WITH
(
        FIRSTROW=2
      , FIELDTERMINATOR=','
      , ROWTERMINATOR='\n'
)""".format(tableName, 'C:\\Users\\chcww\\Downloads\\outhist.csv')

Cursor.execute(BulkInsert)
SQLCon.commit()
t2 = time.time()
print('time elapsed: ' + str(round(t2-t1, 2)) + ' seconds')

conn = pymssql.connect(server='127.0.0.1', 
                    #    user='sa', password='123123', 
                    database='Project', port = 1433)
cursor_ = conn.cursor()
cursor_.execute('SELECT * FROM trainHistory')
data1 = cursor_.fetchall() 
data1[0]
data = pd.DataFrame(data1)
data.shape



def bulkSQLtable1(path, tableName, server, user, password, database) :
        t1 = time.time()
        to = 1
        
        conn = pymssql.connect(server = server, database = database, port = 1433)  
        
        t2 = time.time()
        print('Stage -', str(to) + ' (connect success) -> time elapsed: ' + str(round(t2-t1, 2)) + ' seconds')
        to += 1

        BulkInsert = """BULK INSERT [dbo].[{0}]
        FROM '{1}'
        WITH
        (
                FIRSTROW=2
            , FIELDTERMINATOR=','
            , ROWTERMINATOR='\n'
        )""".format(tableName, path)

        cursor = conn.cursor()
        cursor.execute(BulkInsert)
        conn.commit()

        t2 = time.time()
        print('Stage -', str(to) + ' (executed) -> time elapsed: ' + str(round(t2-t1, 2)) + ' seconds')
        to += 1

tranCols = list(getTransactionsType().keys())
df = pd.read_csv(transactions, dtype = getTransactionsType(), 
                 header = None, skiprows = 1, nrows = 20000000,
                #  parse_dates = ['date'], infer_datetime_format = True,
                 engine = 'c')
df.columns = tranCols
use = generateFeature(offers, df, trainHistory, testHistory)

use.to_csv('C:\\Users\\chcww\\Downloads\\outhist.csv', index=False)

conn = pymssql.connect(server='127.0.0.1', 
                    #    user='sa', password='123123', 
                    database='Project', port = 1433)
bulkSQLtable('C:\\Users\\chcww\\Downloads\\outhist.csv', tableName, '127.0.0.1', 'sa', '123123', 'Project')
# checkUse = use.copy()

tranDtype = typeGenerator(use)
tableCols, tableType, colNames = getSQLtype(tranDtype)
tableName = 'trainHistory'
creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')

t1 = time.time()
SQLCon = pymssql.connect(server='127.0.0.1', 
                    #    user='sa', password='123123', 
                    database='Project', port = 1433)
Cursor = SQLCon.cursor()

BulkInsert = """BULK INSERT [dbo].[{0}]
FROM '{1}'
WITH
(
        FIRSTROW=2
      , FIELDTERMINATOR=','
      , ROWTERMINATOR='\n'
)""".format(tableName, 'C:\\Users\\chcww\\Downloads\\outhist.csv')

Cursor.execute(BulkInsert)
SQLCon.commit()
t2 = time.time()
print('time elapsed: ' + str(round(t2-t1, 2)) + ' seconds')

conn = pymssql.connect(server='127.0.0.1', 
                    #    user='sa', password='123123', 
                    database='Project', port = 1433)
cursor_ = conn.cursor()
cursor_.execute('SELECT * FROM trainHistory')
data1 = cursor_.fetchall() 
data1[0]
data = pd.DataFrame(data1)
data.shape









big_size = 20000000
k = 0
start = datetime.now()

tranCols = list(getTransactionsType().keys())
df = pd.read_csv(transactions, dtype = getTransactionsType(), 
                 header = None, skiprows = 1, nrows = 100000,
                #  parse_dates = ['date'], infer_datetime_format = True,
                 engine = 'c')
df.columns = tranCols
use = generateFeature(offers, df, trainHistory, testHistory)

tranDtype = typeGenerator(use)
tableCols, tableType, colNames = getSQLtype(tranDtype)
tableName = 'trainFULL'
creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')



df = pd.read_csv(transactions, dtype = getTransactionsType(), 
                 header = None, skiprows = 1, nrows = 1000000,
                #  parse_dates = ['date'], infer_datetime_format = True,
                 engine = 'c')
df.columns = tranCols
use = generateFeature(offers, df, trainHistory, testHistory, 1)

path = dirThis + 'sqlout' + str(k) + '.csv'
use.to_csv(path)
SQLCon = pymssql.connect(server='127.0.0.1', 
                    #    user='sa', password='123123', 
                    database='Project', port = 1433)
Cursor = SQLCon.cursor()

BulkInsert = """BULK INSERT [dbo].[{0}]
FROM '{1}'
WITH
(
        FIRSTROW=2
      , FIELDTERMINATOR=','
      , ROWTERMINATOR='\n'
)""".format(tableName, path)

Cursor.execute(BulkInsert)
SQLCon.commit()











bulkSQLtable(path, tableName, '127.0.0.1', 'sa', '123123', 'Project')





import pymssql
conn = pymssql.connect(server = 'database.windows.net', 
                       user = 'chc', password = '6F489bxk', 
                       database = 'chc', port = 1433)


