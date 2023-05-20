import pandas as pd
import pymssql
import time
# conn = pymssql.connect(server='chc.database.windows.net', 
#                        user='chc', password='6F489bxk', 
#                        database='chc', port = 1433)



import sys
sys.path.append(r'C:\\vs_code_python\\Project')
from function import *
dirThis = 'C://Users//chcww//Downloads//'
use = pd.read_csv(dirThis + 'use.csv')
tranDtype = typeGenerator(use)
tableCols, tableType, colNames = getSQLtype(tranDtype)
# tableName = 'Project'
tableName = 'trainFULL'


# kk = use.iloc[1:100, 1:10]
# tranDtype = typeGenerator(use)
# tableCols, tableType, colNames = getSQLtype(tranDtype)
# creatSQLtable(tableName, tableCols, '127.0.0.1', 'sa', '123123', 'Project')

creatSQLtable(tableName, tableCols, 'chc.database.windows.net', 'chc', '6F489bxk', 'chc')

sql = 'insert into ' + tableName + colNames + ' values(%s)'%tableType
data = use

tmp = 31000
while(True) :
    try:
        t1 = time.time()
        to = 1

        t2 = time.time()
        print('Stage -', str(to) + ' (connect success) -> time elapsed: ' + str(round(t2-t1, 2)) + ' seconds')
        to += 1

        datas = []
        for i ,j in data.iterrows():
            if(i >= tmp) :
                datas.append(tuple(j.values))
                if((i+1) % 1000 == 0) :
                    conn = pymssql.connect(server='chc.database.windows.net', 
                                user='chc', password='6F489bxk', 
                                database='chc', port = 1433)
                    cursor = conn.cursor()
                    cursor.executemany(sql, datas)
                    conn.commit()
                    print('insert', i, 'complete')
                    t2 = time.time()
                    print('Stage -', str(i) + ' (insert success) -> time elapsed: ' + str(round(t2-t1, 2)) + ' seconds')
                    datas = []
                    tmp = i+1
                    print('######################')
                    print('######################')
                    print('tmp now is :', tmp)
                    print('######################')
                    print('######################')

        t2 = time.time()
        print('Stage -', str(to) + ' (executed) -> time elapsed: ' + str(round(t2-t1, 2)) + ' seconds')
        to += 1

        break

    except:
        print('insert FAIL')
    finally:
        cursor.close()
        conn.close()
        print('insert COMPLETE')










creatSQLtable(tableName, tableCols, 'chc.database.windows.net', 'chc', '6F489bxk', 'chc')

data = use.iloc[1:10000, :]
data.to_csv('aaa.csv', index = False)
sql = "LOAD DATA INFILE 'aaa.csv' INTO TABLE test"
conn = pymssql.connect(server='chc.database.windows.net', 
                    user='chc', password='6F489bxk', 
                    database='chc', port = 1433)
cursor = conn.cursor()
cursor.executemany(sql, data)
conn.commit()








conn = pymssql.connect(server='chc.database.windows.net', 
                        user='chc', password='6F489bxk', 
                        database='chc', port = 1433)
cursor_ = conn.cursor()
cursor_.execute('SELECT * FROM trainFULL')
data1 = cursor_.fetchall() 
data = pd.DataFrame(data1)

data













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

testHistory.shape

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














import requests
from bs4 import BeautifulSoup
from urllib import quote
import urlparse

search_list = []
keyword = quote('"柯文哲"'.encode('utf8'))
res = requests.get("https://www.google.com.tw/search?num=50&q="+keyword+"&oq="+keyword+"&dcr=0&tbm=nws&source=lnt&tbs=qdr:d")

# 關鍵字多加一個雙引號是精準搜尋
# num: 一次最多要request的筆數, 可減少切換頁面的動作
# tbs: 資料時間, hour(qdr:h), day(qdr:d), week(qdr:w), month(qdr:m), year(qdr:w)

if res.status_code == 200:
    content = res.content
    soup = BeautifulSoup(content, "html.parser")

    items = soup.findAll("div", {"class": "g"})
    for item in items:
        # title
        news_title = item.find("h3", {"class": "r"}).find("a").text

        # url
        href = item.find("h3", {"class": "r"}).find("a").get("href")
        parsed = urlparse.urlparse(href)
        news_link = urlparse.parse_qs(parsed.query)['q'][0]

        # content
        news_text = item.find("div", {"class": "st"}).text

        # source
        news_source = item.find("span", {"class": "f"}).text.split('-')
        news_from = news_source[0]
        time_created = str(news_source[1])

        # add item into json object
        search_list.append({
            "news_title": news_title,
            "news_link": news_link,
            "news_text": news_text,
            "news_from": news_from,
            "time_created": time_created
        })















import sys
sys.path.append(r'C:\\vs_code_python\\Project')
from function import *
dirThis = 'C://Users//chcww//Downloads//'
tt = typeGenerator(use)

use = pd.read_csv(dirThis + 'use.csv', dtype = tt)
trainHistory = pd.read_csv(dirThis + 'trainHistory.csv')

from sklearn.preprocessing import LabelEncoder
codl = LabelEncoder()
target = trainHistory[['id', 'repeater']]
target['repeater'] = codl.fit_transform(trainHistory['repeater'])

id = use['id']
train = use[id.isin(trainHistory['id'])].reset_index(drop = True)
test = data[id.isin(testHistory['id'])].reset_index(drop = True)

realtrain = pd.merge(train, target, on = 'id')
realtrain.to_csv(dirThis + 'realusee.csv', index = False)







def typeGenera1tor(data) :
    trans_int = data.select_dtypes(include=['int'])
    converted_int = trans_int.apply(pd.to_numeric,downcast='unsigned')
    trans_float = data.select_dtypes(include=['float'])
    converted_float = trans_float.apply(pd.to_numeric,downcast='float')
    optimized_trans = data.copy()
    optimized_trans[converted_int.columns] = converted_int
    optimized_trans[converted_float.columns] = converted_float
    converted_obj = pd.DataFrame()

    trans_obj = data.select_dtypes(include=['object']).copy()
    for col in trans_obj.columns:
        num_unique_values = len(trans_obj[col].unique())
        num_total_values = len(trans_obj[col])
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:,col] = trans_obj[col].astype('category')
        else:
            converted_obj.loc[:,col] = trans_obj[col]
    optimized_trans[converted_obj.columns] = converted_obj

    dtypes = optimized_trans.dtypes
    dtypes_col = dtypes.index
    dtypes_type = [i.name for i in dtypes.values]
    column_types = dict(zip(dtypes_col, dtypes_type))
    preview = { key:value for key,value in list(column_types.items())[::] }
    return preview

# ================================================================================================================================
# ================================================================================================================================
# ================================================================================================================================

def getTransactionsType() :
    tranDtype = {
    'id': 'uint64',
    'chain': 'uint16',
    'dept': 'uint8',
    'category': 'uint16',
    'company': 'uint64',
    'brand': 'uint32',
    'date' : 'object',
    'productsize': 'float32',
    'productmeasure': 'category',
    'purchasequantity': 'int64',
    'purchaseamount': 'float32'
    }
    return tranDtype















# import useful package
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler, MinMaxScaler, MaxAbsScaler, RobustScaler, PowerTransformer, QuantileTransformer
from category_encoders import *
from crucio import MTDF, SMOTE, ADASYN, ICOTE, MWMOTE, TKRKNN 
from sklearn.model_selection import train_test_split
from datetime import timedelta
import pandas as pd
import numpy as np
import time
import gc




path = dirThis


class dataLoader() :
  def __init__(self, dt, path = '/content/drive/MyDrive/1經濟學/專題/featureNew/', dd = 0, ratio = 0.2, seed = 777):
    dirThis = '/content/drive/MyDrive/1經濟學/專題/'
    if(dd == 1) :
      self.data = use
    else :
      self.data = dt
    
    self.trainHistory = pd.read_csv(dirThis + 'trainHistory.csv')
    self.testHistory = pd.read_csv(dirThis + 'testHistory.csv')

    codl = LabelEncoder()
    smalltarget = pd.DataFrame(codl.fit_transform(trainHistory['repeater']), 
                 columns = ['repeater'])
    self.targetCont = trainHistory['repeattrips']

    id = data['id']
    data.drop(['id', 'offerdate'], axis = 1, inplace = True)
    smalltrain = data[id.isin(trainHistory['id'])].reset_index(drop = True)
    # self.test = self.data[id.isin(self.testHistory['id'])].reset_index(drop = True)
    stdCols = data.columns[4:]

    train, test, target, testTarget = train_test_split(smalltrain, smalltarget, test_size = 0.2, random_state = 777)

    self.encol = ['market', 'offer', 'chain', 'offervalue']


  def Encoder(self, encode) :
    if(encode == 'ordinal') :
      encoder = OrdinalEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)   
    elif(encode == 'onehot') :
      encoder = OneHotEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)
    elif(encode == 'target') :
      encoder = TargetEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)
    elif(encode == 'binary') :
      encoder = BinaryEncoder(cols = self.encol).fit(self.trainSet)
    elif(encode == 'cat') :
      encoder = CatBoostEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)
    elif(encode == 'woe') :
      encoder = WOEEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)
    elif(encode == 'helmert') :
      encoder = HelmertEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)
    elif(encode == 'leave') :
      encoder = LeaveOneOutEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)
    elif(encode == 'hash') :
      encoder = HashingEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)
    else :
      encoder = OrdinalEncoder(cols = self.encol).fit(self.trainSet, self.targetSet)

    print("encoder : " + encode)
    self.trainSet = encoder.transform(self.trainSet)
    self.testSet = encoder.transform(self.testSet)

  def Standardization(self, st) :
    if(st == 'std') :
      scaler = StandardScaler()
    elif(st == 'minmax') :
      scaler = MinMaxScaler()
    elif(st == 'abs') :
      scaler = MaxAbsScaler()
    elif(st == 'robust') :
      scaler = RobustScaler()
    elif(st == 'power') :
      scaler = PowerTransformer()
    elif(st == 'qnormal') :
      scaler = QuantileTransformer(output_distribution = 'normal')
    elif(st == 'quantile') :
      scaler = QuantileTransformer()
    else :
      scaler = StandardScaler()
    
    print("standardization : " + st)
    tmp = trainSet[stdCols]
    trainSet[stdCols] = scaler.fit_transform(tmp)

    tmp = testSet[stdCols]
    testSet[stdCols] = pd.DataFrame(scaler.fit_transform(tmp), columns = stdCols)

  def Oversampling(self, over) :
    if(over == 'smote') :
      method = SMOTE() 
    elif(over == 'adasyn') :
      method = ADASYN()
    elif(over == 'icote') :
      method = ICOTE()
    elif(over == 'mwmote') :
      method = MWMOTE()
    elif(over == 'tkrknn') :
      method = TKRKNN()
    else :
      method = MTDF()
    
    print("oversampling : " + over)
    newTrain = method.balance(self.trainSet.join(self.targetSet), 'repeater').reset_index(drop = True)
    
    self.targetSet = newTrain['repeater']
    self.trainSet = newTrain.drop(['repeater'], axis = 1)

  def getData(self) :
    return self.data
  def getTarget(self):
    return self.target
  def getTargetCont(self) :
    return self.targetCont
  def getTrain(self) :
    return self.train
  def getTest(self) :
    return self.test
  def getSet(self, encode = 'ordinal', st = 'none', over = 'none') :
    trainSet = train.copy().reset_index(drop = True)
    testSet = test.copy().reset_index(drop = True)
    targetSet = target.copy().reset_index(drop = True)
    if(encode != 'none') :
      self.Encoder(encode)
    if(st != 'none') :
      self.Standardization(st)
    if(over != 'none') :
      self.Oversampling(over)

    return self.trainSet, self.testSet, self.targetSet, self.testTarget