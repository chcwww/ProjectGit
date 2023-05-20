import pymssql
import pandas as pd

dirThis = 'C:\\Users\\chcww\\Downloads\\'

offers = pd.read_csv(dirThis + 'offers.csv', nrows = 1000)

# 資料庫連線資訊

conn = pymssql.connect(

host='(local)',

user='sa',

password='123123',

database='Project',

charset='utf8')

# 分塊處理

big_size = 10000

# 分塊遍歷寫入到 mysql

with pd.read_csv(dirThis + 'offers.csv',chunksize=big_size) as reader:
    for df in reader:

        datas = []

        print('處理：',len(df))

df
# print(df)
import numpy as np
np.array(datas).shape
df.iterrows()
j = df.loc[0]
for i ,j in df.iterrows():

    data = (j['offer'],j['category'],j['quantity'],

    j['company'],j['offervalue'], j['brand'])

    datas.append(data)

    # _values = ','.join(['%s', ] * 6)
    _values = '%d,%d,%d,%d,%d,%d'

    sql = '''
    insert into users(offer,category,quantity

    ,company,offervalue, brand) values(%s)
    '''%_values

    # params = [sname,gender]
    # count=cs1.execute("insert into students(sname,gender) values(%s,%s)",params)

    cursor = conn.cursor()

    cursor.executemany(sql,datas)

    conn.commit()

# 關閉服務

conn.close()

cursor.close()

print('存入成功！')







import pymssql
db = pymssql.connect(host='(local)', user='sa', password='123123', database='Project')
try:
    #创建与数据库的连接
    #参数分别是 指定本机 数据库用户名 数据库密码 数据库名 端口号 autocommit是是否自动提交（非常不建议，万一出问题不好回滚）
    db = pymssql.connect(server='127.0.0.1', user='sa', password='123123', database='Project', port = 1433)
    #创建游标对象cursor
    cursor=db.cursor()
    #使用execute()方法执行sql，如果表存在则删除
    cursor.execute('drop table if EXISTS stusdent')
    #创建表的sql
    sql='''
        create table stusdent(
        sno int,
        sname varchar not null,
        sex varchar ,
        age int,
        score bigint
        )
    '''
    cursor.execute(sql)
    db.commit()

except:
    print('创建表失败')
finally:
    #关闭数据库连接
    cursor.close()
    db.close()


import pymssql
conn = pymssql.connect(server='chc.database.windows.net', 
                       user='chc', password='6F489bxk', 
                       database='chc', port = 1433)


conn = pymssql.connect(server='LAPTOP-BOVL2PU0', user='sa', password='123123', database='Project', port = 1433)