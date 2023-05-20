import os
import sys
import pymssql

server="172.21.111.222"
user="Nuser"
password="NDb"
database="iNek_TestWithPython"

def connectonSqlServer():
        conn=pymssql.connect(server,user,password,database)
        cursor=conn.cursor()
        cursor.execute("""select getdate()""")
        row=cursor.fetchone()
        while row:
                 print("sqlserver version:%s"%(row[0]))
                 row=cursor.fetchone()

        conn.close()

def getCreateTableScript(enodebid):
        script="""IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[rFile{0}]') AND type in (N'U'))
BEGIN
CREATE TABLE [dbo].[rFile{0}](
    [OID] [bigint] IDENTITY(1,1) NOT NULL,
    [TimeStamp] [datetime] NULL,
    [rTime] [datetime] NOT NULL,
    [bTime] [datetime] NOT NULL,
    [eTim] [datetime] NOT NULL,
    [rid] [int] NOT NULL,
    [s] [int] NOT NULL,
    [c] [int] NOT NULL,
    [muid] [decimal](18, 2) NULL,    
    [lsa] [decimal](18, 2) NULL,
    [lsrip] [int] NULL,
    [lcOID] [int] NULL,    
    [lcrq] [decimal](18, 2) NULL,
    [gc2c1] [int] NULL,    
    [tdcCP] [decimal](18, 2) NULL,    ...    ...
 CONSTRAINT [PK_rFile{0}] PRIMARY KEY NONCLUSTERED 
(
    [OID] ASC,
    [rTime] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PS_OnrTime]([rTime])
) ON [PS_OnrTime]([rTime])
END

IF NOT  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'[dbo].[rFile{0}]') AND name = N'IX_rFile_c{0}') BEGIN
        CREATE NONCLUSTERED INDEX [IX_rFile_c{0}] ON [dbo].[rFile{0}] ([c] ASC)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, SORT_IN_TEMPDB = OFF, IGNORE_DUP_KEY = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
End......
""".format(enodebid)
        return script
        
def getBulkInsertScript(enodebid,csvFilePath,formatFilePath):
    script="""BULK INSERT [dbo].[rFile{0}]
FROM '{1}'
WITH
(
        BATCHSIZE=10000,
        FIELDTERMINATOR='\\t',
        ROWTERMINATOR ='\\r\\n',
        FORMATFILE ='{2}'
)""".format(enodebid,csvFilePath,formatFilePath)
    return script


def batchInsertToDB(enodebid,filePath):
        from time import time
        start=time()
        fileExt=os.path.splitext(filePath)[1]
        #print fileExt
        if os.path.isfile(filePath) and (fileExt=='.gz' or fileExt=='.zip' or fileExt=='.xml' or fileExt==".csv"):
                # 1)create table with index
                conn=pymssql.connect(server,user,password,database)
                cursor=conn.cursor()
                cursor.execute(getCreateTableScript(enodebid))
                conn.commit()
                # 2)load csv file to db
                cursor.execute(getBulkInsertScript(enodebid=enodebid,csvFilePath=filePath,formatFilePath="D:\\python_program\\rFileTableFormat.xml"))
                conn.commit()
                conn.close()
        end=time()
        print('file:%s |size:%0.2fMB |timeuse:%0.1fs' % (os.path.basename(filePath),os.path.getsize(filePath)/1024/1024,end-start))

if __name__=="__main__":
        from time import time
        #it's mutilple pro2cess not mutilple thread.
        from multiprocessing import Pool

        start=time()
        pool=Pool()
        
        rootDir="D:\\python_program\\csv"
        for dirName in os.listdir(rootDir):
                for fileName in os.listdir(rootDir+'\\'+dirName):
                        pool.apply_async(batchInsertToDB,args=(dirName,rootDir+'\\'+dirName+'\\'+fileName,))
                        #single pool apply
                        #batchInsertToDB(dirName,rootDir+'\\'+dirName+'\\'+fileName)
                        
        print('Waiting for all subprocesses done.....')
        pool.close()
        pool.join()
        end=time()
        print('use time: %.1fs' %(end-start))


# SQLCon = pymssql.connect(host=ServerNm,database=DatabaseNm)
# Cursor = SQLCon.cursor()

# BulkInsert = '''
#     BULK INSERT OD_List
#     FROM {}
#     WITH (
#         FIRSTROW=2
#       , FIELDTERMINATOR=','
#       , ROWTERMINATOR='\n'
#     )
# '''.format(r"'C:\Users\thomsog1\Desktop\TM Tool\Test\SQL\Inputs\OD_List.txt'")

# Cursor.execute(BulkInsert)
# SQLCon.commit()

        # FIRSTROW=2,
        # BATCHSIZE=10000,
        # FIELDTERMINATOR='\\t',
        # ROWTERMINATOR = '0x0a',
        # DATAFILETYPE = 'char'

