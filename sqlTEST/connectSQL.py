import pymssql
import pandas as pd
 
serverName = '(local)'
db_name = 'Project'
table_name = 'trainFeatureWithRepeater'
conn = pymssql.connect(server = serverName , database = "Project")



def pd_to_sqlDB(input_df: pd.DataFrame,
                table_name: str,
                db_name: str = 'default.db') -> None:

    '''Take a Pandas dataframe `input_df` and upload it to `table_name` SQLITE table

    Args:
        input_df (pd.DataFrame): Dataframe containing data to upload to SQLITE
        table_name (str): Name of the SQLITE table to upload to
        db_name (str, optional): Name of the SQLITE Database in which the table is created. 
                                 Defaults to 'default.db'.
    '''

    # Step 1: Setup local logging
    import logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Step 2: Find columns in the dataframe
    cols = input_df.columns
    cols_string = ','.join(cols)
    val_wildcard_string = ','.join(['?'] * len(cols))

    # Step 3: Connect to a DB file if it exists, else crete a new file
    con = pymssql.connect(server = "(local)", database = db_name)
    cur = con.cursor()
    logging.info(f'SQL DB {db_name} created')

    # Step 4: Create Table
    sql_string = f"""CREATE TABLE {table_name} ({cols_string});"""
    cur.execute(sql_string)
    logging.info(f'SQL Table {table_name} created with {len(cols)} columns')

    # Step 5: Upload the dataframe
    rows_to_upload = input_df.to_dict(orient='split')['data']
    sql_string = f"""INSERT INTO {table_name} ({cols_string}) VALUES ({val_wildcard_string});"""
    cur.executemany(sql_string, rows_to_upload)
    logging.info(f'{len(rows_to_upload)} rows uploaded to {table_name}')
  
    # Step 6: Commit the changes and close the connection
    con.commit()
    con.close()


dirThis = 'C:\\Users\\chcww\\Downloads\\'
# Step 1: Read the csv file into a dataframe
# Dataset from https://www.kaggle.com/gpreda/covid-world-vaccination-progress
input_df = pd.read_csv(dirThis + 'country_vaccinations.csv')
 
# Step 2: Upload the dataframe to a SQL Table
pd_to_sqlDB(input_df,
            table_name='trainFeatureWithRepeater',
            db_name='Project.db')
 
# Step 3: Write the SQL query in a string variable

sql_query_string = """
    select * from trainFeatureWithRepeater
"""

sql_query_string = """
    SELECT country, SUM(daily_vaccinations) as total_vaccinated
    FROM country_vaccinations 
    WHERE daily_vaccinations IS NOT NULL 
    GROUP BY country
    ORDER BY total_vaccinated DESC
"""
 
# Step 4: Exectue the SQL query
result_df = sql_query_to_pd(sql_query_string, db_name='default.db')
result_df

conn = pymssql.connect(server = "(local)", database = db_name)
cursor_1 = conn.cursor()
cursor_1.execute('SELECT * FROM trainFeatureWithRepeater where totalquantity = 116')
 
cursor_2 = conn.cursor()
cursor_2.execute('SELECT * FROM trainEncodeBIG')
 
print( "all persons" )
print( cursor_1.fetchall() )  # 显示出的是cursor_2游标查询出来的结果
 
print( "John Doe" )
print( cursor_2.fetchall() )
data1 = cursor_2.fetchall() 
data1[0]
data = pd.DataFrame(data1)
data.shape







def sql_query_to_pd(sql_query_string: str, db_name: str ='default.db') -> pd.DataFrame:
    '''Execute an SQL query and return the results as a pandas dataframe

    Args:
        sql_query_string (str): SQL query string to execute
        db_name (str, optional): Name of the SQLITE Database to execute the query in.
                                 Defaults to 'default.db'.

    Returns:
        pd.DataFrame: Results of the SQL query in a pandas dataframe
    '''    
    # Step 1: Connect to the SQL DB
    con = sqlite3.connect(db_name)

    # Step 2: Execute the SQL query
    cursor = con.execute(sql_query_string)

    # Step 3: Fetch the data and column names
    result_data = cursor.fetchall()
    cols = [description[0] for description in cursor.description]

    # Step 4: Close the connection
    con.close()

    # Step 5: Return as a dataframe
    return pd.DataFrame(result_data, columns=cols)




db_host = '192.168.0.5'
db_port = '9526'
db_user = 'test'
db_pwd = 'test'
db_name = 'TestDB'
tb_name = 'TestTB'
 
 
class SqlServerOperate(object):
    def __init__(self, server, port, user, password, db_name, as_dict=True):
        self.server = server
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name
        self.conn = self.get_connect(as_dict=as_dict)
        pass
 
    def __del__(self):
        self.conn.close()
 
    def get_connect(self, as_dict=True):
        conn = pymssql.connect(
            server=self.server,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.db_name,
            as_dict=as_dict,
            charset="utf8"
        )
        return conn
 
    def exec_query(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        result_list = list(cur.fetchall())
        cur.close()
 
        # 使用with语句（上下文管理器）来省去显式的调用close方法关闭连接和游标
        # print('****************使用 with 语句******************')
        # with self.get_connect() as cur:
        #     cur.execute(sql)
        #     result_list = list(cur.fetchall())   # 把游标执行后的结果转换成 list
        #     # print(result_list)
 
        return result_list
 
    def exec_non_query(self, sql, params=None):
        cur = self.conn.cursor()
        # cur.execute(sql, params=params)
        cur.execute(sql, params=params)
        self.conn.commit()
        cur.close()
 
    def exec_mutil_sql(self, sql, data_list):
        """
           执行一次 sql, 批量插入多条数据
        :param sql: 参数用 %s 代替 : insert into table_name(col1, col2, col3) values(%s, %s, %s)
        :param data_list:  list类型, list中每个元素都是元组
        :return:
        """
        cur = self.conn.cursor()
        cur.executemany(sql, data_list)
        self.conn.commit()
        cur.close()
 
 
def test():
    ms = SqlServerOperate(db_host, db_port, db_user, db_pwd, db_name)
    sql_string = "select * from SpiderItem where ResourceType = 20"
    temp_result_list = ms.exec_query(sql_string)
    for i in temp_result_list:
        print(i)
    pass
 
 
if __name__ == "__main__":
    test()
    pass











import pyodbc
import pandas as pd
# insert data from csv file into dataframe.
# working directory for csv file: type "pwd" in Azure Data Studio or Linux
# working directory in Windows c:\users\username
df = pd.read_csv("c:\\user\\username\department.csv")
# Some other example server values are
# server = 'localhost\sqlexpress' # for a named instance
# server = 'myserver,port' # to specify an alternate port
server = 'yourservername' 
database = 'AdventureWorks' 
username = 'username' 
password = 'yourpassword' 
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
# Insert Dataframe into SQL Server:
for index, row in df.iterrows():
     cursor.execute("INSERT INTO HumanResources.DepartmentTest (DepartmentID,Name,GroupName) values(?,?,?)", row.DepartmentID, row.Name, row.GroupName)
cnxn.commit()
cursor.close()