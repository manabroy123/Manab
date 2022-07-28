import mysql.connector as connection
import pandas as pd
from sqlalchemy import create_engine
import pymongo

# connecting to MySql
myconn = create_engine("mysql+pymysql://root:manab@localhost/ineuron")

# Read Excel and store data in Dataframe
df1 = pd.read_excel(r"D:\Data Science\Data\Attribute DataSet.xlsx")
df2 = pd.read_excel(r"D:\Data Science\Data\Dress Sales.xlsx")

#Task2
#Do a bulk load for these two table for respective dataset
#Inserting Attribute data from pandas dataframe into Mysql Table
df1.to_sql(con=myconn,name='tbl_attribute',if_exists='append',index=False)

#Inserting Sales data from pandas dataframe into Mysql Table
df2.to_sql(con=myconn,name='tbl_dress_sales',if_exists='append',index=False)

#task4
#Convert attribute dataset in json format
#Converting dataframe into Json

Attributedata= df1.to_dict(orient="records")
Salesdata= df2.to_dict(orient="records")


#Establish connection with MongoDB Cloud
client = pymongo.MongoClient("mongodb://manabroy1:manab1234@cluster0-shard-00-00.buvb7.mongodb.net:27017,cluster0-shard-00-01.buvb7.mongodb.net:27017,cluster0-shard-00-02.buvb7.mongodb.net:27017/?ssl=true&replicaSet=atlas-9wasns-shard-0&authSource=admin&retryWrites=true&w=majority")

#Creating database and tables in MongoDB
database = client['iNeuron']
collection = database["Attribute"]
collection = database["Sales"]

#task 5
#Store this dataset into mongodb
#Inserting Attribute data into MongoDB
collection.insert_many(Attributedata)
#verified data from mongodb

#Inserting Sales data into MongoDB
collection.insert_many(Salesdata)
#verified data from mongodb

#Task3 (read these dataset in pandas as a dataframe )
#Task 6 (Left Join)
# in sql task try to perform left join operation with attrubute dataset and dress dataset on column Dress_ID
sql_query1 = pd.read_sql('select dress_id, style, price,rating from tbl_attribute', myconn)
sql_query2 = pd.read_sql('select * from tbl_dress_sales', myconn)

#Creating Dataframe for Attribute and Sales data
attribute_data = pd.DataFrame(sql_query1)
dress_sales = pd.DataFrame(sql_query2)

#Left Join
joined_data = pd.merge(attribute_data,dress_sales,how = 'left',on = 'dress_id')

print(joined_data)

#Task 7
#Write the SQL query to find out how many unique dress that we have based on Dress_ID
sql_query3 = pd.read_sql('select count(distinct(dress_id)) from tbl_attribute', myconn)
Distinct_Dress = pd.DataFrame(sql_query3)

print(Distinct_Dress)

# Task 8
# Try to find out how many dress is having recommendation as 0
sql_query4 = pd.read_sql('select count(*) from tbl_attribute where recommendation=0', myconn)
Recommendation = pd.DataFrame(sql_query4)

#print(Recommendation)

#Task9
#Try to find out total dress sell for individual dress id
dress_sales['Sales'] = dress_sales.loc[0: , '29/8/2013' : '12/10/2013'].agg('sum', axis=1)

print(dress_sales.groupby('dress_id')['Sales'].sum())


#Task 10
#Finding out the third highest most selling dress

SalesOrder=dress_sales.groupby('dress_id')['Sales'].sum().sort_values(ascending =False)
print(SalesOrder.iloc[2:3]) #output 75979.0

