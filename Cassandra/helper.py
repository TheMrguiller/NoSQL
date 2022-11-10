import pandas as pd
from cassandra.cluster import Cluster

def lectura_datos():
    data=pd.read_csv('books_data/books.csv',sep=";",encoding='latin-1',on_bad_lines='skip')
    data=limpieza_datos(data)
    return data
def limpieza_datos(data):
    data= data.drop(['Image-URL-M','Image-URL-S','Image-URL-L','Publisher'],axis=1)
    data=data[pd.to_numeric(data["Year-Of-Publication"],errors='coerce').notnull()]
    data["Year-Of-Publication"]=data["Year-Of-Publication"].astype('int')
    return data
def insert_100(data,session):
    df_100 = data.iloc[:100]
    for x in range(0,100):
        data_to_insert=df_100.iloc[[x]]
        session.execute("INSERT INTO books (isbn,edition_date,title,author) VALUES (%s,%s,%s,%s)",(data_to_insert["ISBN"].iloc[0],data_to_insert["Year-Of-Publication"].iloc[0],data_to_insert["Book-Title"].iloc[0],data_to_insert["Book-Author"].iloc[0]))
