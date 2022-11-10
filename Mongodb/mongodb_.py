from pymongo import MongoClient
from helper import *

data = lectura_datos()
print(data)
client = MongoClient()
db = client["store"]
collection = db["books"]
insert(data=data,collection=collection)
cursor = collection.find({})
for document in cursor:
    print(document)