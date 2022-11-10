from cassandra.cluster import Cluster
from helper import *

data = lectura_datos()

cluster = Cluster(contact_points=['127.0.0.1'],port=9042)
session = cluster.connect()
try:
    session.execute("CREATE KEYSPACE IF NOT EXISTS store WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1} ")
    session.execute("USE store")
    session.execute("CREATE TABLE IF NOT EXISTS books (isbn VARCHAR, edition_date INT, title VARCHAR, author VARCHAR, PRIMARY KEY ((isbn,author)));")
    insert_100(data,session)
    session.execute("CREATE CUSTOM INDEX IF NOT EXISTS title_idx ON books (title) USING 'org.apache.cassandra.index.sasi.SASIIndex' WITH OPTIONS = {'analyzer_class': 'org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer', 'case_sensitive': true};")
except Exception as ex:
    print("Exception: "+str(ex))