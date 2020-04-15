from pymongo import MongoClient
from pprint import pprint               # pprint library is used to make the output look more pretty

# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = MongoClient("192.168.1.217")
db = client.admin

# Issue the serverStatus command and print the results
serverStatusResult = db.command("serverStatus")
pprint(serverStatusResult)

