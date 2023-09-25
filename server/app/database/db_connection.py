from pymongo import MongoClient

MONGO_CONNECTION_STRING = 'mongodb+srv://pratyushparida18:password%4018@cluster0.gewdlyg.mongodb.net/'
MONGO_DATABASE_NAME = 'Loop'

def get_database():
    client = MongoClient(MONGO_CONNECTION_STRING)
    return client[MONGO_DATABASE_NAME]