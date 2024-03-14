from typing import Any
import os
import pandas as pd
import pymongo
import json
from ensure import ensure_annotations


from typing import Any
import os
import pandas as pd
from pymongo.mongo_client import MongoClient
import json
from ensure import ensure_annotations


class mongo_operation:
    __collection=None # here i have created a private/protected variable
    __database=None
    
    def __init__(self,client_url: str, database_name: str, collection_name: str=None):
        self.client_url=client_url
        self.database_name=database_name
        self.collection_name=collection_name
       
    def create_mongo_client(self,collection=None):
        client=MongoClient(self.client_url)
        return client
    
    def create_database(self,collection=None):
        if mongo_operation.__database==None:
            client=self.create_mongo_client(collection)
            self.database=client[self.database_name]
        return self.database 
    
    def create_collection(self,collection=None):
        if mongo_operation.__collection==None:
            database=self.create_database(collection)
            self.collection=database[self.collection_name]
            mongo_operation.__collection=collection
        
        if mongo_operation.__collection!=collection:
            database=self.create_database(collection)
            self.collection=database[self.collection_name]
            mongo_operation.__collection=collection
            
        return self.collection
    
    def insert_record(self,record: dict, collection_name: str) -> Any:
        if type(record) == list:
            for data in record:
                if type(data) != dict:
                    raise TypeError("record must be in the dict")    
            collection=self.create_collection(collection_name)
            collection.insert_many(record)
        elif type(record)==dict:
            collection=self.create_collection(collection_name)
            collection.insert_one(record)
    
    def bulk_insert(self,datafile,collection_name:str=None):
        self.path=datafile
        
        if self.path.endswith('.csv'):
            dataframe=pd.read_csv(self.path,encoding='utf-8')
            
        elif self.path.endswith(".xlsx"):
            dataframe=pd.read_excel(self.path,encoding='utf-8')
            
        datajson=json.loads(dataframe.to_json(orient='records'))
        collection=self.create_collection()
        result=collection.insert_many(datajson)
        print("# no of records isnserted :"+ str(len(result.inserted_ids)))
    def read_one(self,filter_condition,collection_name:str=None):
        import pprint
        collection=self.create_collection(collection_name)
        result=collection.find_one(filter_condition)
        pprint.pprint(result)
    def read_many(self,filter_condition,collection_name:str=None):
        import pprint
        collection=self.create_collection(collection_name)
        cursor=collection.find(filter_condition)
        num_doc=0
        for doc in cursor:
            num_doc+=1
            pprint.pprint(doc)
            print()
        print("# of documents is :"+str(num_doc))
    def delete_one(self,filter_condition,collection_name:str=None):
        import pprint
        collection=self.create_collection(collection_name)
        result=collection.find_one(filter_condition)
        pprint.pprint(result)
        result=collection.delete_one(filter_condition)
        print("Above mentioned file sucessfully deleted")
    def delete_many(self,filter_condition,collection_name:str=None):
        import pprint
        collection=self.create_collection(collection_name)
        cursor=collection.find(filter_condition)
        num_doc=0
        for doc in cursor:
            num_doc+=1
            pprint.pprint(doc)
            print()
        print("# of documents is :"+str(num_doc))
        collection.delete_many(filter_condition)

        