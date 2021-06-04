##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################
# Author : Devneet Mohanty                                                                                                               #
# Project Name : Database Interaction Application                                                                                        #
# Project Description : This project has been developed using the Flask libraries in order to create a web application which is able to  #
#                       interact with MySQL, MongoDB and Cassandra database.                                                             #
# Python File Description : The mongodb_operations.py file consists of different operations to interact with the MongoDB database in     #
#                           order to perform Create, Read, Update and Delete (CRUD) operations .                                         #
# Date Of Development : 29-05-2021                                                                                                       #
##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Importing Libraries & Initializing Variables :                           #
##########################################################################################################################################

import pymongo
import urllib.parse
from src.setup_logger import logger

##########################################################################################################################################
#                                                 End Block : Importing Libraries & Initializing Variables :                             #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : MongoDB Operation Functions :                                            #
##########################################################################################################################################

class MongoDBOperations:

    ################################################
    #     1) Initialising Function :               #
    ################################################

    def __init__(self, connection_uri, username, password, databaseName):

        '''

        Functionality : Establishing connection with MongoDB server and initialising the logging object required for generating logs for different database operations.
        :param connection_uri: The connection URI required for connecting to the MongoDB cluster on Atlas or the combination of hostname and port number required for
                               connecting to the local MongoDB server.
        :param username: The username required for connecting to MongoDB server, if any.
        :param password: The password required for connecting to MongoDB server, if any.
        :param databaseName: The database name in the MongoDB server where the operations need to be performed.

        '''

        self.connection_uri = connection_uri
        self.username = username
        self.password =password
        self.databaseName = databaseName

        self.log_object = logger()
        self.log_object.logToFile('info', 'Checking mandatory parameters for connection to MongoDB server....')

        self.connection_uri = self.connection_uri.replace("'","").replace('"',"").strip()

        if self.connection_uri.lower().find("<username>") > -1 :
            if username == "" :
                self.log_object.logToFile('error', 'Username field is required but was not provided for MongoDB operation....')
                raise Exception("Missing Mandatory Field : Username field is required but was not provided for MongoDB operation")
            else :
                self.connection_uri = connection_uri.replace("<username>", username)

        if self.connection_uri.lower().find("<password>") > -1 :
            if password == "" :
                self.log_object.logToFile('error','Password field is required but was not provided for MongoDB operation....')
                raise Exception("Missing Mandatory Field : Password field is required but was not provided for MongoDB operation")
            else :
                self.connection_uri = self.connection_uri.replace("<password>", urllib.parse.quote(password))

        self.log_object.logToFile('info', 'Establishing connection to the MongoDB server....')

        self.client = pymongo.MongoClient(self.connection_uri)
        self.client.server_info()

        self.log_object.logToFile('info', 'The connection got established successfully to the MongoDB server....')


    ################################################
    #     2) Inserting Single Document Record :    #
    ################################################

    def insert_single_record(self, collectionName, documentData):

        '''

        Functionality : Inserting a single document data record in the given collection.
        :param collectionName: The name of the collection in the database where the record needs to be inserted.
        :param documentData: The JSON document record which needs to be inserted in the collection.
        :return: None

        '''

        self.log_object.logToFile('info',
                                  'Inserting single record into the collection : ' + collectionName + ' using MongoDB for the database : ' + self.databaseName)

        database_object = self.client[self.databaseName]
        collection_object = database_object[collectionName]

        collection_object.insert_one(documentData)

        self.log_object.logToFile('info', 'The record got inserted successfully in MongoDB....')
        self.log_object.logToFile('info', 'Closing the MongoDB server connection....')
        self.client.close()


    ###################################################
    #     3) Inserting Multiple Document Records :    #
    ###################################################

    def insert_multiple_records(self, collectionName, documentData):

        '''

        Functionality : Inserting multiple data records in the given collection.
        :param collectionName: The name of the collection in the database where the records need to be inserted.
        :param documentData: The list of JSON document records which need to be inserted in the collection.
        :return: None

        '''

        self.log_object.logToFile('info',
                                  'Inserting multiple records into the collection : ' + collectionName + ' using MongoDB for the database : ' + self.databaseName)

        database_object = self.client[self.databaseName]
        collection_object = database_object[collectionName]

        collection_object.insert_many(documentData)

        self.log_object.logToFile('info', 'All the records got inserted successfully in MongoDB....')
        self.log_object.logToFile('info', 'Closing the MongoDB server connection....')
        self.client.close()

    ###################################################
    #     4) Fetching Records From Collection :       #
    ###################################################

    def select_records(self,collectionName,conditionalQuery,projectionQuery,rowLimit):

        '''

        Functionality : Fetching the document records from the specific collection based on conditions, projections and row limit if required.
        :param collectionName: The name of the collection in the database from where the records need to be fetched.
        :param conditionalQuery: The conditional MQL statement in JSON format to be checked while fetching document records, if required.
        :param projectionQuery: The projection MQL statement in JSON format to be checked indicating the fields to be retrieved while fetching document records,
                                if required.
        :param rowLimit: The number of document records to be fetched, if required.
        :return: records

        '''

        self.log_object.logToFile('info',
                                  'Fetching data from collection : ' + collectionName + ' using MongoDB for the database : ' + self.databaseName)

        database_object = self.client[self.databaseName]
        collection_object = database_object[collectionName]

        if rowLimit == "" :
            if projectionQuery != {} :
                results = collection_object.find(conditionalQuery,projectionQuery)
            else :
                results = collection_object.find(conditionalQuery)
        else :
            if projectionQuery != {} :
                results = collection_object.find(conditionalQuery,projectionQuery).limit(int(rowLimit))
            else :
                results = collection_object.find(conditionalQuery).limit(int(rowLimit))

        records = [i for i in results]

        self.log_object.logToFile('info', 'All the records got fetched successfully in MongoDB....')
        self.log_object.logToFile('info', 'Closing the MongoDB server connection....')
        self.client.close()

        return str(records)

    ###################################################
    #     5) Deleting Records :                       #
    ###################################################

    def delete_records(self, collectionName, conditionalQuery):

        '''

        Functionality : Deleting document records from a given collection based on condition, if required.
        :param collectionName: The name of the collection in the database from where the records need to be deleted.
        :param conditionalQuery: The conditional MQL statement in JSON format to be checked while deleting document records, if required.
        :return: None

        '''

        self.log_object.logToFile('info',
                                  'Deleting records from collection : ' + collectionName + ' using MongoDB for the database : ' + self.databaseName)

        database_object = self.client[self.databaseName]
        collection_object = database_object[collectionName]

        if conditionalQuery != {}:

            self.log_object.logToFile('debug',
                                      'Searching for the document with the given conditional statement : ' + str(conditionalQuery))

            results = collection_object.find(conditionalQuery)
            count = len([i for i in results])

            if count == 0:
                self.log_object.logToFile('error',
                                          'No document record found for the given conditional statement in the collection.')
                raise Exception("No document record found for the given conditional statement in the collection.")

        collection_object.delete_many(conditionalQuery)

        self.log_object.logToFile('info', 'All the records with given condition got deleted successfully in MongoDB....')
        self.log_object.logToFile('info', 'Closing the MongoDB server connection....')
        self.client.close()


    ###################################################
    #     6) Updating Records :                       #
    ###################################################

    def update_records(self, collectionName, dataToBeUpdated, conditionalQuery):

        '''

        Functionality : Updating document records from a given collection based on condition, if required.
        :param collectionName: The name of the collection in the database from where the records need to be updated.
        :param dataToBeUpdated: The document data that the existing document record needs to be updated to.
        :param conditionalQuery: The conditional MQL statement in JSON format to be checked while updating document records, if required.
        :return: None

        '''

        self.log_object.logToFile('info',
                                  'Updating records in the collection : ' + collectionName + ' using MongoDB for the database : ' + self.databaseName)

        database_object = self.client[self.databaseName]
        collection_object = database_object[collectionName]

        if conditionalQuery != {} :

            self.log_object.logToFile('debug',
                                      'Searching for the document with the given conditional statement : ' +str(conditionalQuery))

            results = collection_object.find(conditionalQuery)
            count = len([i for i in results])

            if count == 0 :
                self.log_object.logToFile('error',
                                          'No document record found for the given conditional statement in the collection.')
                raise Exception("No document record found for the given conditional statement in the collection.")

        if "$set" in list(dataToBeUpdated.keys()) :

            data = dataToBeUpdated

        else :

            data = {"$set" : dataToBeUpdated}

        collection_object.update_many(filter=conditionalQuery,update=data)

        self.log_object.logToFile('info',
                                  'The document records with given condition got updated successfully in MongoDB....')
        self.log_object.logToFile('info', 'Closing the MongoDB server connection....')
        self.client.close()

##########################################################################################################################################
#                                                 End Block : MongoDB Operation Functions :                                              #
##########################################################################################################################################
