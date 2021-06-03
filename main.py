##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################
# Author : Devneet Mohanty                                                                                                               #
# Project Name : Database Interaction Application                                                                                        #
# Project Description : This project has been developed using the Flask libraries in order to create a web application which is able to  #
#                       interact with MySQL, MongoDB and Cassandra database.                                                             #
# Python File Description : The main.py file consists of the main driver execution code along with all the necessary flask library calls #
#                           required for routing within the web application and call other backend functions.                            #
# Date Of Development : 29-05-2021                                                                                                       #
##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################

##########################################################################################################################################
#                                                 Start Block : Importing Libraries & Initializing Variables :                           #
##########################################################################################################################################

import datetime
from src.setup_logger import logger
from src.file_operations import FileOperations
from src.mysql_operations import MySqlOperations
from src.mongodb_operations import MongoDBOperations
from src.cassandra_operations import CassandraOperations
from flask import Flask, redirect, jsonify, request, render_template,url_for,send_file
import re

app = Flask(__name__)

##########################################################################################################################################
#                                                 End Block : Importing Libraries & Initializing Variables :                             #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : MongoDB Database Operation Functions :                                 #
##########################################################################################################################################

################################################
#     1) Insert Single Record Function :       #
################################################

@app.route('/insert_table_single_record_mongodb/', methods = ["GET","POST"])
def table_insertion_single_record_schema_input_mongodb():

    try:

        log_object.logToFile('debug', 'Routed to the Insert Single Record schema generation page for Mongo DB....')

        username = request.form['username']
        password = request.form['password']
        connection_uri = request.form['hostName']
        databaseName = request.form['databaseName']
        collectionName = request.form['collectionName']
        documentData = request.form['documentData']

        file_object = FileOperations()
        data = file_object.convertStringToJson(documentData)

        if data == False :
            raise Exception("The document data provided is not in a proper JSON format.")

        table_object = MongoDBOperations(connection_uri,username,password,databaseName)
        table_object.insert_single_record(collectionName,data)

        log_object.logToFile('info',
                             'Rendering the Table Insertion For Single Record Form page with status for MongoDB....')
        return render_template('insertIntoTableSingleRecordMongoDB.html', db_type="MongoDB",
                               status=[True, "SUCCESS", "Document record got inserted successfully."])

    except Exception as e:

        log_object.logToFile('exception',"The record could not get inserted into the collection due to the following exception: " + str(e))

        return render_template('insertIntoTableSingleRecordMongoDB.html', db_type="MongoDB",
                               status=[True, "ERROR",
                                       "The document record could not get inserted into the table due to the following exception: " + str(e)])



###################################################
#     2) Insert Multiple Records Function :       #
###################################################

@app.route('/insert_table_multiple_records_mongodb/', methods = ["GET","POST"])
def table_insertion_multiple_records_schema_input_mongodb():

    try:

        log_object.logToFile('debug', 'Initiating bulk data insertion for Mongo DB....')

        username = request.form['username']
        password = request.form['password']
        connection_uri = request.form['hostName']
        databaseName = request.form['databaseName']
        collectionName = request.form['collectionName']
        dataFile = request.files['documentFile']

        file_object = FileOperations()
        file_object.saveFile(dataFile,3)

        if dataFile.filename.find(".csv") > -1 :

            data = file_object.csvToJson(dataFile.filename)


        elif dataFile.filename.find(".json") > -1 :

            data = file_object.readJsonFile(dataFile.filename)

        if data == False :
            raise Exception("The document data provided is not in a proper JSON format.")

        table_object = MongoDBOperations(connection_uri,username,password,databaseName)
        table_object.insert_multiple_records(collectionName,data)

        log_object.logToFile('info',
                             'Rendering the Table Insertion For Multiple Records Form page with status for MongoDB....')
        return render_template('insertIntoTableMultipleRecordsMongoDB.html', db_type="MongoDB",
                               status=[True, "SUCCESS", "All document records got inserted successfully"])

    except Exception as e:

        log_object.logToFile('exception',"The document records could not get inserted into the collection due to the following exception: " + str(e))

        return render_template('insertIntoTableMultipleRecordsMongoDB.html', db_type="MongoDB",
                               status=[True, "ERROR",
                                       "The document records could not get inserted into the table due to the following exception: " + str(e)])



###################################################
#     3) Download Table Data Function :           #
###################################################

@app.route('/download_table_data_mongodb/', methods = ["GET","POST"])
def table_download_data_mongodb():

    try:

        log_object.logToFile('debug', 'Initiating table data download for MongoDB....')

        username = request.form['username']
        password = request.form['password']
        connection_uri = request.form['hostName']
        databaseName = request.form['databaseName']
        collectionName = request.form['collectionName']
        rowLimit = request.form['rowLimit']
        conditionalQuery = request.form['conditionalQuery']
        projectionQuery = request.form['projectionQuery']

        file_object = FileOperations()

        file_object.removeFilesWithExtension(".","json")

        if conditionalQuery != "":
            conditionalQuery_data = file_object.convertStringToJson(conditionalQuery)

            if conditionalQuery_data == False :
                raise Exception("The conditional query provided is not in a proper JSON format.")
        else :
            conditionalQuery_data = ""

        if projectionQuery != "":
            projectionQuery_data = file_object.convertStringToJson(projectionQuery)

            if projectionQuery_data == False:
                raise Exception("The projection query provided is not in a proper JSON format.")
        else:
            projectionQuery_data = ""


        table_object = MongoDBOperations(connection_uri,username,password,databaseName)
        result = table_object.select_records(collectionName,conditionalQuery_data,projectionQuery_data,rowLimit)

        if len(result) == 0 :
            raise Exception("No document records are found for the given parameters in the collection.")

        json_file_name = "MongoDB_" + collectionName + "_" + datetime.datetime.now().strftime("%d%b%Y") + ".json"
        file_object.writeToJsonFile(result,json_file_name)

        log_object.logToFile('debug', 'Downloading file from Flask server....')
        return send_file(json_file_name, as_attachment=True)

    except Exception as e:

        log_object.logToFile('exception',
                             "Collection data could not get downloaded due to the following exception: " + str(e))
        return render_template('downloadDataMongoDB.html', db_type="MongoDB",
                               status=[True, "ERROR",
                                       "Collection data could not get downloaded due to the following exception: " + str(e)])


################################################
#     4) Delete Records From Table Function :  #
################################################

@app.route('/delete_data_from_table_mongodb/', methods = ["GET","POST"])
def table_delete_data_mongodb():

    try:

        log_object.logToFile('debug', 'Initiating data deletion from table for MongoDB....')

        username = request.form['username']
        password = request.form['password']
        connection_uri = request.form['hostName']
        databaseName = request.form['databaseName']
        collectionName = request.form['collectionName']
        conditionalQuery = request.form['conditionalQuery']

        file_object = FileOperations()

        if conditionalQuery != "":
            conditionalQuery_data = file_object.convertStringToJson(conditionalQuery)

            if conditionalQuery_data == False:
                raise Exception("The conditional query provided is not in a proper JSON format.")

        else:
            conditionalQuery_data = ""

        table_object = MongoDBOperations(connection_uri,username,password,databaseName)
        table_object.delete_records(collectionName,conditionalQuery_data)

        log_object.logToFile('info',
                             'Rendering the Table Deletion Form page for MongoDB....')
        return render_template('deleteFromTableMongoDB.html', db_type="MongoDB",
                               status=[True, "SUCCESS", "Document data got deleted from the collection successfully"])

    except Exception as e:

        log_object.logToFile('exception',
                             "Document data could not be deleted due to the following exception: " + str(e))

        return render_template('deleteFromTableMongoDB.html', db_type="MongoDB",
                               status=[True, "ERROR",
                                       "Document data could not be deleted due to the following exception: " + str(e)])


################################################
#     5) Update Table Function :               #
################################################
@app.route('/update_table_record_mongodb/', methods = ["GET","POST"])
def table_update_schema_input_mongodb():

    try:

        log_object.logToFile('debug', 'Updating the document in the collection for MongoDB....')

        username = request.form['username']
        password = request.form['password']
        connection_uri = request.form['hostName']
        databaseName = request.form['databaseName']
        collectionName = request.form['collectionName']
        dataToBeUpdated = request.form['updateFieldData']
        conditionalQuery = request.form['conditionalQuery']

        file_object = FileOperations()

        if conditionalQuery != "":
            conditionalQuery_data = file_object.convertStringToJson(conditionalQuery)

            if conditionalQuery_data == False:
                raise Exception("The conditional query provided is not in a proper JSON format.")

        else:
            conditionalQuery_data = ""

        dataToBeUpdatedData = file_object.convertStringToJson(dataToBeUpdated)

        if dataToBeUpdatedData == False:
            raise Exception("The document data which needs to be updated is not in a proper JSON format.")

        table_object = MongoDBOperations(connection_uri,username,password,databaseName)
        table_object.update_records(collectionName,dataToBeUpdatedData,conditionalQuery_data)

        log_object.logToFile('info',
                             'Rendering the Update Data Form page for MongoDB....')
        return render_template('updateTableMongoDB.html', db_type="MongoDB",
                               status=[True, "SUCCESS", "Document data got updated successfully"])

    except Exception as e:

        log_object.logToFile('exception',
                             "Document data could not get updated due to the following exception: " + str(e))

        return render_template('updateTableMongoDB.html', db_type="MongoDB",
                               status=[True, "ERROR",
                                       "Document data could not get updated due to the following exception: " + str(e)])



##########################################################################################################################################
#                                                 End Block : MongoDB Database Operation Functions :                                 #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Cassandra Database Operation Functions :                                 #
##########################################################################################################################################

################################################
#          1) Create Table Function :          #
################################################

@app.route('/create_table_cassandra/', methods = ["GET","POST"])
def initiate_table_creation_cassandra():

    try:

        log_object.logToFile('debug', 'Initiating table creation for Cassandra DB....')
        clientID = request.form['clientId']
        clientSecret = request.form['clientSecret']
        keySpaceName = request.form['keySpaceName']
        tableName = request.form['tableName']
        connectionBundle = request.files['connectionBundle']

        file_object = FileOperations()
        file_object.saveFile(connectionBundle, 3)

        fields = {}

        log_object.logToFile('debug', 'Preparing the fields details list for table creation in Cassandra DB....')
        for element in request.form.keys():

            if element.find("fieldName") > -1:
                field_no = re.findall('[0-9]+', element)[0]
                field_type_element = "fieldType" + field_no
                fields[request.form.get(element)] = request.form.get(field_type_element)

        log_object.logToFile('debug', 'Fields details list have been prepared for table creation in Cassandra DB....')

        table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
        table_obj.create_table(tableName, fields)

        file_object.deleteFile(str(connectionBundle.filename))

        log_object.logToFile('info', 'Rendering the Table Creation Form page for Cassandra DB....')
        return render_template('createTableCassandra.html', db_type="Cassandra",
                               status=[True, "SUCCESS", "Table got created successfully"])

    except Exception as e:

        log_object.logToFile('exception', "Table could not be created due to the following exception: " + str(e))
        file_object.deleteFile(str(connectionBundle.filename))
        return render_template('createTableCassandra.html', db_type="Cassandra",
                               status=[True, "ERROR",
                                       "Table could not be created due to the following exception: " + str(e)])


################################################
#     2) Insert Single Record Function :       #
################################################

@app.route('/insert_table_single_record_cassandra/', methods = ["GET","POST"])
def table_insertion_single_record_schema_input_cassandra():

    try :
        log_object.logToFile('debug', 'Routed to the Insert Single Record schema generation page for Cassandra DB....')

        clientID = request.form['clientId']
        clientSecret = request.form['clientSecret']
        keySpaceName = request.form['keySpaceName']
        tableName = request.form['tableName']
        connectionBundle = request.files['connectionBundle']

        file_object = FileOperations()
        file_object.saveFile(connectionBundle, 3)

        value_inserted = False

        log_object.logToFile('debug', 'Checking if schema generation has already been completed or not for Cassandra DB....')

        for element in request.form.keys():
            if element.find("_field") > -1 :
                value_inserted = True

        if value_inserted == False :

            log_object.logToFile('debug', 'The schema is being generated for the table for Cassandra DB....')

            try :
                table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
                schema = table_obj.generate_schema(tableName)
                fields = []

                for i in schema :
                    fields.append(i)

                log_object.logToFile('info', 'Rendering the Table Insertion For Single Record Form page with generated schema for Cassandra DB....')
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('insertIntoTableSingleRecordCassandra.html', db_type="MySQL", status=[True, "SUCCESS", "Table schema has been generated."], fields = fields)

            except Exception as e :
                log_object.logToFile('exception', "Table schema could not be generated due to the following exception: " + str(e))
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('insertIntoTableSingleRecordCassandra.html', db_type="MySQL",
                                   status=[True, "ERROR",
                                           "Table schema could not be generated due to the following exception: " + str(e)], fields = [])

        else :

            log_object.logToFile('debug', 'The record is being inserted into the table for Cassandra DB....')
            try:


                fields = {}

                for element in request.form.keys():
                    if element.find("_field") > -1:
                        fields[element.replace("_field","")] = request.form.get(element)

                table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
                table_obj.insert_into_table_single_record(tableName, fields)

                log_object.logToFile('info',
                                     'Rendering the Table Insertion For Single Record Form page with record insertion message for Cassandra DB....')
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('insertIntoTableSingleRecordCassandra.html', db_type="Cassandra",
                                       status=[True, "SUCCESS",
                                               "The record got inserted successfully"], fields=[])
            except Exception as e :
                log_object.logToFile('exception',
                                     "The record could not get inserted into the table due to the following exception: " + str(e))
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('insertIntoTableSingleRecordCassandra.html', db_type="Cassandra",
                                       status=[True, "ERROR",
                                               "The record could not get inserted into the table due to the following exception: " + str(
                                                   e)], fields=[])

    except Exception as e :
        log_object.logToFile('exception',
                             "An unknown exception occurred : " + str(
                                 e))
        return render_template('insertIntoTableSingleRecordCassandra.html', db_type="Cassandra",
                               status=[True, "ERROR",
                                       "An unknown exception occurred : " + str(
                                           e)], fields=[])

################################################
#   3) Insert Multiple Records Function :      #
################################################

@app.route('/insert_table_mutliple_records_cassandra', methods = ["GET","POST"])
def table_insertion_multiple_records_cassandra():

    try:

        log_object.logToFile('debug', 'Initiating bulk data insertion for Cassandra DB....')
        clientID = request.form['clientId']
        clientSecret = request.form['clientSecret']
        keySpaceName = request.form['keySpaceName']
        tableName = request.form['tableName']
        connectionBundle = request.files['connectionBundle']

        file_object = FileOperations()
        file_object.saveFile(connectionBundle, 3)

        log_object.logToFile('debug',
                             'Dataset with multiple records is being inserted into the table for Cassandra DB....')

        try :

            includeHeaders = request.form['includeHeaders']

        except Exception as e :

            includeHeaders = 'off'

        data_file = request.files['insert_file']

        file_object = FileOperations()
        file_object.saveFile(data_file,3)
        headers, values = file_object.readCSVFile(str(data_file.filename),includeHeaders)

        try :

            table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
            table_obj.insert_into_table_multiple_records(tableName,headers,values)

            log_object.logToFile('info',
                                 'Rendering the Table Insertion For Multiple Records Form page with record insertion message for Cassandra DB....')

            file_object.deleteFile(str(connectionBundle.filename))
            file_object.deleteFile(str(data_file.filename))

            return render_template('insertIntoTableMultipleRecordsCassandra.html', db_type="Cassandra",
                                   status=[True, "SUCCESS", "All records got inserted successfully"])

        except Exception as e :
            log_object.logToFile('exception',
                                 "Bulk insertion failed due to the following exception: " + str(
                                     e))
            file_object.deleteFile(str(connectionBundle.filename))
            file_object.deleteFile(str(data_file.filename))
            return render_template('insertIntoTableMultipleRecordsCassandra.html', db_type="Cassandra",
                                   status=[True, "ERROR",
                                           "Bulk insertion failed due to the following exception: " + str(e)])

    except Exception as e :
        log_object.logToFile('exception',
                             "An unknown exception occurred : " + str(
                                 e))

        return render_template('insertIntoTableMultipleRecordsCassandra.html', db_type="Cassandra",
                               status=[True, "ERROR",
                                       "An unknown exception occurred : " + str(e)])


################################################
#     4) Download Table Data Function :        #
################################################

@app.route('/download_table_data_cassandra/', methods = ["GET","POST"])
def table_download_data_cassandra():

    try :

        log_object.logToFile('debug', 'Initiating table data download for Cassandra DB....')

        clientID = request.form['clientId']
        clientSecret = request.form['clientSecret']
        keySpaceName = request.form['keySpaceName']
        tableName = request.form['tableName']
        connectionBundle = request.files['connectionBundle']
        noOfRows = request.form['rowLimit']

        file_object = FileOperations()
        file_object.removeFilesWithExtension('.', 'csv')

        file_object.saveFile(connectionBundle, 3)

        conditional_fields = []

        log_object.logToFile('debug', 'Preparing conditional fields list for Cassandra DB....')
        for element in request.form.keys():
            if element.find("fieldName") > -1:
                field_no = re.findall('[0-9]+', element)[0]
                conditional_fields.append([request.form.get(element), request.form.get("fieldOperator" + str(field_no)),
                                           request.form.get("fieldValue" + str(field_no)),
                                           request.form.get("recordOperator" + str(field_no))])

        log_object.logToFile('debug', 'Fetching data from the table for Cassandra DB....')

        table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
        headers,results = table_obj.select_records(tableName,conditional_fields,noOfRows)

        log_object.logToFile('debug', 'Writing CSV file with table data for Cassandra DB....')
        file_name = "Cassandra_"+tableName+"_"+datetime.datetime.now().strftime("%d%b%Y")+".csv"

        file_object = FileOperations()
        file_object.writeToCSV(file_name,results,headers)

        log_object.logToFile('debug', 'Downloading file from Flask server for Cassandra DB....')

        file_object.deleteFile(str(connectionBundle.filename))

        return send_file(file_name, as_attachment=True)

    except Exception as e :

        file_object.deleteFile(str(connectionBundle.filename))
        return render_template('downloadData.html', db_type="Cassandra",
                               status=[True, "ERROR", "Table data could not get downloaded due to the following exception: "+str(e)])


################################################
#   5) Delete Records From Table Function :    #
################################################

@app.route('/delete_data_from_table_cassandra/', methods = ["GET","POST"])
def table_delete_data_cassandra():

    try :

        log_object.logToFile('debug', 'Initiating data deletion from table for Cassandra DB....')

        clientID = request.form['clientId']
        clientSecret = request.form['clientSecret']
        keySpaceName = request.form['keySpaceName']
        tableName = request.form['tableName']
        connectionBundle = request.files['connectionBundle']

        file_object = FileOperations()
        file_object.saveFile(connectionBundle, 3)

        conditional_fields = []

        log_object.logToFile('debug', 'Preparing conditional fields list for Cassandra DB....')

        for element in request.form.keys():
            if element.find("fieldName") > -1:
                field_no = re.findall('[0-9]+', element)[0]
                conditional_fields.append([request.form.get(element), request.form.get("fieldOperator" + str(field_no)),
                                           request.form.get("fieldValue" + str(field_no)),
                                           request.form.get("recordOperator" + str(field_no))])

        log_object.logToFile('debug', 'Deleting data from the table for Cassandra DB....')

        table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
        table_obj.delete_records(tableName,conditional_fields)

        log_object.logToFile('info', 'Rendering the Data Deletion Form page for Cassandra DB....')
        file_object.deleteFile(str(connectionBundle.filename))

        return render_template('deleteFromTableCassandra.html', db_type="Cassandra",
                               status=[True, "SUCCESS", "Data got deleted from the table successfully"])
    except Exception as e :
        log_object.logToFile('exception',
                             "Table data could not be deleted due to the following exception: " + str(e))
        file_object.deleteFile(str(connectionBundle.filename))
        return render_template('deleteFromTableCassandra.html', db_type="Cassandra",
                               status=[True, "ERROR", "Table data could not be deleted due to the following exception: "+str(e)])


################################################
#          6) Update Table Function :          #
################################################

@app.route('/update_table_record_cassandra/', methods = ["GET","POST"])
def table_update_schema_input_cassandra():

    try :

        log_object.logToFile('debug', 'Routed to the Update Table schema generation page for Cassandra DB....')

        clientID = request.form['clientId']
        clientSecret = request.form['clientSecret']
        keySpaceName = request.form['keySpaceName']
        tableName = request.form['tableName']
        connectionBundle = request.files['connectionBundle']

        file_object = FileOperations()
        file_object.saveFile(connectionBundle, 3)

        value_inserted = False

        log_object.logToFile('debug', 'Checking if schema generation has already been completed or not for Cassandra DB....')

        for element in request.form.keys():
            if element.find("_field") > -1 :
                value_inserted = True

        if value_inserted == False :

            log_object.logToFile('debug', 'The schema is being generated for the table for Cassandra DB....')

            try :
                table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
                schema = table_obj.generate_schema(tableName)
                fields = []
                for i in schema :
                    fields.append(i)

                log_object.logToFile('info', 'Rendering the Update Table Form page with generated schema for Cassandra DB....')
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('updateTableCassandra.html', db_type="Cassandra", status=[True, "SUCCESS", "Table schema has been generated."], fields = fields)
            except Exception as e :
                log_object.logToFile('exception', "Table schema could not be generated due to the following exception: " + str(e))
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('updateTableCassandra.html', db_type="Cassandra",
                                   status=[True, "ERROR",
                                           "Table schema could not be generated due to the following exception: " + str(e)], fields = [])

        else :

            log_object.logToFile('debug', 'The table has been updated for Cassandra DB....')
            try:

                fields = {}
                log_object.logToFile('debug', 'Preparing fields to be updated list for Cassandra DB....')
                for element in request.form.keys():
                    if element.find("_field") > -1:
                        fields[element.replace("_field","")] = request.form.get(element)


                conditional_fields = []

                log_object.logToFile('debug', 'Preparing conditional fields list....')
                for element in request.form.keys():
                    if element.find("fieldName") > -1:
                        field_no = re.findall('[0-9]+', element)[0]
                        conditional_fields.append(
                            [request.form.get(element), request.form.get("fieldOperator" + str(field_no)),
                             request.form.get("fieldValue" + str(field_no)),
                             request.form.get("recordOperator" + str(field_no))])

                table_obj = CassandraOperations(clientID, clientSecret, connectionBundle.filename, keySpaceName)
                table_obj.update_table(tableName, fields, conditional_fields)

                log_object.logToFile('info',
                                     'Rendering the Update Table Form page with table updation message for Cassandra DB....')
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('updateTableCassandra.html', db_type="Cassandra",
                                      status=[True, "SUCCESS",
                                            "The table has been updated successfully"], fields=[])
            except Exception as e :
                log_object.logToFile('exception',
                                     "The table could not get updated due to the following exception: " + str(e))
                file_object.deleteFile(str(connectionBundle.filename))
                return render_template('updateTableCassandra.html', db_type="Cassandra",
                                       status=[True, "ERROR",
                                               "The table could not get updated due to the following exception: " + str(
                                                   e)], fields=[])

    except Exception as e :

        log_object.logToFile('exception',
                             "An unknown exception occurred : " + str(
                                 e))

        return render_template('updateTableCassandra.html', db_type="Cassandra",
                               status=[True, "ERROR",
                                       "An unknown exception occurred : " + str(e)],
                               fields=[])


##########################################################################################################################################
#                                                 End Block : Cassandra Database Operation Functions :                                   #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : MySQL Database Operation Functions :                                     #
##########################################################################################################################################

################################################
#          1) Create Table Function :          #
################################################

@app.route('/create_table/', methods = ["GET","POST"])
def initiate_table_creation():

    try :

        log_object.logToFile('debug', 'Initiating table creation....')
        userName = request.form['username']
        password = request.form['password']
        database_name = request.form['database_name']
        table_name = request.form['table_name']

        fields = {}

        log_object.logToFile('debug', 'Preparing the fields details list for table creation....')

        for element in request.form.keys():
            if element.find("fieldName") > -1:
                field_no = re.findall('[0-9]+', element)[0]
                field_type_element = "fieldType" + field_no
                fields[request.form.get(element)] = request.form.get(field_type_element)

        log_object.logToFile('debug', 'Fields details list have been prepared for table creation....')

        table_obj = MySqlOperations(userName,password,database_name)
        table_obj.create_table(table_name,fields)

        log_object.logToFile('info', 'Rendering the Table Creation Form page....')
        return render_template('createTable.html', db_type="MySQL", status = [True,"SUCCESS" ,"Table got created successfully"])

    except Exception as e :

        log_object.logToFile('exception', "Table could not be created due to the following exception: "+str(e))
        return render_template('createTable.html', db_type="MySQL",
                               status=[True, "ERROR", "Table could not be created due to the following exception: "+str(e)])


################################################
#     2) Insert Single Record Function :       #
################################################

@app.route('/insert_table_single_record/', methods = ["GET","POST"])
def table_insertion_single_record_schema_input():

        try :

            log_object.logToFile('debug', 'Routed to the Insert Single Record schema generation page....')

            userName = request.form['username']
            password = request.form['password']
            database_name = request.form['database_name']
            table_name = request.form['table_name']

            value_inserted = False


            log_object.logToFile('debug', 'Checking if schema generation has already been completed or not....')

            for element in request.form.keys():
                if element.find("_field") > -1:
                    value_inserted = True

            if value_inserted == False:

                try:
                    log_object.logToFile('debug', 'The schema is being generated for the table....')

                    table_obj = MySqlOperations(userName,password,database_name)
                    schema = table_obj.generate_schema(table_name)

                    fields = []

                    for i in schema :
                        fields.append(i[0])

                    log_object.logToFile('info', 'Rendering the Table Insertion For Single Record Form page with generated schema....')
                    return render_template('insertIntoTableSingleRecord.html', db_type="MySQL", status=[True, "SUCCESS", "Table schema has been generated."], fields = fields)

                except Exception as e :
                    log_object.logToFile('exception', "Table schema could not be generated due to the following exception: " + str(e))
                    return render_template('insertIntoTableSingleRecord.html', db_type="MySQL",
                                       status=[True, "ERROR",
                                               "Table schema could not be generated due to the following exception: " + str(e)], fields = [])

            else :

                log_object.logToFile('debug', 'The record is being inserted into the table....')
                try:

                    userName = request.form['username']
                    password = request.form['password']
                    database_name = request.form['database_name']
                    table_name = request.form['table_name']

                    fields = {}

                    for element in request.form.keys():
                        if element.find("_field") > -1:
                            fields[element.replace("_field","")] = request.form.get(element)

                    table_obj = MySqlOperations(userName, password, database_name)
                    table_obj.insert_into_table_single_record(table_name, fields)

                    log_object.logToFile('info',
                                         'Rendering the Table Insertion For Single Record Form page with record insertion message....')
                    return render_template('insertIntoTableSingleRecord.html', db_type="MySQL",
                                           status=[True, "SUCCESS",
                                                   "The record got inserted successfully"], fields=[])
                except Exception as e :
                    log_object.logToFile('exception',
                                         "The record could not get inserted into the table due to the following exception: " + str(e))
                    return render_template('insertIntoTableSingleRecord.html', db_type="MySQL",
                                           status=[True, "ERROR",
                                                   "The record could not get inserted into the table due to the following exception: " + str(
                                                       e)], fields=[])

        except Exception as e:

            log_object.logToFile('exception',
                                 "An unknown exception occurred : " + str(
                                     e))

            return render_template('insertIntoTableSingleRecord.html', db_type="MySQL",
                                   status=[True, "ERROR",
                                           "An unknown exception occurred : " + str(e)],
                                   fields=[])

################################################
#   3) Insert Multiple Records Function :      #
################################################

@app.route('/insert_table_mutliple_records', methods = ["GET","POST"])
def table_insertion_multiple_records():

    try :
        log_object.logToFile('debug', 'Initiating bulk data insertion....')
        userName = request.form['username']
        password = request.form['password']
        database_name = request.form['database_name']
        table_name = request.form['table_name']

        log_object.logToFile('debug', 'Dataset with multiple records is being inserted into the table....')

        try:
            includeHeaders = request.form['includeHeaders']
        except Exception as e :
            includeHeaders = 'off'

        data_file = request.files['insert_file']

        file_object = FileOperations()
        file_object.saveFile(data_file,3)
        headers, values = file_object.readCSVFile(str(data_file.filename),includeHeaders)

        try :

            table_obj = MySqlOperations(userName, password, database_name)
            table_obj.insert_into_table_multiple_records(table_name,headers,values)
            file_object.deleteFile(str(data_file.filename))

            log_object.logToFile('info',
                                 'Rendering the Table Insertion For Multiple Records Form page with record insertion message....')
            return render_template('insertIntoTableMultipleRecords.html', db_type="MySQL",
                                   status=[True, "SUCCESS", "All records got inserted successfully"])

        except Exception as e :
            log_object.logToFile('exception',
                                 "Bulk insertion failed due to the following exception: " + str(
                                     e))
            return render_template('insertIntoTableMultipleRecords.html', db_type="MySQL",
                                   status=[True, "ERROR",
                                           "Bulk insertion failed due to the following exception: " + str(e)])

    except Exception as e:

        log_object.logToFile('exception',
                             "An unknown exception occurred : " + str(
                                 e))

        return render_template('insertIntoTableMultipleRecords.html', db_type="MySQL",
                               status=[True, "ERROR",
                                       "An unknown exception occurred : " + str(e)],
                               fields=[])

################################################
#     4) Download Table Data Function :        #
################################################

@app.route('/download_table_data/', methods = ["GET","POST"])
def table_download_data():

    try :

        log_object.logToFile('debug', 'Initiating table data download....')

        userName = request.form['username']
        password = request.form['password']
        database_name = request.form['database_name']
        table_name = request.form['table_name']
        noOfRows = request.form['rowLimit']

        conditional_fields = []

        log_object.logToFile('debug', 'Preparing conditional fields list....')
        for element in request.form.keys():
            if element.find("fieldName") > -1:
                field_no = re.findall('[0-9]+', element)[0]
                conditional_fields.append([request.form.get(element), request.form.get("fieldOperator" + str(field_no)),
                                           request.form.get("fieldValue" + str(field_no)),
                                           request.form.get("recordOperator" + str(field_no))])

        log_object.logToFile('debug', 'Fetching data from the table....')

        table_obj = MySqlOperations(userName,password,database_name)
        headers,results = table_obj.select_records(table_name,conditional_fields,noOfRows)

        log_object.logToFile('debug', 'Writing CSV file with table data....')
        file_name = "MySQL_"+table_name+"_"+datetime.datetime.now().strftime("%d%b%Y")+".csv"

        file_object = FileOperations()
        file_object.removeFilesWithExtension('.', 'csv')
        file_object.writeToCSV(file_name,results,headers)

        log_object.logToFile('debug', 'Downloading file from Flask server....')

        return send_file(file_name, as_attachment=True)

    except Exception as e :

        log_object.logToFile('exception',
                             "Table data could not get downloaded due to the following exception: " + str(e))
        return render_template('downloadData.html', db_type="MySQL",
                               status=[True, "ERROR", "Table data could not get downloaded due to the following exception: "+str(e)])


################################################
#   5) Delete Records From Table Function :    #
################################################

@app.route('/delete_data_from_table/', methods = ["GET","POST"])
def table_delete_data():

    try :

        log_object.logToFile('debug', 'Initiating data deletion from table....')

        userName = request.form['username']
        password = request.form['password']
        database_name = request.form['database_name']
        table_name = request.form['table_name']

        conditional_fields = []

        log_object.logToFile('debug', 'Preparing conditional fields list....')
        for element in request.form.keys():
            if element.find("fieldName") > -1:
                field_no = re.findall('[0-9]+', element)[0]
                conditional_fields.append([request.form.get(element), request.form.get("fieldOperator" + str(field_no)),
                                           request.form.get("fieldValue" + str(field_no)),
                                           request.form.get("recordOperator" + str(field_no))])

        log_object.logToFile('debug', 'Deleting data from the table....')

        table_obj = MySqlOperations(userName,password,database_name)
        table_obj.delete_records(table_name,conditional_fields)

        log_object.logToFile('info', 'Rendering the Data Deletion Form page....')
        return render_template('deleteFromTable.html', db_type="MySQL",
                               status=[True, "SUCCESS", "Data got deleted from the table successfully"])

    except Exception as e :

        log_object.logToFile('exception',
                             "Table data could not be deleted due to the following exception: " + str(e))
        return render_template('deleteFromTable.html', db_type="MySQL",
                               status=[True, "ERROR", "Table data could not be deleted due to the following exception: "+str(e)])


################################################
#          6) Update Table Function :          #
################################################

@app.route('/update_table_record/', methods = ["GET","POST"])
def table_update_schema_input():

    try :

        log_object.logToFile('debug', 'Routed to the Update Table schema generation page....')

        userName = request.form['username']
        password = request.form['password']
        database_name = request.form['database_name']
        table_name = request.form['table_name']

        value_inserted = False

        log_object.logToFile('debug', 'Checking if schema generation has already been completed or not....')

        for element in request.form.keys():
            if element.find("_field") > -1 :
                value_inserted = True

        if value_inserted == False :

            log_object.logToFile('debug', 'The schema is being generated for the table....')

            try :
                table_obj = MySqlOperations(userName,password,database_name)
                schema = table_obj.generate_schema(table_name)
                fields = []
                for i in schema :
                    fields.append(i[0])

                log_object.logToFile('info', 'Rendering the Update Table Form page with generated schema....')
                return render_template('updateTable.html', db_type="MySQL", status=[True, "SUCCESS", "Table schema has been generated."], fields = fields)
            except Exception as e :
                log_object.logToFile('exception', "Table schema could not be generated due to the following exception: " + str(e))
                return render_template('updateTable.html', db_type="MySQL",
                                   status=[True, "ERROR",
                                           "Table schema could not be generated due to the following exception: " + str(e)], fields = [])

        else :

            log_object.logToFile('debug', 'The table has been updated....')
            try:

                userName = request.form['username']
                password = request.form['password']
                database_name = request.form['database_name']
                table_name = request.form['table_name']

                fields = {}
                log_object.logToFile('debug', 'Preparing fields to be updated list....')
                for element in request.form.keys():
                    if element.find("_field") > -1:
                        fields[element.replace("_field","")] = request.form.get(element)


                conditional_fields = []

                log_object.logToFile('debug', 'Preparing conditional fields list....')
                for element in request.form.keys():
                    if element.find("fieldName") > -1:
                        field_no = re.findall('[0-9]+', element)[0]
                        conditional_fields.append(
                            [request.form.get(element), request.form.get("fieldOperator" + str(field_no)),
                             request.form.get("fieldValue" + str(field_no)),
                             request.form.get("recordOperator" + str(field_no))])

                table_obj = MySqlOperations(userName, password, database_name)
                table_obj.update_table(table_name, fields, conditional_fields)

                log_object.logToFile('info',
                                     'Rendering the Update Table Form page with table updation message....')

                return render_template('updateTable.html', db_type="MySQL",
                                      status=[True, "SUCCESS",
                                            "The table has been updated successfully"], fields=[])
            except Exception as e :
                log_object.logToFile('exception',
                                     "The table could not get updated due to the following exception: " + str(e))
                return render_template('updateTable.html', db_type="MySQL",
                                       status=[True, "ERROR",
                                               "The table could not get updated due to the following exception: " + str(
                                                   e)], fields=[])

    except Exception as e:

        log_object.logToFile('exception',
                             "An unknown exception occurred : " + str(
                                 e))

        return render_template('updateTable.html', db_type="MySQL",
                               status=[True, "ERROR",
                                       "An unknown exception occurred : " + str(e)],
                               fields=[])

##########################################################################################################################################
#                                             End Block : MySQL Database Operation Functions :                                           #
##########################################################################################################################################

##########################################################################################################################################
#                                               Start Block : MySQL Routing Functions :                                                  #
##########################################################################################################################################


#################################################################
#   1) Routing To Create Table Function For MySQL :             #
#################################################################

@app.route('/dboperation/<db_selected>/create_table/')
def table_creation_page(db_selected):

    log_object.logToFile('debug', 'Routed to the Create Table form page....')
    return render_template('createTable.html', db_type=db_selected, status = [False,"" ,""])


######################################################################
# 2) Routing To Insert Single Record Into Table Function For MySQL : #
######################################################################

@app.route('/dboperation/<db_selected>/insert_table_single_record/')
def table_insertion_single_record_page(db_selected):

    log_object.logToFile('debug', 'Routed to the Insert Into Table For Single Record form page....')
    return render_template('insertIntoTableSingleRecord.html', db_type=db_selected, status=[False, "", ""], fields=[])

############################################################################
#   3) Routing To Insert Multiple Records Into Table Function For MySQL :  #
############################################################################

@app.route('/dboperation/<db_selected>/insert_table_multiple_records/')
def table_insertion_multiple_records_page(db_selected):

    log_object.logToFile('debug', 'Routed to the Insert Into Table For Multiple Records form page....')
    return render_template('insertIntoTableMultipleRecords.html', db_type=db_selected, status=[False, "", ""], fields=[])


##################################################################
#   4) Routing To Download Table Data Function For MySQL :       #
##################################################################

@app.route('/dboperation/<db_selected>/download_data/')
def table_download_data_page(db_selected):

    log_object.logToFile('debug', 'Routed to the Download Data From Table form page....')
    return render_template('downloadData.html', db_type=db_selected, status=[False, "", ""])


##################################################################
#   5) Routing To Delete Records From Table Function For MySQL : #
##################################################################

@app.route('/dboperation/<db_selected>/delete_from_table/')
def table_delete_data_page(db_selected):

    log_object.logToFile('debug', 'Routed to the Delete Data From Table form page....')
    return render_template('deleteFromTable.html', db_type=db_selected, status=[False, "", ""])

##################################################################
#   6) Routing To Update Table Data Function For MySQL :         #
##################################################################

@app.route('/dboperation/<db_selected>/update_table/')
def table_update_data_page(db_selected):

    log_object.logToFile('debug', 'Routed to the Update Table form page....')
    return render_template('updateTable.html', db_type=db_selected, status=[False, "", ""], fields=[])

#################################################################
#   7) Routing To Create Table Function For Cassandra :         #
#################################################################

@app.route('/dboperation/<db_selected>/create_table_cassandra/')
def table_creation_page_cassandra(db_selected):

    log_object.logToFile('debug', 'Routed to the Create Table form page for Cassandra....')
    return render_template('createTableCassandra.html', db_type=db_selected, status = [False,"" ,""])

##########################################################################
# 8) Routing To Insert Single Record Into Table Function For Cassandra : #
##########################################################################

@app.route('/dboperation/<db_selected>/insert_table_single_record_cassandra/')
def table_insertion_single_record_page_cassandra(db_selected):

    log_object.logToFile('debug', 'Routed to the Insert Into Table For Single Record form page for Cassandra...')
    return render_template('insertIntoTableSingleRecordCassandra.html', db_type=db_selected, status=[False, "", ""], fields=[])

################################################################################
#   9) Routing To Insert Multiple Records Into Table Function For Cassandra :  #
################################################################################

@app.route('/dboperation/<db_selected>/insert_table_multiple_records_cassandra/')
def table_insertion_multiple_records_page_cassandra(db_selected):

    log_object.logToFile('debug', 'Routed to the Insert Into Table For Multiple Records form page for Cassandra....')
    return render_template('insertIntoTableMultipleRecordsCassandra.html', db_type=db_selected, status=[False, "", ""], fields=[])


##################################################################
#   10) Routing To Download Table Data Function For Cassandra :  #
##################################################################

@app.route('/dboperation/<db_selected>/download_data_cassandra/')
def table_download_data_page_cassandra(db_selected):

    log_object.logToFile('debug', 'Routed to the Download Data From Table form page for Cassandra....')
    return render_template('downloadDataCassandra.html', db_type=db_selected, status=[False, "", ""])


#######################################################################
#   11) Routing To Delete Records From Table Function For Cassandra : #
#######################################################################

@app.route('/dboperation/<db_selected>/delete_from_table_cassandra/')
def table_delete_data_page_cassandra(db_selected):

    log_object.logToFile('debug', 'Routed to the Delete Data From Table form page for Cassandra....')
    return render_template('deleteFromTableCassandra.html', db_type=db_selected, status=[False, "", ""])

##################################################################
#   12) Routing To Update Table Data Function For Cassandra :    #
##################################################################

@app.route('/dboperation/<db_selected>/update_table_cassandra/')
def table_update_data_page_cassandra(db_selected):

    log_object.logToFile('debug', 'Routed to the Update Table form page for Cassandra....')
    return render_template('updateTableCassandra.html', db_type=db_selected, status=[False, "", ""], fields=[])


##########################################################################
# 13) Routing To Insert Single Record Into Table Function For Mongo DB : #
##########################################################################

@app.route('/dboperation/<db_selected>/insert_table_single_record_mongodb/')
def table_insertion_single_record_page_mongodb(db_selected):

    log_object.logToFile('debug', 'Routed to the Insert Into Table For Single Record form page for Mongo DB...')
    return render_template('insertIntoTableSingleRecordMongoDB.html', db_type=db_selected, status=[False, "", ""], fields=[])

################################################################################
#  14) Routing To Insert Multiple Records Into Table Function For Mongo DB :   #
################################################################################

@app.route('/dboperation/<db_selected>/insert_table_multiple_records_mongodb/')
def table_insertion_multiple_records_page_mongodb(db_selected):

    log_object.logToFile('debug', 'Routed to the Insert Into Table For Multiple Records form page for Mongo DB....')
    return render_template('insertIntoTableMultipleRecordsMongoDB.html', db_type=db_selected, status=[False, "", ""], fields=[])


##################################################################
#   15) Routing To Download Table Data Function For Mongo DB :   #
##################################################################

@app.route('/dboperation/<db_selected>/download_data_mongodb/')
def table_download_data_page_mongodb(db_selected):

    log_object.logToFile('debug', 'Routed to the Download Data From Table form page for MongoDB....')
    return render_template('downloadDataMongoDB.html', db_type=db_selected, status=[False, "", ""])


#######################################################################
#   16) Routing To Delete Records From Table Function For Mongo DB  : #
#######################################################################

@app.route('/dboperation/<db_selected>/delete_from_table_mongodb/')
def table_delete_data_page_mongodb(db_selected):

    log_object.logToFile('debug', 'Routed to the Delete Data From Table form page for MongoDB....')
    return render_template('deleteFromTableMongoDB.html', db_type=db_selected, status=[False, "", ""])

##################################################################
#   17) Routing To Update Table Data Function For Mongo DB  :    #
##################################################################

@app.route('/dboperation/<db_selected>/update_table_mongodb/')
def table_update_data_page_mongodb(db_selected):

    log_object.logToFile('debug', 'Routed to the Update Table form page for Cassandra....')
    return render_template('updateTableMongoDB.html', db_type=db_selected, status=[False, "", ""], fields=[])

##################################################################
#   18) Routing To Database Actions Function :                   #
##################################################################

@app.route('/response', methods = ['POST'])
def home_page_response():

    dbType = request.form['dbtype']
    actionType = request.form['dbActiontype']

    log_object.logToFile('debug', 'User selected the database as : {0} and action type as {1}....'.format(dbType,actionType))

    if actionType.lower() == "create" and dbType.lower() == 'mysql' :
        log_object.logToFile('debug', 'Redirecting to create_table URl....')
        return redirect(url_for('table_creation_page',db_selected = dbType))
    elif actionType.lower() == "insert" and dbType.lower() == 'mysql' :
        log_object.logToFile('debug', 'Redirecting to insert_single_record URl....')
        return redirect(url_for('table_insertion_single_record_page', db_selected=dbType))
    elif actionType.lower() == "bulk insert" and dbType.lower() == 'mysql' :
        log_object.logToFile('debug', 'Redirecting to insert_multiple_records URl....')
        return redirect(url_for('table_insertion_multiple_records_page', db_selected=dbType))
    elif actionType.lower() == "select" and dbType.lower() == 'mysql' :
        log_object.logToFile('debug', 'Redirecting to download_data URl....')
        return redirect(url_for('table_download_data_page', db_selected=dbType))
    elif actionType.lower() == "delete" and dbType.lower() == 'mysql' :
        log_object.logToFile('debug', 'Redirecting to delete_from_table URl....')
        return redirect(url_for('table_delete_data_page', db_selected=dbType))
    elif actionType.lower() == "update" and dbType.lower() == 'mysql' :
        log_object.logToFile('debug', 'Redirecting to update_table URl....')
        return redirect(url_for('table_update_data_page', db_selected=dbType))

    elif actionType.lower() == "create" and dbType.lower() == 'cassandra' :
        log_object.logToFile('debug', 'Redirecting to create_table_cassandra URl....')
        return redirect(url_for('table_creation_page_cassandra',db_selected = dbType))
    elif actionType.lower() == "insert" and dbType.lower() == 'cassandra' :
        log_object.logToFile('debug', 'Redirecting to insert_single_record_cassandra URl....')
        return redirect(url_for('table_insertion_single_record_page_cassandra',db_selected = dbType))
    elif actionType.lower() == "bulk insert" and dbType.lower() == 'cassandra' :
        log_object.logToFile('debug', 'Redirecting to insert_multiple_records_cassandra URl....')
        return redirect(url_for('table_insertion_multiple_records_page_cassandra',db_selected = dbType))
    elif actionType.lower() == "select" and dbType.lower() == 'cassandra' :
        log_object.logToFile('debug', 'Redirecting to download_data_cassandra URl....')
        return redirect(url_for('table_download_data_page_cassandra',db_selected = dbType))
    elif actionType.lower() == "delete" and dbType.lower() == 'cassandra' :
        log_object.logToFile('debug', 'Redirecting to delete_from_table_cassandra URl....')
        return redirect(url_for('table_delete_data_page_cassandra',db_selected = dbType))
    elif actionType.lower() == "update" and dbType.lower() == 'cassandra' :
        log_object.logToFile('debug', 'Redirecting to update_table_cassandra URl....')
        return redirect(url_for('table_update_data_page_cassandra',db_selected = dbType))

    elif actionType.lower() == "insert" and dbType.lower() == 'mongodb' :
        log_object.logToFile('debug', 'Redirecting to insert_single_record_mongodb URl....')
        return redirect(url_for('table_insertion_single_record_page_mongodb',db_selected = dbType))
    elif actionType.lower() == "bulk insert" and dbType.lower() == 'mongodb' :
        log_object.logToFile('debug', 'Redirecting to insert_multiple_records_mongodb URl....')
        return redirect(url_for('table_insertion_multiple_records_page_mongodb',db_selected = dbType))
    elif actionType.lower() == "select" and dbType.lower() == 'mongodb' :
        log_object.logToFile('debug', 'Redirecting to download_data_mongodb URl....')
        return redirect(url_for('table_download_data_page_mongodb',db_selected = dbType))
    elif actionType.lower() == "delete" and dbType.lower() == 'mongodb' :
        log_object.logToFile('debug', 'Redirecting to delete_from_table_mongodb URl....')
        return redirect(url_for('table_delete_data_page_mongodb',db_selected = dbType))
    elif actionType.lower() == "update" and dbType.lower() == 'mongodb' :
        log_object.logToFile('debug', 'Redirecting to update_table_mongodb URl....')
        return redirect(url_for('table_update_data_page_mongodb',db_selected = dbType))

##################################################################
#   19) Routing To Home Page Function :                          #
##################################################################

@app.route('/', methods = ['GET','POST'])
def home_page():

    log_object.logToFile('debug', 'Routed to the home page....')
    return render_template('index.html')

##########################################################################################################################################
#                                               End Block : MySQL Routing Functions :                                                    #
##########################################################################################################################################

##########################################################################################################################################
#                                               Start Block : Driver Code :                                                              #
##########################################################################################################################################


if __name__ == '__main__':

    log_object = logger()
    log_object.logToFile('info','The process has started....')

    log_object.logToFile('info', 'Starting up the flask server....')

    app.run()

##########################################################################################################################################
#                                               End Block : Driver Code :                                                                #
##########################################################################################################################################
