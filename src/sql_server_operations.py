##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################
# Author : Devneet Mohanty                                                                                                               #
# Project Name : Database Interaction Application                                                                                        #
# Project Description : This project has been developed using the Flask libraries in order to create a web application which is able to  #
#                       interact with Microsoft SQL Server, MongoDB and Cassandra database.                                              #
# Python File Description : The sql_server_operations.py file consists of different operations to interact with the                      #
#                           Microsoft SQL Server database in order to perform Create, Read, Update and Delete (CRUD) operations .        #                                       #
# Date Of Development : 29-05-2021                                                                                                       #
##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Importing Libraries & Initializing Variables :                           #
##########################################################################################################################################

from src.setup_logger import logger
import pyodbc

##########################################################################################################################################
#                                                 End Block : Importing Libraries & Initializing Variables :                             #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Microsoft SQL Server Operation Functions :                                              #
##########################################################################################################################################

class MicrosoftSQLServerOperations :

    ################################################
    #     1) Initialising Function :               #
    ################################################

    def __init__(self,username,password,db_name,server_name):

        '''

        Functionality : Establishing connection with Microsoft SQL Server localhost and initialising the logging object required for generating logs for different database operations
        :param username: The username required for connecting to Microsoft SQL Server, if any.
        :param password: The password required for connecting to Microsoft SQL Server, if any.
        :param db_name: The database name in the Microsoft SQL Server server where the operations need to be performed.
        :param server_name : The server name required to connect to the Microsoft SQL Server.

        '''

        self.username = username
        self.password = password
        self.db_name = db_name
        self.server_name = server_name

        self.log_object = logger()
        self.log_object.logToFile('info','Establishing connection to the Microsoft SQL Server database....')

        try:

            if username == "" and password == "":
                self.conn = pyodbc.connect(r'Driver=SQL Server;Server='+server_name+r';Trusted_Connection=yes;',autocommit=True)
            else:
                self.conn = pyodbc.connect(r'Driver=SQL Server;Server= {0} ;UID = {1}; PWD ={2};Trusted_Connection=yes;'.format(server_name,username,password),
                                           autocommit=True)

            self.cursor = self.conn.cursor()
            sql_query = "SELECT * FROM sys.databases WHERE name = '{0}'".format(self.db_name)
            self.cursor.execute(sql_query)

            rows = self.cursor.fetchall()

            if len(list(rows)) == 0 :
                sql_query = "CREATE DATABASE "+self.db_name
                self.cursor.execute(sql_query)

            self.cursor.execute("USE " + self.db_name)

        except Exception as e :
            self.log_object.logToFile('critical', 'An error occurred while connecting to the Microsoft SQL Server database : '+str(e))
            raise Exception(str(e))


    ################################################
    #     2) Creating Table :                      #
    ################################################

    def create_table(self,table_name,fields_dict):

        '''

        Functionality : Creating a table with the defined fields and their data types.
        :param table_name: The name of the table to be created in the database.
        :param fields_dict: The dictionary of fields and their data types which will be included in the table.
        :return: None

        '''

        self.log_object.logToFile('info', 'Creating table '+table_name)

        self.log_object.logToFile('debug', 'Creating the SQL Query....')
        table_def_string = ""
        for i in range(0,len(fields_dict)):

            table_def_string += list(fields_dict.keys())[i] +" "+fields_dict[list(fields_dict.keys())[i]]
            if i != len(fields_dict)-1 :
                table_def_string += ','
            else :
                table_def_string += ')'

        sql_query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}'".format(table_name)
        self.cursor.execute(sql_query)

        rows = self.cursor.fetchall()

        if (len(list(rows)) != 0):
            sql_query = "DROP TABLE "+table_name
            self.cursor.execute(sql_query)

        sql_query = "CREATE TABLE "+table_name+"( "+table_def_string

        self.log_object.logToFile('debug', 'SQL query got created as : '+sql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.cursor.execute(sql_query)
        self.log_object.logToFile('debug', 'The query got executed successfully....')

        self.log_object.logToFile('info', 'The table has been created....')
        self.log_object.logToFile('info', 'Closing the database connection....')
        self.conn.close()


    ################################################
    #     3) Generating Table Schema :             #
    ################################################

    def generate_schema(self,table_name):

        '''

        Functionality : Generating the schema list of the specified table defining all the fields being used inside it.
        :param table_name: The name of the table in the database whose field definition list needs to be generated.
        :return:result

        '''

        self.log_object.logToFile('info', 'Generating table schema for : '+table_name)

        self.log_object.logToFile('debug', 'Creating the SQL Query....')

        sql_query = "SELECT * FROM "+table_name

        self.log_object.logToFile('debug', 'SQL query got created as : '+sql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.cursor.execute(sql_query)

        result = list(self.cursor.description)

        self.log_object.logToFile('debug', 'The query got executed successfully....')

        self.log_object.logToFile('info', 'The table schema has been generated successfully....')
        self.log_object.logToFile('info', 'Closing the database connection....')
        self.conn.close()

        return result


    ################################################
    #     4) Insert Single Record :                #
    ################################################

    def insert_into_table_single_record(self, table_name, insert_fields):

        '''

        Functionality : Inserting a single record in the given table.
        :param table_name: The name of the table in the database where the record needs to be inserted.
        :param insert_fields: The dictionary of field and value pair for the record which needs to be inserted in the table.
        :return: None

        '''

        self.log_object.logToFile('info', 'Inserting single record into the table : ' + table_name)

        field_string = "("
        value_string = "VALUES("

        for field_no in range(0,len(insert_fields)):
            field_string+= list(insert_fields.keys())[field_no]
            value_string+= "'"+list(insert_fields.values())[field_no]+"'"

            if field_no != len(insert_fields)-1 :
                field_string+=","
                value_string+=","
            else :
                field_string+=")"
                value_string+=")"

        self.log_object.logToFile('debug', 'Creating the SQL Query....')

        sql_string = "INSERT INTO "+table_name+" "+field_string+" "+value_string

        self.log_object.logToFile('debug', 'SQL query got created as : ' + sql_string)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.cursor.execute(sql_string)
        self.conn.commit()

        self.log_object.logToFile('debug', 'The query got executed successfully....')

        self.log_object.logToFile('info', 'The record got inserted successfully....')
        self.log_object.logToFile('info', 'Closing the database connection....')

        self.conn.close()


    ################################################
    #     5) Insert Multiple Records :             #
    ################################################

    def insert_into_table_multiple_records(self, table_name, headers, values):

        '''

        Functionality : Inserting multiple data records in the given table.
        :param table_name: The name of the table in the database where the records need to be inserted.
        :param headers: The list of the field headers defining the fields in the table.
        :param values: The list of all the values of all the records to be inserted in the table.
        :return: None

        '''

        self.log_object.logToFile('info', 'Inserting multiple records into the table : ' + table_name)

        for record_idx, record_value in enumerate(values):

            initial_string = "INSERT INTO "+table_name

            if len(headers)>0 :

                header_string = " ("

                for idx,header_value in enumerate(headers):

                    header_string += header_value

                    if idx != len(headers)-1:

                        header_string += ","

                    else :

                        header_string += ")"

            else:

                header_string = ""

            value_string = "VALUES("

            for val_idx, current_value in enumerate(record_value):

                value_string += "'"+current_value+"'"

                if val_idx != len(record_value) - 1:

                    value_string += ","

                else:

                    value_string += ")"

            self.log_object.logToFile('debug', 'Creating the SQL Query for record no. : '+str(record_idx)+'....')

            sql_string = initial_string + header_string + " " + value_string

            self.log_object.logToFile('debug', 'SQL query got created as : ' + sql_string)
            self.log_object.logToFile('debug', 'Executing the query for record no. : '+str(record_idx)+'....')

            self.cursor.execute(sql_string)
            self.conn.commit()

            self.log_object.logToFile('debug', 'The query got executed successfully....')

            self.log_object.logToFile('info', 'The record got inserted successfully....')


        self.log_object.logToFile('info', 'Closing the database connection....')
        self.conn.close()


    ###################################################
    #     6) Fetching Records From Collection :       #
    ###################################################

    def select_records(self, table_name, conditional_fields, rowLimit):

        '''

        Functionality : Fetching the list of headers and values from the specified table as per the conditions and row limit if required.
        :param table_name: The name of the table in the database from where the records need to be fetched.
        :param conditional_fields: The dictionary of conditional fields to be checked while fetching the records, if required.
        :param rowLimit: The row limit defining the number of records to be returned from the table.
        :return: headers, results

        '''

        self.log_object.logToFile('info', 'Downloading table data from the table : ' + table_name)

        if len(conditional_fields) == 0 :
            conditional_string = ""

        else :
            conditional_string = " WHERE "

            for idx,condition in enumerate(conditional_fields):

                conditional_string += condition[0].strip()+" "

                if condition[1].lower() == "equals":
                    conditional_operator = "="
                    conditional_string += conditional_operator+" '"+condition[2]+"' "+ \
                                          condition[3]+" "
                elif condition[1].lower() == "not equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3]+" "
                elif condition[1].lower() == "greater than":
                    conditional_operator = ">"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3]+" "
                elif condition[1].lower() == "greater than equals":
                    conditional_operator = ">="
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3]+" "
                elif condition[1].lower() == "less than":
                    conditional_operator = "<"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3]+" "
                elif condition[1].lower() == "less than equals":
                    conditional_operator = "<="
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3]+" "
                elif condition[1].lower() == "like":
                    conditional_operator = "LIKE"
                    conditional_string += conditional_operator + " '%" + condition[2] + "%' " + \
                                          condition[3]+" "

                elif condition[1].lower() == "in":
                    conditional_operator = "IN({0})"
                    value = ""
                    for val in condition[2].split(","):
                        value+=val.replace("'","").strip()+","
                    value = value.rstrip(",")
                    conditional_string += conditional_operator.format(str(value)) + \
                                          condition[3] + " "

        if rowLimit == "":
            initial_string = "SELECT * FROM "+table_name
        else :
            initial_string = "SELECT TOP "+rowLimit+" * FROM "+table_name

        self.log_object.logToFile('debug', 'Creating the SQL Query....')

        sql_query = initial_string+conditional_string

        self.log_object.logToFile('debug', 'SQL query got created as : ' + sql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.cursor.execute(sql_query)

        results = list(self.cursor.fetchall())
        headers = [i[0] for i in self.cursor.description]

        self.log_object.logToFile('info', 'Closing the database connection....')
        self.conn.close()

        return headers, results


    ###################################################
    #     7) Deleting Records :                       #
    ###################################################

    def delete_records(self,table_name, conditional_fields):

        '''

        Functionality : Deleting records from a given table based on condition, if required.
        :param table_name: The name of the table in the database from where the records need to be deleted.
        :param conditional_fields: The dictionary of conditional fields to be checked while deleting the records, if required.
        :return: None

        '''

        self.log_object.logToFile('info', 'Deleting data from the table : ' + table_name)

        initial_string = "DELETE FROM " + table_name

        if len(conditional_fields) == 0:
            conditional_string = ""

        else:
            conditional_string = " WHERE "

            for idx, condition in enumerate(conditional_fields):

                conditional_string += condition[0].strip() + " "

                if condition[1].lower() == "equals":
                    conditional_operator = "="
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "not equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "like":
                    conditional_operator = "LIKE"
                    conditional_string += conditional_operator + " '%" + condition[2] + "%' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "in":
                    conditional_operator = "IN({0})"
                    value = ""
                    for val in condition[2].split(","):
                        value+=val.replace("'","").strip()+","
                    value = value.rstrip(",")
                    conditional_string += conditional_operator.format(str(value)) + \
                                          condition[3] + " "

        self.log_object.logToFile('debug', 'Creating the SQL Query....')

        sql_query = initial_string + conditional_string

        self.log_object.logToFile('debug', 'SQL query got created as : ' + sql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.cursor.execute(sql_query)
        self.conn.commit()

        self.log_object.logToFile('info', 'Closing the database connection....')
        self.conn.close()


    ###################################################
    #     8) Updating Records :                       #
    ###################################################

    def update_table(self,table_name, fields_to_be_updated, conditional_fields):

        '''

        Functionality : Updating records in a given table based on condition, if required.
        :param table_name: The name of the table in the datavase where the records need to be updated.
        :param fields_to_be_updated: The dictionary of fields and the values which the existing fields need to be updated to.
        :param conditional_fields: The dictionary of conditional fields to be checked while updating the records, if required.
        :return: None

        '''

        self.log_object.logToFile('info', 'Updating table data : ' + table_name)

        initial_string = "UPDATE " + table_name

        update_string = " SET"

        for idx,element in enumerate(list(fields_to_be_updated.keys())):

            if fields_to_be_updated[element].lower() == "no change" :
                continue

            else :

                update_string+= " "+element+" = '"+fields_to_be_updated[element]+"'"

                if idx != len(list(fields_to_be_updated)):
                    update_string+= ","
                else :
                    update_string += ""

            if update_string[-1] == "," :

                update_string = update_string.rstrip(",")

        if len(conditional_fields) == 0:
            conditional_string = ""

        else:
            conditional_string = " WHERE "

            for idx, condition in enumerate(conditional_fields):

                conditional_string += condition[0].strip() + " "

                if condition[1].lower() == "equals":
                    conditional_operator = "="
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "not equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + " '" + condition[2] + "' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "like":
                    conditional_operator = "LIKE"
                    conditional_string += conditional_operator + " '%" + condition[2] + "%' " + \
                                          condition[3] + " "
                elif condition[1].lower() == "in":
                    conditional_operator = "IN({0})"
                    value = ""
                    for val in condition[2].split(","):
                        value+=val.replace("'","").strip()+","
                    value = value.rstrip(",")
                    conditional_string += conditional_operator.format(str(value)) + \
                                          condition[3] + " "

        self.log_object.logToFile('debug', 'Creating the SQL Query....')

        sql_query = initial_string + update_string + conditional_string

        self.log_object.logToFile('debug', 'SQL query got created as : ' + sql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.cursor.execute(sql_query)
        self.conn.commit()

        self.log_object.logToFile('info', 'Closing the database connection....')
        self.conn.close()

##########################################################################################################################################
#                                                 End Block : Microsoft SQL Server Operation Functions :                                                #
##########################################################################################################################################
