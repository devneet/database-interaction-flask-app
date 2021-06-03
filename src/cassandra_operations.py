##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################
# Author : Devneet Mohanty                                                                                                               #
# Project Name : Database Interaction Application                                                                                        #
# Project Description : This project has been developed using the Flask libraries in order to create a web application which is able to  #
#                       interact with MySQL, MongoDB and Cassandra database.                                                             #
# Python File Description : The cassandra_operations.py file consists of different operations to interact with the Cassandra database in #
#                           order to perform Create, Read, Update and Delete (CRUD) operations .                                         #
# Date Of Development : 29-05-2021                                                                                                       #
##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Importing Libraries & Initializing Variables :                           #
##########################################################################################################################################

from src.setup_logger import logger
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

##########################################################################################################################################
#                                                 End Block : Importing Libraries & Initializing Variables :                             #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Cassandra Operation Functions :                                          #
##########################################################################################################################################

class CassandraOperations :

    ################################################
    #     1) Initialising Function :               #
    ################################################

    def __init__(self, clientID, clientSecret, connectionBundlePath, keySpaceName):

        '''

        Functionality : Establishing connection with Cassandra server and initialising the logging object required for generating logs for different database operations.
        :param clientID: The client ID required for connecting to Cassandra server.
        :param clientSecret: The client secret required for connecting to Cassandra server.
        :param connectionBundlePath: The file path to the secure connection bundle required for connecting to Cassandra server on Data Stax cluster.
        :param keySpaceName: The keyspace name in the Cassandra server where the operations need to be performed.

        '''

        self.clientID = clientID
        self.clientSecret = clientSecret
        self.connectionBundlePath = connectionBundlePath
        self.keySpaceName = keySpaceName

        self.log_object = logger()
        self.log_object.logToFile('info', 'Establishing connection to the Cassandra database....')

        cloud_config = {
            'secure_connect_bundle': self.connectionBundlePath
        }

        auth_provider = PlainTextAuthProvider(self.clientID,self.clientSecret)

        self.cluster = Cluster(cloud=cloud_config,auth_provider=auth_provider)

        self.session = self.cluster.connect()

        records = self.session.execute("SELECT release_version FROM system.local")

        if records:
            self.log_object.logToFile('info', 'Connection established successfully with the Cassandra database release version : '+str(records[0])+'....')
        else:
            self.log_object.logToFile('error', 'Connection could not be established with the Cassandra database.')
            raise Exception('Error while establishing connection with Cassandra database.')


    ################################################
    #     2) Creating Table :                      #
    ################################################

    def create_table(self, table_name, fields_dict):

        '''

        Functionality : Creating a table with the defined fields and their data types.
        :param table_name: The name of the table to be created in the keyspace.
        :param fields_dict: The dictionary of fields and their data types which will be included in the table.
        :return: None

        '''

        self.log_object.logToFile('info', 'Creating table : ' + table_name + ' using Cassandra DB for the keyspace : '+self.keySpaceName)

        self.log_object.logToFile('debug', 'Using the keyspace....')
        self.session.execute('USE "'+self.keySpaceName+'"')

        self.log_object.logToFile('debug', 'Creating the CQL Query....')
        table_def_string = ""
        for i in range(0, len(fields_dict)):

            table_def_string += list(fields_dict.keys())[i] + " " + fields_dict[list(fields_dict.keys())[i]]
            if i != len(fields_dict) - 1:
                table_def_string += ','
            else:
                table_def_string += ')'

        cql_query = "CREATE TABLE IF NOT EXISTS " + table_name + "( " + table_def_string

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.session.execute(cql_query)

        self.log_object.logToFile('debug', 'The query got executed successfully....')
        self.log_object.logToFile('info', 'The table has been created in Cassandra DB....')

        self.log_object.logToFile('info', 'Closing the Cassandra database connection....')
        self.cluster.shutdown()

    ################################################
    #     3) Generating Table Schema :             #
    ################################################

    def generate_schema(self, table_name):

        '''

        Functionality : Generating the schema list of the specified table defining all the fields being used inside it.
        :param table_name: The name of the table in the keyspace whose field definition list needs to be generated.
        :return:result

        '''

        self.log_object.logToFile('info', 'Generating table schema for : ' + table_name + ' using Cassandra DB for the keyspace : '+self.keySpaceName)

        self.log_object.logToFile('debug', 'Using the keyspace....')
        self.session.execute('USE "' + self.keySpaceName + '"')

        self.log_object.logToFile('debug', 'Creating the CQL Query....')

        cql_query = "SELECT * FROM system_schema.columns WHERE table_name = '" + table_name + "' AND keyspace_name = '" + self.keySpaceName + "' ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)

        result = [i[2] for i in records]

        self.log_object.logToFile('debug', 'The query got executed successfully....')

        self.log_object.logToFile('info', 'The table schema has been generated successfully for Cassandra DB....')
        self.log_object.logToFile('info', 'Closing the Cassandra database connection....')

        self.cluster.shutdown()

        return result


    ################################################
    #     4) Insert Single Record :                #
    ################################################

    def insert_into_table_single_record(self, table_name, insert_fields):

        '''

        Functionality : Inserting a single record in the given table.
        :param table_name: The name of the table in the keyspace where the record needs to be inserted.
        :param insert_fields: The dictionary of field and value pair for the record which needs to be inserted in the table.
        :return: None

        '''

        self.log_object.logToFile('info', 'Inserting single record into the table : ' + table_name + ' using Cassandra DB for the keyspace : '+self.keySpaceName)

        self.log_object.logToFile('debug', 'Using the keyspace....')
        self.session.execute('USE "' + self.keySpaceName + '"')

        self.log_object.logToFile('debug', 'Creating the CQL Query for describing table....')

        cql_query = "SELECT * FROM system_schema.columns WHERE table_name = '" + table_name + "' AND keyspace_name = '" + self.keySpaceName + "' ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)

        result = [i[8] for i in records]

        field_string = "("
        value_string = "VALUES("

        for field_no in range(0, len(insert_fields)):
            field_string += list(insert_fields.keys())[field_no]

            if result[field_no].lower() == 'int':

                value_string += list(insert_fields.values())[field_no]

            elif result[field_no].lower() == 'text':

                value_string += "'" + list(insert_fields.values())[field_no] + "'"

            if field_no != len(insert_fields) - 1:
                field_string += ","
                value_string += ","
            else:
                field_string += ")"
                value_string += ")"

        self.log_object.logToFile('debug', 'Creating the CQL Query....')

        cql_string = "INSERT INTO " + table_name + " " + field_string + " " + value_string

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_string)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.session.execute(cql_string)

        self.log_object.logToFile('debug', 'The query got executed successfully....')

        self.log_object.logToFile('info', 'The record got inserted successfully in Cassandra DB....')
        self.log_object.logToFile('info', 'Closing the Cassandra database connection....')

        self.cluster.shutdown()


    ################################################
    #     5) Insert Multiple Records :             #
    ################################################

    def insert_into_table_multiple_records(self, table_name, headers, values):

        '''

        Functionality : Inserting multiple data records in the given table.
        :param table_name: The name of the table in the keyspace where the records need to be inserted.
        :param headers: The list of the field headers defining the fields in the table.
        :param values: The list of all the values of all the records to be inserted in the table.
        :return: None

        '''

        self.log_object.logToFile('info', 'Inserting multiple records into the table : ' + table_name + ' using Cassandra DB for the keyspace : '+self.keySpaceName)

        self.log_object.logToFile('debug', 'Using the keyspace....')
        self.session.execute('USE "' + self.keySpaceName + '"')

        self.log_object.logToFile('debug', 'Creating the CQL Query for describing table....')

        cql_query = "SELECT * FROM system_schema.columns WHERE table_name = '" + table_name + "' AND keyspace_name = '" + self.keySpaceName + "' ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)

        result = [i[8] for i in records]

        for record_idx, record_value in enumerate(values):

            initial_string = "INSERT INTO " + table_name

            if len(headers) > 0:

                header_string = " ("

                for idx, header_value in enumerate(headers):

                    header_string += header_value

                    if idx != len(headers) - 1:

                        header_string += ","

                    else:

                        header_string += ")"

            else:

                header_string = ""

            value_string = "VALUES("

            for val_idx, current_value in enumerate(record_value):

                if result[val_idx].lower() == 'int':

                    value_string += current_value

                elif result[val_idx].lower() == 'text':

                    value_string += "'" + current_value + "'"

                if val_idx != len(record_value) - 1:

                    value_string += ","

                else:

                    value_string += ")"

            self.log_object.logToFile('debug',
                                      'Creating the CQL Query for record no. : ' + str(record_idx) + '....')

            cql_string = initial_string + header_string + " " + value_string

            self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_string)
            self.log_object.logToFile('debug', 'Executing the query for record no. : ' + str(record_idx) + '....')

            self.session.execute(cql_string)

            self.log_object.logToFile('debug', 'The query got executed successfully....')
            self.log_object.logToFile('info', 'The record got inserted successfully....')

        self.log_object.logToFile('info', 'All the records got inserted successfully in Cassandra DB....')
        self.log_object.logToFile('info', 'Closing the Cassandra database connection....')

        self.cluster.shutdown()

    ###################################################
    #     6) Fetching Records From Collection :       #
    ###################################################

    def select_records(self, table_name, conditional_fields, rowLimit):

        '''

        Functionality : Fetching the list of headers and values from the specified table as per the conditions and row limit if required.
        :param table_name: The name of the table in the keyspace from where the records need to be fetched.
        :param conditional_fields: The dictionary of conditional fields to be checked while fetching the records, if required.
        :param rowLimit: The row limit defining the number of records to be returned from the table.
        :return: headers, results

        '''

        self.log_object.logToFile('info', 'Downloading table data from the table : ' + table_name + "' AND keyspace_name = '" + self.keySpaceName)

        self.log_object.logToFile('debug', 'Using the keyspace....')
        self.session.execute('USE "' + self.keySpaceName + '"')

        self.log_object.logToFile('debug', 'Creating the CQL Query for describing table....')

        cql_query = "SELECT * FROM system_schema.columns WHERE table_name = '" + table_name + "' AND keyspace_name = '" + self.keySpaceName + "' ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)

        field_types = {i[2]:i[8] for i in records}

        headers = [i for i in list(field_types.keys())]

        initial_string = "SELECT * FROM " + table_name

        if len(conditional_fields) == 0:

            conditional_string = ""

        else:

            conditional_string = " WHERE "

            for idx, condition in enumerate(conditional_fields):

                conditional_string += condition[0].strip() + " "
                value = ""

                if condition[0] in list(field_types.keys()):
                    if field_types[condition[0]] == 'int':

                        if condition[1].lower() == 'in':
                            for i in condition[2].split(","):
                                value += i.replace("'", "").strip() + ","
                            value = value.rstrip(",")

                        else:
                            value = condition[2] + " "

                    else:

                        if condition[1].lower() == 'in':
                            for i in condition[2].split(","):
                                value += "'" + i.replace("'", "").strip() + "',"
                            value = value.rstrip(",")

                        else:

                            value = "'" + condition[2] + "' "

                if condition[1].lower() == "equals":
                    conditional_operator = "="
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "

                elif condition[1].lower() == "not equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than":
                    conditional_operator = ">"
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than equals":
                    conditional_operator = ">="
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than":
                    conditional_operator = "<"
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than equals":
                    conditional_operator = "<="
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "

                elif condition[1].lower() == "in":
                    conditional_operator = "IN({0})"
                    conditional_string += conditional_operator.format(value) + \
                                          + condition[3] + " "

        if rowLimit != "":
            limiting_string = " LIMIT " + str(rowLimit)
        else:
            limiting_string = ""

        self.log_object.logToFile('debug', 'Creating the CQL Query....')

        cql_query = initial_string + conditional_string + limiting_string + " ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)
        results = []

        for row in records :
            results.append(list(row))

        self.log_object.logToFile('info', 'Closing the Cassandra database connection....')
        self.cluster.shutdown()

        return headers, results

    ###################################################
    #     7) Deleting Records :                       #
    ###################################################

    def delete_records(self,table_name, conditional_fields):

        '''

        Functionality : Deleting records from a given table based on condition, if required.
        :param table_name: The name of the table in the keyspace from where the records need to be deleted.
        :param conditional_fields: The dictionary of conditional fields to be checked while deleting the records, if required.
        :return: None

        '''

        self.log_object.logToFile('info', 'Deleting data from the table : ' + table_name + "' AND keyspace_name = '" + self.keySpaceName)

        self.log_object.logToFile('debug', 'Using the keyspace....')
        self.session.execute('USE "' + self.keySpaceName + '"')

        self.log_object.logToFile('debug', 'Creating the CQL Query for describing table....')

        cql_query = "SELECT * FROM system_schema.columns WHERE table_name = '" + table_name + "' AND keyspace_name = '" + self.keySpaceName + "' ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)

        field_types= {}

        for element in records:
            if element[5] == 'partition_key':
                partition_field = element[2]

            field_types[element[2]] = element[8]

        initial_string = "SELECT "+partition_field+" FROM " + table_name

        if len(conditional_fields) == 0:
            conditional_string = ""

        else:
            conditional_string = " WHERE "

            for idx, condition in enumerate(conditional_fields):

                conditional_string += condition[0].strip() + " "
                value = ""

                if condition[0] in list(field_types.keys()):
                    if field_types[condition[0]] == 'int':

                        if condition[1].lower() == 'in':
                            for i in condition[2].split(","):
                                value += i.replace("'","").strip()+","
                            value = value.rstrip(",")

                        else:
                            value = condition[2] + " "

                    else:

                        if condition[1].lower() == 'in':
                            for i in condition[2].split(","):
                                value += "'"+i.replace("'","").strip()+"',"

                            value = value.rstrip(",")

                        else :

                            value = "'" + condition[2] + "' "

                if condition[1].lower() == "equals":
                    conditional_operator = "="
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "not equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than":
                    conditional_operator = ">"
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than equals":
                    conditional_operator = ">="
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than":
                    conditional_operator = "<"
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "
                elif condition[1].lower() == "less than equals":
                    conditional_operator = "<="
                    conditional_string += conditional_operator + value + \
                                          condition[3] + " "

                elif condition[1].lower() == "in":
                    conditional_operator = "IN({0})"
                    conditional_string += conditional_operator.format(value) \
                                          + condition[3] + " "

        self.log_object.logToFile('debug', 'Creating the CQL Query....')

        cql_query = initial_string + conditional_string + " ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)
        results = []

        for row in records:
            results.append(list(row))

        if len(results) == 0:

            self.log_object.logToFile('exception',"No search results are found for the given filter conditions to be deleted.")

            self.log_object.logToFile('info', 'Closing the Cassandra database connection....')
            self.cluster.shutdown()

            raise Exception("No search results are found for the given filter conditions to be deleted.")

        self.log_object.logToFile('debug', 'Creating the Delete CQL Query....')

        delete_sql_query = "DELETE FROM "+table_name+" WHERE "+partition_field+" IN ("

        for idx, partition_field_value in enumerate(results):

            delete_sql_query += str(partition_field_value[0])

            if idx == len(results)-1 :
                delete_sql_query+=")"
            else :
                delete_sql_query+=","

        self.log_object.logToFile('debug', 'Delete CQL query got created as : ' + delete_sql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(delete_sql_query)

        self.log_object.logToFile('info', 'Closing the Cassandra database connection....')
        self.cluster.shutdown()


    ###################################################
    #     8) Updating Records :                       #
    ###################################################

    def update_table(self,table_name, fields_to_be_updated, conditional_fields):

        '''

        Functionality : Updating records in a given table based on condition, if required.
        :param table_name: The name of the table in the keyspace where the records need to be updated.
        :param fields_to_be_updated: The dictionary of fields and the values which the existing fields need to be updated to.
        :param conditional_fields: The dictionary of conditional fields to be checked while updated the records, if required.
        :return: None

        '''

        self.log_object.logToFile('info', 'Updating table data : ' + table_name + "' AND keyspace_name = '" + self.keySpaceName)

        self.log_object.logToFile('debug', 'Using the keyspace....')
        self.session.execute('USE "' + self.keySpaceName + '"')

        self.log_object.logToFile('debug', 'Creating the CQL Query for describing table....')

        cql_query = "SELECT * FROM system_schema.columns WHERE table_name = '" + table_name + "' AND keyspace_name = '" + self.keySpaceName + "' ALLOW FILTERING"

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        records = self.session.execute(cql_query)

        field_types = {}

        for element in records:
            field_types[element[2]] = element[8]

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
                value = ""

                if condition[0] in list(field_types.keys()):
                    if field_types[condition[0]] == 'int':

                        if condition[1].lower() == 'in':
                            for i in condition[2].split(","):
                                value += i.replace("'", "").strip() + ","
                            value = value.rstrip(",")

                        else:
                            value = condition[2] + " "

                    else:

                        if condition[1].lower() == 'in':
                            for i in condition[2].split(","):
                                value += "'" + i.replace("'", "").strip() + "',"

                            value = value.rstrip(",")

                        else:

                            value = "'" + condition[2] + "' "

                if condition[1].lower() == "equals":
                    conditional_operator = "="
                    conditional_string += conditional_operator + value +  \
                                          condition[3] + " "
                elif condition[1].lower() == "not equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + value +  \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + value +  \
                                          condition[3] + " "
                elif condition[1].lower() == "greater than equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + value +  \
                                          condition[3] + " "
                elif condition[1].lower() == "less than":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator  + value +  \
                                          condition[3] + " "
                elif condition[1].lower() == "less than equals":
                    conditional_operator = "<>"
                    conditional_string += conditional_operator + value +  \
                                          condition[3] + " "
                elif condition[1].lower() == "in":
                    conditional_operator = "IN({0})"
                    conditional_string += conditional_operator.format(value) \
                                          + condition[3] + " "

        self.log_object.logToFile('debug', 'Creating the CQL Query....')

        cql_query = initial_string + update_string + conditional_string

        self.log_object.logToFile('debug', 'CQL query got created as : ' + cql_query)
        self.log_object.logToFile('debug', 'Executing the query....')

        self.session.execute(cql_query)

        self.log_object.logToFile('info', 'Closing the Cassandra database connection....')
        self.cluster.shutdown()

##########################################################################################################################################
#                                                 End Block : Cassandra Operation Functions :                                            #
##########################################################################################################################################