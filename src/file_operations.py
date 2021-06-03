##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################
# Author : Devneet Mohanty                                                                                                               #
# Project Name : Database Interaction Application                                                                                        #
# Project Description : This project has been developed using the Flask libraries in order to create a web application which is able to  #
#                       interact with MySQL, MongoDB and Cassandra database.                                                             #
# Python File Description : The file_operations.py file consists of different operations to interact with files present on both Flask    #
#                           server and local drive system along with operations related to JSON and CSV file formats.                    #
#                           required for routing within the web application and call other backend functions.                            #
# Date Of Development : 29-05-2021                                                                                                       #
##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Importing Libraries & Initializing Variables :                           #
##########################################################################################################################################

import csv
import time
from werkzeug.utils import secure_filename
from src import setup_logger
import os
import json

##########################################################################################################################################
#                                                 End Block : Importing Libraries & Initializing Variables :                             #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : File Operation Functions :                                               #
##########################################################################################################################################

class FileOperations :

    ################################################
    #     1) Initialising Function :               #
    ################################################
    
    def __init__(self):

        '''

        Functionality : Initialising the logging object required for generating logs for different file operations.

        '''

        self.log_object = setup_logger.logger()


    ################################################
    #     2) Download File From Flask Server :     #
    ################################################

    def saveFile(self,data_file,timeoutValue):

        '''

        Functionality : Downloading file in secure file format from Flask server on to local drive system.

        :param data_file: The data file object present in the encoded format on Flask server as a part of response.
        :param timeoutValue: The time out value in seconds indicating the time to wait for once the file has been downloaded to the local drive system.
        :return: None

        '''

        self.log_object.logToFile('info', 'Saving file from Flask server to local drive....')
        data_file.save(secure_filename(data_file.filename))
        time.sleep(timeoutValue)
        self.log_object.logToFile('info', 'The file got saved from Flask server to local drive....')


    ###########################################################
    #     3) Read CSV File Into List Of Values & Headers :    #
    ###########################################################

    def readCSVFile(self,filePath,includeHeader):

        '''

        Functionality : Reading data from a CSV file into a combination of list of headers and values if required.

        :param filePath: The file path of the CSV file consisting of the data which needs to be converted into the list of headers and values.
        :param includeHeader: The flag value indicating if the header is included in the CSV file or not. Possible values are : "on" and "off".
        :return: headers, values

        '''

        headers = []
        values = []

        self.log_object.logToFile('info', 'Reading CSV file from local drive....')

        file = open(filePath)
        content = csv.reader(file)

        for index,lines in enumerate(content):

            if includeHeader.lower() == 'on' and index == 0:
                headers = [i for i in lines]
                continue

            values.append(lines)

        self.log_object.logToFile('info', 'The CSV file has been read from local drive....')

        return headers,values

    ###########################################################
    #     4) Delete File From Local Drive Path :              #
    ###########################################################

    def deleteFile(self, filePath):

        '''

        Functionality : Deleting file present at a specific file path in the local drive system.
        :param filePath: The file path of the file which needs to be deleted from local drive system.
        :return: None

        '''

        try:

            self.log_object.logToFile('info', 'Deleting file from local drive with the following path : '+filePath+'....')
            os.remove(filePath)
            self.log_object.logToFile('info', 'The file has been removed from the local drive....')

        except  Exception as e:

            self.log_object.logToFile('error',
                                      str(e))
            raise Exception("There was an error while deleting the file with the following error details : "+str(e))

    ###########################################################
    #     5) Write List Of Headers & Values To CSV File :     #
    ###########################################################

    def writeToCSV(self,filepath,content,header):

        '''

        Functionality : Reading data from a CSV file into a combination of list of headers and values if required.
        :param filepath: The file path where the list of headers and content need to be written onto in CSV format.
        :param content: The list of content values to be written onto the CSV file.
        :param header: The list of header values to be written onto the CSV file.
        :return: None

        '''

        self.log_object.logToFile('info',
                                  'Writing CSV data into the file with the following path : ' + filepath + '....')

        with open(filepath,'w', encoding='UTF8', newline='') as file :
            writer_object = csv.writer(file)
            writer_object.writerow(header)
            writer_object.writerows(content)

    ###############################################################
    #     6) Delete Files With Extension From Local Drive Path :  #
    ###############################################################

    def removeFilesWithExtension(self, folderPath, fileExtenstion):

        '''

        Functionality : Deleting all the files present in a specific directory with a specific extension.
        :param folderPath: The folder path of the directory from where the files need to be deleted.
        :param fileExtenstion: The file extension of the files to be deleted.
        :return: None

        '''

        try:

            self.log_object.logToFile('info',
                                      'Deleting the files with extension : '+fileExtenstion+' , following path : ' + folderPath + '....')
            for file in os.listdir(folderPath):
                if file.find(fileExtenstion) > -1 :
                    self.deleteFile(file)

        except  Exception as e:

            self.log_object.logToFile('error',
                                      str(e))
            raise Exception("An error occurred while deleting all the files with the following error : "+str(e))


    ###############################################################
    #     7) Convert JSON String To JSON Data :                   #
    ###############################################################

    def convertStringToJson(self, jsonData):

        '''

        Functionality : Converting JSON string into JSON data format.
        :param jsonData: The JSON string which needs to be parsed into a JSON object.
        :return: data --> Returns false, in case of invalid JSON format.

        '''

        try:

            self.log_object.logToFile('info',
                                      'Converting the following data into JSON object : ' + jsonData + '....')
            data = json.loads(jsonData)
            return data

        except ValueError as e:

            self.log_object.logToFile('error',
                                      str(e))
            return False


    ############################################
    #     8) Read JSON File Into JSON Data :   #
    ############################################

    def readJsonFile(self, jsonFilePath):

        '''

        Functionality : Reading JSON file into JSON data format variable.
        :param jsonFilePath: The file path of the JSON file which needs to be parsed into a JSON object.
        :return: data --> Returns false, in case of invalid JSON format.

        '''

        try:

            self.log_object.logToFile('info',
                                      'Reading the JSON string from the following file path : ' + jsonFilePath + '....')

            file = open(jsonFilePath,'r')
            data = json.loads(file.read())

            return data

        except ValueError as e:

            self.log_object.logToFile('error',
                                      str(e))
            return False


    ############################################
    #     9) Read CSV File Into JSON Data :    #
    ############################################

    def csvToJson(self, csvFilePath):

        '''

        Functionality : Reading CSV file into JSON data format variable.
        :param csvFilePath: The file path of the CSV file which needs to be parsed into a JSON object.
        :return: data --> Returns false, in case of invalid JSON format.

        '''

        try:

            self.log_object.logToFile('info',
                                      'Converting the CSV file following file path to equivalent JSON object : ' + csvFilePath + '....')

            data = []

            with open(csvFilePath,mode='r',encoding='utf-8') as csv_file:
                csvReader = csv.DictReader(csv_file)

                for row in csvReader:
                    data.append(row)

            json.dumps(data, indent=4)

            return data

        except Exception as e :

            self.log_object.logToFile('error',
                                      str(e))
            return False


    ############################################
    #     10) Write JSON Data To JSON File :   #
    ############################################

    def writeToJsonFile(self, jsonData, jsonFileName):

        '''

        Functionality : Writing JSON data format variable into JSON file.
        :param jsonData: The JSON data which needs to be written on the file.
        :param jsonFileName: The file name of the JSON file which needs to be written.
        :return: None
        '''

        try:

            jsonFilePath = os.getcwd()+'\\'+jsonFileName

            self.log_object.logToFile('info',
                                     'Writing JSON data to following file path : ' + jsonFilePath + '....')

            with open(jsonFilePath, mode='w') as file :
                file.write(jsonData)
                file.close()

        except Exception as e :

            self.log_object.logToFile('error',
                                      str(e))

            raise Exception("An error occurred while converting the JSON data into CSV file format with the following details : "+str(e))


##########################################################################################################################################
#                                                 End Block : File Operation Functions :                                                 #
##########################################################################################################################################




