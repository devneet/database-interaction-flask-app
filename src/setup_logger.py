##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################
# Author : Devneet Mohanty                                                                                                               #
# Project Name : Database Interaction Application                                                                                        #
# Project Description : This project has been developed using the Flask libraries in order to create a web application which is able to  #
#                       interact with MySQL, MongoDB and Cassandra database.                                                             #
# Python File Description : The setup_logger.py file consists of the logging functions for different type of logging and debugging       #
#                           functionalities within the entire project.                                                                   #
#                           required for routing within the web application and call other backend functions.                            #
# Date Of Development : 29-05-2021                                                                                                       #
##########################################################################################################################################
#                                                       Header Block :                                                                   #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Importing Libraries & Initializing Variables :                           #
##########################################################################################################################################

import logging as lg
import socket
import datetime

##########################################################################################################################################
#                                                 End Block : Importing Libraries & Initializing Variables :                             #
##########################################################################################################################################


##########################################################################################################################################
#                                                 Start Block : Log Operation Functions :                                                #
##########################################################################################################################################

class logger :

    ################################################
    #     1) Initialising Function :               #
    ################################################

    def __init__(self):

        '''

        Functionality : Initialising the basic configuration setup for the logs to be generated using the logging library.

        '''

        currentDate =  datetime.datetime.now().strftime("%d%m%y")

        lg.basicConfig(filemode='a', filename='flask-app_'+currentDate+'.log',
                       format='%(asctime)s | %(name)s | %(levelname)s | %(message)s', datefmt='%d-%b-%Y %H:%M:%S', level=lg.DEBUG)

        self.log_obj = lg.getLogger(socket.gethostname())


    ################################################
    #     2) Log To File Function :                #
    ################################################

    def logToFile(self,logLevel,logMessage):

        '''

        Functionality : Logging different log messages having different log levels in order to debug issues if any.

        :param logLevel: The level of logging to be used for the current log message.
        :param logMessage:  The message to be logged for current function call.
        :return: None

        '''

        if logLevel.lower() == 'error':

            self.log_obj.error(logMessage)

        elif logLevel.lower() == 'exception':

            self.log_obj.exception(logMessage)

        elif logLevel.lower() == 'critical':

            self.log_obj.critical(logMessage)

        elif logLevel.lower() == 'debug':

            self.log_obj.debug(logMessage)

        elif logLevel.lower() == 'info':

            self.log_obj.info(logMessage)

        elif logLevel.lower() == 'warn':

            self.log_obj.warning(logMessage)

##########################################################################################################################################
#                                                 End Block : Log Operation Functions :                                                  #
##########################################################################################################################################
