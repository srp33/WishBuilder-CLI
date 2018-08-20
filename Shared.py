import os, datetime
from Constants import *

def printToLog(message, pr=None):
    modMessage = "{:%Y-%m-%d %H:%M:%S} | {}\n".format(datetime.datetime.now(), message)

    print(modMessage, flush=True)

    if pr != None and pr.log_file_path != None:
        #print("Logging to {}.".format(pr.log_file_path))
        with open(pr.log_file_path, 'a') as logFile:
            logFile.write(modMessage)
