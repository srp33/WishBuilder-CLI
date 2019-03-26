import datetime, os, os.path, subprocess, sys
from Constants import *
from Shared import *

def printToLog(message, pr=None):
    modMessage = "{:%Y-%m-%d %H:%M:%S}".format(datetime.datetime.now())

    if pr != None and pr.branch != None:
        modMessage += " | {}".format(pr.branch)

    modMessage += " | {}\n".format(message)

    print(modMessage.rstrip("\n"), flush=True)

    if pr != None and pr.log_file_path != None:
        #print("Logging to {}.".format(pr.log_file_path))
        if os.path.exists(os.path.dirname(pr.log_file_path)):
            with open(pr.log_file_path, 'a') as logFile:
                logFile.write(modMessage)
        else:
            print("Could not write log message to {}.".format(pr.log_file_path))

def execShellCommand(command, pr):
    printToLog(command, pr)
    retcode = subprocess.call(command, shell=True)

    if retcode < 0:
        printToLog("Child was terminated by signal {}".format(-retcode), pr)

    return retcode

def listdir_fullpath(directory: str) -> []:
    return [os.path.join(directory, file) for file in os.listdir(directory)]

def get_files(directory: str) -> []:
    files = []
    file_list = listdir_fullpath(directory)
    for file in file_list:
        if os.path.isdir(file):
            files.extend(get_files(file))
        else:
            files.append(file)
    return files

def parse_file_ext(file_path):
    return os.path.splitext(file_path)[1]
