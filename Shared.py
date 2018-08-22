import datetime, os, subprocess, sys
from Constants import *

def printToLog(message, pr=None):
    modMessage = "{:%Y-%m-%d %H:%M:%S} | {}\n".format(datetime.datetime.now(), message)

    print(modMessage.rstrip("\n"), flush=True)

    if pr != None and pr.log_file_path != None:
        #print("Logging to {}.".format(pr.log_file_path))
        if os.path.exists(os.path.dirname(pr.log_file_path)):
            with open(pr.log_file_path, 'a') as logFile:
                logFile.write(modMessage)
        else:
            print("Could not write log message to {}.".format(pr.log_file_path))

def execShellCommand(command):
    try:
        retcode = subprocess.call(command, shell=True)
        if retcode < 0:
            print("Child was terminated by signal", -retcode, file=sys.stderr)
#        else:
#            print("Child returned", retcode, file=sys.stderr)

        return retcode
    except OSError as e:
        print("Execution failed:", e, file=sys.stderr)
        return 1
