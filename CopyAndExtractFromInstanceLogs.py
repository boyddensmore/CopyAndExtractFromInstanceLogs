import xml.etree.ElementTree as ET
import datetime
import os
from Queue import Queue
from threading import Thread
import subprocess
import fnmatch
import re


def robocopy(name, sourcedir, destdir, logfilter, logfile):
    if not os.path.exists(destdir):
        os.makedirs(destdir)

    print "Working on " + name

    command = "robocopy " + sourcedir + " " + destdir + " " + logfilter + " /MAXAGE:1 /log+:" + logfile + " /NP /NJH"
    # command = "robocopy " + sourcedir + " " + destdir + " " + logfilter + " /log+:" + logfile + " /NP /NJH"

    output = None
    try:
        output = subprocess.check_output(command)
    except subprocess.CalledProcessError:
        if output:
            if output.returncode == 16:
                print "Copy of " + name + " complete.  ***FATAL ERROR***"
            elif output.returncode == 15:
                print "Copy of " + name + " complete.  OKCOPY + FAIL + MISMATCHES + XTRA"
            elif output.returncode == 14:
                print "Copy of " + name + " complete.  FAIL + MISMATCHES + XTRA"
            elif output.returncode == 13:
                print "Copy of " + name + " complete.  OKCOPY + FAIL + MISMATCHES"
            elif output.returncode == 12:
                print "Copy of " + name + " complete.  FAIL + MISMATCHES& goto end"
            elif output.returncode == 11:
                print "Copy of " + name + " complete.  OKCOPY + FAIL + XTRA"
            elif output.returncode == 10:
                print "Copy of " + name + " complete.  FAIL + XTRA"
            elif output.returncode == 9:
                print "Copy of " + name + " complete.  OKCOPY + FAIL"
            elif output.returncode == 8:
                print "Copy of " + name + " complete.  FAIL"
            elif output.returncode == 7:
                print "Copy of " + name + " complete.  OKCOPY + MISMATCHES + XTRA"
            elif output.returncode == 6:
                print "Copy of " + name + " complete.  MISMATCHES + XTRA"
            elif output.returncode == 5:
                print "Copy of " + name + " complete.  OKCOPY + MISMATCHES"
            elif output.returncode == 4:
                print "Copy of " + name + " complete.  MISMATCHES"
            elif output.returncode == 3:
                print "Copy of " + name + " complete.  OKCOPY + XTRA"
            elif output.returncode == 2:
                print "Copy of " + name + " complete.  XTRA"
            elif output.returncode == 1:
                print "Copy of " + name + " complete.  OKCOPY"
            elif output.returncode == 0:
                print "Copy of " + name + " complete.  No Change"
    print "Completed " + name


class RCWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            name, sourcedir, destdir, logfilter, logfile = self.queue.get()
            robocopy(name, sourcedir, destdir, logfilter, logfile)
            print "Estimated instances left in queue: " + str(queue.qsize())
            self.queue.task_done()


copyflag = True
extractflag = True

currdate = datetime.date.today().strftime("%y-%m-%d")
rootdir = "C:\\Temp\\Logs\\LongRunningSQLLogs"

tree = ET.parse("C:\\Temp\\Logs\\LongRunningSQLLogs\\instancedetails.xml")
root = tree.getroot()

if copyflag:

    # Create a queue to communicate with the worker threads
    queue = Queue()

    # Create 8 worker threads
    for x in range(8):
        worker = RCWorker(queue)
        # Setting daemon to True will let the main thread exit even though the workers are blocking
        worker.daemon = True
        worker.start()

    # Load instance details from xml into the queue
    for instance in root.findall("instance"):
        name = instance.get("name")
        sourcedir = instance.find("sourcedir").text
        logfilter = instance.find("logfilter").text
        destdir = rootdir + "\\CopiedLogs\\" + name + "\\" + currdate
        logfile = rootdir + "\\RCLogsLogs\\RCLog-" + name + "-" + currdate + ".log"
        # Delete today's logfile, if it exists.
        try:
            os.remove(logfile)
        except OSError:
            pass
        # print name, sourcedir, destdir
        queue.put((name, sourcedir, destdir, logfilter, logfile))

    # Causes the main thread to wait for the queue to finish processing all the tasks
    queue.join()

if extractflag:
    # Extract each vessel's logs into a single file
    extractlogdir = rootdir + "\\ExtractedLogs"
    extractlogfile = None

    print "Copy complete.  Extracting."

    for instance in root.findall("instance"):
        name = instance.get("name")
        extractlogfile = extractlogdir + "\\ExtractedLogs-" + name + "-" + currdate + ".log"
        print "Extracting " + extractlogfile
        # Delete today's extracted file, if it exists
        try:
            os.remove(extractlogfile)
        except OSError:
                pass

        instancelogdir = rootdir + "\\CopiedLogs\\" + name
        print "Starting extract to " + extractlogfile

        with open(extractlogfile, "w") as outfile:
            for root, dirs, files in os.walk(instancelogdir):
                for file in files:
                    if fnmatch.fnmatch(file, "*"):
                        filepath = root + os.sep + file
                        print "Extracting from " + filepath
                        logfile = open(filepath, "r")

                        for line in logfile:
                            if re.match("(.*)execution took(.*)", line):
                                outfile.write(line)
        outfile.close()

        print "Finished extract for " + extractlogfile

    # Delete all empty extract files
    print "Deleting all empty extract files."
    for root, dirs, files in os.walk(extractlogdir):
        for file in files:
            fullname = os.path.join(root, file)
            try:
                if os.path.getsize(fullname) == 0:
                    print fullname + " is empty.  Removing it."
                    os.remove(fullname)
            except WindowsError:
                continue
