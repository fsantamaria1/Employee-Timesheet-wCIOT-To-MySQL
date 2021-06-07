import csv
import pathlib
import os
import glob
from datetime import *
import time
import shutil
import pandas as pd
import sqlite3
import MySQLdb

# CREATE HEADERS
headers = [
    "ASSOCIATE_ID",
    "WORKER_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "PAY_PERIOD_START",
    "PAY_PERIOD_END",
    "DATE",
    "REGULAR",
    "OVERTIME",
    "SALARY",
    "EXCEPTIONS",
    "CLOCK_IN_TIME_1",
    "CLOCK_OUT_TIME_1",
    "CLOCK_IN_TIME_2",
    "CLOCK_OUT_TIME_2"
]
dbHeaders = [
    "BATCH_ID",
    "ASSOCIATE_ID",
    "WORKER_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "PAY_PERIOD_START",
    "PAY_PERIOD_END",
    "DATE",
    "REGULAR",
    "OVERTIME",
    "SALARY",
    "EXCEPTIONS",
    "CLOCK_IN_TIME_1",
    "CLOCK_OUT_TIME_1",
    "CLOCK_IN_TIME_2",
    "CLOCK_OUT_TIME_2",
    "STATUS"
]

requiredHeaders = [
    "ASSOCIATE_ID",
    "WORKER_ID",
    "FIRST_NAME",
    "LAST_NAME",
    "PAY_PERIOD_START",
    "PAY_PERIOD_END",
    "DATE"
]


class CSV:
    def findCSV(path, message):
        path = path
        csvFiles = []
        fileCount = 0

        print("Looking for {0} files...".format(str(message)))
        for file in glob.glob(r"{0}\*.csv".format(path)):
            csvFiles.append(file)
            fileCount += 1
        print("{0} Files found: ".format(str(message)), fileCount)
        if len(csvFiles) >= 1:
            return csvFiles[0]
        else:
            return "Error"

    #############################################################
    ##Open file and replace headers to match database
    def replaceHeaders(file1, file2):
        oldFileName = file1
        newFileName = file2
        if oldFileName != "File":
            with open(str(oldFileName), "r") as fp:
                reader = csv.DictReader(fp, fieldnames=headers, restval="")
                with open(newFileName, 'w', newline='') as fh:
                    writer = csv.DictWriter(fh, fieldnames=reader.fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    header_mapping = next(reader)
                    writer.writerows(reader)


class DF:
    def dataframeFile(file1, ID, CD, CT, NFP):
        ##    def dataframeFile():
        ##Initialize variables
        eventID = ID
        newFileName = file1
        currentDate = CD
        currentT = CT
        importPath = NFP

        # Read CVS to be edited
        file = pd.read_csv(
            newFileName,
            header=0,
            usecols=headers,
            parse_dates=True)
        file.columns = headers

        ####MySQL Statements
        ##Delete records with the same date to write new ones
        # Create connection
        conn = MySQLdb.connect(host="192.168.2.227", port=3306, user="MyVM", password="BerryDev20!", db="berrydb")
        # Select statement select 2nd row
        select_statement = "SELECT * FROM clock_in_records where DATE = '{0}';".format(str(file.at[2, 'DATE']))
        ##delete statement selects 2nd row
        delete_statement = "DELETE FROM clock_in_records where DATE = '{0}';".format(str(file.at[2, 'DATE']))

        # Create cursor
        cursor = conn.cursor()

        ##Assing number of selected records to variable
        numberOfSelectedRecords = cursor.execute(select_statement)
        if numberOfSelectedRecords == 0:
            # String will be added to dataframe
            status = "Not Confirmed"
        elif numberOfSelectedRecords > 0:
            ##Delete old records
            cursor.execute(delete_statement)
            # String will be added to dataframe
            status = "Confirmed"

        ##Escape and commit changes
        conn.escape_string(select_statement)
        conn.escape_string(delete_statement)
        conn.commit()
        # End of MySQL Statements

        ##Convert strings holding dates to date format
        file['PAY_PERIOD_START'] = pd.to_datetime(file.PAY_PERIOD_START)
        file['PAY_PERIOD_END'] = pd.to_datetime(file.PAY_PERIOD_END)
        file['DATE'] = pd.to_datetime(file.DATE)
        ##Insert variable which will be used as the primary key
        file.insert(0, 'BATCH_ID', eventID)
        ##        file['LOAD_DATE_TIME']= currentDate + " " +currentT
        file['STATUS'] = status
        # gets rid of columns with empy records
        file.dropna(subset=requiredHeaders, inplace=True)
        # create New CSV for DATABASE transfer
        file.to_csv(r'{0}\{1}.csv'.format(importPath, eventID), encoding='utf-8', index=False)
        ##Print columns
        ##print(file['UNITS'])
        print(file.head())
        print(file)


class Files:
    def moveFile(rootPath, fileNameOld, fileNameNew, oldFilePath, newFilePath):
        rootPath = rootPath
        fileNameOld = os.path.basename(fileNameOld)
        fileNameNew = os.path.basename(fileNameNew)

        ##        try:
        ##        shutil.move(r'Y:\ADP Import\{0}{1}'.format(file2Name, '.csv'), r'Y:\ADP Import\Archive\{0}{1}'.format(file2Name, '.csv'))
        shutil.move(r'{0}\{1}'.format(rootPath, fileNameOld), r'{0}\{1}'.format(oldFilePath, fileNameOld))
        shutil.move(r'{0}\{1}'.format(rootPath, fileNameNew), r'{0}\{1}'.format(newFilePath, fileNameNew))

    ##            shutil.move(r'{0}'.format(fileNameNew), r'C:\Users\dev\Desktop\DB\New\New\{0}'.format(fileNameNew))
    ##        except:
    ##            print("Error moving files to Output Folder")
    ##directory = createDirectory(str(yesterday))
    ##        moveFile(oldFileName, newFileName, oldFilePath, newFilePath)
    def moveErrorFile(rootPath, oldErrorFile, newErrorFile, filePath):
        try:
            rootPath = rootPath
            oldErrorFile = os.path.basename(oldErrorFile)
            newErrorFile = os.path.basename(newErrorFile)
            ##        shutil.move(r'Y:\ADP Import\{0}{1}'.format(file2Name, '.csv'), r'Y:\ADP Import\Archive\{0}{1}'.format(file2Name, '.csv'))
            shutil.move(r'{0}'.format(rootPath, oldErrorFile), r'{0}\{1}'.format(filePath, oldErrorFile))
            shutil.move(r'{0}'.format(rootPath, newErrorFile), r'{0}\{1}'.format(filePath, str("Error", newErrorFile)))
        except:
            print("Error moving files to FilesWithErrors folder")


class SQL:
    def setConnection():
        # Connection statement
        ##        conn = MySQLdb.connect(host="localhost", port=3306, user="root", passwd="BerryDev2020!", db="berrydb")
        conn = MySQLdb.connect(host="192.168.2.227", port=3306, user="MyVM", password="BerryDev20!", db="berrydb")
        cursor = conn.cursor()
        # Creates table
        createTable = '''CREATE TABLE IF NOT EXISTS clock_in_records(ID INT AUTO_INCREMENT PRIMARY KEY, BATCH_ID TEXT NOT NULL, ASSOCIATE_ID TEXT NOT NULL, WORKER_ID TEXT NOT NULL, FIRST_NAME TEXT NOT NULL, LAST_NAME TEXT NOT NULL, PAY_PERIOD_START DATE NOT NULL, PAY_PERIOD_END DATE NOT NULL, DATE DATE NOT NULL, REGULAR DECIMAL(3,2), OVERTIME DECIMAL(3,2), SALARY DECIMAL(3,2), EXCEPTIONS TEXT, CLOCK_IN_TIME_1 DATETIME, CLOCK_OUT_TIME_1 DATETIME, CLOCK_IN_TIME_2 DATETIME, CLOCK_OUT_TIME_2 DATETIME, STATUS TEXT NOT NULL)'''
        cursor.execute(createTable)
        ##Disables strict mode (null integer records beoome 0's)
        cursor.execute("SET GLOBAL sql_mode = 'NO_ENGINE_SUBSTITUTION';")

    def insertCSV(filePath1):
        # filePath = filePath
        # Opens CSV File
        with open(r'{0}'.format(filePath1)) as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            ##will hold a list of keys (column headers)
            keyList = []
            ##Will hold a list of values (cell contents)
            valueList = []

            for row in reader:
                ##Will hold multiple %s in string format
                placeHolderList = ''
                keyList.clear()
                valueList.clear()
                # Iterates through headers
                for header in dbHeaders:
                    # Skips headers with empty cells
                    if row[header] != '':
                        keyList.append(header)
                        valueList.append(row[header])
                        if placeHolderList == '':
                            placeHolderList = '%s'
                        else:
                            placeHolderList += ',%s'

                ##Add list to variable which will be used by the insert statement since it contains the necessary values
                ##                val = [(valueList)]
                ##Wil be added to the insert statement, needs to be in string format
                keyString = ','.join(keyList)

                # Create connection
                conn = MySQLdb.connect(host="192.168.2.227", port=3306, user="MyVM", password="BerryDev20!",
                                       db="berrydb")

                # Insert statement
                insert_statement = """INSERT INTO clock_in_records({0})
                                        VALUES ({1})""".format(str(keyString), str(placeHolderList))
                # Create cursor
                cur = conn.cursor()

                # Execute insert statement
                # if row['STATUS'] != 'Confirmed'
                if row['ASSOCIATE_ID'] != "":
                    cur.executemany(insert_statement, [(valueList)])

                ##Escape and commit changes
                conn.escape_string(insert_statement)
                conn.commit()

        os.remove(r"{0}".format(filePath1))


########################################################################################################################################################################
def _main_():
    try:
        # Path of the original files
        # rootFolderPath = r"Y:\Timesheets\Labor Hour Imports\CSVs"
        rootFolderPath = r"C:\Python\Test"
        # Path of the final files that will be imported into MySQL
        # newFolderPath = r"C:\DBFiles"
        newFolderPath = r"C:\Python\Test\Dataframes"
        # Current time will be added as a field
        currentT = datetime.now().strftime('%H:%M:%S')
        # Current date will be added to the same field as currentT
        currentDate = datetime.now().strftime('%Y-%m-%d')
        # Event ID will be used as the primary key
        eventID = str(datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
        ###########################################################
        # Path where the original files will be moved to
        # oldFilePath = r"Y:\Timesheets\Labor Hour Imports\CSVs\Archived"
        oldFilePath = r"C:\Python\Test\Archived"
        # Path where the new files will be moved to
        # newFilePath = r"Y:\Timesheets\Labor Hour Imports\CSVs\Archived\Output"
        newFilePath = r"C:\Python\Test\Dataframes\Archived"
        print("\nEvent ID: {0} \n".format(eventID))
        ##        newFileName = "{0}-Output.csv".format(eventID)
        ##        print(newFileName)
        ##########################################################
        oldFileName = "File"
        oldFileName = CSV.findCSV(rootFolderPath, "CSV")
        ##        print(oldFileName)
        ##        eventID = str(datetime.now().strftime('%Y%m%d-%H%M%S-%f'))
        newFileName = r"{0}\{1}-Output.csv".format(rootFolderPath, eventID)
        CSV.replaceHeaders(oldFileName, newFileName)
        ##        CSV.replaceHeaders(oldFileName, newFileName)
        ##        oldFilePath = 'Input'
        ##        newFilePath = 'Output'
        DF.dataframeFile(newFileName, eventID, currentDate, currentT, newFolderPath)
        try:
            Files.moveFile(rootFolderPath, oldFileName, newFileName, oldFilePath, newFilePath)
        except:
            errorFilePath = r"Y:\Timesheets\Labor Hour Imports\CSVs\Archived\FilesWithErrors"
            Files.moveErrorFile(rootFolderPath, oldFileName, newFileName, errorFilePath)
        name = CSV.findCSV(newFolderPath, "DataFrame")
        SQL.setConnection()
        SQL.insertCSV(name)
        print("Records added to MySQL")
    except:
        time.sleep(5)


while True:
    _main_()

