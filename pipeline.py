import os
import connector.database as dbConnector
import extract as e
import transform as t
import load as l
from dotenv import load_dotenv

def fromSQLServerToCSV():

    load_dotenv()
    
    sqlServerConnection = dbConnector.SQLServerConnection(driver=os.getenv('DRIVER'),
                                                        server=os.getenv('DB_SERVER'),
                                                        database = os.getenv('DATABASE'),
                                                        user_id = os.getenv('UID'),
                                                        password = os.getenv('PASSWORD'))
    sqlServerConnection.connect()

    # Extract from sql server db
    extract = e.Extract()
    fd = open('query/SQL-server/caregiver kode 3.sql', 'r')
    sqlFile = fd.read()
    fd.close()

    queryResult, columns = extract.FromDb(sqlServerConnection, sqlFile)

    # Transform to dataframe
    transform = t.Transform()
    transformedData = transform.queryResultToDF(queryResult, columns)
    transformedData.head()

    # Load to csv
    load = l.Load()
    load.ToFSCSV(transformedData, 'hasil/caregiver kode 3.csv')

def fromGsheetToGsheet():

    load_dotenv()

    extract = e.Extract()
    transform = t.Transform()
    load = l.Load()
    spreadSheetId='1YgCZwN6e4m4xeRkNvxisT-28fgWAJQqBpuDt9DqUhOE'
    sheetName='matrix anomali 06-02-2023'
    spreadSheetIdOutput="1gzkXxhW_Pe8lQyLp-OVHLiOOAI7zb6-wUhwgD7nrJk0"
    sheetNameOutput="matrix anomali"
    api_key=os.getenv('GSHEET_API_KEY')

    # extract
    response = extract.FromGsheet(spreadSheetId, sheetName, api_key)
    # transform
    dfData = transform.gSheetToDf(response)

    # load
    respones = load.toGsheet(spreadSheetIdOutput, sheetNameOutput, api_key, dfData)
    print(respones)
    # print(dfData)