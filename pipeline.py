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
    extractor = e.Extract()
    query = "select top(3) * from Regsosek22.dbo.tab_view_k where [r407] <= 4 AND [r427] IS NULL"
    queryResult, columns = extractor.FromDb(sqlServerConnection, query)

    # Transform to dataframe
    transformer = t.Transform()
    transformedData = transformer.queryResultToDF(queryResult, columns)
    transformedData.head()

    # Load to csv
    loader = l.Load()
    loader.ToFSCSV(transformedData, 'hasil/test.csv')

