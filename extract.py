import pandas as pd
import requests

class Extract :
    def FromDb(self, dbConnection , query):
        queryResult, columns = dbConnection.read(query)
        return queryResult, columns
    
    def FromCsv(self, path)->pd.DataFrame:
        return pd.read_csv(path)
    
    def FromGsheet(self, spreadSheetId, sheetName, api_key, api_ver='v4'):
        tempData = requests.get('https://sheets.googleapis.com/'+api_ver+'/spreadsheets/'+spreadSheetId+'/values/'+sheetName+'?key='+api_key)
        return tempData

