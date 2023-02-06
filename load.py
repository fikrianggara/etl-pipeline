import pandas as pd
import requests

class Load:
    def toGsheet(self, spreadSheetId, sheetName, api_key, data, api_ver='v4'):
        # tambahin logic send
        try:
            values = []
            values.append(data.columns.values)

            for item in range(1,len(data)):
                values.append(data[item])

            rangeStr = "'matrix anomali'!A1:AF"

            jsonObj = {
                "majorDimension":"ROWS",
                "range":rangeStr,
                "values":values
            }
            response = requests.put('https://sheets.googleapis.com/'+api_ver+'/spreadsheets/'+spreadSheetId+'/values/'+sheetName+'?key='+api_key, json=jsonObj)
            print(response)
            return response
        except Exception as e:
            print(e)

    def ToFSCSV(self, data, pathname):
        try :
            data.to_csv(pathname, index=False)
            print('success load to csv')
        except Exception as e:
            print(e)

    def ToBigQuery(self, BQConnection):
        # tambahin logic send
        pass
            
    def ToDrive(self, DriveConnection):
        # tambahin logic send
        pass

