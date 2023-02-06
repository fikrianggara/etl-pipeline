import pandas as pd
import json

class Transform :
    def queryResultToDF(self, queryResult, columns)->pd.DataFrame:
        return pd.DataFrame.from_records(queryResult, columns=columns)
    
    def gSheetToDf(self, gsheetResponse)->pd.DataFrame:
        values = list(gsheetResponse.json().values())
        rawData = pd.DataFrame.from_records(values[2], columns=values[2][0])
        rawData = rawData[1:]
        return rawData