import pandas as pd


class Load:
    def toGsheet(self, GsheetConnection):
        # tambahin logic send
        pass
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

