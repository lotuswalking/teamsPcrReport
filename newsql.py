
import re
from datetime import datetime
def newSql(month='2023-05'):
    with open("./combineByStream.sql",'r') as file:
        data = file.read()
        newData, count = re.subn("PCRCallRecordDetails",
                                "History_PCRCallRecordDetails_"+month, data)

        newData, count = re.subn("PCRCallRecordNetworks",
                                "History_PCRCallRecordNetworks_"+month, newData)
    
        newData, count = re.subn("PCRCallRecordStreams",
                                  "History_PCRCallRecordStreams_"+month, newData)

        with open("combineByStream"+month+".sql",'w') as file:
            file.write(newData);


# newSql("2023-04") #四月network表没有数据
newSql("2023-05")
newSql("2023-06")
newSql("2023-07")
# now = datetime.now()
# formatted_date = now.strftime("%Y-%m")
# print(formatted_date)
