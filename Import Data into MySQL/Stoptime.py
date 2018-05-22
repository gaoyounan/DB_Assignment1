import MySQLdb
import csv

PATH = "/Users/gaoyounan/Desktop/Summer Term/Data Management/a1/CSV files/"
db = MySQLdb.connect("35.183.6.252", "myuser", "mypass", "busDB", charset='utf8' )
cursor = db.cursor()
files=open(PATH+'stoptimes.csv','rb')
Reader = csv.DictReader(files)
li = []
t = 0
for row in Reader:
    t = t+1
    row['stop_id'] = int(row['stop_id'])
    str_arr = row['trip_id'].split('-')
    row['day'] = str_arr[len(str_arr)-2]
    row['sequence'] = str_arr[len(str_arr) - 1]
    row['trip_identification'] = int(str_arr[0])

    qmarks = ', '.join(['%s'] * len(row))
    cols = ', '.join(row.keys())
    sql = "INSERT INTO %s (%s) VALUES (%s)" % ('stoptimes', cols, qmarks)
    li.append(row.values())
    if t % 1000 == 0:
        cursor.executemany(sql, li)
        db.commit()
        li = []

if len(li) != 0:
    cursor.executemany(sql, li)
    db.commit()


files.close()
db.close()