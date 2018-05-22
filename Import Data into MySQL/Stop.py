import MySQLdb
import csv

PATH = "/Users/gaoyounan/Desktop/Summer Term/Data Management/a1/CSV files/"
db = MySQLdb.connect("35.183.6.252", "myuser", "mypass", "busDB", charset='utf8' )
cursor = db.cursor()
files=open(PATH+'stops.csv','rb')
Reader = csv.DictReader(files)
li = []
t = 0
for row in Reader:
    t = t+1
    row['stop_id'] = int(row['stop_id'])
    qmarks = ', '.join(['%s'] * len(row))
    cols = ', '.join(row.keys())
    sql = "INSERT INTO %s (%s) VALUES (%s)" % ('stops', cols, qmarks)

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