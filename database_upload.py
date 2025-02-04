import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password="",
    database="research_db"
)

cursor = conn.cursor()


cursor.execute("SELECT * FROM research_data")
rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()