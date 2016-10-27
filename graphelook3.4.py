import pymongo
#
from pymongo import MongoClient

######################################################


cn = MongoClient("localhost:27017")
db = cn.test

db.my_employees.drop()

db.my_employees.insert({"_id": 1, "name": "Dev", "title": "CEO"})
db.my_employees.insert({"_id": 2, "name": "Eliot", "title": "CTO", "boss": 1})
db.my_employees.insert({"_id": 3, "name": "Meagan", "title": "CMO", "boss": 1})
db.my_employees.insert({"_id": 4, "name": "Carlos", "title": "CRO", "boss": 1})
db.my_employees.insert({"_id": 5, "name": "Andrew", "title": "VP Eng", "boss": 2})
db.my_employees.insert({"_id": 6, "name": "Ron", "title": "VP PM", "boss": 2})
db.my_employees.insert({"_id": 7, "name": "arthur", "title": "COO", "boss": 2})
db.my_employees.insert({"_id": 8, "name": "Richard", "title": "VP PS", "boss": 1})
db.my_employees.insert({"_id": 9, "name": "SONG", "title": "VP PS", "boss": 4})
db.my_employees.insert({"_id": 10, "name": "arthur", "title": "VP PS", "boss": 9})
db.my_employees.insert({"_id": 11, "name": "Kirsten", "title": "VP People", "boss": 1})
db.my_employees.insert({"_id": 12, "name": "emmanuel", "title": "Serf", "boss": 10})

######################################################


print("  ")
print("Example: $graphLookup operator in aggregate.")
print("  For every employee, list all of their bosses.")
print("  ")
sss = list(db.my_employees.aggregate(
    [
        {
            "$graphLookup":
                {
                    "from": "my_employees",
                    "startWith": "$boss",
                    "connectFromField": "boss",
                    "connectToField": "_id",
                    #
                    # "maxDepth"            : 6,
                    "depthField": "level",
                    "as": "allBosses"
                }
        }
    ]))
for s in sss:
    print(s)

######################################################


print("  ")
print("Example: $graphLookup operator in aggregate.")
print("  For every employee, .")
print("  ")
sss = list(db.my_employees.aggregate(
    [
        {
            "$graphLookup":
                {
                    "from": "my_employees",
                    "startWith": "$_id",
                    "connectFromField": "_id",
                    "connectToField": "boss",
                    #
                    # "maxDepth"            : 6,
                    "depthField": "level",
                    "as": "allReports"
                }
        },
        {
            "$match":
                {
                    "allReports": {"$ne": []}
                }
        }
    ]))
for s in sss:
    print(s)

######################################################


print("  ")
print("Example: $graphLookup operator in aggregate.")
print("  Using maxDepth, listing only direct reports.")
print("  ")

sss = list(db.my_employees.aggregate(
    [
        {
            "$graphLookup":
                {
                    "from": "my_employees",
                    "startWith": "$_id",
                    "connectFromField": "_id",
                    "connectToField": "boss",
                    #
                    "maxDepth": 0,
                    "depthField": "level",
                    "as": "allReports"
                }
        },
        {
            "$match":
                {
                    "allReports": {"$ne": []}
                }
        }
    ]))
for s in sss:
    print(s)


