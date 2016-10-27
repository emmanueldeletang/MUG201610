import pymongo
   #
from pymongo import MongoClient


######################################################


cn = MongoClient("localhost:27017")
db = cn.test_ed

db.my_coll.drop()
   #
db.my_coll.insert( { "price" : 10 } )
db.my_coll.insert( { "price" : 10 } )
db.my_coll.insert( { "price" : 10 } )
db.my_coll.insert( { "price" : 20 } )
db.my_coll.insert( { "price" : 20 } )
db.my_coll.insert( { "price" : 50 } )


######################################################


print ("  ")
print ("Example 1: 6 documents, 2 groups.")
print ("  ")
sss = list ( db.my_coll.aggregate(
   [
   {
   "$bucketAuto" :
      {
      "groupBy" : "$price",
      "buckets" : 2
      }
   }
   ] ) )
for s in sss:
   print (s)


######################################################


print ("  ")
print ("Example 2: 6 documents, 4 groups.")
print ("  ")
sss = list ( db.my_coll.aggregate(
   [
   {
   "$bucketAuto" :
      {
      "groupBy" : "$price",
      "buckets" : 4
      }
   }
   ] ) )
for s in sss:
   print (s)


######################################################


print ("  ")
print ("Example 3: 6 documents, 2 groups, custom accumulators.")
print ("  ")
sss = list ( db.my_coll.aggregate(
   [
   {
   "$bucketAuto" :
      {
      "groupBy" : "$price",
      "buckets" : 2,
      "output"  :
         {
         "count"     : { "$sum" : 1        },
         "totPrice"  : { "$sum" : "$price" }
         }
      }
   }
   ] ) )
for s in sss:
   print (s)


######################################################


print ("  ")
print ("Example 4: 6 documents, 2 groups, custom accumulators, specified grouping.")
print ("  ")
sss = list ( db.my_coll.aggregate(
   [
   {
   "$bucketAuto" :
      {
      "groupBy" : "$price",
      "buckets" : 2,
      "output"  :
         {
         "count"     : { "$sum" : 1        },
         "totPrice"  : { "$sum" : "$price" }
         },
      "granularity" : "R5"
      }
   }
   ] ) )
for s in sss:
   print (s)


db.my_coll.drop()





