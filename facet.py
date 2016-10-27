
import pymongo
#
from pymongo import MongoClient

cn = MongoClient("localhost:27017")
db = cn.test

db.my_coll.drop()
   #
db.my_coll.insert( {
   "category"     : "Computer peripherals"   ,
   "sub-category" : "Mouse"                  ,
   "attributes"   :
      [
      { "color"     : "red"       },
      { "plug"      : "USB"       },
      { "buttons"   : 3           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "Computer peripherals"   ,
   "sub-category" : "Mouse"                  ,
   "attributes"   :
      [
      { "color"     : "blue"      },
      { "plug"      : "USB"       },
      { "buttons"   : 2           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "Computer peripherals"   ,
   "sub-category" : "Mouse"                  ,
   "attributes"   :
      [
      { "color"     : "silver"    },
      { "plug"      : "PS2"       },
      { "buttons"   : 2           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "Computer peripherals"   ,
   "sub-category" : "Monitor"                ,
   "attributes"   :
      [
      { "size"      : 27          },
      { "res"       : "CGA"       }
      ]
   } )
db.my_coll.insert( {
   "category"     : "Computer peripherals"   ,
   "sub-category" : "Monitor"                ,
   "attributes"   :
      [
      { "size"      : 29          },
      { "res"       : "VGA"       }
      ]
   } )


######################################################


print ("  ")
print ("Example: $facet stage in aggregate.")
print ("  ")
sss = list ( db.my_coll.aggregate(
   [
   {
   "$match"             :
      { "category"      : "Computer peripherals" }
   },
   {
   "$facet"             :
      {
      "interface"       :
         [
         {
         "$match"       : { "sub-category" : "Mouse" }
         },
         {
         "$group"       :
            {
            "_id"       : "$attributes.plug"  ,
               #
            "count"     : { "$sum" : 1 }
            }
         }
         ],
      "sub-category"    : [
         {
         "$group"       :
            {
            "_id"       : "$sub-category",
               #
            "count"     : { "$sum" : 1 }
            }
         }
         ]
      }
   }
   ] ) )
for s in sss:
   print (s)



















