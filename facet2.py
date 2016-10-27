import pymongo
#
from pymongo import MongoClient

cn = MongoClient("localhost:27017")
db = cn.test

db.my_coll.drop()
   #
db.my_coll.insert( {
   "category"     : "sports"   ,
   "sub-category" : "rugby"                  ,
   "attributes"   :
      [
      { "color"     : "red"       },
      { "plug"      : "jersey"       },
      { "buttons"   : 3           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "sports"   ,
   "sub-category" : "rugby"                  ,
   "attributes"   :
      [
      { "color"     : "red"       },
      { "plug"      : "shoes"       },
      { "buttons"   : 3           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "sports"   ,
   "sub-category" : "rugby"                  ,
   "attributes"   :
      [
      { "color"     : "blue"      },
      { "plug"      : "jersey"       },
      { "buttons"   : 2           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "sports"   ,
   "sub-category" : "rugby"                  ,
   "attributes"   :
      [
      { "color"     : "blue"      },
      { "plug"      : "tee"       },
      { "buttons"   : 2           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "sports"   ,
   "sub-category" : "basket"                  ,
   "attributes"   :
      [
      { "color"     : "silver"    },
      { "plug"      : "Pjersey"       },
      { "buttons"   : 2           }
      ]
   } )
db.my_coll.insert( {
   "category"     : "sports"   ,
   "sub-category" : "basket"                ,
   "attributes"   :
      [
      { "size"      : 27          },
      { "res"       : "shoes"       }
      ]
   } )
db.my_coll.insert( {
   "category"     : "sports"   ,
   "sub-category" : "basket"                ,
   "attributes"   :
      [
      { "size"      : 29          },
      { "res"       : "shirt"       }
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
      { "category"      : "sports" }
   },
   {
   "$facet"             :
      {
      "interface"       :
         [
         {
         "$match"       : { "sub-category" : "rugby" }
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




