# -*- coding: utf-8 -*-
"""
Created on Fri May 29 20:53:24 2015

@author: Richard
"""
from pymongo import MongoClient
import pprint


client = MongoClient("mongodb://localhost:27017")
db = client.brisbane

#Top 100 users by constributions
def userContributions():
    pipeline = [
        {"$group": { "_id" : "$created.user",  "count" : {"$sum" : 1 }}},
        {"$sort": {"count" : -1}},
        {"$limit" : 100}
    
    ]    
    result = db.map.aggregate(pipeline)
    
    return result

#total number of addresses  
def countAddresses():
    return db.map.count( {"address" : {"$exists" : 1}}  )
    
def addressGroupByCitySuburb():
    pipeline = [
        {"$match":  {"address" : {"$exists" : 1}}},   
        {"$group": {"_id": {"city": "$address.city", "suburb" : "$address.suburb" }, "count" : {"$sum" : 1}}},
        {"$sort": {"count" : -1}},
        {"$limit" : 100}
    ]
    result = db.map.aggregate(pipeline)
    
    return result

#Top 100 count of addresses grouped by a field
#input: field = field name in address subdocument to group by
def addressGroupBy(field):
    pipeline = [
        {"$match":  {"address" : {"$exists" : 1}}},   
        {"$group": {"_id": "$address." + field, "count" : {"$sum" : 1}}},
        {"$sort": {"count" : -1}},
        {"$limit" : 100}
    ]
    result = db.map.aggregate(pipeline)
    
    return result

#count of incomplete addresses based on the existence of a field
#input: field = field name in address subdocument 
def count_incomplete_addresses(field):
    pipeline = [
        {"$match":  {"address" : {"$exists" : 1}, "address." + field : {"$exists" : 0}}},        
        {"$group": {"_id": "Addresses", "count" : {"$sum" : 1}}},
    ]
    result = db.map.aggregate(pipeline)
    
    return result

#List of documents for specific building types
#input: type of building to filter by

def getBuilding(building):
    return db.map.find( {"address" : {"$exists" : 1}, "building" : building}  )

#Count of documents grouped by any field 
#input: field: name of field in document to group by
#limit: number of documents to return
def docGroupBy(field,limit = 100):
      pipeline = [
        {"$group": {"_id": "$" + field, "count" : {"$sum" : 1}}},
        {"$sort": {"count" : -1}},
        {"$limit" : limit}
      ]
      result = db.map.aggregate(pipeline)
    
      return result

print "Total number of addresses"
pprint.pprint(countAddresses())
print "============================================================"


print "Address by City and suburb"
pprint.pprint(list(addressGroupByCitySuburb()))
print "============================================================"


print "Addresses with no Street"
pprint.pprint(list(count_incomplete_addresses("street")))

print "============================================================"

print "Addresses with no postcode"
pprint.pprint(list(count_incomplete_addresses("postcode")))

print "============================================================"

print "Addresses with no House number"
pprint.pprint(list(count_incomplete_addresses("housenumber")))
