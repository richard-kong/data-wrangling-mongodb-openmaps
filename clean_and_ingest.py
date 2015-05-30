#!/usr/bin/env python
# -*- coding: utf-8 -*-
import xml.etree.ElementTree as ET
#import pprint
import re
from pymongo import MongoClient
import phonenumbers as pn #pip install phonenumbers

"""
Reads in an osm file as defied by the "file" variable
Parses the XML, cleans up the phone numbers and addresses and inserts documents in the following format into mongo db.

data format for mongo:

{
"id": "2406124091",
"type: "node",
"visible":"true",
"created": {
          "version":"2",
          "changeset":"17206049",
          "timestamp":"2013-08-03T16:43:42Z",
          "user":"linuxUser16",
          "uid":"1219059"
        },
"pos": [41.9757030, -87.6921867],
"address": {
          "housenumber": "5157",
          "postcode": "60625",
          "street": "North Lincoln Ave"
        },
"amenity": "restaurant",
"cuisine": "mexican",
"name": "La Cabana De Don Luis",
"phone": "1 (773)-271-5176"
}
"""

#file = 'c:\\temp\\brisbane_australia.osm'
file = 'sample.osm'

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road", 
            "Trail", "Parkway", "Commons"]

mapping = { "St": "Street",
            "St.": "Street",
            "street": "Street",            
            "Rd.": "Road",
            "Rd": "Road",
            "road": "Road",
            "Ave": "Avenue",
            "Cirtcuit": "Circuit",
            "Pde": "Parade",
            "Tce": "Terrace"
            }
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

#Clean up bad street names and replace with a cleaned version
def clean_street_name(name, mapping):
    m = street_type_re.search(name)
    if m:
        street_type = m.group()
        if street_type in mapping:
            name =  re.sub(street_type_re,mapping[street_type] , name)

    return name	
  
def remove_state(value, state):
    if state in value:
        value = value.replace(state,"").rstrip().lstrip()
    return value


#cleans up phone numbers and converts them to international format +61 7 1234 5678
#invalid numbers are removed
def clean_phone(key, value):
    #australian phone numbers have a 2 digit area code following for a 8 digit number
    #if less than 8 digits are supplied, assume that it is missing the area code
     
    list_of_phone_num = []
    phone_numbers = value.split(";")
    for number in phone_numbers:
        number = number.replace("+61","").lstrip().rstrip()   #strip out country code so that area code can be prefixed if required 
        numbers_only = re.sub("[^0-9]", "", value)
        
        if len(numbers_only) == 8 :
           number = "07" +  number
        
        try:
           phone_num = pn.parse(value,"AU")
           if pn.is_valid_number(phone_num):
               cleaned = pn.format_number(phone_num,pn.PhoneNumberFormat.INTERNATIONAL)
               list_of_phone_num.append(cleaned)
           
        except:
            pass
    return list_of_phone_num
    
    
        
def process_map_json(file_in, pretty = False):
    data = []
    for _, element in ET.iterparse(file_in):
        el = shape_element(element)
        if el:
            data.append(el)

    return data 

def clean_address(key, value):
   key =  key.replace("addr:","")     
   if key == "street":
       value = clean_street_name(value, mapping)
   elif key == "postcode":
       value =  re.sub("[^0-9]", "", value) #remove non-numeric characters
   elif key == "city":
       value = remove_state(value,"QLD")
  
   return key, value
       
     
    
#shape way and node elements into proper format for mongodb
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
            created = {}
            address = {}
            pos = []
            node_refs = []
           
            #build node_regs for way tag
            if element.tag == "way":
                for tag in element.iter("nd"):
                    node_refs.append(tag.attrib["ref"])
                if len(node_refs) > 0:
                    node["node_refs"] = node_refs
            
            #read attributes of tag
            for attribute in element.items():
                key = attribute[0]
                value =  attribute[1]
                if key in CREATED:
                    created[key] = value
                elif key == 'lat':
                    pos.insert(0, float(value))
                elif key == 'lon':
                    pos.append(float(value))
                else:
                    node[key] = value
            
            #iterate through tag elements and build key, value pairs
            for tag in element.iter("tag"):
                value = tag.attrib["v"]
                key = tag.attrib["k"]
                
                if problemchars.search(key) or key.count(":") > 1:
                    pass
                else: 
                    if key.startswith("addr:"):                    
                        key, value = clean_address(key, value)
                        address[key] = value 
                    if key == "phone":
                        value = clean_phone(key, value)
  
                    node[key] = value
  
            node["created"] = created
            node["type"] = element.tag
            if len(pos) > 0 :
                node["pos"] = pos
            
            if len(address) > 0:
                node["address"] = address
            #pprint.pprint(node)
            return node    


def main():
    
    client = MongoClient("mongodb://localhost:27017")
    db = client.brisbane
    data = process_map_json(file)
    
    if "map" in db.collection_names():
        db.drop_collection("map")
    
    db.map.insert_many(data)

    print db.map.count()    
    
   
main()