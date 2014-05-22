#!/usr/bin/env python
# coding: utf-8

import MySQLdb
import requests
from time import sleep
import json
from difflib import SequenceMatcher
import pickle
import os

HOST = "localhost"
USER = "********"
PASSWORD= "*********"
DB = "***********"
USER_TABLE = "prefix_user"
GEOTARGET_TABLE = "prefix_geo_target"
GEOCOUNTRY_TABLE = "prefix_geo_country"
GEOREGION_TABLE = "prefix_geo_region"
GEOCITY_TABLE = "prefix_geo_city"

def search_db(targettable, name_en, targetid=None):
    sql = u'''SELECT * FROM %s \
          WHERE'''  % (targettable)
    if targettable==GEOREGION_TABLE:
       field = 'country_id'
    if targettable==GEOCITY_TABLE:
       field = 'region_id' 
    if targetid:
        sql += u''' %s=%s AND name_en="%s"''' % (field, targetid, name_en)
    else:
        sql += u''' name_en="%s"''' % name_en
    cursor.execute(sql)
    return cursor.fetchone()


def searchsynonym(targettable, targetid, name_en):
    if os.path.isfile("ls-geo.dat"):
        f = open("ls-geo.dat", "rb")
        synonyms = pickle.load(f)
        f.close()
    else:
        synonyms = {u"""Moskovskaya Oblast'""": u"""Moscow & Moscow Region""", u"""Moskva""":u"""Moscow & Moscow Region"""}
        f = open("ls-geo.dat", "wb")
        pickle.dump(synonyms, f)
        f.close()
    
    if name_en in synonyms.keys():
        sname_en = synonyms[name_en]
        return search_db(targettable, targetid=targetid, name_en=sname_en)
    else:
        print "! Cannot find target for %s in DB." % name_en
        if targettable==GEOREGION_TABLE:
            field = 'country_id'
        if targettable==GEOCITY_TABLE:
            field = 'region_id'
 
        sql = u"""SELECT name_en FROM %s \
              WHERE %s=%s""" \
              % (targettable, field, targetid )
        cursor.execute(sql)
        row = cursor.fetchone()
        ratiolist = []
        while row is not None:
            s = SequenceMatcher(None, name_en, row[0])
            ratiolist.append({'name':row[0],'ratio':s.ratio()})
            row = cursor.fetchone()
            sratiolist = sorted(ratiolist, key=lambda k: k['ratio'])
        for w in sratiolist:
            print "%d - %s" % (sratiolist.index(w), w['name'])
        try:
            x = int(raw_input('Choose synonym for %s (Any symbol for None) ' % name_en))
        except ValueError:
            return None
        sname_en = sratiolist[x]['name']
        synonyms[name_en] = sname_en
        f = open("ls-geo.dat", "wb")   
        pickle.dump(synonyms, f)   
        f.close()
        
        return search_db(targettable, targetid=targetid, name_en=sname_en)


db = MySQLdb.connect(HOST,USER,PASSWORD,DB, charset="utf8")

cursor = db.cursor()

cursor.execute("""SELECT user_id, user_ip_register FROM %s""" % (USER_TABLE))

users  = cursor.fetchall()

print "There are %d users in DB" % len(users)

cursor.execute("""SELECT target_id FROM %s WHERE target_type='user'""" % (GEOTARGET_TABLE))

data = cursor.fetchall()
users_in_geo = [item for sublist in data for item in sublist]

users_not_in_geo = []
for user in users:
    if not(user[0] in users_in_geo):
        users_not_in_geo.append({"user_id":user[0],"ip":user[1]})

print "%d of them haven\'t location info" % len(users_not_in_geo)

for user in users_not_in_geo:
    if user["ip"]:
        print "Trying to find location for %s" % user["ip"]
        #sleep(1)
        text = "http://api.sypexgeo.net/json/%s" % user["ip"]
        r = requests.get(text)
        if r.status_code == 200:
            j = json.loads(r.text)

            try:
                country_en = j['country']['name_en']
            except TypeError:
                print "! Exception trying fetch data for %s" % user["ip"]
                continue
            if not country_en:
                continue
            country = search_db(GEOCOUNTRY_TABLE, name_en=country_en)
            if country:
                country_id = country[0]
                country_name = country[2]
                print "-- %s" % country_name
            else:
                print "! Cannot find country %s in DB" % country_en
                continue
            try:
                region_en = j['region']['name_en']
            except TypeError:
                print "! Exception while searching for data in %s" % user["ip"]
                continue
            region = search_db(GEOREGION_TABLE, targetid=country_id, name_en=region_en)
            if not region:
                region = searchsynonym(targettable=GEOREGION_TABLE, 
                                       targetid=country_id,
                                       name_en =region_en)
                if not region: 
                    continue    
            region_id = region[0]
            print "-- %s" % region[3]
            try:
                city_en = j['city']['name_en']
            except TypeError:
                print "! Exception while searching for city in %s geodata" % user["ip"]
            city = search_db(GEOCITY_TABLE, targetid=region_id, name_en=city_en)
            if not city:
                city = searchsynonym(targettable=GEOCITY_TABLE, targetid=region_id, name_en=city_en)
                if not city:
                    continue
            city_id = city[0]
            print "-- %s" % city[4]

            ### exporting data in table
            geo_type = 'city'
            geo_id = city_id
            target_type = 'user'
            target_id = user['user_id']
            #country_id
            #region_id
            #city_id
            sql = """INSERT INTO %s \
                   VALUES ("%s",%s,"%s",%s,%s,%s,%s)""" \
                  % (GEOTARGET_TABLE,
                     geo_type,
                     geo_id,
                     target_type,
                     target_id,
                     country_id,
                     region_id,
                     city_id)

            cursor.execute(sql)
            db.commit()

        else:
            print "Cannot fetch info. Error code %d" % r.status_code



db.close()
