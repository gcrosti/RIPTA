import requests
import sqlite3
from pathlib import Path
import pandas as pd
import os

INSERT_STATEMENT_VP = """
INSERT INTO vehicle_positions (
    update_timestamp, trip_id, route_id, trip_start_time, trip_start_date, position_lat, position_lon, bearing, speed, current_status, stop_id, timestamp, vehicle_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

INSERT_STATEMENT = """
INSERT INTO trip_updates_temp (
    update_timestamp, trip_id, stop_id, delay, timestamp)
    VALUES (?, ?, ?, ?, ?);
"""

def test():
    print('hello!')

def query_tripupdates():
    tripupdates_df = pd.DataFrame()
    r = requests.get('http://realtime.ripta.com:81/api/tripupdates?format=json')
    outl = []
    for feedentity in r.json()['entity']:
        if feedentity['trip_update']['stop_time_update'] != None:
            for stoptimeupdate in feedentity['trip_update']['stop_time_update']: 
                if stoptimeupdate['arrival'] != None:
                    if stoptimeupdate['arrival']['delay'] != 0:
                        outdict = {}
                        outdict['timestamp'] = feedentity['trip_update']['timestamp']
                        outdict['trip_id'] = feedentity['trip_update']['trip']['trip_id']
                        outdict['stop_id'] = stoptimeupdate['stop_id']
                        outdict['delay'] = stoptimeupdate['arrival']['delay']
                        outdict['update_timestamp'] = r.json()['header']['timestamp']
                        outl.append(outdict)
    for key in outl[0].keys():
        tripupdates_df[key] = [d[key] for d in outl]
    values = tripupdates_df[['update_timestamp', 'trip_id', 'stop_id', 'delay','timestamp']].values
    print(tripupdates_df.shape)
    conn = sqlite3.connect('RIPTA.db')
    conn.executemany(INSERT_STATEMENT,values)
    conn.commit()
    conn.close()

def query_vehicle_pos():
    r = requests.get('http://realtime.ripta.com:81/api/vehiclepositions?format=json')
    outl = []
    vehiclepos_df = pd.DataFrame()
    for feedentity in r.json()['entity']:
        outdict = {}
        outdict['update_timestamp'] = r.json()['header']['timestamp']
        outdict['trip_id'] = feedentity['vehicle']['trip']['trip_id']
        outdict['route_id'] = feedentity['vehicle']['trip']['route_id']
        outdict['start_time'] = feedentity['vehicle']['trip']['start_time']
        outdict['start_date'] = feedentity['vehicle']['trip']['start_date']
        outdict['position_lat'] = feedentity['vehicle']['position']['latitude']
        outdict['position_lon'] = feedentity['vehicle']['position']['longitude']
        outdict['bearing'] = feedentity['vehicle']['position']['bearing']
        outdict['speed'] = feedentity['vehicle']['position']['speed']
        outdict['current_status'] = feedentity['vehicle']['current_status']
        outdict['stop_id'] = feedentity['vehicle']['stop_id']
        outdict['timestamp'] = feedentity['vehicle']['timestamp']
        outdict['vehicle_id'] = feedentity['vehicle']['vehicle']['id']
        outl.append(outdict)
    for key in outl[0].keys():
        vehiclepos_df[key] = [d[key] for d in outl]
    print(vehiclepos_df.shape)
    values_vp = vehiclepos_df[['update_timestamp', 'trip_id', 'route_id','start_time','start_date','position_lat',
                           'position_lon','bearing','speed','current_status','stop_id','timestamp','vehicle_id']].values
    conn = sqlite3.connect('RIPTA.db')
    conn.executemany(INSERT_STATEMENT_VP,values_vp)
    conn.commit()
    conn.close()
