"""
Deserialisation of rosbag database file

Reference:
https://pypi.org/project/rosbags/
https://ternaris.gitlab.io/rosbags/topics/serde.html

Author: haris.khan@fountech.ai

last modified: 16 March 2023
"""

import pandas as pd
import sqlite3
from rosbags.serde import deserialize_cdr
import os
import sys
import requests
import json
import warnings


warnings.filterwarnings('ignore')

motion_topic_id = 1
infer_topic_id = 2
speed_topic_id = 3
gps_topic_id = 4

def main():
    """
    input arguments: URL-of-the-rosbag data-type (if data-type is not specified it will return the value for al the data types)
    return: the requested field for the rosbag URL provided
    """
    temp_local_path = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'temp.db3')
    #temp_local_path = "/home/haris/rosbag_decode/car-48b02d822516-rosbag-Mon-30-Jan-2023-05.48.00.pm-to-05.48.30.pm.db3"
    
  
    URL = sys.argv[1]

    try:
        data_type = sys.argv[2]
    except IndexError:
        data_type="default"

    db3_file = requests.get(URL, allow_redirects=True)

    open(temp_local_path, 'wb').write(db3_file.content)
    
    conn = sqlite3.connect(temp_local_path)

    # read messages table from database file into pandas dataframe
    messages = pd.read_sql_query("SELECT * from messages", conn)

    # read topics table from database file into pandas dataframe
    topics = pd.read_sql_query("SELECT * from topics", conn)

    # from message datframe seaprate out each sensor data into separaet dataframes
    df_motion=messages[messages['topic_id'] == motion_topic_id]
    df_infer=messages[messages['topic_id'] == infer_topic_id]
    df_speed=messages[messages['topic_id'] == speed_topic_id]
    df_gps=messages[messages['topic_id'] == gps_topic_id]

    #decoding motion data
    if data_type == "motion":

        motion=motion_data(df_motion)
        print({"motion":motion})

    #decoding gps data
    elif data_type == "gps":

        gps=gps_data(df_gps)
        print({"gps":gps})


    #decoding speed data
    elif data_type == "speed":

        speed=speed_data(df_speed)
        print({"speed":speed})

    #decoding inference data
    elif data_type == "infer":

        infer=infer_data(df_infer)
        print({"infer":infer})
    
    #default runs for all of the rosbag data types
    else:
        motion=motion_data(df_motion)
        gps=gps_data(df_gps)
        speed=speed_data(df_speed)
        infer=infer_data(df_infer)
        print({"gps":gps,"motion":motion,"speed":speed,"infer":infer})


    #df_all_data.to_json(temp_local_path)

    #rosbag_json_data=df_all_rows.to_json(indent=False)
  
    os.remove(temp_local_path)

#print(location)
def gps_data(df_gps):

    df_gps['data_deserialise']=0
    latitude=[]
    longitude=[]
    gps={}
    for i in range(len(df_gps['data'])):
        df_gps.iloc[i,4]=deserialize_cdr(df_gps.iloc[i,3], 'sensor_msgs/msg/NavSatFix')
        latitude.append(json.loads(pd.Series(df_gps.iloc[i,4]).to_json(orient = 'columns'))['0']['latitude'])
        longitude.append(json.loads(pd.Series(df_gps.iloc[i,4]).to_json(orient = 'columns'))['0']['longitude'])
        gps[df_gps.iloc[i,2]]={'latitude':latitude[i],'longitude':longitude[i]}
    return gps


def motion_data(df_motion):
    
    df_motion['data_deserialise']=0
    linear_acceleration=[]
    angular_velocity=[]
    orientation=[]
    motion={}
    for i in range(len(df_motion['data'])):
        df_motion.iloc[i,4]=deserialize_cdr(df_motion.iloc[i,3], 'sensor_msgs/msg/Imu')
        linear_acceleration.append(json.loads(pd.Series(df_motion.iloc[i,4]).to_json(orient = 'columns'))['0']['linear_acceleration'])
        angular_velocity.append(json.loads(pd.Series(df_motion.iloc[i,4]).to_json(orient = 'columns'))['0']['angular_velocity'])
        orientation.append(json.loads(pd.Series(df_motion.iloc[i,4]).to_json(orient = 'columns'))['0']['orientation'])

        motion[df_motion.iloc[i,2]]={'linear_acceleration':linear_acceleration[i],'angular_velocity':angular_velocity[i],'orientation':orientation[i]}
    return motion

def speed_data(df_speed):

    df_speed['data_deserialise']=0
    speed={}
    for i in range(len(df_speed['data'])):     
        df_speed.iloc[i,4]=deserialize_cdr(df_speed.iloc[i,3], 'std_msgs/msg/Float32')
        speed[df_speed.iloc[i,2]]={'speed':json.loads(pd.Series(df_speed.iloc[i,4]).to_json(orient = 'columns'))['0']['data']}
    return speed

def infer_data(df_infer):

    df_infer['data_deserialise']=0
    infer={}
    for i in range(len(df_infer['data'])):
        df_infer.iloc[i,4]=deserialize_cdr(df_infer.iloc[i,3], 'visualization_msgs/msg/ImageMarker')
        infer[df_infer.iloc[i,2]]=json.loads(pd.Series(df_infer.iloc[i,4]).to_json(orient = 'columns'))['0']
    return infer


if __name__ == "__main__":
    main()