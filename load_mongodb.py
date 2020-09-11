from pymongo import MongoClient
import json

import random
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import re
import plotly as py
import ipywidgets as widgets
import numpy as np
from scipy import special
import plotly.graph_objects as go
import plotly.express as px



def loadData():

    try:
        
        print("Connecting to MongoDB....\n\n")
        client = MongoClient("mongodb://localhost:27017")
        db = client['Hotels']
        collection1 = db.Cleartrip
        collection2 = db.Booking
        collection3 = db.Stayzilla
        collection4 = db.Goibibo

        print("Connection established successfully\n\n")
        
        #reading json file for storing file in Mongodb
        data1 = json.load(open("cleartrip_com-cleartrip_com.json"))
        data2 = json.load(open("booking_com_to_json.json"))
        data3 = json.load(open("stayzilla_com-travel_sample.json"))
        data4 = json.load(open("goibibo_com-travel_sample.json"))

        print("Inserting data for cleartrip.com.....\n\n")
        collection1.insert_many(data1)
        
        print("Inserting data for booking.com.....\n\n")
        collection2.insert_many(data2)
        
        print("Inserting data for stayzilla.com.....\n\n")
        collection3.insert_many(data3)
        
        print("Inserting data for Goibibo.com.....\n\n")
        collection4.insert_many(data4)
        
    
    except (Exception) as dbError:
        print ("Error while connecting to MongoDB", dbError)
        
    finally:
        print("Data load successful\n\n")
        
        
    