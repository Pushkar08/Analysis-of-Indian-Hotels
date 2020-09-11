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


def etlProcess():

    try:
        client = MongoClient("mongodb://localhost:27017")
        db = client['Hotels']
        collection1 = db.Cleartrip
        collection2 = db.Booking
        collection3 = db.Stayzilla
        collection4 = db.Goibibo
        
        print("Extracting data from mongoDB....\n\n")
        df_cleartrip = pd.DataFrame(list(collection1.find({})))
        df_booking = pd.DataFrame(list(collection2.find({})))
        df_stayzilla = pd.DataFrame(list(collection3.find({})))
        df_goibibo = pd.DataFrame(list(collection4.find({})))
        
        print("Processing data...\n\n")
        df1 = transform_cleartrip(df_cleartrip)
        df2 = transform_booking(df_booking)
        df3 = transform_stayzilla(df_stayzilla)

        print("Loading data into Postgre....\n\n")
        loadCleartrip(df1)
        loadBooking(df2)
        loadStayzilla(df3)
        loadGoibibo(df_goibibo)
        print("Data load successful\n\n")
    except (Exception) as dbError:
        print ("Error while connecting to MongoDB", dbError)
        
    finally:
        print("ETL process finished")
        
    
          
    

def transform_cleartrip(df):
    
    df.rename({'tad_review_rating': 'site_review_rating', 'tad_review_count': 'site_review_count'}, axis=1, inplace=True)


    #property name
    df=df.dropna(subset = ['property_name'])
    df_propert_name = df[['property_name']]
    df_propert_name = df_propert_name['property_name'].str.replace("'", "")
    df['property_name'] = df_propert_name
    
    df = df.dropna(subset = ['hotel_star_rating'])
    df1 = df[['hotel_star_rating']]
    df1 = df1['hotel_star_rating'].str.split(" ",expand = True)
    new = df1[0]
    df['hotel_star_rating'] = new
    
    
    df = df.dropna(subset = ['state','room_type','latitude','longitude','property_type'])
    df = df.dropna(subset = ['site_review_count','site_review_rating'])
    
    
    star_rating_median = df['hotel_star_rating'].median()
    star_rating_median = round(star_rating_median,2)
    df['hotel_star_rating'] = df['hotel_star_rating'].fillna(star_rating_median)
    df['hotel_star_rating'] = df['hotel_star_rating'].astype(int)
    
    df['site_review_count'] = df['site_review_count'].astype(int)
    
    
    df['sitename'] = df['sitename'].replace("http://www.cleartrip.com", "cleartrip")
    df['sitename'] = df['sitename'].replace("https://www.cleartrip.com", "cleartrip")
    
    #Selecting required data columns - dimensionality reduction
    df = df[['country', 'hotel_star_rating', 'image_count', 'latitude', 'longitude', 'property_type', 'room_count', 'room_type', 'site_review_rating', 'sitename', 'state', 'uniq_id', 'city', 'property_name']]
    
    return df
    
    
def transform_booking(df):
    # Removing special characters
    df = df.dropna(subset = ['property_name'])
    property_name_list = list(df['property_name'])
    new_property_list = []
    
    for property_name in property_name_list:
        property_name = str(property_name)
        new_property_list.append(property_name.encode('ascii', errors = "ignore").decode())
    
    df = df.drop(axis = 1, columns = 'property_name')
    df['property_name'] = new_property_list
    
    
    # Removing special characters
    df = df.dropna(subset = ['city'])
    city_list = list(df['city'])
    new_city_list = []
    for city in city_list:
        city = str(city)
        new_city_list.append(city.encode('ascii', errors = 'ignore').decode())
    df = df.drop(axis = 1, columns = 'city')
    df['city'] = new_city_list
    
    
    #property name
    df=df.dropna(subset = ['property_name'])
    df_propert_name = df[['property_name']]
    df_propert_name = df_propert_name['property_name'].str.replace("'", "")
    df['property_name'] = df_propert_name
    
    
    #Filling NA values with mean
    #hotel star rating
    df=df.dropna(subset = ['hotel_star_rating'])
    df_rating = df[['hotel_star_rating']]
    df_rating = df_rating['hotel_star_rating'].str.split("-",expand = True)
    rating = df_rating[0]
    rating = rating.str.replace("stars", " ")
    df['hotel_star_rating'] = rating.str.strip()
    df['hotel_star_rating'] = df['hotel_star_rating'].astype(int)
    
    #Replacing with mean
    star_rating_mean = df['hotel_star_rating'].mean()
    star_rating_mean = round(star_rating_mean, 0)
    df['hotel_star_rating'] = df['hotel_star_rating'].fillna(star_rating_mean)
    
    #image count
    image_count_mean = df['image_count'].mean()
    image_count_mean = round(image_count_mean, 0)
    df['image_count'] = df['image_count'].fillna(image_count_mean)
    df['image_count'] = df['image_count'].astype(int)
    
    #Removing na values
    df = df.dropna(subset = [ 'room_type', 'state', 'site_review_rating'])
    
    #Selecting required data columns - dimensionality reduction
    df = df[['country', 'hotel_star_rating', 'image_count', 'latitude', 'longitude', 'property_type', 'room_count', 'room_type', 'site_review_rating', 'state', 'uniq_id', 'city', 'property_name', 'sitename']]
    
    #Changing datatype
    df['room_count'] = df['room_count'].astype(int)
    
    return df
        
    
def transform_stayzilla(df):
    #Checking for null values
    df.isnull().sum()
    
    #Step 3 : Remove the unwanted columns
    df1 = df[['uniq_id', 'city', 'country', 'image_count', 'latitude', 'longitude', 'property_name', 'property_type', 'room_price' ,'room_types', 'sitename', 'hotel_star_rating']]
    
    #Step 4 : treating the missing values
    df1.isnull().sum()
    
    #repalcing null values in country column
    df1['country'].fillna('India', inplace = True)
    df1.isnull().sum()
    
    #calculating mean of image_count column
    image_count_mean = round(df1['image_count'].mean(),0)
    image_count_mean
    
    #replacing the missing values by mean in image_count column
    df1['image_count'].fillna(image_count_mean, inplace = True)
    df1.isnull().sum()
    
    #calculating mean of hotel_star_rating column
    hotel_star_rating_mean = round(df1['hotel_star_rating'].mean(),0)
    
    #replacing the missing values by mean in image_count column
    df1['hotel_star_rating'].fillna(hotel_star_rating_mean, inplace = True)
    
    #Dropping rows in which price is null and type casting it
    df2 = df1[['room_price']]
    df2 = df2.dropna()
    df2 = df2['room_price'].str.split("p",expand = True)
    new = df2[0]
    df1=df1.dropna(subset = ['room_price'])
    df1['room_price'] = new
    
    
    #changing the data type of image_count
    df1['image_count'] = df1['image_count'].astype(int)
    df1['room_price'] = df1['room_price'].astype(int)
    
    df1['sitename'] = df1['sitename'].replace("www.stayzilla.com", "stayzilla")

    return df1   



def createdatabase():
    try:
   
        dbConnection = psycopg2.connect(
            user = "postgres",
            password = "2574929",
            host = "localhost",
            port = "5432")
        dbConnection.set_isolation_level(0)  # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        print("Creating database...\n\n")
        dbCursor.execute("""create database Hotels""")
        
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
        
    finally:
        print("Database created successfully\n\n")
        if(dbConnection): dbConnection.close()

def loadCleartrip(df):
    
    try:
   
        dbConnection = psycopg2.connect(
            user = "postgres",
            password = "2574929",
            host = "localhost",
            port = "5432",
            database = "hotels")
        dbConnection.set_isolation_level(0)  # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("""
        CREATE TABLE Cleartrip(
        country TEXT,
        hotel_star_rating integer,
        image_count integer,
        latitude numeric(15,5),
        longitude numeric(15,5),
        property_type TEXT,
        room_count integer,
        room_type TEXT,
        site_review_rating numeric(3,2),
        site_name TEXT,
        state TEXT,
        unique_id TEXT PRIMARY KEY,
        city TEXT,
        property_name TEXT
        
        );
        """)
        
        insertString = "INSERT INTO Cleartrip VALUES('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')"
        for index, row in df.iterrows():
            hotel_info=[]
            for i in df.columns:
                hotel_info.append(row[i])
            dbCursor.execute(insertString.format(*hotel_info))
        dbCursor.close()
        
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
        
    finally:
        print("cleartrip.com data is uploaded successfully\n\n")
        if(dbConnection): dbConnection.close()
        

def loadBooking(df):
    try:
        dbConnection = psycopg2.connect(
            user = "postgres",
            password = "2574929",
            host = "localhost",
            port = "5432",
            database = "hotels")
        
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        
        dbCursor.execute("""
        CREATE TABLE booking(
        country TEXT,
        hotel_star_rating integer,
        image_count integer,
        latitute numeric(10,5),
        longitude numeric(10,5),
        property_type TEXT,
        room_count integer,
        room_type TEXT,
        site_review_rating numeric(3,2),
        state TEXT,
        unique_id TEXT PRIMARY KEY,
        city TEXT,
        property_name TEXT,
        site_name TEXT
        );
        """)
        
        insertString = "INSERT INTO booking VALUES('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')"
        for index, row in df.iterrows():
            hotel_info=[]
            for i in df.columns:
                hotel_info.append(row[i])
            dbCursor.execute(insertString.format(*hotel_info))
       # INSERT INTO COMPANY (ID, NAME, AGE) VALUES (2, "FSFSDF", 22)
        dbCursor.close()
        
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
        
    finally:
        print("booking.com data is uploaded successfully\n\n")
        if(dbConnection): dbConnection.close()
        
def loadStayzilla(df1):
    try:
        #setting up the connection
        dbConnection = psycopg2.connect(
            user = "postgres",
            password = "2574929",
            host = "localhost",
            port = "5432",
            database = "hotels")
        
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        
        #Creating the table
        dbCursor.execute("""
        CREATE TABLE Stayzilla(
        unique_id TEXT PRIMARY KEY,
        city TEXT,
        country TEXT,
        image_count integer,
        latitute numeric(10,5),
        longitude numeric(10,5),
        property_name TEXT,
        property_type TEXT,
        room_price integer,
        room_types TEXT,
        site_name TEXT,
        hotel_star_rating numeric(3,2)
        );
        """)
        #Inserting data into postgres
        insertString = "INSERT INTO Stayzilla VALUES('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')"
                 
        for index, row in df1.iterrows():
            hotel_info=[]
            for i in df1.columns:
                hotel_info.append(row[i])
            dbCursor.execute(insertString.format(*hotel_info))    
            
        dbCursor.close()
        
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
        
    finally:
        print("stayzilla.com data is uploaded successfully\n\n")
        if(dbConnection): dbConnection.close()
                    
        
        
def loadGoibibo(df):
    try:
        # data insertion into postgress
        df = df.drop(columns = '_id')
        df.rename({'sitename': 'site_name'}, axis=1, inplace=True)
        engine = create_engine('postgresql://postgres:2574929@localhost:5432/hotels')
        df.to_sql('Goibibo', engine)   # commented for  while
        #fetch data from postgress
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    
    finally:
        print("goibibo.com data is uploaded successfully\n\n")
                        
