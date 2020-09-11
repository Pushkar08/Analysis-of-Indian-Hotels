#Main Controller calling All related files


import load_mongodb
import extract_transfrom_load
import visualisation
import preprocess_dataset




preprocess_dataset.api_to_json()
preprocess_dataset.csv_to_json()


#==============================================================================
#Create dataabase in MongoDB
#Create collections for respective online travel portals
#Load the data into corressponding collections
#==============================================================================
load_mongodb.loadData()

#==============================================================================
#Create database in Postgre to store online travel portals data
#==============================================================================
extract_transfrom_load.createdatabase()

#==============================================================================
#Extract data from mongoDB
#Transfom data - Cleaning Phase
#Load data into Postgre
#==============================================================================
extract_transfrom_load.etlProcess()

#==============================================================================
#Visualize data
#==============================================================================
visualisation.aggregationAndVisualization()






