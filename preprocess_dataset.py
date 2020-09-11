import requests, zipfile, io
import pandas as pd


def api_to_json():

    try:
        session = requests.Session()
        session.auth = ('ht78346', '266c255a522c32911751cc86daf556d4')
        hostname = 'www.kaggle.com';
        auth = session.post('http://' + hostname)
        response = session.get('http://' + hostname + '/api/v1/datasets/download/PromptCloudHQ/hotels-on-goibibo',
                               stream = True)
    
        print(response.status_code)
        z = zipfile.ZipFile(io.BytesIO(response.content))
        print(z.filelist)
        csvfile = z.extract('goibibo_com-travel_sample.csv')
        df = pd.DataFrame(pd.read_csv(csvfile, sep = ",", header = 0))
        df.to_json("goibibo_com-travel_sample.json", orient = "records", date_format = "epoch")
    
    except:
        print("Error while extracting the file from API")
        
    finally:
        print("Exit")
    
 
    
def csv_to_json():
    #CSV path
    booking_csv= "booking_com-travel_sample.csv"
    stayzilla_csv= "stayzilla_com-travel_sample.csv"
    cleartrip_csv= "cleartrip_com-travel_sample.csv"
    
    
    #read CSV
    booking_df = pd.DataFrame(pd.read_csv(booking_csv, sep = ",", header = 0))
    stayzilla_df = pd.DataFrame(pd.read_csv(stayzilla_csv, sep = ",", header = 0))
    cleartrip_df = pd.DataFrame(pd.read_csv(cleartrip_csv, sep = ",", header = 0))
        
    #CSV to JSON    
    booking_df.to_json("booking_com_to_json.json", orient = "records", date_format = "epoch")
    cleartrip_df.to_json("cleartrip_com-cleartrip_com.json", orient = "records", date_format = "epoch")
    stayzilla_df.to_json("stayzilla_com-travel_sample.json", orient = "records", date_format = "epoch")
