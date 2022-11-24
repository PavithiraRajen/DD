import datetime
import logging

import azure.functions as func

import time
import io 
import pandas as pd
import requests
from io import StringIO
from bs4 import BeautifulSoup	
from datetime import date
from apify_client import ApifyClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobClient , ContentSettings

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    f_df = serp_final(keywords) 
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)



keywords = """software documentation tools
software documentation
product documentation software
create a knowledge base
internal knowledge base software
knowledge base software
create user manual
user documentation
knowledge management software
knowledge management
wiki software
technical documentation
self service knowledge base
customer service knowledge base
faq software
project documentation
api documentation
create faq page
process documentation
internal documentation
helpjuice alternatives
bloomfire alternatives
proprofs knowledge base alternative"""


def serp(keywords):
    # Initialize the ApifyClient with your API token
    client = ApifyClient("apify_api_igRmVyIFlHdR5H5vPTU2hsKlfs6DDQ1q2xQB")
    
    # Prepare the actor input
    run_input = {
        "queries": keywords,
        "resultsPerPage": 25,
        "maxPagesPerQuery": 1,
        "countryCode": "us",
        "customDataFunction": """async ({ input, $, request, response, html }) => {
      return {
        pageTitle: $('title').text(),
      };
    };""",
    }
    
    # Run the actor and wait for it to finish
    run = client.actor("apify/google-search-scraper").call(run_input=run_input)
    
    a = []
    # Fetch and print actor results from the run's dataset (if there are any)
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        print(item)
        a.append(item)
        
    
    keyword = []
    link = []
    emphasizedKeywords = []    
    title = []
    position = []
    
    for i in a:
        res = i['organicResults']
        p =0
        for r in res:
            p = p+1
            u = r['url']
            t = r['title']
            ek = r['emphasizedKeywords']
            k = i['customData']['pageTitle']
            
            keyword.append(k)
            link.append(u)
            emphasizedKeywords.append(ek) 
            title.append(t)
            position.append(p)
            
    df = pd.DataFrame()
    
    df['Keyword'] = keyword
    df['Position'] = position
    df['Link'] = link
    df['EmphasizedKeywords'] = emphasizedKeywords
    df['Title'] = title
    df['Date'] = date.today()
    #df.to_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.csv",  index=False)

    
    return df


#to find the ranking of D360
def d360(df1):
    df = df1[~df1["Position"].isin([21,22, 23, 24, 25])].reset_index()
    k = []
    r = []
    
    for i in range(0, len(df)):
        if "document360.com" in df['Link'][i]:
            k.append(df['Keyword'][i])
            r.append(df['Position'][i])
            pass

    d = pd.DataFrame()
    d['Keyword'] = k
    d['Rank'] = r
    d['Site'] = "Document360"
    d['Date'] = date.today()
    d = d.drop_duplicates(subset=['Keyword'], keep='first')
    #d.to_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\d360.csv",  index=False)
    
    return d

   

def serp_final(keywords):
    df1 = serp(keywords) 
    df2 = d360(df1) 
    logging.info("Running SERP and D360 function")

    #logging.info("Connetion to SERP storage account")
    storage_account_key = "dGmbkWa7FreeHGq3OukLOKFyXn/1jblM7mTsK840dnQLQ9Cm51cjHwYA/QB0Nc8WaUTYZvRX3u9x+ASt9WDqUw=="
    connection_str= "DefaultEndpointsProtocol=https;AccountName=testdsaccount;AccountKey=dGmbkWa7FreeHGq3OukLOKFyXn/1jblM7mTsK840dnQLQ9Cm51cjHwYA/QB0Nc8WaUTYZvRX3u9x+ASt9WDqUw==;EndpointSuffix=core.windows.net"
    account_url = "https://testdsaccount.blob.core.windows.net/"
    blob_service_client = BlobServiceClient(account_url=account_url , credential=storage_account_key)
    logging.info('Connection made to SERP storage account')

    #print("Creating a SERP D360 Container")
    #container_client = ContainerClient.from_connection_string(conn_str=connection_str , container_name="serp-d360")
    #container_client.create_container()
    #rint("Container SERP D360  Created")

    #logging.info("Downloading the csv blob to Azure SERP function...")

    blob_client_instance= blob_service_client.get_blob_client(container="serpdb",blob="serp.csv",snapshot= None)
    blob_data = blob_client_instance.download_blob()
    data = blob_data.readall()

    s=str(data,'utf-8')
    table = StringIO(s)
    k1=pd.read_csv(table)

    logging.info("serp.csv is called to Azure function")
    
    #k1 = pd.read_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.csv")
    #k = pd.read_excel(r'C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.xlsx', sheet_name='serp_visibility')
    #k['Date'] = k['Date'].dt.strftime('%d-%m-%Y')
    if df1['Date'][0].strftime("%Y-%m-%d") in k1['Date'].unique():
        logging.info("Data Available")
        
    else:
        j1 = k1.append(df1)
        #j1.to_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.csv",  index=False)

        serp.csv = io.StringIO()
        serp.csv = j1.to_csv(encoding = 'utf-8', index = False)

        print("uploading a CSV file to blob...")
        blob = BlobClient.from_connection_string(conn_str=connection_str,
        container_name="serpdb", blob_name="serp.csv")

        blob.upload_blob(serp.csv,overwrite= True)
        logging.info("serp.csv is updated and uploaded to container")


        #j.to_excel(r'C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.xlsx', sheet_name='serp_visibility', index=False)
    #k1 = pd.read_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.csv")

    

    blob_client_instance= blob_service_client.get_blob_client(container="serpdb",blob="d360.csv",snapshot= None)
    blob_data = blob_client_instance.download_blob()
    data = blob_data.readall()

    s=str(data,'utf-8')
    table = StringIO(s)
    k2=pd.read_csv(table)

    logging.info("d360.csv is called to Azure function")
    

    
    
    #k2 = pd.read_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\d360.csv")
    #k = pd.read_excel(r'C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.xlsx', sheet_name='serp_visibility')
    #k['Date'] = k['Date'].dt.strftime('%d-%m-%Y')


    if df2['Date'][0].strftime("%Y-%m-%d") in k2['Date'].unique():
        logging.info("Data Available")
    else:
        j2 = k2.append(df2)
        #j2.to_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\d360.csv",  index=False)

        d360.csv = io.StringIO()
        d360.csv= j2.to_csv(encoding = 'utf-8', index = False)

        print("uploading a CSV file to blob...")
        blob = BlobClient.from_connection_string(conn_str=connection_str,
        container_name="serpdb", blob_name="d360.csv")

        blob.upload_blob(d360.csv,overwrite= True)
        logging.info("d360.csv is updated and uploaded to container")

        #j.to_excel(r'C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\serp.xlsx', sheet_name='serp_visibility', index=False)
    #k2 = pd.read_csv(r"C:\Users\PavithiraRajendran\OneDrive - Kovai Ltd\Desktop\SEO_Automation\d360.csv")
   
   
    return df1, df2

#f_df = serp_final(keywords) 

       
    


