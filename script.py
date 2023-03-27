import pandas as pd
import xml.etree.ElementTree as ET
import glob
from datetime import datetime

#ETL INTO SINGLE TABLE
targetFile = 'transformed.csv'
logFile = 'logfile.txt'

#EXTRACT PHASE
    #CSV
def extract_csv(file):
    dataframe = pd.read_csv(file)
    return dataframe
    #JSON
def extract_json(file):
    dataframe = pd.read_json(file,lines=True)
    return dataframe
    #XML
def extract_xml(file):
    dataframe = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    tree = ET.parse(file)
    root = tree.getroot()
    for i in root:
        car = i.find('car_model').text
        year = i.find('year_of_manufacture').text
        price = float(i.find('price').text)
        fuel = i.find('fuel').text
        dataframe = dataframe.append({'car_model':car,'year_of_manufacture':year,'price':price,'fuel':fuel},ignore_index=True)
    return dataframe
    #extract_all
def extract():
    extractedData = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    for csv in glob.glob('datasource/*.csv'):
        extractedData = extractedData.append(extract_csv(csv),ignore_index = True)
    for json in glob.glob('datasource/*.json'):
        extractedData = extractedData.append(extract_json(json),ignore_index = True)
    for xml in glob.glob('datasource/*.xml'):
        extractedData = extractedData.append(extract_xml(xml),ignore_index = True)
    return extractedData

#TRANSFORM PHASE
    #round price into 2 decimal
def transform(data):
    data['price'] = round(data.price,2)
    return data

#LOAD PHASE
    #load all data to csv
def load(source,target):
    source.to_csv(target)

#LOGGING
def log(message):
    format = '%d-%m-%Y / %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(format)
    with open(logFile,'a') as f:
        f.write(f'{message} ...Updated : {timestamp}' + '\n')

#RUN ETL
log('ETL START')

log('Extract Phase Started')
extracted = extract()
log('Extract Phase Ended')

log('Transform Phase Started')
transformed = transform(extracted)
log('Transform Phase Ended')

log('Load Phase Start')
load(transformed,targetFile)
log('Load Phase Ended')

log('ETL ENDED')
