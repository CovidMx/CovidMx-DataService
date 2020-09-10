""" Script to get and analyze dataset"""

import urllib.request
import io
from zipfile import ZipFile
import pandas as pd

    #Step 1: Download the zip file
dataset_url = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
response = urllib.request.urlopen(dataset_url)
data_zip = response.read()
data_zip = io.BytesIO(data_zip)
data_zip = ZipFile(file = data_zip)

    #Step 2: Extract the csv file
csv_filename = data_zip.namelist()[0]
data_csv = data_zip.read(csv_filename)
data_csv = io.BytesIO(data_csv)

    #Step 3: Read the csv (in chunks because the csv is large)
chunks = pd.read_csv(data_csv, encoding='ANSI', chunksize=100000)



