""" Script to get and analyze dataset"""

import urllib.request
import io
from zipfile import ZipFile
import pandas as pd

    #Download the zip file
dataset_url = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
response = urllib.request.urlopen(dataset_url)
data_zip = response.read()
data_zip = io.BytesIO(data_zip)
data_zip = ZipFile(file = data_zip)

    #Extract the csv file
csv_filename = data_zip.namelist()[0]
data_csv = data_zip.read(csv_filename)
data_csv = io.BytesIO(data_csv)

    #Read the csv (in chunks because the csv is large)
chunks = pd.read_csv(data_csv, encoding='ANSI', chunksize=100000)

    #Clases for data

class StatsPerAges:
    cases = 0
    deaths = 0
    recovered = 0
    mortality = 0
    c_diabetes = 0
    d_diabetes = 0
    m_diabetes = 0
    c_epoc = 0
    d_epoc = 0
    m_epoc = 0
    c_asma = 0
    d_asma = 0
    m_asma = 0
    c_hipertension = 0
    d_hipertension = 0
    m_hipertension = 0
    c_obesidad = 0
    d_obesidad = 0
    m_obesidad = 0
    c_tabaquismo = 0
    d_tabaquismo = 0
    m_tabaquismo = 0
    def calc_stats(self):
        self.mortality = self.deaths / self.cases if self.cases > 0 else 0
        self.m_diabetes = self.d_diabetes / self.c_diabetes if self.c_diabetes > 0 else 0
        self.m_epoc = self.d_epoc / self.c_epoc if self.c_epoc > 0 else 0
        self.m_asma = self.d_asma / self.c_asma if self.c_asma > 0 else 0
        self.m_hipertension = self.d_hipertension / self.c_hipertension if self.c_hipertension > 0 else 0
        self.m_obesidad = self.d_obesidad / self.c_obesidad if self.c_obesidad > 0 else 0
        self.m_tabaquismo = self.d_tabaquismo / self.c_tabaquismo if self.c_tabaquismo > 0 else 0
    
class casesPerState:
    active_cases = 0
    recovered = 0
    deaths = 0
    cases_per_1k = 0

stats = list()
for i in range(10):
    stats.append(list())
    stats[i].append(StatsPerAges())
    stats[i].append(StatsPerAges())

    #Analyze the data
for chunk in chunks:
    for index,patient in chunk.iterrows():
        if patient['RESULTADO'] == 1:
            age_range = patient['EDAD'] // 10
            if age_range > 8: age_range = 8
            sex = (patient['SEXO'] - 1 if patient['SEXO'] != 99 else 0)

            stats[age_range][sex].cases += 1
            stats[9][sex].cases += 1

            if patient["FECHA_DEF"] != "9999-99-99":
                stats[age_range][sex].deaths += 1
                stats[9][sex].deaths += 1

                if patient["DIABETES"] == 1:
                    stats[age_range][sex].d_diabetes += 1
                    stats[9][sex].d_diabetes += 1
                if patient["EPOC"] == 1:
                    stats[age_range][sex].d_epoc += 1
                    stats[9][sex].d_epoc += 1
                if patient["ASMA"] == 1:
                    stats[age_range][sex].d_asma += 1
                    stats[9][sex].d_asma += 1
                if patient["HIPERTENSION"] == 1:
                    stats[age_range][sex].d_hipertension += 1
                    stats[9][sex].d_hipertension += 1
                if patient["OBESIDAD"] == 1:
                    stats[age_range][sex].d_obesidad += 1
                    stats[9][sex].d_obesidad += 1
                if patient["TABAQUISMO"] == 1:
                    stats[age_range][sex].d_tabaquismo += 1
                    stats[9][sex].d_tabaquismo += 1

            if patient["DIABETES"] == 1:
                stats[age_range][sex].c_diabetes += 1
                stats[9][sex].c_diabetes += 1
            if patient["EPOC"] == 1:
                stats[age_range][sex].c_epoc += 1
                stats[9][sex].c_epoc += 1
            if patient["ASMA"] == 1:
                stats[age_range][sex].c_asma += 1
                stats[9][sex].c_asma += 1
            if patient["HIPERTENSION"] == 1:
                stats[age_range][sex].c_hipertension += 1
                stats[9][sex].c_hipertension += 1
            if patient["OBESIDAD"] == 1:
                stats[age_range][sex].c_obesidad += 1
                stats[9][sex].c_obesidad += 1
            if patient["TABAQUISMO"] == 1:
                stats[age_range][sex].c_tabaquismo += 1
                stats[9][sex].c_tabaquismo += 1

for i in range(10):
    print(i,stats[i][0].cases,stats[i][0].mortality)
    stats[i][0].calc_stats()
    stats[i][1].calc_stats()
    print(i,stats[i][0].cases,stats[i][0].mortality)




            
        
    


