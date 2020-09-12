""" Script to get and analyze dataset"""

import urllib.request
import io
from zipfile import ZipFile
import pandas as pd
import mysql.connector
from datetime import date,datetime,timedelta

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
chunks = pd.read_csv(data_csv, encoding='ANSI', chunksize=100000, low_memory=False)

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
    cases = 0
    active_cases = 0
    recovered = 0
    deaths = 0
    cases_per_1k = 0
    def calc_stats(self,population):
        self.active_cases = self.cases - self.recovered - self.deaths
        self.cases_per_1k = self.cases / population * 1000

stats_per_age = list()
for i in range(10):
    stats_per_age.append(list())
    stats_per_age[i].append(StatsPerAges())
    stats_per_age[i].append(StatsPerAges())

stats_per_state = list()
for i in range(32):
    stats_per_state.append(casesPerState())

def sum(result,age,sex,fecha_def,diabetes,epoc,asma,hipertension,obesidad,tabaquismo,entidad,fecha_sintomas,tipo):
    if result == 1:
        age_range = age // 10
        if age_range > 8: age_range = 8
        sex = (sex - 1 if sex != 99 else 0)
        entidad = entidad-1

        stats_per_age[age_range][sex].cases += 1
        stats_per_age[9][sex].cases += 1

        if entidad<32: stats_per_state[entidad].cases += 1

        if fecha_def != "9999-99-99":
            stats_per_age[age_range][sex].deaths += 1
            stats_per_age[9][sex].deaths += 1
            if entidad<32: stats_per_state[entidad].deaths += 1
            
            if diabetes == 1:
                stats_per_age[age_range][sex].d_diabetes += 1
                stats_per_age[9][sex].d_diabetes += 1
            if epoc == 1:
                stats_per_age[age_range][sex].d_epoc += 1
                stats_per_age[9][sex].d_epoc += 1
            if asma == 1:
                stats_per_age[age_range][sex].d_asma += 1
                stats_per_age[9][sex].d_asma += 1
            if hipertension == 1:
                stats_per_age[age_range][sex].d_hipertension += 1
                stats_per_age[9][sex].d_hipertension += 1
            if obesidad == 1:
                stats_per_age[age_range][sex].d_obesidad += 1
                stats_per_age[9][sex].d_obesidad += 1
            if tabaquismo == 1:
                stats_per_age[age_range][sex].d_tabaquismo += 1
                stats_per_age[9][sex].d_tabaquismo += 1
        else:
            today = date.today()
            fecha_sintomas = datetime.strptime(fecha_sintomas,"%Y-%m-%d").date()
            if tipo == 1 and fecha_sintomas < today + timedelta(days=14):
                stats_per_age[age_range][sex].recovered += 1
                stats_per_age[9][sex].recovered += 1
                if entidad < 32 : stats_per_state[entidad].recovered += 1
        

        if diabetes == 1:
            stats_per_age[age_range][sex].c_diabetes += 1
            stats_per_age[9][sex].c_diabetes += 1
        if epoc == 1:
            stats_per_age[age_range][sex].c_epoc += 1
            stats_per_age[9][sex].c_epoc += 1
        if asma == 1:
            stats_per_age[age_range][sex].c_asma += 1
            stats_per_age[9][sex].c_asma += 1
        if hipertension == 1:
            stats_per_age[age_range][sex].c_hipertension += 1
            stats_per_age[9][sex].c_hipertension += 1
        if obesidad == 1:
            stats_per_age[age_range][sex].c_obesidad += 1
            stats_per_age[9][sex].c_obesidad += 1
        if tabaquismo == 1:
            stats_per_age[age_range][sex].c_tabaquismo += 1
            stats_per_age[9][sex].c_tabaquismo += 1
    
            
    #Analyze the data
for chunk in chunks:
    [sum(result,age,sex,fecha_def,diabetes,epoc,asma,hipertension,obesidad,tabaquismo,int(entidad),fecha_sintomas,tipo) for result,age,sex,fecha_def,diabetes,epoc,asma,hipertension,obesidad,tabaquismo,entidad,fecha_sintomas,tipo in zip(chunk['RESULTADO'],chunk["EDAD"],chunk["SEXO"],chunk["FECHA_DEF"],chunk["DIABETES"],chunk["EPOC"],chunk["ASMA"],chunk["HIPERTENSION"],chunk["OBESIDAD"],chunk["TABAQUISMO"],chunk["ENTIDAD_RES"],chunk["FECHA_SINTOMAS"],chunk["TIPO_PACIENTE"])]

for i in range(10):
    print("/"*10,str(i*10) + "-" + str(i*10+9),"/"*10)
    stats_per_age[i][0].calc_stats()
    stats_per_age[i][1].calc_stats()


    #connect to DB
db = mysql.connector.connect(
  host="3.129.172.84",
  user="root",
  password="adminsql.server",
  database="covidmx",
  port = "3320"
)

db_cursor=db.cursor()


get_population_query = "SELECT poblacion FROM Entidades ORDER BY entidad_id ASC"

db_cursor.execute(get_population_query)
populations = db_cursor.fetchall()

for i in range(32):
    stats_per_state[i].calc_stats(populations[i][0])
    print(i,stats_per_state[i].cases,stats_per_state[i].cases_per_1k)

entidades_query=("REPLACE INTO CasosPorEstado(casos_por_estado_id,casos_activos,recuperados,muertes,casos_por_mil,entidad_id_id) "
                 "VALUES (%s,%s,%s,%s,%s,%s)")
entidades_data = list()
for i in range(32):
    id = i+1
    entidades_data.append((id,stats_per_state[i].active_cases,stats_per_state[i].recovered,stats_per_state[i].deaths,stats_per_state[i].cases_per_1k,id))
db_cursor.executemany(entidades_query,entidades_data)
db.commit()
stats_query=("REPLACE INTO EstadisticasPorEdad(estadisticas_por_edad_id,rango,cantidad_casos,porcentaje_mortalidad,m_diabetes,m_epoc,m_asma,m_hipertension,m_obesidad,m_tabaquismo,muertes,recuperados,sexo) "
             "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
stats_data=list()
for i in range(10):
    idH,idM = i,10+i
    sexM,sexH = "F","M"
    rango = "General"
    if i<8 : rango = str(i*10) + "-" + str(i*10+9)
    elif i==8 : rango = "80+"
    tupleH = (idH,rango,stats_per_age[i][1].cases,stats_per_age[i][1].mortality,stats_per_age[i][1].m_diabetes,stats_per_age[i][1].m_epoc,stats_per_age[i][1].m_asma,stats_per_age[i][1].m_hipertension,stats_per_age[i][1].m_obesidad,stats_per_age[i][1].m_tabaquismo,stats_per_age[i][1].deaths,stats_per_age[i][1].recovered,sexH)
    tupleM = (idM,rango,stats_per_age[i][0].cases,stats_per_age[i][0].mortality,stats_per_age[i][0].m_diabetes,stats_per_age[i][0].m_epoc,stats_per_age[i][0].m_asma,stats_per_age[i][0].m_hipertension,stats_per_age[i][0].m_obesidad,stats_per_age[i][0].m_tabaquismo,stats_per_age[i][0].deaths,stats_per_age[i][0].recovered,sexM)
    stats_data.append(tupleM)
    stats_data.append(tupleH)

db_cursor.executemany(stats_query,stats_data)
db.commit()




            
        
    


