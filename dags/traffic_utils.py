import pandas as pd
import os
#from sodapy import Socrata

#_airflow_root = os.path.join('Intervals', 'airflow')
#_pipe_root = os.path.join(_airflow_root, 'us-central1-transit-pipe-en-af986b93-bucket')
#_dags_root = os.path.join(_pipe_root, 'dags')
_pipe_root = os.path.dirname(__file__)
_pipe_root = _pipe_root.split("/")
_data_root = os.path.join(_pipe_root[-2], 'data')

def import_data():               #verificar argumentos
   
   #novos dados
   #client = Socrata("data.cityofnewyork.us", "TwSER6tYF74jBi5yYFZAj9HYy")
   #results = client.get("i4gi-tjb9", limit=2000)
   #df_new = pd.DataFrame.from_records(results)
   
   df_new = pd.read_json("https://data.cityofnewyork.us/resource/i4gi-tjb9.json")

   df_new.to_csv(os.path.join(_data_root, "01_orig_data.csv"))
   return 

def pre_proc_calc_medias():   
   df = pd.read_csv(os.path.join(_data_root, "01_orig_data.csv"), index_col=0)

   link_points = df.link_points
   media_lat = []
   media_long = []

   for x in link_points:     #para cada linha 
      points = x.split(' ')  #separando as coordenadas
      lat = []
      long = []
      for y in points:    #para cada ponto (lat, long) de points_str
         point = y.split(',')   #separa lat de long
         
         if(point[0] == ''):
               lat.append(None)
         else:
               lat.append(float(point[0]))
               
         if(len(point) == 2 and point[1] != ''):          #Alguns estao vazios(?)
               if(point[1] != "-"):
                  long.append(float(point[1]))
               else:
                  long.append(None)
         else:
               long.append(None)
         
      long = list(filter(None, long))
      lat = list(filter(None, lat))

      media_lat.append(sum(lat)/len(lat))
      media_long.append(sum(long)/len(long))
   
   #novo dataset com as medias e sem colunas que nao serao utilizadas
   obj = {'data_as_of': df.data_as_of,'link_points': df.link_points, 'media_lat': media_lat, 'media_long': media_long, 'speed': df.speed, 'travel_time': df.travel_time}
   data_frame = pd.DataFrame(data=obj)
   data_frame.to_csv(os.path.join(_data_root, "02_pre_data_medias.csv"))
   

def pre_proc_clean_data():
   df = pd.read_csv(os.path.join(_data_root, "02_pre_data_medias.csv"), index_col=0)

   df = df.drop(df[df.speed == 0].index) #removendo linhas onde speed = 0
   df.to_csv(os.path.join(_data_root, "03_pre_data_clean.csv"))

def pre_proc_merge_new_data():
   df_old = pd.read_csv(os.path.join(_data_root,"04_data.csv"), index_col=0)   #dados gerados na requisicao anterior
   df_new = pd.read_csv(os.path.join(_data_root,"03_pre_data_clean.csv"), index_col=0)   #dados gerados na requisicao atual

   #conversoes necessarias para fazer o merge dos data_frames
   df_new['data_as_of'] = pd.to_datetime(df_new.data_as_of)
   df_old['data_as_of'] = pd.to_datetime(df_old.data_as_of)

   frames = [df_old, df_new]
   df = pd.concat(frames)
   #df = pd.merge(df_old, df_new)
   df.to_csv(os.path.join(_data_root,"04_data.csv"))