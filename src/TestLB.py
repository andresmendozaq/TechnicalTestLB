#Se importan las librarias a utilizar

from urllib.request import urlopen
import urllib.request
import json
import pandas as pd
import pandas_profiling
import os
import sqlite3
from datetime import datetime
from datetime import timedelta



#Se toma la ruta de la carpeta donde se aloja el archivo .py para guardar los futuros archivos
url_arch = os.getcwd()
Ruta=url_arch[:url_arch.find("src")]

#Se crean y definen las listas y Diccionarios auxiliares:
Datos_Json = list([]) #para albergar los datos de cada json obtenido
Dict_Series = dict([]) #para albergar los datos del subdiccionario Series
lista_generos = list([]) #para albergar los datos de los generos de cada serie

#Se crean y definen los dataframes a utilizar en el modelo:
EMISIONES = pd.DataFrame([])
SERIES = pd.DataFrame([])
GENEROS = pd.DataFrame([])

#Se crean y definen las variables de fecha para conocen el periodo que se desea modelar
fecha_inicial="2020-12-01"
fecha_final="2020-12-31"


#Se crea la funcion para calcular el número de dias a consultar
def num_dias(f_inicial,f_final):
    er_c=0
    if not isinstance(f_inicial,(str)):
        raise ValueError("La fecha inicial debe ser una fecha tipo AAAA-MM-DD")
    if not isinstance(f_final,(str)):
        raise ValueError("La fecha final debe ser una fecha tipo AAAA-MM-DD")

    try:
        f_ini = datetime.strptime(f_inicial, "%Y-%m-%d")
    except:
        er_c=1
        raise ValueError("La fecha inicial no está en formato AAAA-MM-DD")

    if er_c!=1:


        try:
            f_fin = datetime.strptime(f_final, "%Y-%m-%d")
        except:
            er_c = 2
            raise ValueError("La fecha final no está en formato AAAA-MM-DD")

        if er_c !=2:

            if datetime.strptime(f_inicial, "%Y-%m-%d") > datetime.strptime(f_final, "%Y-%m-%d"):
                raise ValueError("La fecha inicial debe ser anterior a la fecha final")

            if datetime.strptime(f_inicial, "%Y-%m-%d") > datetime.today():
                raise ValueError("La fecha inicial debe ser anterior al día de hoy")

            if datetime.strptime(f_final, "%Y-%m-%d") > datetime.today():
                raise ValueError("La fecha final debe ser anterior al día de hoy")

            else:

                dif= datetime.strptime(f_final, '%Y-%m-%d')-datetime.strptime(f_inicial, '%Y-%m-%d')
                n_Dias=dif.days+1
                return n_Dias

#Se calcula los días a consultar
nDias=num_dias(fecha_inicial,fecha_final)

#Se calcula el número de días a consultar (número de archivos json a cargar)
nDias=num_dias(fecha_inicial,fecha_final)
fec=datetime.strptime(fecha_inicial, '%Y-%m-%d')



#Se crea una función para verificar si hay conexión a internet
def coneccion(host='http://google.com'):
    try:
        urllib.request.urlopen(host)
        return True
    except:
        return False

#Se verifica si hay conexión, si es el caso se ejecuta el programa normalmente
if coneccion()==False:
    raise ValueError("Debes estar conectado a internet para poder realizar las consultas a la API")

else:


    #Se crea la función para la consulta y exportación de los archivos json
    def consulta_jsons(fecha_ini,Dias):

        if not isinstance(fecha_ini,(datetime)):
            raise ValueError("La fecha inicial debe estar en formato de fecha")
        if not isinstance(Dias, (int)):
            raise ValueError("El número de dias debe ser un número entero")
        if Dias<=0:
            raise ValueError("El número de dias debe ser mayor a cero")
        if fecha_ini> datetime.today():
            raise ValueError("La fecha inicial debe ser anterior al día de hoy")
        if fecha_ini+timedelta(days=Dias)> datetime.today():
            raise ValueError("La fecha final estimada debe ser anterior al día de hoy")
        else:

            fec_a=fecha_ini
            Datos_JAux = list([])
            for i in range(0, Dias):

                fec_str = fec_a.strftime('%Y-%m-%d')
                url = "http://api.tvmaze.com/schedule/web?date="+ fec_str
                response = urlopen(url)
                data_json = json.loads(response.read())
                json_string= json.dumps(data_json)
                with open(Ruta + "/json/Datos " + fec_str + ".json", "w+") as f:
                    f.write(json_string)

                Datos_JAux=Datos_JAux+data_json

                fec_a=fec_a+timedelta(days=1)

            return Datos_JAux


    #Se realizan las consulta de todos los días en la API y se guarda la información en la lista Datos_Json
    Datos_Json=consulta_jsons(fec,nDias)


    #Se lleva la información de la lista Datos_Json a al diccionario Dict_Emisiones para facilidad en las consultas
    Dict_Emisiones=dict([])
    for i in range(0,len(Datos_Json)):
        Dict_Emisiones[i]=Datos_Json[i]
    
    #Se ingresa la información de Dict_Emisiones en el Dataframe EMISIONES y se eliminan los subdiccionarios
    EMISIONES=pd.DataFrame.from_dict(Dict_Emisiones,orient="index")
    EMISIONES.drop(["rating","_embedded","image","_links"], inplace=True, axis=1)
    
    #Se ingresa a EMISIONES la información relevante de los subdiccionarios eliminados
    #Se ingresa la información del subdiccionario Show de Dict_Emisiones en el Dataframe en diccionario auxiliar Dict_Series
    for i in range(0,len(Dict_Emisiones)):
        EMISIONES.loc[i,"rating.average"]=Dict_Emisiones[i]["rating"]["average"]
        EMISIONES.loc[i, "_links.self.href"] = Dict_Emisiones[i]["_links"]["self"]["href"]
        EMISIONES.loc[i, "Id Serie"] = Dict_Emisiones[i]["_embedded"]["show"]["id"]
        Dict_Series[i]=Dict_Emisiones[i]["_embedded"]["show"]
    
    #Se ingresa la información de Dict_Series en el Dataframe SERIES y se eliminan los subdiccionarios
    #Se renombra el id del DataFrame para evitar errores con el id del Dataframe EMISIONES
    SERIES=pd.DataFrame.from_dict(Dict_Series,orient="index")
    SERIES.drop(["genres","_links","network",'dvdCountry',"schedule","rating","webChannel","externals","image"], inplace=True, axis=1)
    SERIES.rename(columns = {'id':'Id Serie'}, inplace = True)
    
    #Se ingresa a GENEROS la información de las listas de generos de cada serie
    #Se ingresa a SERIES la información relevante de los subdiccionarios eliminados (la información de paises vacios, se reemplaza por "No disponible"
    
    for i in range(0,len(Dict_Emisiones)):
        lista_generos=Dict_Emisiones[i]["_embedded"]["show"]["genres"]
        for j in range(0,len(lista_generos)):
            ln=len(GENEROS)
            GENEROS.loc[ln, "Id Serie"] = Dict_Emisiones[i]["_embedded"]["show"]["id"]
            GENEROS.loc[ln,"Genero"]=lista_generos[j]
    
        try:
            SERIES.loc[i, "Pais"] = Dict_Emisiones[i]["_embedded"]["show"]["webChannel"]["country"]["name"]
        except:
            SERIES.loc[i, "Pais"] = "No disponible"
    
    

    #Se genera el profiling en HTML para los dataframes EMISIONES, SERIES y GENEROS
    reporte=EMISIONES.profile_report(sort="ascending",html={'style':{'full_width':True}})
    reporte.to_file(Ruta + "profiling/Profiling EMISIONES.html")
    
    reporte=SERIES.profile_report(sort="ascending",html={'style':{'full_width':True}})
    reporte.to_file(Ruta + "profiling/Profiling SERIES.html")
    
    reporte=GENEROS.profile_report(sort="ascending",html={'style':{'full_width':True}})
    reporte.to_file(Ruta + "profiling/Profiling GENEROS.html")
    
    
    #Se ingresan los dataframes en la base de datos de sqlite
    #Se revisa si la base de datos ya existe para evitar errores en el registro de información
    if os.path.isfile(Ruta + "db/Base de datos.db")==True:
        conn = sqlite3.connect(Ruta + "db/Base de datos.db")
        cur=conn.cursor()
        cur.execute("DROP TABLE EMISIONES")
        cur.execute("DROP TABLE SERIES")
        cur.execute("DROP TABLE GENEROS")
        EMISIONES.to_sql(name='EMISIONES', con=conn)
        SERIES.to_sql(name='SERIES', con=conn)
        GENEROS.to_sql(name='GENEROS', con=conn)
    
    else:
        conn=sqlite3.connect(Ruta + "db/Base de datos.db")
        EMISIONES.to_sql(name='EMISIONES', con=conn)
        SERIES.to_sql(name='SERIES', con=conn)
        GENEROS.to_sql(name='GENEROS', con=conn)
    
    #Se generan los procesos ETL  para crear el modelo nuevo de datos>>
    
    
    #Se toman sólo las columnas relevantes para el modelo
    MODELO_EMISIONES=EMISIONES[["id","name","runtime","airdate","type","Id Serie","rating.average"]]
    MODELO_SERIES=SERIES[["Id Serie","Pais","name"]]
    MODELO_GENEROS=GENEROS[["Id Serie","Genero"]]
    
    #Se renombran las columnas para facilidad de análisis
    MODELO_EMISIONES = MODELO_EMISIONES.rename(columns={'name': 'Nombre Episodio',
                                                        'airdate': 'Fecha',
                                                        'type': 'Tipo',
                                                        'rating.average': 'Rating'
                                                        })
    
    MODELO_SERIES = MODELO_SERIES.rename(columns={'name': 'Nombre Serie'})
    MODELO_SERIES.drop_duplicates(inplace = True)
    MODELO_GENEROS.drop_duplicates(inplace = True)
    
    #Se exporta el modelo de datos nuevo
    MODELO_EMISIONES.to_csv(Ruta +"model/MODELO_EMISIONES.csv")
    MODELO_SERIES.to_csv(Ruta +"model/MODELO_SERIES.csv")
    MODELO_GENEROS.to_csv(Ruta +"model/MODELO_GENEROS.csv")
    
