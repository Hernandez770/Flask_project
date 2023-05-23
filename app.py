from flask import Flask, jsonify, request
import requests
from sqlalchemy import create_engine, text
import pandas as pd
from Scripts.funcs import *

app = Flask(__name__)
app.config["DEBUG"]=True

@app.route('/')
def hello():
    welcome = """<h1><strong><center>Welcome to my first API</h1></strong></center><br>
    <h3><center>Please, read the '/documentation' to access a basic database with values of demand at
    electrical energy in Spain</h3></center>"""
    return welcome

@app.route('/documentation')
def documentation():
    title = "<h2><center><p padding-block:20px>Endlines</h2></center></p>"
    docu = """ 
    Endpoint plotting: /get_demand?start_date=2018-01-01T00:00&end_date=2018-01-02T23:59&plot_type='bar'&orientation='vertical'
        start_date: The date you want to begin with. Format: YYYY-MM-DDTHH:mm 
        end_date: The final date that close the range of data. Format: YYYY-MM-DDTHH:mm 
        plot_type: Type of plot to show. Only can be 'bar' or 'line'.
        orientation: Reverse the plot axes. You can chose 'vertical' or 'horizontal'.

    Endpoint data-json: /get_db_data?start_date=2018-01-01T00:00&end_date=2018-01-02T23:59
        start_date (optional): Same as before.
        end_date (optional): Idem.
    """
    return title, docu

@app.route('/get_demand')
def demand():
    # Obtenemos los valores de la url:
    start_date = str(request.args["start_date"])
    end_date = str(request.args["end_date"])
    plot_type = str(request.args["plot_type"])
    orientation = str(request.args["bar_type"])

    # La API de REE solo nos deja tomar 744 horas como límite, así que implementamos funciones para solventar la limitación.
    # Creamos el dataframe con los datos obtenidos de la API:
    start_date2 = pd.to_datetime(start_date)
    end_date2 = pd.to_datetime(end_date)
    limit_hours = 743
    if total_hours(start_date2, end_date2) < limit_hours:
        url = f"https://apidatos.ree.es/en/datos/demanda/evolucion?start_date={start_date}&end_date={end_date}&time_trunc=hour&geo_trunc=electric_system&geo_limit=peninsular&geo_ids=8741"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()["included"][0]["attributes"]["values"]
            df = pd.DataFrame(data).drop("percentage", axis=1)
            df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert(None)
        else:
            return f"Error de conexión: {response.status_code}"
    else:
        range_of_date = time_period(start_date2, end_date2, limit_hours=limit_hours)
        df = pd.DataFrame()
        for period in range_of_date:
            url = f"https://apidatos.ree.es/en/datos/demanda/evolucion?start_date={period[0]}&end_date={period[1]}&time_trunc=hour&geo_trunc=electric_system&geo_limit=peninsular&geo_ids=8741"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()["included"][0]["attributes"]["values"]
                df_aux = pd.DataFrame(data).drop("percentage", axis=1)
                df = pd.concat([df, df_aux], ignore_index=True)
                df = df.drop_duplicates()
            else:
                return f"Error de conexión: {response.status_code}"
        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_convert(None)

    # Conectamos con la base de datos en Railway y nos traemos las fechas:
    engine = create_engine("postgresql://postgres:PLi33GtM5p0iuKmFQEkw@containers-us-west-76.railway.app:7787/railway")
    with engine.connect() as connection:
        table_db = pd.read_sql("SELECT datetime FROM reedb", connection)
        connection.close()
    table_db["datetime"] = pd.to_datetime(table_db["datetime"]).dt.tz_convert(None)
    table_db = table_db.sort_values(by= "datetime")

    # Comparamos los datos obtenidos de REE con la base de datos en Railway para no generar duplicados. 
    # Ploteamos la gráfica demandada después de actualizar los datos:
    if len(table_db) == 0:
        with engine.connect() as connection:
            df = df.to_string(columns= "datetime")
            df.to_sql("reedb", connection)
            connection.commit()
            connection.close()
        return plot_show(df= df, plot_type= plot_type, orientation= orientation)

    elif len(table_db) != 0:
        # Creamos un filtro para poder comparar dataframes y subir a Railway solo los que no estén duplicados:
        table_bool = pd.Series(data=[range(len(df["datetime"]))], name= "filter")

        for ind,new_date in enumerate(df["datetime"]):
            if str(new_date) in str(table_db):
                table_bool[ind] = True
            elif str(new_date) not in str(table_db):
                table_bool[ind] = False

        df["duplicated"] = table_bool.values
        add = df.drop(index= df[df["duplicated"] == True].index, axis=0).drop("duplicated", axis=1)
        with engine.connect() as connection:
            add = add.to_string(columns= "datetime")
            add.to_sql("reedb", connection)
            connection.commit()
            connection.close()
        return plot_show(df= df, plot_type= plot_type, orientation= orientation)

    else:
        return "Cannot read the database in railway"
   

@app.route('/get_db_data')
def get_data():
    engine = create_engine("postgresql://postgres:PLi33GtM5p0iuKmFQEkw@containers-us-west-76.railway.app:7787/railway")
    with engine.connect() as connection:

        # Descargamos los datos del database en Railway en base a los parámetros (opcionales):
        if request.args["start_date"]:
            start_date = str(request.args["start_date"])
            if request.args["end_date"]:
                end_date = str(request.args["end_date"])
                table_db = pd.read_sql(f"SELECT * FROM reedb WHERE datetime BETWEEN {start_date} AND {end_date}", connection)
            else:
                table_db = pd.read_sql(f"SELECT * FROM reedb WHERE datetime >= {start_date}", connection)
        elif request.args["end_date"]:
            end_date = str(request.args["end_date"])
            table_db = pd.read_sql(f"SELECT * FROM reedb WHERE datetime <= {end_date}", connection)
        else:
            table_db = pd.read_sql("SELECT * FROM reedb", connection)
        connection.close()

    return jsonify(table_db)


@app.route('/wipe_data')
def wipeout():
    if request.args["secret"]:
        code = int(request.args["secret"])
        if code == 123:
            # Conectamos con la base de datos en Railway y eliminamos los datos de la tabla:
            engine = create_engine("postgresql://postgres:PLi33GtM5p0iuKmFQEkw@containers-us-west-76.railway.app:7787/railway")
            with engine.connect() as connection:
                connection.execute(text("TRUNCATE TABLE reedb"))
                connection.commit()
                connection.close()


app.run()