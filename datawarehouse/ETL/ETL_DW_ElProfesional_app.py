# =====================================
#  ETL: DataBase "El Profesional"
#  Author: Lautaro Caupolicán Ré
#  Email: caupolicanre@gmailcom
#  GitHub: https://github.com/caupolicanre
# =====================================


# ===========
#  Libraries
# ===========

import pyodbc # Connection with the database
import configparser # Configuration of the database
from sqlalchemy import create_engine # Creation of the connection to the DB

import pandas as pd # Handling of dataframes
import numpy as np # Handling of arrays



# =================================
#  Connection with the Original DB
#  
#  Requirements:
#  - ODBC Driver: https://learn.microsoft.com/es-es/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16
#  - Microsoft Access Driver: https://www.microsoft.com/en-us/download/details.aspx?id=13255
# =================================

# Name of the ODBC DSN
dsn_name = 'base_ElProfesional'

# Read the configuration from the secrets.ini file (contains username and password)
config = configparser.ConfigParser()
config.read('secrets.ini')
username = config['database']['username']
password = config['database']['password']

conn = pyodbc.connect(f'DSN={dsn_name};UID={username};PWD={password}')
cursor = conn.cursor()



# =======================================
#  Save Information from the Original DB
# =======================================

DB_tablesNamesToConsult = ['Articulos', 'CabVentas', 'Clientes', 'ItemVentas', 'Rubros', 'TipoCliente', 'Vendedor']

# Store DB Tables
table_info_list = [] # List of dictionaries to store information about the tables
DB_tables = {}  # Stores the dataframes of the tables that contain data


for table in DB_tablesNamesToConsult:
    query = f'SELECT * FROM {table}'
    # Store the result in a dataframe
    DB_tables[table] = pd.read_sql(query, conn)

conn.close()



# ================================================
#  Data loading, cleaning, and dimension creation
# ================================================

# Create a dataframe with the data from the CabVentas table
df_CabVentas = DB_tables['CabVentas']

# Create a dataframe with the data from the ItemVentas table
df_ItemVentas = DB_tables['ItemVentas']


# ===================
#  Dimension: Rubros
# ===================
# Crear un dataframe con los datos de la tabla Rubros
df_Rubros = DB_tables['Rubros']

df_RubrosFiltered = df_Rubros[['Rubro', 'Subrubro1', 'Subrubro2', 'Subrubro3', 'Nombre']]

# Convertir a string las columnas que voy a usar
df_RubrosFiltered['Rubro'] = df_RubrosFiltered['Rubro'].astype(str)
df_RubrosFiltered['Subrubro1'] = df_RubrosFiltered['Subrubro1'].astype(str)
df_RubrosFiltered['Subrubro2'] = df_RubrosFiltered['Subrubro2'].astype(str)
df_RubrosFiltered['Subrubro3'] = df_RubrosFiltered['Subrubro3'].astype(str)

# Rellenar con ceros a la izquierda para que todos los valores tengan 3 dígitos
df_RubrosFiltered['Rubro'] = df_RubrosFiltered['Rubro'].str.zfill(3)
df_RubrosFiltered['Subrubro1'] = df_RubrosFiltered['Subrubro1'].str.zfill(3)
df_RubrosFiltered['Subrubro2'] = df_RubrosFiltered['Subrubro2'].str.zfill(2)
df_RubrosFiltered['Subrubro3'] = df_RubrosFiltered['Subrubro3'].str.zfill(1)

# Concatenar las columnas
df_RubrosFiltered['IDRubro'] = df_RubrosFiltered['Rubro'] + df_RubrosFiltered['Subrubro1'] + df_RubrosFiltered['Subrubro2'] + df_RubrosFiltered['Subrubro3']


# Eliminar las columnas que no me sirven
df_RubrosFiltered = df_RubrosFiltered.drop(columns=['Rubro', 'Subrubro1', 'Subrubro2', 'Subrubro3'])

# Renombrar la columna IDRubro a idrubro
df_RubrosFiltered = df_RubrosFiltered.rename(columns={'IDRubro': 'idrubro',
                                                    'Nombre': 'nombre'})


# Create category 'SIN RUBRO'
df_RubrosFiltered.loc[-1] = ['SIN RUBRO', '000000000']
df_RubrosFiltered.index = df_RubrosFiltered.index + 1

# Sort by 'idrubro'
df_RubrosFiltered = df_RubrosFiltered.sort_values(by=['idrubro'])
df_RubrosFiltered = df_RubrosFiltered[['idrubro', 'nombre']]

# Convert to int the column 'idrubro'
df_RubrosFiltered['idrubro'] = df_RubrosFiltered['idrubro'].astype(int)