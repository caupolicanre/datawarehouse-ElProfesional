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
import re # Regular expressions

from modules.update_dimensions_table import updateDimensionTable, updateDimensionTableIntPK # Function to update dimensions tables



# =================================
#  Connection with the Original DB
#  
#  Requirements:
#  - ODBC Driver: https://learn.microsoft.com/es-es/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16
#  - Microsoft Access Driver: https://www.microsoft.com/en-us/download/details.aspx?id=13255
# =================================

# Name of the ODBC DSN
dsn_name = 'base_ElProfesional'

# Read the configuration from the 'secrets.ini' file (contains username and password)
config = configparser.ConfigParser()
config.read('./datawarehouse/ETL/secrets.ini')
username = config['database']['username']
password = config['database']['password']

conn = pyodbc.connect(f'DSN={dsn_name};UID={username};PWD={password}')
cursor = conn.cursor()


# ===================================
#  Connection with the DataWarehouse
# ===================================
# Read the configuration from the 'secrets.ini' file (contains username and password)
DW_username = config['datawarehouse']['DW_username']
DW_password = config['datawarehouse']['DW_password']

# Create the connection engine
engine_cubo = create_engine(f"postgresql://{DW_username}:{DW_password}@localhost/ElProfesional_DW")



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

# Create a dataframe with the data from the 'CabVentas' table
df_CabVentas = DB_tables['CabVentas']

# Create a dataframe with the data from the 'ItemVentas' table
df_ItemVentas = DB_tables['ItemVentas']



# ===================
#  Dimension: Rubros
# ===================
# Create a dataframe with the data from the 'Rubros' table
df_Rubros = DB_tables['Rubros']

df_RubrosFiltered = df_Rubros[['Rubro', 'Subrubro1', 'Subrubro2', 'Subrubro3', 'Nombre']]

# Convert the columns that are going to be used to string
df_RubrosFiltered['Rubro'] = df_RubrosFiltered['Rubro'].astype(str)
df_RubrosFiltered['Subrubro1'] = df_RubrosFiltered['Subrubro1'].astype(str)
df_RubrosFiltered['Subrubro2'] = df_RubrosFiltered['Subrubro2'].astype(str)
df_RubrosFiltered['Subrubro3'] = df_RubrosFiltered['Subrubro3'].astype(str)

# Fill with leading zeros to ensure all values have 3 digits
df_RubrosFiltered['Rubro'] = df_RubrosFiltered['Rubro'].str.zfill(3)
df_RubrosFiltered['Subrubro1'] = df_RubrosFiltered['Subrubro1'].str.zfill(3)
df_RubrosFiltered['Subrubro2'] = df_RubrosFiltered['Subrubro2'].str.zfill(2)
df_RubrosFiltered['Subrubro3'] = df_RubrosFiltered['Subrubro3'].str.zfill(1)

# Concatenate the columns
df_RubrosFiltered['IDRubro'] = df_RubrosFiltered['Rubro'] + df_RubrosFiltered['Subrubro1'] + df_RubrosFiltered['Subrubro2'] + df_RubrosFiltered['Subrubro3']


# Remove columns that are not needed
df_RubrosFiltered = df_RubrosFiltered.drop(columns=['Rubro', 'Subrubro1', 'Subrubro2', 'Subrubro3'])

# Rename columns
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



# ======================
#  Dimension: Articulos
# ======================
# Create a dataframe with the data from the 'Articulos' table
df_Articulos = DB_tables['Articulos']

df_ArticulosFiltered = df_Articulos[['codigo', 'subcodigo', 'nombre', 'rubro', 'subrubro', 'subrubro2', 'subrubro3']]

# Convert NaN values in columns to 0
df_ArticulosFiltered['codigo'] = df_ArticulosFiltered['codigo'].fillna(999998)  # 999998 is the code for 'OTRO'
df_ArticulosFiltered['subcodigo'] = df_ArticulosFiltered['subcodigo'].fillna(0)
df_ArticulosFiltered['rubro'] = df_ArticulosFiltered['rubro'].fillna(0)
df_ArticulosFiltered['subrubro'] = df_ArticulosFiltered['subrubro'].fillna(0)
df_ArticulosFiltered['subrubro2'] = df_ArticulosFiltered['subrubro2'].fillna(0)
df_ArticulosFiltered['subrubro3'] = df_ArticulosFiltered['subrubro3'].fillna(0)


# If there is a negative or zero value in 'codigo', is replaced with 999998 (code for 'OTRO').
df_ArticulosFiltered.loc[df_ArticulosFiltered['codigo'] <= 0, 'codigo'] = 999998

# If there is a negative value in 'subcodigo' or 'rubro', is replaced with 0
df_ArticulosFiltered.loc[df_ArticulosFiltered['subcodigo'] < 0, 'subcodigo'] = 0
df_ArticulosFiltered.loc[df_ArticulosFiltered['rubro'] < 0, 'rubro'] = 0
df_ArticulosFiltered.loc[df_ArticulosFiltered['subrubro'] < 0, 'subrubro'] = 0
df_ArticulosFiltered.loc[df_ArticulosFiltered['subrubro2'] < 0, 'subrubro2'] = 0
df_ArticulosFiltered.loc[df_ArticulosFiltered['subrubro3'] < 0, 'subrubro3'] = 0


# Convert the columns that are going to be used to string
df_ArticulosFiltered['codigo'] = df_ArticulosFiltered['codigo'].astype(str)
df_ArticulosFiltered['subcodigo'] = df_ArticulosFiltered['subcodigo'].astype(str)

df_ArticulosFiltered['rubro'] = df_ArticulosFiltered['rubro'].astype(str)
df_ArticulosFiltered['subrubro'] = df_ArticulosFiltered['subrubro'].astype(str)
df_ArticulosFiltered['subrubro2'] = df_ArticulosFiltered['subrubro2'].astype(str)
df_ArticulosFiltered['subrubro3'] = df_ArticulosFiltered['subrubro3'].astype(str)


# Fill with leading zeros to ensure all values have 6 digits
df_ArticulosFiltered['codigo'] = df_ArticulosFiltered['codigo'].str.zfill(6)
df_ArticulosFiltered['subcodigo'] = df_ArticulosFiltered['subcodigo'].str.zfill(2)
df_ArticulosFiltered['rubro'] = df_ArticulosFiltered['rubro'].str.zfill(3)
df_ArticulosFiltered['subrubro'] = df_ArticulosFiltered['subrubro'].str.zfill(3)
df_ArticulosFiltered['subrubro2'] = df_ArticulosFiltered['subrubro2'].str.zfill(2)
df_ArticulosFiltered['subrubro3'] = df_ArticulosFiltered['subrubro3'].str.zfill(1)

# Concatenate the columns
df_ArticulosFiltered['IDArticulo'] = df_ArticulosFiltered['codigo'] + df_ArticulosFiltered['subcodigo']
df_ArticulosFiltered['Rubro'] = df_ArticulosFiltered['rubro'] + df_ArticulosFiltered['subrubro'] + df_ArticulosFiltered['subrubro2'] + df_ArticulosFiltered['subrubro3']

# Remove columns that are not needed
df_ArticulosFiltered = df_ArticulosFiltered.drop(columns=['codigo', 'subcodigo', 'rubro', 'subrubro', 'subrubro2', 'subrubro3'])

# Rename columns
df_ArticulosFiltered = df_ArticulosFiltered.rename(columns={'IDArticulo': 'idarticulo',
                                                          'nombre': 'nombre',
                                                          'Rubro': 'rubro'})

# Convert the columns 'idarticulo' and 'rubro' to int
df_ArticulosFiltered['idarticulo'] = df_ArticulosFiltered['idarticulo'].astype(int)
df_ArticulosFiltered['rubro'] = df_ArticulosFiltered['rubro'].astype(int)


# Remove the record with 'idarticulo' 99999700
df_ArticulosFiltered = df_ArticulosFiltered[df_ArticulosFiltered['idarticulo'] != 99999700]

# If the 'idarticulo' is 99999900, set the name to 'DESCUENTO', if it is 99999800, set it to 'OTRO'
df_ArticulosFiltered.loc[df_ArticulosFiltered['idarticulo'] == 99999900, 'nombre'] = 'DESCUENTO'
df_ArticulosFiltered.loc[df_ArticulosFiltered['idarticulo'] == 99999800, 'nombre'] = 'OTRO'


# If any rubro id is not found in the 'df_RubrosFiltered' dataframe, replace it with the "SIN RUBRO" id from the 'df_RubrosFiltered' dataframe
id_SinRubro = df_RubrosFiltered[df_RubrosFiltered['nombre'] == 'SIN RUBRO'].index[0]
df_ArticulosFiltered.loc[~df_ArticulosFiltered['rubro'].isin(df_RubrosFiltered['idrubro']), 'rubro'] = id_SinRubro

# If any article name is empty or NaN, remove the record
df_ArticulosFiltered = df_ArticulosFiltered.dropna(subset=['nombre'])


# Sort by 'idarticulo'
df_ArticulosFiltered = df_ArticulosFiltered.sort_values(by=['idarticulo'])
df_ArticulosFiltered = df_ArticulosFiltered[['idarticulo', 'nombre', 'rubro']]



# ========================
#  Dimension: TipoCliente
# ========================
# Create a dataframe with the data from the 'TipoCliente' table
df_TipoCliente = DB_tables['TipoCliente']

df_TipoClienteFiltered = df_TipoCliente[['Tipo_cliente', 'Detalle']]

# Dictionary with replacement mappings
mapping_TipoCliente = {
    1: 'CUENTA CORRIENTE',
    2: 'MOROSO',
    3: 'MOROSO NO VENDER'
}

# Detect if there is a new value in the 'Tipo_cliente' column that is not in the dictionary, and add it
for value in df_TipoClienteFiltered['Tipo_cliente'].unique():
    if value not in mapping_TipoCliente.keys():
        mapping_TipoCliente[value] = 'DESCONOCIDO'

# Apply mapping to the 'Detalle' column based on 'Tipo_cliente'
df_TipoClienteFiltered['Detalle'] = df_TipoClienteFiltered['Tipo_cliente'].map(mapping_TipoCliente)



# ========================
#  Dimension: Localidades
# ========================
df_LocalidadesFiltered = pd.DataFrame({
    'nombre': ['PARANÁ', 'SANTA FE', 'OTRO']
})



# =====================
#  Dimension: Clientes
# =====================
# Create a dataframe with the data from the 'Clientes' table
df_Clientes = DB_tables['Clientes']

df_ClientesFiltered = df_Clientes[['NroCuenta', 'localidad', 'Razon_Social', 'Tipo_cliente']]


# Replace null values in the 'localidad' column with 'OTHER'
df_ClientesFiltered.loc[df_ClientesFiltered['localidad'].isnull(), 'localidad'] = 3

condition_LocalidadOtro = (
    (df_ClientesFiltered['localidad'].isnull()) |
    (df_ClientesFiltered['localidad'] == '') |
    (df_ClientesFiltered['localidad'] == 'None')
)
df_ClientesFiltered.loc[condition_LocalidadOtro, 'localidad'] = 3

condition_LocalidadParana = (
    (df_ClientesFiltered['localidad'].str.upper().str.startswith('PA')) |
    (df_ClientesFiltered['localidad'].str.upper().str.contains('PRANA')) |
    (df_ClientesFiltered['localidad'].str.upper().str.contains('PARANA')) |
    (df_ClientesFiltered['localidad'].str.upper().str.contains('PARANÁ'))    
)
df_ClientesFiltered.loc[condition_LocalidadParana, 'localidad'] = 1

condition_LocalidadSantaFe = (
    (df_ClientesFiltered['localidad'].str.upper().str.upper().str.startswith('SANT')) |
    (df_ClientesFiltered['localidad'].str.upper().str.contains('SANTA FE')) |
    (df_ClientesFiltered['localidad'].str.upper().str.contains('SANTAFE'))
)
df_ClientesFiltered.loc[condition_LocalidadSantaFe, 'localidad'] = 2

# Replace with 'OTRO' if 'PARANÁ' or 'SANTA FE' is not met
df_ClientesFiltered.loc[~(condition_LocalidadParana | condition_LocalidadSantaFe), 'localidad'] = 3


# If the 'NroCuenta' is 9997 or 9999, or if the 'Razon_Social' is 'PRESUPUESTO' or 'TOTAL DEL TICKET', remove that row
condition_nroCuenta = (
    (df_ClientesFiltered['NroCuenta'] == 9997) |
    (df_ClientesFiltered['NroCuenta'] == 9999) |
    (df_ClientesFiltered['Razon_Social'] == 'PRESUPUESTO') |
    (df_ClientesFiltered['Razon_Social'] == 'TOTAL DEL TICKET')
)

df_ClientesFiltered = df_ClientesFiltered.loc[~condition_nroCuenta]


# If the 'Tipo_cliente' id is null, zero, or not found in the 'df_TipoClienteFiltered' dataframe, replace it with the id of the 'CUENTA CORRIENTE' type of client from the 'df_TipoClienteFiltered' dataframe
tipo_cliente_ctacte = df_TipoClienteFiltered[df_TipoClienteFiltered['Detalle'] == 'CUENTA CORRIENTE']['Tipo_cliente'].values[0]

df_ClientesFiltered.loc[df_ClientesFiltered['Tipo_cliente'].isnull(), 'Tipo_cliente'] = tipo_cliente_ctacte
df_ClientesFiltered.loc[df_ClientesFiltered['Tipo_cliente'] == 0, 'Tipo_cliente'] = tipo_cliente_ctacte
df_ClientesFiltered.loc[~df_ClientesFiltered['Tipo_cliente'].isin(df_TipoClienteFiltered['Tipo_cliente'].values), 'Tipo_cliente'] = tipo_cliente_ctacte

# Convert 'Tipo_cliente' and 'localidad' columns to integer
df_ClientesFiltered['Tipo_cliente'] = df_ClientesFiltered['Tipo_cliente'].astype(int)
df_ClientesFiltered['localidad'] = df_ClientesFiltered['localidad'].astype(int)



# =======================
#  Dimension: Vendedores
# =======================
# Create a dataframe with the data from the 'Vendedor' table
df_Vendedor = DB_tables['Vendedor']

# Remove index column and sort by 'Cod_Vendedor' column
df_Vendedor = df_Vendedor.sort_values(by=['Cod_Vendedor'])

df_Vendedor = df_Vendedor.reset_index()
df_Vendedor = df_Vendedor.drop(columns=['index'])

df_VendedorFiltered = df_Vendedor[['Cod_Vendedor', 'Nombre']]


# Remove the vendors that do not have a name, or are named "NOTA DE CREDITO", or have a NaN value in the 'Nombre' column from the dimension
df_VendedorFiltered = df_VendedorFiltered.dropna(subset=['Nombre'])
df_VendedorFiltered = df_VendedorFiltered[df_VendedorFiltered['Nombre'] != 'NOTA DE CREDITO']
df_VendedorFiltered = df_VendedorFiltered[df_VendedorFiltered['Nombre'] != '']

# If the vendor code is 0, or NaN, or empty, they are removed from the dimension
df_VendedorFiltered = df_VendedorFiltered.dropna(subset=['Cod_Vendedor'])
df_VendedorFiltered = df_VendedorFiltered[df_VendedorFiltered['Cod_Vendedor'] != 0]
df_VendedorFiltered = df_VendedorFiltered[df_VendedorFiltered['Cod_Vendedor'] != '']

# Remove duplicate records
df_VendedorFiltered = df_VendedorFiltered.drop_duplicates()




# =======================
#  'CabVentas' Filtering
# =======================

df_CabVentasFiltered = df_CabVentas[['NroOrden',
                                     'Cod_Comprob',
                                     'Cod_Vendedor',
                                     'FechaComp',
                                     'Hora',
                                     'NroCuenta',
                                     'Razon_Social',
                                     'total'
                                     ]]


# Remove records with null values in the 'Cod_Comprob', 'FechaComp', and 'Hora' columns
df_CabVentasFiltered = df_CabVentasFiltered.dropna(subset=['Cod_Comprob'])
df_CabVentasFiltered = df_CabVentasFiltered.dropna(subset=['FechaComp'])
df_CabVentasFiltered = df_CabVentasFiltered.dropna(subset=['Hora'])
df_CabVentasFiltered = df_CabVentasFiltered.dropna(subset=['total'])


# Filter the records to keep only the "facturas" (invoices)
condition_factura = df_CabVentasFiltered['Cod_Comprob'].str.startswith('F')
df_CabVentasFiltered = df_CabVentasFiltered.loc[condition_factura]


# Convert the 'Hora' column to string
df_CabVentasFiltered['Hora'] = df_CabVentasFiltered['Hora'].astype(str)
# Extract the date from the 'Hora' column
df_CabVentasFiltered['Hora'] = df_CabVentasFiltered['Hora'].str.split(' ').str[1]

# Concatenate the 'FechaComp' column with the 'Hora' column
df_CabVentasFiltered['Fecha'] = df_CabVentasFiltered['FechaComp'].astype(str) + ' ' + df_CabVentasFiltered['Hora']
# Convert the 'Fecha' column to datetime
df_CabVentasFiltered['Fecha'] = pd.to_datetime(df_CabVentasFiltered['Fecha'], format='%Y-%m-%d %H:%M:%S')

# Remove duplicate records from the 'Fecha' column
df_CabVentasFiltered = df_CabVentasFiltered.drop_duplicates(subset=['Fecha'])


# If any value in the column is 0, or NaN, or empty, replace it with the code of the vendor named "TODOS" from the 'df_VendedorFiltered' dataframe
# Get the code of the vendor named "TODOS"
codigo_vendedor_todos = df_VendedorFiltered.loc[df_VendedorFiltered['Nombre'] == 'TODOS', 'Cod_Vendedor'].iloc[0]

df_CabVentasFiltered['Cod_Vendedor'] = df_CabVentasFiltered['Cod_Vendedor'].replace(0, codigo_vendedor_todos)
df_CabVentasFiltered['Cod_Vendedor'] = df_CabVentasFiltered['Cod_Vendedor'].replace(np.nan, codigo_vendedor_todos)
df_CabVentasFiltered['Cod_Vendedor'] = df_CabVentasFiltered['Cod_Vendedor'].replace('', codigo_vendedor_todos)

# If any value in the 'Cod_Vendedor' column is not found in the 'df_VendedorFiltered' dataframe, replace it with the code of the vendor named "TODOS" from the 'df_VendedorFiltered' dataframe.
codigos_vendedores = df_VendedorFiltered['Cod_Vendedor'].unique()

# Vendor codes in 'df_CabVentasFiltered' that are not in 'df_VendedorFiltered'
codigos_no_existen = df_CabVentasFiltered[~df_CabVentasFiltered['Cod_Vendedor'].isin(codigos_vendedores)]['Cod_Vendedor']
# Replace those codes in df_CabVentasFiltered with the code of the vendor "TODOS"
df_CabVentasFiltered.loc[df_CabVentasFiltered['Cod_Vendedor'].isin(codigos_no_existen), 'Cod_Vendedor'] = codigo_vendedor_todos


# If any value in 'NroCuenta' is 0, NaN, or empty, replace it with the value of the 'Cuenta de Consumidor Final'.
nroCuenta_consumidorFinal = df_ClientesFiltered[df_ClientesFiltered['Razon_Social'] == 'CONSUMIDOR FINAL']['NroCuenta'].values[0]

df_CabVentasFiltered['NroCuenta'] = df_CabVentasFiltered['NroCuenta'].replace(0, nroCuenta_consumidorFinal)
df_CabVentasFiltered['NroCuenta'] = df_CabVentasFiltered['NroCuenta'].replace(np.nan, nroCuenta_consumidorFinal)
df_CabVentasFiltered['NroCuenta'] = df_CabVentasFiltered['NroCuenta'].replace('', nroCuenta_consumidorFinal)
df_CabVentasFiltered['NroCuenta'] = df_CabVentasFiltered['NroCuenta'].replace(' ', nroCuenta_consumidorFinal)

# If any value in 'NroCuenta' is not found in the 'df_ClientesFiltered' dataframe, replace it with the value of 'Cuenta de Consumidor Final'.
nroCuentas_no_existen = df_CabVentasFiltered[~df_CabVentasFiltered['NroCuenta'].isin(df_ClientesFiltered['NroCuenta'].values)]['NroCuenta']
df_CabVentasFiltered.loc[df_CabVentasFiltered['NroCuenta'].isin(nroCuentas_no_existen), 'NroCuenta'] = nroCuenta_consumidorFinal


# If any 'Razon_Social' is NaN or empty, replace it with the value "CONSUMIDOR FINAL"
df_CabVentasFiltered['Razon_Social'] = df_CabVentasFiltered['Razon_Social'].replace(np.nan, 'CONSUMIDOR FINAL')
df_CabVentasFiltered['Razon_Social'] = df_CabVentasFiltered['Razon_Social'].replace('', 'CONSUMIDOR FINAL')

# If any 'Razon_Social' is "CANCELADO", "CANCELADA", "ANULADO", "ANULADA", 'A N U L A D A' or similar variations (recognized with regex), replace it with the value "CONSUMIDOR FINAL"
df_CabVentasFiltered['Razon_Social'] = df_CabVentasFiltered['Razon_Social'].replace(regex=r'^CANCELADO.*', value='CONSUMIDOR FINAL')
df_CabVentasFiltered['Razon_Social'] = df_CabVentasFiltered['Razon_Social'].replace(regex=r'^CANCELADA.*', value='CONSUMIDOR FINAL')
df_CabVentasFiltered['Razon_Social'] = df_CabVentasFiltered['Razon_Social'].replace(regex=r'^ANULADO.*', value='CONSUMIDOR FINAL')
df_CabVentasFiltered['Razon_Social'] = df_CabVentasFiltered['Razon_Social'].replace(regex=r'^ANULADA.*', value='CONSUMIDOR FINAL')
df_CabVentasFiltered['Razon_Social'] = df_CabVentasFiltered['Razon_Social'].replace(regex=r'^A N U L A D A.*', value='CONSUMIDOR FINAL')

# If the 'NroCuenta' is equal to the one for "Consumidor Final", replace the 'Razon_Social' with the value "CONSUMIDOR FINAL"
df_CabVentasFiltered.loc[df_CabVentasFiltered['NroCuenta'] == nroCuenta_consumidorFinal, 'Razon_Social'] = 'CONSUMIDOR FINAL'


# If the total is 0, negative, or empty, delete the record
df_CabVentasFiltered = df_CabVentasFiltered[df_CabVentasFiltered['total'] > 0]


# Convert the 'Cod_Vendedor' and 'NroCuenta' columns to int
df_CabVentasFiltered['Cod_Vendedor'] = df_CabVentasFiltered['Cod_Vendedor'].astype(int)
df_CabVentasFiltered['NroCuenta'] = df_CabVentasFiltered['NroCuenta'].astype(int)


# Remove the columns 'FechaComp' and 'Hora', and move the column 'Fecha' to the position where 'FechaComp' was
df_CabVentasFiltered = df_CabVentasFiltered.drop(columns=['FechaComp', 'Hora'])
df_CabVentasFiltered = df_CabVentasFiltered[['NroOrden',
                                             'Cod_Comprob',
                                             'Cod_Vendedor',
                                             'Fecha',
                                             'NroCuenta',
                                             'Razon_Social',
                                             'total'
                                             ]]

# Rename columns
df_CabVentasFiltered = df_CabVentasFiltered.rename(columns={'NroOrden': 'NroOrden',
                                                            'Cod_Comprob': 'Cod_Comprob',
                                                            'Cod_Vendedor': 'Cod_Vendedor',
                                                            'Fecha': 'Fecha',
                                                            'NroCuenta': 'NroCuenta',
                                                            'Razon_Social': 'Razon_Social',
                                                            'total': 'total_orden'
                                                            })




# ========================
#  'ItemVentas' Filtering
# ========================

df_ItemVentasFiltered = df_ItemVentas[['nroorden',
                                       'codigo',
                                       'subcodigo',
                                       'cantidad',
                                       'prec_unit',
                                       'prec_unit_iv',
                                       'total',
                                       'descripcion']]


# Filter the records by 'nroorden' that are in 'df_CabVentasFiltered'
df_ItemVentasFiltered = df_ItemVentasFiltered[df_ItemVentasFiltered['nroorden'].isin(df_CabVentasFiltered['NroOrden'])]

# Convert NaN values in the 'codigo' and 'subcodigo' columns to 0
df_ItemVentasFiltered['codigo'] = df_ItemVentasFiltered['codigo'].fillna(999998)  # 999998 es el código de 'OTRO'
df_ItemVentasFiltered['subcodigo'] = df_ItemVentasFiltered['subcodigo'].fillna(0)

# If there is a value in 'codigo' that is negative or 0, replace it with 999998 (code for 'OTRO')
df_ItemVentasFiltered.loc[df_ItemVentasFiltered['codigo'] <= 0, 'codigo'] = 999998

# If there is a value in 'subcodigo' that is negative, replace it with 0
df_ItemVentasFiltered.loc[df_ItemVentasFiltered['subcodigo'] < 0, 'subcodigo'] = 0


# Convert the columns to string that will be used
df_ItemVentasFiltered['codigo'] = df_ItemVentasFiltered['codigo'].astype(str)
df_ItemVentasFiltered['subcodigo'] = df_ItemVentasFiltered['subcodigo'].astype(str)


# Fill with leading zeros so that all values have 6 digits
df_ItemVentasFiltered['codigo'] = df_ItemVentasFiltered['codigo'].str.zfill(6)
df_ItemVentasFiltered['subcodigo'] = df_ItemVentasFiltered['subcodigo'].str.zfill(2)

# Concatenate the columns
df_ItemVentasFiltered['IDArticulo'] = df_ItemVentasFiltered['codigo'] + df_ItemVentasFiltered['subcodigo']


df_ItemVentasFiltered['IDArticulo'] = df_ItemVentasFiltered['IDArticulo'].astype(int)

codigo_articulo_otro = df_ArticulosFiltered[df_ArticulosFiltered['nombre'] == 'OTRO']['idarticulo'].values[0]

# If any value in the 'IDArticulo' column is not found in the 'df_ArticulosFiltered' dataframe, replace it with the code of the article named "OTRO" from the 'df_ArticulosFiltered' dataframe.
codigos_articulos = df_ArticulosFiltered['idarticulo'].unique()

# Find the article codes in 'df_ItemVentasFiltered' that are not in 'df_ArticulosFiltered'
codigos_articulos_no_existen = df_ItemVentasFiltered[~df_ItemVentasFiltered['IDArticulo'].isin(codigos_articulos)]['IDArticulo']

# Replace those codes in 'df_ItemVentasFiltered' with the code of the article "OTRO"
df_ItemVentasFiltered.loc[df_ItemVentasFiltered['IDArticulo'].isin(codigos_articulos_no_existen), 'IDArticulo'] = codigo_articulo_otro


# Convert to float the columns 'cantidad', 'prec_unit', 'prec_unit_iv' and 'total'
df_ItemVentasFiltered['cantidad'] = df_ItemVentasFiltered['cantidad'].astype(float)
df_ItemVentasFiltered['prec_unit'] = df_ItemVentasFiltered['prec_unit'].astype(float)
df_ItemVentasFiltered['prec_unit_iv'] = df_ItemVentasFiltered['prec_unit_iv'].astype(float)
df_ItemVentasFiltered['total'] = df_ItemVentasFiltered['total'].astype(float)


# If 'cantidad' is 0, negative, or empty, delete the record
df_ItemVentasFiltered = df_ItemVentasFiltered[df_ItemVentasFiltered['cantidad'] > 0]

# If 'prec_unit' is negative, or empty, delete the record
df_ItemVentasFiltered = df_ItemVentasFiltered[df_ItemVentasFiltered['prec_unit'] >= 0]

# If 'prec_unit_iv' with IVA is negative, or empty, delete the record
df_ItemVentasFiltered = df_ItemVentasFiltered[df_ItemVentasFiltered['prec_unit_iv'] >= 0]

# If 'total' is 0, negative, or empty, delete the record
df_ItemVentasFiltered = df_ItemVentasFiltered[df_ItemVentasFiltered['total'] > 0]



df_ItemVentasFiltered = df_ItemVentasFiltered.drop(columns=['codigo', 'subcodigo'])

df_ItemVentasFiltered = df_ItemVentasFiltered.rename(columns={'nroorden': 'NroOrden',
                                                            'IDArticulo': 'idarticulo',
                                                            'cantidad': 'cantidad',
                                                            'prec_unit': 'precio_unitario',
                                                            'prec_unit_iv': 'precio_unitario_iva',
                                                            'total': 'total_renglon'})


# Convert 'nroorden' and 'idarticulo' columns to int
df_ItemVentasFiltered['NroOrden'] = df_ItemVentasFiltered['NroOrden'].astype(int)


df_ItemVentasFiltered = df_ItemVentasFiltered[['NroOrden',
                                               'idarticulo',
                                               'cantidad',
                                               'precio_unitario',
                                               'precio_unitario_iva',
                                               'total_renglon']]



# ===================
#  Dimension: Tiempo
# ===================
df_TiempoFiltered = df_CabVentasFiltered[['Fecha']]


# Rename the 'Fecha' column
df_TiempoFiltered = df_TiempoFiltered.rename(columns={'Fecha': 'fecha'})


# Add 'periodo': if it is morning or afternoon
df_TiempoFiltered['periodo'] = np.where(df_TiempoFiltered['fecha'].dt.hour < 12, 'Mañana', 'Tarde')

# Add 'dia_nombre': days of the week
df_TiempoFiltered['dia_nombre'] = df_TiempoFiltered['fecha'].dt.day_name()

# Add 'diames_numero': days of the month
df_TiempoFiltered['diames_numero'] = df_TiempoFiltered['fecha'].dt.day

# Add 'mes_nombre': names of the months of the year
df_TiempoFiltered['mes_nombre'] = df_TiempoFiltered['fecha'].dt.month_name()

# Add 'mes_numero': numbers of the months of the year
df_TiempoFiltered['mes_numero'] = df_TiempoFiltered['fecha'].dt.month

# Add 'trimestre': quarter of the year
df_TiempoFiltered['trimestre'] = df_TiempoFiltered['fecha'].dt.quarter

# Add 'semestre': semester of the year
df_TiempoFiltered['semestre'] = np.where(df_TiempoFiltered['trimestre'] <= 2, 1, 2)

# Add 'anio': years
df_TiempoFiltered['anio'] = df_TiempoFiltered['fecha'].dt.year


# Convert the columns to integers
df_TiempoFiltered['diames_numero'] = df_TiempoFiltered['diames_numero'].astype(int)
df_TiempoFiltered['mes_numero'] = df_TiempoFiltered['mes_numero'].astype(int)
df_TiempoFiltered['trimestre'] = df_TiempoFiltered['trimestre'].astype(int)
df_TiempoFiltered['semestre'] = df_TiempoFiltered['semestre'].astype(int)
df_TiempoFiltered['anio'] = df_TiempoFiltered['anio'].astype(int)


# Translate the names of the days of the week to Spanish
mapeo_dias = {
    'Monday': 'Lunes',
    'Tuesday': 'Martes',
    'Wednesday': 'Miércoles',
    'Thursday': 'Jueves',
    'Friday': 'Viernes',
    'Saturday': 'Sábado',
    'Sunday': 'Domingo'
}
df_TiempoFiltered['dia_nombre'] = df_TiempoFiltered['dia_nombre'].map(mapeo_dias)

# Translate the names of the months to Spanish
mapeo_meses = {
    'January': 'Enero',
    'February': 'Febrero',
    'March': 'Marzo',
    'April': 'Abril',
    'May': 'Mayo',
    'June': 'Junio',
    'July': 'Julio',
    'August': 'Agosto',
    'September': 'Septiembre',
    'October': 'Octubre',
    'November': 'Noviembre',
    'December': 'Diciembre'
}
df_TiempoFiltered['mes_nombre'] = df_TiempoFiltered['mes_nombre'].map(mapeo_meses)




# =====================
#  Updating Dimensions
# =====================

# ====================
#  Dimension: Cliente
# ====================
# Update 'TipoCliente' table
# Rename columns
dimension_TipoCliente = df_TipoClienteFiltered.rename(columns={'Tipo_cliente': 'idtipocliente',
                                                              'Detalle': 'tipo_cliente'})
dimension_TipoCliente = updateDimensionTable(engine_cubo, 'tipocliente', dimension_TipoCliente, pk='idtipocliente')

# Update 'Localidades' table
dimension_Localidades = updateDimensionTable(engine_cubo, 'localidades', df_LocalidadesFiltered, pk='idlocalidad')

# Update 'Clientes' dimension
dimension_Clientes = pd.DataFrame({
    'NroCuenta': df_ClientesFiltered['NroCuenta'],
    'Razon_Social': df_ClientesFiltered['Razon_Social'],
    'Tipo_cliente': df_ClientesFiltered['Tipo_cliente'],
    'localidad': df_ClientesFiltered['localidad']
})

dimension_Clientes = dimension_Clientes.sort_values(by=['NroCuenta'])

dimension_Clientes = dimension_Clientes.rename(columns={'NroCuenta': 'idcliente',
                                                        'Razon_Social': 'razon_social',
                                                        'Tipo_cliente': 'tipo_cliente'})

dimension_Clientes = updateDimensionTableIntPK(engine_cubo, 'clientes', dimension_Clientes, pk='idcliente')


# ======================
#  Dimension: Articulos
# ======================
# Update 'Rubros' table
dimension_Rubros = updateDimensionTableIntPK(engine_cubo, 'rubros', df_RubrosFiltered, pk='idrubro')

# Update 'Articulos' dimension
dimension_Articulos = updateDimensionTableIntPK(engine_cubo, 'articulos', df_ArticulosFiltered, pk='idarticulo')


# =====================
#  Dimension: Vendedor
# =====================
# Update 'Vendedores' dimension
# Rename columns
dimension_Vendedores = df_VendedorFiltered.rename(columns={'Cod_Vendedor': 'idvendedor',
                                                          'Nombre': 'nombre'})
dimension_Vendedores = updateDimensionTableIntPK(engine_cubo, 'vendedores', dimension_Vendedores, pk='idvendedor')


# ===================
#  Dimension: Tiempo
# ===================
# Update 'Tiempo' dimension
dimension_Tiempo = updateDimensionTable(engine_cubo, 'tiempo', df_TiempoFiltered, pk='idfecha')


# ==================
#  Dimension: Orden
# ==================
# Update 'Orden' dimension
dimension_Orden = df_CabVentasFiltered[['NroOrden', 'total_orden']]

# Sort by 'NroOrden'
dimension_Orden = dimension_Orden.sort_values(by=['NroOrden'])

# Rename columns
dimension_Orden = dimension_Orden.rename(columns={'NroOrden': 'nroorden',
                                                  'total_orden': 'total_venta'})

dimension_Orden = updateDimensionTableIntPK(engine_cubo, 'orden', dimension_Orden, pk='nroorden')



# =======================
#  Fact: Renglon_Factura
# =======================
# Create Sales dataframe
# JOIN between 'df_CabVentasFiltered' and 'df_ItemVentasFiltered' to obtain the dataframe 'df_Ventas' using the 'NroOrden' column
df_Ventas = pd.merge(df_CabVentasFiltered, df_ItemVentasFiltered, on='NroOrden', how='inner')

# Ordenar por NroOrden
df_Ventas = df_Ventas.sort_values(by=['NroOrden'])

# Create Fact table 'HechosRenglonFactura' wich means 'invoice line facts'
df_HechosRenglonFactura = pd.DataFrame({
    # Dimensions
    'idfecha': df_Ventas['Fecha'].map(dimension_Tiempo.set_index('fecha')['idfecha']),
    'idarticulo': df_Ventas['idarticulo'],
    'idcliente': df_Ventas['NroCuenta'],
    'idvendedor': df_Ventas['Cod_Vendedor'],
    'nroorden': df_Ventas['NroOrden'],

    # Metrics
    'total_venta_renglon': df_Ventas['total_renglon'],
    'cantidad_articulos_renglon': df_Ventas['cantidad'],
    'precio_unitario': df_Ventas['precio_unitario'],
    'precio_unitario_iva': df_Ventas['precio_unitario_iva']
})

# Update 'HechosRenglonFactura' fact table
fact_HechosRenglonFactura = updateDimensionTable(engine_cubo, 'renglon_factura', df_HechosRenglonFactura, pk='idrenglon_factura')