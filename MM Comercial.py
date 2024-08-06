import pandas as pd
import os
import time

# Hora en que inició el proceso
start_time = time.time()
print("Inicio:", time.strftime("%H:%M:%S", time.localtime(start_time)))

# Ruta de los archivos CSV
file_directory = r'C:\Users\Pc-01\Documents\Portafolio\ETLs'
file_name_ventas = 'sales_data_sample.csv'

# Cargar el dataset
file_path = os.path.join(file_directory, file_name_ventas)
data = pd.read_csv(file_path, encoding='ISO-8859-1')

# Dimensión Clientes
dim_clientes = data[['CUSTOMERNAME', 'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'CITY', 'STATE', 'POSTALCODE', 'COUNTRY', 'TERRITORY', 'CONTACTLASTNAME', 'CONTACTFIRSTNAME']].drop_duplicates().reset_index(drop=True)
dim_clientes['CustomerID'] = dim_clientes.index + 1

# Dimensión Productos
dim_productos = data[['PRODUCTCODE', 'PRODUCTLINE', 'MSRP']].drop_duplicates().reset_index(drop=True)
dim_productos['ProductID'] = dim_productos.index + 1

# Dimensión Tiempo
data['ORDERDATE'] = pd.to_datetime(data['ORDERDATE'])
dim_tiempo = data[['ORDERDATE']].drop_duplicates().reset_index(drop=True)
dim_tiempo['Day'] = dim_tiempo['ORDERDATE'].dt.day
dim_tiempo['Month'] = dim_tiempo['ORDERDATE'].dt.month
dim_tiempo['Quarter'] = data['QTR_ID']
dim_tiempo['Year'] = data['YEAR_ID']
dim_tiempo['DateID'] = dim_tiempo.index + 1

# Merge para obtener los IDs
data = data.merge(dim_clientes, how='left', on=['CUSTOMERNAME', 'PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2', 'CITY', 'STATE', 'POSTALCODE', 'COUNTRY', 'TERRITORY', 'CONTACTLASTNAME', 'CONTACTFIRSTNAME'])
data = data.merge(dim_productos, how='left', on=['PRODUCTCODE', 'PRODUCTLINE', 'MSRP'])
data = data.merge(dim_tiempo, how='left', on=['ORDERDATE'])

# Tabla de Hechos
fact_ventas = data[['ORDERNUMBER', 'QUANTITYORDERED', 'PRICEEACH', 'ORDERLINENUMBER', 'SALES', 'ORDERDATE', 'STATUS', 'QTR_ID', 'MONTH_ID', 'YEAR_ID', 'ProductID', 'CustomerID', 'DEALSIZE']].copy()
fact_ventas['Fact_VentaID'] = fact_ventas.index + 1

# Guardar las tablas en archivos CSV
dim_clientes.to_csv(os.path.join(file_directory, 'dim_clientes.csv'), index=False)
dim_productos.to_csv(os.path.join(file_directory, 'dim_productos.csv'), index=False)
dim_tiempo.to_csv(os.path.join(file_directory, 'dim_tiempo.csv'), index=False)
fact_ventas.to_csv(os.path.join(file_directory, 'fact_ventas.csv'), index=False)

# Hora en que finalizó el proceso
end_time = time.time()
print("Fin:", time.strftime("%H:%M:%S", time.localtime(end_time)))

# Tiempo que demoró el proceso
execution_time = end_time - start_time
formatted_time = time.strftime("%H:%M:%S", time.gmtime(execution_time))
print(f"Tiempo de ejecución: {formatted_time}")