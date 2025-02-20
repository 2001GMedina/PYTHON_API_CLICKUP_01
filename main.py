import requests
import pandas as pd
import re
from datetime import datetime, timedelta
import holidays
import os
import sys
import pyodbc
import numpy as np
from mods.oracle_connector import db_connection


# Normalazing the JSON data frame columns
def normalize_columns(df):
    while True:
        columns_to_expand = [col for col in df.columns if df[col].apply(lambda x: isinstance(x, dict) or isinstance(x, list)).any()]
        if not columns_to_expand:
            break
        for column in columns_to_expand:
            if df[column].apply(lambda x: isinstance(x, dict)).any():
                json_df = pd.json_normalize(df[column])
                json_df.columns = [f"{column}_{subcol}" for subcol in json_df.columns]
                df = pd.concat([df.drop(columns=[column]), json_df], axis=1)
            elif df[column].apply(lambda x: isinstance(x, list)).any():
                array_df = df[column].apply(lambda x: pd.Series(x) if isinstance(x, list) else pd.Series([x]))
                array_df.columns = [f"{column}_{i}" for i in array_df.columns]
                df = pd.concat([df.drop(columns=[column]), array_df], axis=1)
    return df

# Function to convert timestamps to dates and format them as DD/MM/YYYY
def convert_and_format_dates(df, columns):
    for col in columns:
        if col in df.columns:
            # Checks if the value is a timestamp number and converts it to datetime.
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y')
    return df

def rename_columns(df, column_mapping):
    df = df.rename(columns=column_mapping)
    return df

def add_mes_ref_column():
    today = datetime.now()
    if today.day in [30, 31]:
        if today.month == 12:
            return f'01/01/{today.year + 1}'
        else:
            next_month = today.month + 1
            return f'01/{next_month:02d}/{today.year}'
    else:
        return f'01/{today.month:02d}/{today.year}' 

# Function to calculate the start date based on the current day.
def calculate_start_date():
    today = datetime.now()
    if today.day > 29:
        start_date = today.replace(day=29).strftime('%Y-%m-%d')
    else:
        if today.month == 1:
            start_date = today.replace(year=today.year - 1, month=12, day=29).strftime('%Y-%m-%d')
        else:
            start_date = today.replace(month=today.month - 1, day=29).strftime('%Y-%m-%d')
    
    return start_date
def adjust_sla_column(df):
    df['SLA_AJUSTADO'] = df['SLA'].apply(lambda x: ''.join(re.findall(r'\d+', str(x))))
    return df

def add_filtro_column(df):
    df['DIA_UTEIS'] = df.apply(lambda row: count_business_days(row['DATA_CRIACAO'], row['DATA_FECHAMENTO']), axis=1)
    df['FILTRO'] = df['DIA_UTEIS'].apply(lambda x: 'OK' if x <= 2 else 'FORA')
    return df

# Function to count working days
def count_business_days(creation_date, closing_date):
    brazil = holidays.Brazil()  # Feriados nacionais
    creation_date = pd.to_datetime(creation_date, format='%d/%m/%Y')
    closing_date = pd.to_datetime(closing_date, format='%d/%m/%Y')
    
    business_days = 0
    current_date = creation_date

    while current_date <= closing_date:
        if current_date.weekday() < 6 and current_date not in brazil:  # Verifica se não é domingo(6) e não é feriado
            business_days += 1
        current_date += timedelta(days=1)

    return business_days

#------------------------------------------------------------------------
# API Variables
token = os.getenv('CLICKUP_KEY')
url = 'https://api.clickup.com/api/v2/view/12zuj6-6773/task'
headers = { 'Authorization': token }
#------------------------------------------------------------------------

start_date = calculate_start_date()
yesterday = datetime.now() - timedelta(1)
end_date = yesterday.strftime('%Y-%m-%d')

# API Request function
def fetch_all_tasks(url, headers, start_date, end_date):
    all_tasks = []
    page = 0
    while True:
        response = requests.get(f"{url}?page={page}&start_date={start_date}&end_date={end_date}", headers=headers)
        if response.status_code == 200:
            data = response.json()
            tasks = data.get('tasks', [])
            all_tasks.extend(tasks)
            if len(tasks) == 0:
                break
            page += 1
        else:
            print(f"Falha na requisição. Status code: {response.status_code}")
            print(f"Mensagem de erro: {response.text}")
            break
    return all_tasks

tasks = fetch_all_tasks(url, headers, start_date, end_date)

# Converting to a Pandas Data Frame
df = pd.DataFrame(tasks)
df = normalize_columns(df)

desired_columns = [
    'name', 
    'assignees_0_username', 
    'status_status', 
    'date_created', 
    'date_closed', 
    'custom_fields_3_value'
]

# Checks which desired columns are available.
available_columns = [col for col in desired_columns if col in df.columns]

# If 'assignees_0_username' is not available, add the column with null values.
if 'assignees_0_username' not in available_columns:
    print("Coluna 'assignees_0_username' não encontrada. Será adicionada com valores nulos.")
    df['assignees_0_username'] = np.nan  # Adiciona a coluna com valores nulos

df_filtered = df[available_columns]

# Ensure that df_filtered maintains the desired column order.
df_filtered = df_filtered.reindex(columns=desired_columns)


# Convert the timestamps to dates and format them as DD/MM/YYYY.
df_filtered = convert_and_format_dates(df_filtered, ['date_created', 'date_closed'])

column_mapping = {
    'name': 'NOME_TAREFA',
    'assignees_0_username': 'RESPONSAVEL',
    'status_status': 'STATUS',
    'date_created': 'DATA_CRIACAO',
    'date_closed': 'DATA_FECHAMENTO',
    'custom_fields_3_value': 'SLA'
}

df_filtered = rename_columns(df_filtered, column_mapping)

df_filtered['MES_REF'] = add_mes_ref_column()

df_filtered = adjust_sla_column(df_filtered)

df_filtered = add_filtro_column(df_filtered)

final_columns = [
    'NOME_TAREFA', 
    'RESPONSAVEL', 
    'STATUS', 
    'DATA_CRIACAO', 
    'DATA_FECHAMENTO', 
    'SLA',
    'MES_REF',
    'SLA_AJUSTADO',
    'FILTRO'
]
df_filtered = df_filtered[final_columns]

# Processing the DataFrame and inserting it into the database.
try:
    with db_connection() as connection:
        with connection.cursor() as cursor:
            table = 'DADOS_OUVIDORIA'
            
            # Get the structure of the Oracle table.
            cursor.execute(f"SELECT COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '{table}'")
            columns = [row[0] for row in cursor.fetchall()]
            num_columns = len(columns)
            
            # Check if the number of columns in the Oracle table matches the number of columns in the DataFrame.
            if len(df_filtered.columns) != num_columns:
                print(f"Número de colunas no DataFrame ({len(df_filtered.columns)}) não corresponde ao número de colunas na tabela Oracle ({num_columns}).")
                exit()
            
            df_filtered = df_filtered[columns]
            
            # Convert the data types if necessary.
            for col in columns:
                if 'DATA' in col.upper():
                    continue
                elif 'NUM' in col.upper():
                    df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce')

            # Command to delete data from the database.
            delete_command = f"DELETE FROM {table} WHERE MES_REF = '{df_filtered['MES_REF'][0]}'"  # Usando o primeiro valor de MES_REF
            
            # Execute delete
            try:
                cursor.execute(delete_command)
                print("Dados deletados com sucesso!")
            except pyodbc.Error as e:
                print(f"Erro ao deletar dados: {e}")
                exit()

            # Create insert query
            placeholders = ', '.join(['?' for _ in range(num_columns)])
            insert_command = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            for index, row in df_filtered.iterrows():
                values = row.tolist()
                values = [None if v == '' or pd.isna(v) else v for v in values]
                try:
                    cursor.execute(insert_command, values)
                except pyodbc.Error as e:
                    print(f"Erro ao inserir dados na linha {index}: {e}")
                    print(f"Valores que causaram o erro: {values}")
                    exit()


            connection.commit()
            print("Dados importados com sucesso!")

except pyodbc.Error as e:
    print(f"Erro ao conectar ou interagir com o banco de dados: {e}")
