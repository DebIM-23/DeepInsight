# Code for ETL operations on Bank Market Cap data
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import sqlite3
import logging
from datetime import datetime

# Initialize known variables
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_csv_path = 'exchange_rate.csv'
output_csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

# Task 1: Logging function
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' 
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format)
    
    with open(log_file, 'a') as f:
        f.write(f'{timestamp} : {message}\n')

# Task 2: Extraction function
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    tables = soup.find_all('tbody')
    target_table = None
    
    for table in tables:
        if table.find_previous('h2').span.text == 'By market capitalization':
            target_table = table
            break
    
    if target_table is None:
        raise ValueError("Could not find the target table on the page.")
    
    data = []
    rows = target_table.find_all('tr')[1:]  
    
    for row in rows:
        cols = row.find_all('td')
        if len(cols) >= 2:  # Ensure we have at least 2 columns
            name = cols[1].text.strip()
            mc_usd = cols[2].text.strip().replace('\n', '')
            try:
                mc_usd = float(mc_usd)
            except ValueError:
                mc_usd = None
            data.append({table_attribs[0]: name, table_attribs[1]: mc_usd})
    
    df = pd.DataFrame(data, columns=table_attribs)
    return df

# Task 3: Transformation function
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    # Read exchange rate data
    exchange_rate = pd.read_csv(csv_path)
    exchange_rate_dict = exchange_rate.set_index('Currency').to_dict()['Rates']
    
    # Add new columns
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

# Task 4: Loading to CSV
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

# Task 5: Loading to Database
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Task 6: Running queries on Database
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"Query: {query_statement}")
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    print()

# Main ETL process
log_progress("Preliminaries complete. Initiating ETL process")

# Extract data
table_attribs = ['Name', 'MC_USD_Billion']
df = extract(url, table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")

# Transform data
df = transform(df, exchange_rate_csv_path)
log_progress("Data transformation complete. Initiating Loading process")

# Load to CSV
load_to_csv(df, output_csv_path)
log_progress("Data saved to CSV file")

# SQL operations
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

# Load to DB
load_to_db(df, conn, table_name)
log_progress("Data loaded to Database as a table, Executing queries")

# Run queries
query_statements = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT Name from Largest_banks LIMIT 5"
]

for query in query_statements:
    run_query(query, conn)

log_progress("Process Complete")

# Close connection
conn.close()
log_progress("Server Connection closed")