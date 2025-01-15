# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Logging operation to record progress
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the 
    code execution to a log file. Function returns nothing.'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

def extract(url, table_attribs):
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                "Name": col[1].text.strip(),
                "MC_USD_Billion": col[2].text.strip()
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    
    return df  # Return the DataFrame instead of printing it

def transform(data): 
    # Read the exchange rate CSV file into a DataFrame
    exchange_rate_df = pd.read_csv('exchange_rate.csv')

    # Convert DataFrame to dictionary using provided syntax
    exchange_rate_dict = exchange_rate_df.set_index('Currency')['Rate'].to_dict()
    
    data['MC_USD_Billion'] = data['MC_USD_Billion'].astype(float)

    # Add columns for GBP, EUR, and INR conversions
    data['MC_EUR_Billion'] = np.round(data['MC_USD_Billion'] * exchange_rate_dict['EUR'], 2)
    data['MC_GBP_Billion'] = np.round(data['MC_USD_Billion'] * exchange_rate_dict['GBP'], 2)
    data['MC_INR_Billion'] = np.round(data['MC_USD_Billion'] * exchange_rate_dict['INR'], 2)

    return data

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)

def load_to_db(df, db_name, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print("Query Statement:")
    print(query_statement)
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    rows = cursor.fetchall()
    for row in rows:
        print(row)


# Here, you define the required entities and call the relevant 
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]  # corrected column name
db_name = 'Banks.db'
table_name = 'largest_banks'
csv_path = './Largest_banks_data.csv'


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)

log_progress('SQL Connection initiated.')

load_to_db(df, db_name, table_name)

log_progress('Data loaded to Database as table. Running the query')

# Print the contents of the entire table
query_statement = "SELECT * FROM largest_banks"
run_query(query_statement, sql_connection)

# Print the average market capitalization of all the banks in Billion USD
query_statement = "SELECT AVG(MC_GBP_Billion) FROM largest_banks"
run_query(query_statement, sql_connection)

# Print only the names of the top 5 banks
query_statement = "SELECT Name FROM largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
