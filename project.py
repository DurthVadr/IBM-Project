import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
import datetime

csv_path= 'exchange_rate.csv'
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    with open('code_log.txt', 'a') as file:
        file.write(f'{datetime.datetime.now()} - {message}\n')
    
def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing.   '''
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', table_attribs)
    rows = table.find_all('tr')
    data = []
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele])
    df = pd.DataFrame(data, columns=[ 'Name', 'Market Cap', 'Country'])
    
    
    
    
    
    return df



def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    dataframe= pd.read_csv(csv_path)
    exchange_rate= dataframe.set_index('Currency').to_dict()
    
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_JPY_Billion'] = [np.round(x*exchange_rate['JPY'],2) for x in df['MC_USD_Billion']]
    
    return df



def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    
    df.to_csv(output_path, index=False)
    
    
    
    
    
    
    
    
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    
    
    #Before calling this function, initiate the connection to the SQLite3 database server with the name Banks.db. Pass this connection object, along with the required table name Largest_banks and the transformed data frame, to the load_to_db() function in the function call.
    
    sql_connection = sqlite3.connect('Banks.db')
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    sql_connection.close()
    
    
    
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    sql_connection = sqlite3.connect('Banks.db')
    df = pd.read_sql_query(query_statement, sql_connection)
    print(df)
    
    
    
    
    
''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

if __name__ == '__main__':
    log_progress('Starting the process')
    df = extract(url, {'class':'wikitable'})
    df = transform(df, csv_path)
    load_to_csv(df, 'largest_banks.csv')
    sql_connection = sqlite3.connect('Banks.db')
    load_to_db(df, sql_connection, 'Largest_banks')
    query_statement = 'SELECT * FROM Largest_banks'
    run_query(query_statement, sql_connection)
    log_progress('Process completed')
    






    
    
    
    