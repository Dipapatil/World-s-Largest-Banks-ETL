#creating an automated script that can extract the list of all countries in order of their GDPs in billion USDs (rounded to 2 decimal places), as logged by the International Monetary Fund (IMF). Since IMF releases this evaluation twice a year, this code will be used by the organization to extract the information as it is updated.
# Directions
# Write a function to extract the tabular information from the given URL under the heading By Market Capitalization, and save it to a data frame.
# Write a function to transform the data frame by adding columns for Market Capitalization in GBP, EUR, and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
# Write a function to load the transformed data frame to an output CSV file.
# Write a function to load the transformed data frame to an SQL database server as a table.
# Write a function to run queries on the database table.
# Run the following queries on the database table:
# a. Extract the information for the London office, that is Name and MC_GBP_Billion
# b. Extract the information for the Berlin office, that is Name and MC_EUR_Billion
# c. Extract the information for New Delhi office, that is Name and MC_INR_Billion
# Write a function to log the progress of the code.
# While executing the data initialization commands and function calls, maintain appropriate log entries.

# You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.
#
# Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.

import pandas as pd
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import sqlite3

url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    now = str(datetime.now())
    with open("code_log_top_10_banks.txt", "a") as file:
        file.write(now + ',' + message + '\n')


def extract(url):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    df = pd.DataFrame(columns=["Rank", "Bank_Name", "Market Cap"])
    count = 0
    for row in rows:
        if count <= 10:

            col = row.find_all('td')
            # print(col)
            if len(col) != 0:
                data_dict = {"Rank": col[0].get_text(strip=True),
                             "Bank_Name": col[1].get_text(strip=True),
                             "Market Cap": col[2].get_text(strip=True)}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df1, df], ignore_index=True)
                count += 1
        else:
            break
    df["Rank"] = pd.to_numeric(df["Rank"], errors='coerce')
    df["Market Cap"] = pd.to_numeric(df["Market Cap"], errors="coerce")
    df = df.sort_values(by="Rank").reset_index(drop=True)
    print(df)
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    # print(df.dtypes)
    exchange_rates = pd.read_csv(csv_path)
    # print(exchange_rates)
    eur_rate = exchange_rates.loc[exchange_rates["Currency"] == "EUR", "Rate"].values[0]
    inr_rate = exchange_rates.loc[exchange_rates["Currency"] == "INR", "Rate"].values[0]
    gbp_rate = exchange_rates.loc[exchange_rates["Currency"] == "GBP", "Rate"].values[0]

    df["MC_EUR"] = round(df["Market Cap"] * eur_rate, 2)
    df["MC_GBP"] = round(df["Market Cap"] * gbp_rate, 2)
    df["MC_INR"] = round(df["Market Cap"] * inr_rate, 2)
    df = df.rename(columns={"Market Cap": "MC_USD"})

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    # print(df)
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path, index=False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''

    output = pd.read_sql(query_statement, sql_connection)
    print(output)
    return output


log_progress("  Preliminaries complete. Initiating ETL process ")

log_progress("  Extraction Started ")
df = extract(url)
log_progress("  Data extraction complete. Initiating Transformation process")

df_transformed = transform(df, "exchange_rate.csv")
log_progress("  Data transformation complete. Initiating Loading process")

load_to_csv(df_transformed, "top_10_banks.csv")

log_progress("  Data saved to CSV file ")

sql_connection = sqlite3.connect("Banks.db")
table_name = 'Largest_banks'

log_progress("  SQL Connection Initiated")
load_to_db(df_transformed, sql_connection, table_name)
log_progress("  Data loaded to Database as a table, Executing Queries")

query_statement_1 = "SELECT * FROM Largest_banks"
query_statement_2 = "SELECT AVG(MC_GBP) FROM Largest_banks"
query_statement_3 = "SELECT Bank_Name from Largest_banks LIMIT 5"
run_query(query_statement_1, sql_connection)
run_query(query_statement_2, sql_connection)
run_query(query_statement_3, sql_connection)

log_progress("  Process Complete")

log_progress("  Server Connection Closed")
