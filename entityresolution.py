import pandas as pds

import pypyodbc as odbc

import os

#import time

from dagster import MetadataValue, asset, repository, AutoMaterializePolicy, observable_source_asset, DataVersion, AutoMaterializeRule

from query import *
from config import server_name as SERVER_NAME

DRIVER_NAME = 'SQL SERVER'
DATABASE_NAME = 'entityresolution'

NAN_STR = "nan"

NA_STR = "N/A"
NA_STR_QUOTES = "\'" + NA_STR + "\'"

sql_time = 0

NAME = "name"
CONTACT_ID = "contact_id"
COUNTRY = "country"
COMPANY_NAME = "company_name"
COMPANY_EMPLOYEES = "company_employees"
COMPANY_REVENUE = "company_revenue"
COMPANY_INDUSTRY = "company_industry"
PHONE_NUMBER = "phone_number"
INTENT_SIGNALS = "intent_signals"
DO_NOT_CALL = "do_not_call"
CREATED_AT = "created_at"
UPDATED_AT = "updated_at"

ACME_COLUMNS = [COUNTRY, COMPANY_REVENUE, COMPANY_EMPLOYEES, COMPANY_INDUSTRY]

'''
Connection string used to connect to SQL Server database 'entityresolution'
using server name in string defined in config.py
'''
connection_string = f"""
    DRIVER={{{DRIVER_NAME}}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    Trust_Connection=yes;
"""

'''
Object used for connecting to SQL Server database
'''
conn = odbc.connect(connection_string, autocommit = True)

'''
Object used for managing the context of query operations
'''
cursor = conn.cursor()

'''
Rule for updating or executing an asset if the previous asset has been automatically executed
'''
wait_for_updated = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.materialize_on_parent_updated()
)

'''
Convert date string of format YYYY-MM-DD to MM/DD/YYYY
'''
def to_date_time(date_str):
    return date_str[5:7] + '/' + date_str[-2:] + '/' + date_str[2:4]

'''
Find the maximum date for the "updated_at" attribute for a list of rows
'''
def max_udate_in_row(rows):
    max_value = ''
    # If rows is [{"name": "name1", "updated_at": "2023-12-31", "created_at": "2023-12-30"},
    #             {"name": "name1", "updated_at": "2023-12-21", "created_at": "2023-12-20"}],
    # return "2023-12-31"
    for value in rows:
        if to_date_time(max_value) == '' or to_date_time(value[UPDATED_AT]) > max_value:
            max_value = to_date_time(value[UPDATED_AT])
    return max_value

'''
Find the minimum date for the "created_at" attribute for a list of rows
'''
def min_cdate_in_row(row):
    min_value = ''
    # If rows is [{"name": "name1", "updated_at": "2023-12-31", "created_at": "2023-12-30"},
    #             {"name": "name1", "updated_at": "2023-12-21", "created_at": "2023-12-20"}],
    # return "2023-12-20"
    for value in row:
        if to_date_time(min_value) == '' or to_date_time(value[CREATED_AT]) < min_value:
            min_value = to_date_time(value[CREATED_AT])
    return min_value     

'''
Insert all rows of DataFrame from CSV file into an SQL table

For instance, if the row is:
{name: "Eric Wolfson", email_address: "ericwolfson@mail.com", phone_number: "555-555-5555",
 title: "Software Engineer", company_name: "CompanyName1", company_domain: "domain1.com"
 favorite_color: "red", updated_at: "2023-12-30", created_at: "2023-12-29"}
 
The insert statement is:

INSERT INTO crm_contacts VALUES('Eric Wolfson', 'ericwolfson@mail.com', '555-555-5555','Software Engineer', 
company_name: 'CompanyName1', company_domain: 'domain1.com', 'red', '2023-12-29', '2023-12-30')

If the table doesn't exist, it is created and if it is already full from a different run, it is
cleared first
'''
def load_table_in_db(merge_dataframes, create_string, insert_prefix, table_name):
    data = merge_dataframes

    # check if table already exists in the database
    cursor.execute(check_table_query + "\'" + table_name + "\'")
    
    dbs = cursor.fetchall()

    # if the table does not exists, create it
    if not dbs:        
        cursor.execute(create_string)
        conn.commit()

    # delete all contents of the table if it was already filled from the last run
    if table_name == 'crm_contacts':   
        cursor.execute("DELETE FROM crm_contacts")
    elif table_name == 'acme_contacts':    
        cursor.execute("DELETE FROM acme_contacts")
    elif table_name == 'rapid_data_contacts':    
        cursor.execute("DELETE FROM rapid_data_contacts")
        
    conn.commit()

    columns = data.columns

    # for each row in the dataframe create and execute an insert operation
    for data_index in range(0, len(data)):
        insert_string = insert_prefix

        # for each column name for that row, add the value of that field to the insert statement
        for column_index in range(0, len(columns)):

            # get the column name based on index
            column = columns[column_index]

            # get the field value based on column name and row number
            value = data[column][data_index]            

            # add field to insert string unless it is blank
            if str(value) != NAN_STR:
                # convert do_not_call field to BIT type in SQL
                if column == DO_NOT_CALL:
                    insert_string += "1" if str(value) == "TRUE" else "0"
                # add value to insert statement
                elif not (column == PHONE_NUMBER and str(value)[0] == '-'):
                    insert_string += "\'" + str(value) + "\'"
                # add 'N/A' if phone number is invalid (a negative number)
                else:
                    insert_string += NA_STR_QUOTES
            # insert the value as "null" if field is missing.
            # insert value as N/A if the field is missing and the column is "name"
            else:
                insert_string += "null" if column != NAME else NA_STR_QUOTES

            # add comma after each field in the VALUES(...) portion except after the last field
            if column_index < len(columns) - 1:
                insert_string += ","
                
        insert_string += ")"

        # execute the insert statement
        cursor.execute(insert_string)
        
        conn.commit()

def execute_sql_query(view_name, view_query):
    cursor.execute("DROP VIEW IF EXISTS " + view_name)

    conn.commit()	
    
    cursor.execute(view_query)
    
    conn.commit()

'''
Create asset that get observed every 45 seconds and triggers the next asset
if the crm__contacts.csv file was updated during that duration of time
'''
@observable_source_asset(auto_observe_interval_minutes=0.75, description="auto trigger for crm load")
def check_crm_update():
    return DataVersion(str(os.path.getmtime("crm__contacts.csv")))    

'''
Create asset that get observed every 45 seconds and triggers the next asset
if the acme__contacts.csv file was updated during that duration of time
'''
@observable_source_asset(auto_observe_interval_minutes=0.75, description="auto trigger for acme load")
def check_acme_update():
    return DataVersion(str(os.path.getmtime("acme__contacts.csv")))

'''
Create asset that get observed every 45 seconds and triggers the next asset
if the rapid_data__contacts.csv file was updated during that duration of time
'''
@observable_source_asset(auto_observe_interval_minutes=0.75, description="auto trigger for rapid data load")
def check_rapid_data_update():
    return DataVersion(str(os.path.getmtime("rapid_data__contacts.csv")))    

'''
Read data from the crm__contacts csv file and convert into and return the equivalent dataframe with all
fields as strings. If auto materialize is on and crm__contacts was updated, this will automatically trigger as well
as all subsequent assets in the pipeline
'''
@asset(auto_materialize_policy=wait_for_updated, deps=[check_crm_update],description="Load csv into CRM DataFrame")
def crm_dataframe_from_csv() -> pds.DataFrame:
    data = pds.read_csv('crm__contacts.csv', dtype=str)
    return data

'''
Read data from the acme__contacts csv file and convert into and return the equivalent dataframe with all
fields as strings. If auto materialize is on and crm__contacts was updated, this will automatically trigger as well
as all subsequent assets in the pipeline
'''
@asset(auto_materialize_policy=wait_for_updated, deps=[check_acme_update], description="Load csv into Acme DataFrame")
def acme_dataframe_from_csv() -> pds.DataFrame:
    data = pds.read_csv('acme__contacts.csv', dtype=str)
    return data

'''
Read data from the rapid_data__contacts csv file and convert into and return the equivalent dataframe with all
fields as strings. If auto materialize is on and crm__contacts was updated, this will automatically trigger as well
as all subsequent assets in the pipeline
'''
@asset(auto_materialize_policy=wait_for_updated, deps=[check_rapid_data_update], description="Load csv into RapidData DataFrame")
def rapid_data_dataframe_from_csv() -> pds.DataFrame:
    data = pds.read_csv('rapid_data__contacts.csv', dtype=str)
    return data

'''
Load the dataframe returned by the crm_dataframe_from_csv asset into the crm_contacts table in SQL Server
'''
@asset(auto_materialize_policy=wait_for_updated, description="Load CRM Contacts DataFrame Into Database")
def load_crm_into_db(crm_dataframe_from_csv):
    # call the common load_table_in_db function with crm variable parameters
    load_table_in_db(crm_dataframe_from_csv, create_crm, crm_insert, 'crm_contacts')  

'''
Load the dataframe returned by the crm_dataframe_from_csv asset into the acme_contacts table in SQL Server
'''
@asset(auto_materialize_policy=wait_for_updated, description="Load Acme Contacts DataFrame Into Database")
def load_acme_into_db(acme_dataframe_from_csv):
    # call the common load_table_in_db function with acme variable parameters
    load_table_in_db(acme_dataframe_from_csv, create_acme, acme_insert, 'acme_contacts')

'''
Load the dataframe returned by the rapid_data_dataframe_from_csv asset into the rapid_data_contacts table in SQL Server
'''
@asset(auto_materialize_policy=wait_for_updated, description="Load RapidData Contacts DataFrame Into Database")
def load_rapid_data_into_db(rapid_data_dataframe_from_csv):
    # call the common load_table_in_db function with rapid_data variable parameters
    load_table_in_db(rapid_data_dataframe_from_csv, create_rapid_data, rapid_data_insert, 'rapid_data_contacts')

'''
Resolve duplicates in the rapid_data_contacts SQL table using the create_reduced_rapid_data_view query from query.py
'''
@asset(auto_materialize_policy=wait_for_updated, description="Resolve Duplicates for RapidData")
def resolve_rapid_data_duplicates(load_rapid_data_into_db):
    execute_sql_query("rd_duplicates_removed", create_reduced_rapid_data_view)

'''
Join the 3 tables (rd_duplicates_removed, crm_contacts and acme_contacts joining exact duplicates for common
column names and filling in non-common column names based on values from the other two tables.
Find the greatest updated_at value for the common records and least created_at value to preserve as much
date data as possible, since the result will contain fields in columns that have different dates
'''
@asset(auto_materialize_policy=wait_for_updated, deps=[load_crm_into_db, load_acme_into_db, resolve_rapid_data_duplicates], description="Create View for merged contacts")
def combine_exact_contacts():
    #start_time = time.time()
    # Join the rd_duplicates_removed table with the crm_contacts table
    execute_sql_query("rdc_combined", create_rapid_data_crm_combined_view)
    # Join the combined table above with the acme_contacts table
    execute_sql_query("total_combined_dates", create_total_combined_view_dates)
    # Create a table with the merged dates from the last table without the ip_address column
    execute_sql_query("total_combined", create_total_combined_view)
    #sql_time = (time.time() - start_time)
    #print("seconds %s" % sql_time)

'''
Pull the combined data table from the database and read it into a dataframe
'''
@asset(auto_materialize_policy=wait_for_updated, deps=[combine_exact_contacts], description="extract DB contents into DataFrame Format")
def query_db_into_dataframe() -> pds.DataFrame:   
    get_all_query = "SELECT * FROM total_combined ORDER BY name"
    data = pds.read_sql(get_all_query, conn)
    rearranged_data = data.iloc[:,range(0,15)] 
    return rearranged_data

'''
Convert null values (interpreted as None or "nan" from the dataframe into
a default value that matches the expected data type for that column
'''
@asset(auto_materialize_policy=wait_for_updated, description="fill in null values with appropriate type")
def clean_data(query_db_into_dataframe) -> pds.DataFrame:
    data = query_db_into_dataframe

    # for each column...
    for column in data.columns:
        num_values = len(data[column])
        # for each value in that column
        for index in range(0,num_values):
            value = data[column][index]
            if value == None or str(value) == NAN_STR:
                # convert company_employees and company_revenue to a default integer type if "nan"
                if column == COMPANY_EMPLOYEES or column == COMPANY_REVENUE:
                    value = -1
                # convert do_not_call to a default boolean if None
                elif column == DO_NOT_CALL:
                    value = "FALSE"
                # convert intent_signals to a default empty JSON array
                elif column == INTENT_SIGNALS:
                    value = "\"[]\""
                # otherwise for strings, fill in "N/A"
                else:
                    value = NA_STR
                data[column][index] = value

    return data

'''
Fill in missing country, company_employees, company_revenue, and company_industry fields based on other records corresponding
to the same company that have those values specific to those columns specified
1) Name: “Full Name”, Company Name: “SemiConductors Inc.”Country: “USA”, Company Employees: 1000, Company Revenue: 1000000, Company Industry “Semi Conductors”
2) Name: “Full Name 2”, Company Name: “SemiConductors Inc.”, Country: “N/A”, Company Employees: -1, Company Revenue: -1, Company Industry: “N/A”
We can fill in Country, Company Employees, Company Revenue and Company Industry for the second record since we can infer them from the first record that has the same company name
'''
@asset(auto_materialize_policy=wait_for_updated, description="Extrapolate for Acme only columns")
def extrapolate_data(clean_data) -> pds.DataFrame:
    data = clean_data

    # get list of all company values from the "company_name" column
    company_names_list = data[COMPANY_NAME]

    # create a set that eliminates duplicate values
    company_names_set = set(company_names_list)

    # create a dictionary with a key having the company name, and a set containing the records with that company name
    company_dictionary = {}

    # initialize all key value pairs of the dictionary to an empty list
    for element in company_names_set:
        company_dictionary[element] = []        

    # for each company name in the company name list...
    for i in range(0, len(company_names_list)):
        # get the country, company_revenue, company_employees, and company_industry values for that company
        country = data[COUNTRY][i]
        company_revenue = data[COMPANY_REVENUE][i]
        company_employees = data[COMPANY_EMPLOYEES][i]
        company_industry =  data[COMPANY_INDUSTRY][i]
        # add this combination as a value in the list corresponding to the company name key
        company_dictionary[company_names_list[i]].append([country, company_revenue, company_employees, company_industry])

    # leave out all companies in the list that have null values
    for element in company_names_set:
        dict_element = company_dictionary[element]
        company_dictionary[element] = [value for value in dict_element if value[0] != NA_STR and value[1] != -1 and value[2] != -1 and value[3] != NA_STR]

    # for each non-null value in the company lists for each company name,
    # choose the remaining combination, which is supposed to have the values we want to put
    # in the empty fields (extrapolate the data)
    for i in range(0, len(company_names_list)):
        company_name = company_names_list[i]
        if len(company_dictionary[company_name]) > 0:
            for c in range(0, len(ACME_COLUMNS)):
               data[ACME_COLUMNS[c]][i] = company_dictionary[company_name][0][c]
               
    return data

'''
Try to merge rows that have common name and phone number (might decrease data quality if two different
individuals (contacts) have the same name and phone number)
'''
@asset(auto_materialize_policy=wait_for_updated, description="Combine based on phone numbers")
def combine_post_merge(extrapolate_data) -> pds.DataFrame:
    data = extrapolate_data

    # get final list of records
    final_data_list = []

    # key is tuple of name and phone number
    # values are entire records for that combination
    phone_number_dictionary = {}

    # result that contains one record for that that
    # key pair above in the last statement
    phone_number_final_values_dictionary = {}

    # for each row in the dataframe
    for index in data.index:
        key = (data[NAME][index], data[PHONE_NUMBER][index])
        # append record to list for the tuple key
        if key in phone_number_dictionary.keys():
            phone_number_dictionary[key].append(data.iloc[index])
        # create list with only the record
        else:
            phone_number_dictionary[key] = [data.iloc[index]]

    # for each key in the dictionary filled above
    for key in phone_number_dictionary.keys():
        # result dictionary after merge
        phone_number_final_values_dictionary[key] = {}
        # get value for key in phone_number_dictionary
        value = phone_number_dictionary[key]
        # get the max updated_at date for the records for that tuple key value pair
        max_udate = max_udate_in_row(value)
        # get the min created_at date for the records for that tuple key value pair
        min_cdate = min_cdate_in_row(value)
        # for each record for that tuple key
        for item in value:
            # find record with latest latest date
            if to_date_time(item[UPDATED_AT]) == max_udate:
               # set the field to the latest record's row value
               for column in data.columns:
                   # for each non date value...
                   if column is not UPDATED_AT and column is not CREATED_AT:
                       phone_number_final_values_dictionary[key][column] = item[column]
                   # and for the maximum updated_at value
                   elif column == UPDATED_AT:
                       phone_number_final_values_dictionary[key][UPDATED_AT] = max_udate
                   # and for the minimum created_at value
                   elif column == CREATED_AT:
                       phone_number_final_values_dictionary[key][CREATED_AT] = min_cdate

    # build list for final combination of records
    for key in phone_number_final_values_dictionary:
        final_data_list.append(phone_number_final_values_dictionary[key])

    # return DataFrame of that list
    return pds.DataFrame(final_data_list)

'''
Add the unique contacts_id column to the final dataframe, and store the end result in a csv file.
'''
@asset(auto_materialize_policy=wait_for_updated, description="Store data into CSV file")
def create_csv(combine_post_merge):
    combine_post_merge.insert(loc=0, column=CONTACT_ID, value=list(range(0,len(combine_post_merge))))
    combine_post_merge.to_csv('current_state_final.csv', index=False)