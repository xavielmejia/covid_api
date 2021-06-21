'''
Extract info from: 'https://api.covid19api.com/dayone/country/{country}/status/confirmed'

The data is inserted into a database called postgres, in with a 
'''

# import libraries
import pandas as pd
import requests
import pprint
import datetime
import logging
import sys
import os
import sqlalchemy

FORMAT = '%(asctime)s: %(levelname)s - %(message)s'
logging.basicConfig(filename='log.txt', format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)



def insert_data(con, country, start_date=None):
    '''
    Function to insert covid data into xm_stg_covid_api table in postgres database, localhost server.
    using an api 'https://api.covid19api.com/dayone/country/{country}/status/confirmed' it extract 
    information from the web and update the table base on the start date specified to today.

    params:
    con: connection to be use, in order to connect to the database
    start_date: date from which it will insert data
    country: country to download data from
    '''
    end_date = datetime.date.today()
    url_countries = 'https://api.covid19api.com/countries'
    response_countries = requests.get(url_countries)
    if response_countries.status_code == 200:
        json_countries_result = response_countries.json()
        countries = [c['Country'].lower() for c in json_countries_result]
        
        if country.lower() in countries:
            url_data = f'https://api.covid19api.com/dayone/country/{country}/status/confirmed'
            params = {'from':str(start_date),'to':str(end_date)}
            
            # validate if the start_date parameter is None, if yes then use params else load all data
            if start_date - datetime.timedelta(days=1)==end_date:
                logger.info('Data is up to date!')
                sys.exit(0)
            elif start_date!=None:
                response_data = requests.get(url_data, params=params )
            else:
                response_data = requests.get(url_data)

            # validate the status code return by the request
            if response_data.status_code==200:
                logger.info('Data have been read succesfully!')
                json_result = response_data.json()
                df = pd.DataFrame(json_result)
                
                # save data to postgres database
                try:
                    df.to_sql('xm_stg_covid_api', con_pg, if_exists='append', index=False)
                    logger.info('Data inserted sucessfully in postgres database!')
                except:
                    err_type = str(sys.exc_info()[0])
                    err_msg = str(sys.exc_info()[1])
                    logger.info(f'Somethig went wrong with the connection to postgres, error type: {err_type}: error message: {err_msg}')

            # handle response status different from 200
            elif response_data.status_code==401:
                logger.info('Response status was 401 - unathorized: your request requires some additional permissions')
            elif response_data.status_code==404:
                logger.info('Response status was 404 - not found: the request resource does not exist')
            elif response_data.status_code==405:
                logger_data.info('Response status was 401 - method not allowed: the endpoint does not allow for that specific HTTP method')
            elif response_data.status_code==500:
                logger.info('Response status was 500 - Internal Server Error: your request was not expected and probably broke something on the server side')
            else:
                logger.info(f'Response status was {response_data.status_code} - {response_data.reason}')  
        else:
            logger.error('Country is not in the list of available countries')
    else:
        logger.error(f'Response status from countries was {response_countries.status_code} - {response_countries.status_code}')


# connect to postgres database
try:
    con_pg = sqlalchemy.create_engine(os.environ.get('POSTGRES_POSTGRES'))
    if con_pg==None:
        logger.error('The connection is returning a None object, situation needs to be validated, program has exited')
        sys.exit(1)
except psycopg2.OperationalError as e:
    logger.error(f'There was an operational error when trying to connect to postgres, error: {str(e)}, program has exited')
    sys.exit(1)
except:
    error_type = str(sys.exc_info()[0])
    error_message = str(sys.exc_info()[1])
    logger.info('Something went wrong when trying to connect to the database, error: {error_type}; error message: {error_message}, program has exited')
    sys.exit(1)

# validate if table exists in postgres database
query_tbl_exists = ''' 
select tablename 
from pg_catalog.pg_tables 
where schemaname='public' 
    and tableowner='postgres' 
    and tablename='xm_stg_covid_api'  
'''
df_tbl_exists = pd.read_sql(query_tbl_exists, con_pg)

# country to select data from
country = 'dominican republic'

# if the resulting value of the dataframe exist in the database
if not(df_tbl_exists.empty) and df_tbl_exists.values[0][0]=='xm_stg_covid_api':

    # get the last day inserted in api_covid table located in the postgres database
    query = ''' 
    select max("Date") as start_date 
    from xm_stg_covid_api
    '''
    df_start_date = pd.read_sql(query, con_pg) # get a dataframe with the value for max date

    start_date = datetime.datetime.fromisoformat(df_start_date.values[0][0][:-1]).date() + datetime.timedelta(days=1) # get the last day inserted to database and sum 1 day 
    
    # the there is not start date in the result then 
    if start_date!=None:
        insert_data(con=con_pg, start_date=start_date, country=country)
        sys.exit(0)

# if table does not exists or there is not start_date create it for the first time
logger.info('Table created succesfully!')
insert_data(con=con_pg, country=country)




