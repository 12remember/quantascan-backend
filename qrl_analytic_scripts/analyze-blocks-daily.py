
import psycopg2
import psycopg2.extras

import os
import pandas as pd
import numpy as np
from django_pandas.io import read_frame
import datetime as dt
import sys

from settings import connection , cur


def analyze_qrl_blocks():
    cur.execute('SELECT "date" FROM public."qrl_aggregated_block_data" ORDER BY "date" DESC LIMIT 1')     
    latest_analytics_date = cur.fetchone()
    
    if latest_analytics_date:
        cur.execute('DELETE FROM public."qrl_aggregated_block_data" WHERE "date" = %s', (latest_analytics_date,))
        connection.commit()
        cur.execute('SELECT "block_number", "block_found_datetime", "block_size", "block_reward_block",  "block_reward_fee" FROM public."qrl_blockchain_blocks" WHERE "block_found_datetime" >= %s ORDER BY "block_found_datetime" ASC',(latest_analytics_date,))  
    else:
        cur.execute('SELECT "block_number", "block_found_datetime", "block_size", "block_reward_block",  "block_reward_fee" FROM public."qrl_blockchain_blocks" ORDER BY "block_found_datetime" ASC')
     
    df = pd.DataFrame(cur.fetchall(),columns=["block_number", "block_found_datetime", "block_size", "block_reward_block", "block_reward_fee"]) 
    s = pd.to_datetime(df['block_found_datetime'], utc=True)

    df['date']=s.dt.floor('d')

    df = df.sort_values(['block_found_datetime'], ascending = True)
    df['time_diff'] = df['block_found_datetime'].diff().dt.seconds
    #pd.set_option('display.max_rows', None)

    df_grouped = df.groupby(['date',]).agg({'block_number':['count',],'block_size': ['mean','min','max'], 'block_reward_block':['mean', 'sum'], 'time_diff': ['mean', 'min', 'max'], 'block_reward_fee':['sum', 'mean'] }) #print(df_grouped)
    df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()]
    
        
    df_grouped['date'] = df_grouped.index
    df_grouped["block_number_count"] = df_grouped["block_number_count"]    
    df_grouped["block_reward_block_sum"] = df_grouped["block_reward_block_sum"]
    df_grouped["block_reward_fee_mean"] = df_grouped["block_reward_fee_mean"]
    df_grouped["block_size_mean"] = df_grouped["block_size_mean"]
    df_grouped["block_size_min"] = df_grouped["block_size_min"]
    df_grouped["block_size_max"] = df_grouped["block_size_max"]
    df_grouped = df_grouped.rename(columns={'time_diff_max':'block_timestamp_seconds_max','time_diff_min': 'block_timestamp_seconds_min', 'time_diff_mean':'block_timestamp_seconds_mean' })

    #print(df_grouped['block_found_datetime_max'])
    #print(df_grouped['block_found_datetime_min'])

    
    df_grouped['analyze_script_date'] = pd.Timestamp.now()
    df_grouped['analyze_script_name'] = 'analyze-blocks-daily'
    df_grouped['analyze_script_version'] = '0.01'
    

    
    """
    Using cursor.executemany() to insert the dataframe
    """
    print('Start Dumping in DB')
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df_grouped.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df_grouped.columns))
    # SQL quert to execute

    query  = '''INSERT INTO public.%s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)''' % ('qrl_aggregated_block_data', cols)
    
    cursor = connection.cursor()
    try:
        print('excecute many')
        cursor.executemany(query, tuples)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()



analyze_qrl_blocks()

