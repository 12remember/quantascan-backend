
import psycopg2
import psycopg2.extras

import os
import pandas as pd
import numpy as np
from django_pandas.io import read_frame
import datetime as dt

from settings import connection , cur


def analyze_qrl_transactions():
    
    cur.execute('SELECT "date" FROM public."qrl_aggregated_transaction_data" ORDER BY "date" DESC LIMIT 1')     
    latest_analytics_date = cur.fetchone()
    print(latest_analytics_date)
    
    if latest_analytics_date:
        cur.execute('DELETE FROM public."qrl_aggregated_transaction_data" WHERE "date" = %s', (latest_analytics_date,))
        connection.commit()        
        cur.execute('SELECT "transaction_block_number", public.qrl_blockchain_transactions.block_found_datetime, "transaction_type", "transaction_amount_send", "transaction_result", qrl_blockchain_transactions.master_addr_fee FROM public."qrl_blockchain_transactions" INNER JOIN "qrl_blockchain_blocks" ON "transaction_block_number" = "block_number" WHERE public.qrl_blockchain_transactions.block_found_datetime >= %s ORDER BY "transaction_block_number" ASC  ', (latest_analytics_date,)) 
    else:
        cur.execute('SELECT "transaction_block_number", public.qrl_blockchain_transactions.block_found_datetime, "transaction_type", "transaction_amount_send", "transaction_result", qrl_blockchain_transactions.master_addr_fee FROM public."qrl_blockchain_transactions" INNER JOIN "qrl_blockchain_blocks" ON "transaction_block_number" = "block_number" ORDER BY "transaction_block_number" ASC ', )

    
    df = pd.DataFrame(cur.fetchall(),columns=["transaction_block_number", "block_found_datetime", "transaction_type", "transaction_amount_send", "transaction_result", "master_addr_fee"]) #,columns=["block_found_datetime", "transaction_block_number"]
    s = pd.to_datetime(df['block_found_datetime'], utc=True)
    
    df['datetime']=s.dt.floor('d')
    df["master_addr_fee_no_0"] = df["master_addr_fee"].replace(0, np.NaN)

    #df_grouped = df.groupby(['datetime',]).agg({'transaction_block_number': 'count', 'transaction_amount_send':['sum','count'], 'master_addr_fee_no_0':['sum','mean','min','max']})
    #df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()]

    
    df_grouped = df.groupby(['datetime', 'transaction_type']).agg({'transaction_block_number': 'count', 'transaction_amount_send':['sum','count'],'master_addr_fee_no_0':['sum','mean','min','max']})
    df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()]

    df_grouped = df_grouped.reset_index()

        
    df_grouped['date'] = df_grouped['datetime']# df_grouped.index

    df_grouped['master_addr_fee_no_0_sum'] = df_grouped["master_addr_fee_no_0_sum"]
    df_grouped['master_addr_fee_no_0_mean'] = df_grouped["master_addr_fee_no_0_mean"].fillna(0)
    df_grouped['master_addr_fee_no_0_min'] = df_grouped["master_addr_fee_no_0_min"].fillna(0) 
    df_grouped['master_addr_fee_no_0_max'] = df_grouped["master_addr_fee_no_0_max"].fillna(0) 

    df_grouped['analyze_script_date'] = pd.Timestamp.now()
    df_grouped['analyze_script_name'] = 'analyze-transactions-daily'
    df_grouped['analyze_script_version'] = '0.01'
    
    
    df_grouped = df_grouped.rename(columns={
    'transaction_block_number_count':'total_blocks_found', 
    'transaction_amount_send_count':'total_number_of_transactions',
    'transaction_amount_send_sum':'total_amount_transfered',
    'master_addr_fee_no_0_sum':'transaction_fee_total',
    'master_addr_fee_no_0_mean':'transaction_fee_mean',
    'master_addr_fee_no_0_min':'transaction_fee_min',
    'master_addr_fee_no_0_max':'transaction_fee_max'   })

    del df_grouped['datetime']
    
    print('Start Dumping in DB')
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df_grouped.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df_grouped.columns))
    # SQL quert to execute

    query  = '''INSERT INTO public.%s(%s) VALUES(%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s,%%s)''' % ('qrl_aggregated_transaction_data', cols)
    
    cursor = connection.cursor()
    try:
        cursor.executemany(query, tuples)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        connection.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()






analyze_qrl_transactions()