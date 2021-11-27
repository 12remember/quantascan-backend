
from django.db.models import Count, Sum, F
from django.db.models.expressions import Window
from django.db.models.functions import Rank

from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from django.views.decorators.vary import vary_on_cookie
from django.views.decorators.csrf import csrf_exempt
from django.db import connection
from django.http import Http404

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import viewsets

import pandas as pd
from django_pandas.io import read_frame
import datetime as dt
from datetime import date, timedelta

from silk.profiling.profiler import silk_profile
from .models import *
from .serializers import *
import numpy as np 


class walletRichList(APIView):
    @method_decorator(cache_page(5*60))
    @silk_profile(name='walletRichList') 
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlWalletAddress.objects.values('wallet_address', 'address_balance', 'wallet_custom_name','address_first_found', 'wallet_type').annotate(rank=Window(expression=Rank(),order_by=F('address_balance').desc()))[:500]   
        df_total = read_frame(qs)
        
        total_number_of_wallets = df_total["address_balance"].count()  # count number of wallets
        total_amount_in_wallets = df_total["address_balance"].sum() # get total amount in wallets
        df_total["holding_in_percentage"] = df_total["address_balance"] / total_amount_in_wallets * 100 # calculate share of wallet / total 

        wallet_list = df_total.to_dict(orient='records')
        return Response({
        'wallet_list':wallet_list, 
        })

class walletDistribution(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='walletDistribution')
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlWalletAddress.objects.values('address_balance').annotate(rank=Window(expression=Rank(),order_by=F('address_balance').desc()))                
        df_total = read_frame(qs)
        
        totalnumber_wallets = df_total['address_balance'].count()
        s=pd.Series(df_total['address_balance']>= 999999999).count()
        totalnumber_wallets_with_balance_1 = df_total[df_total['address_balance'] > 999999999].count()["address_balance"] # 1 quanta
        totalnumber_wallets_with_balance_5 =df_total[df_total['address_balance'] > 4999999999].count()["address_balance"] # 5 quanta
        totalnumber_wallets_with_balance_10 =df_total[df_total['address_balance'] > 9999999999].count()["address_balance"] # 10 quanta
        totalnumber_wallets_with_balance_100 =df_total[df_total['address_balance'] > 99999999999].count()["address_balance"] # 100 quanta
        totalnumber_wallets_with_balance_1000 =df_total[df_total['address_balance'] > 999999999999].count()["address_balance"] # 1000 quanta
        totalnumber_wallets_with_balance_10000 =df_total[df_total['address_balance'] > 9999999999999].count()["address_balance"] # 10000 quanta
        totalnumber_wallets_with_balance_100000 =df_total[df_total['address_balance'] > 99999999999999].count()["address_balance"] # 100000 quanta

        totalnumber_wallets_with_balance_x = [
        {'id':1 ,'count':totalnumber_wallets_with_balance_1, 'amount':'1'},
        {'id':2,'count':totalnumber_wallets_with_balance_5, 'amount':'5'},
        {'id':3, 'count':totalnumber_wallets_with_balance_10, 'amount':'10'},
        {'id':4, 'count':totalnumber_wallets_with_balance_100, 'amount':'100'},
        {'id':5, 'count':totalnumber_wallets_with_balance_1000, 'amount':'1.000'},
        {'id':6, 'count':totalnumber_wallets_with_balance_10000, 'amount':'10.000'},
        {'id':7,'count':totalnumber_wallets_with_balance_100000, 'amount':'100.000'}
        ]
        
        df_total = df_total[df_total['address_balance'] > 0]
        df_total.dropna()
        
        df_perct = df_total

        labels = [ i for i in range(1, 1001, 1)]
        df_perct['perct_group'] = pd.qcut(df_perct['address_balance'].rank(method='first'), q=1000, labels=labels )
        df_perct = df_perct.groupby('perct_group', as_index=False).agg({'address_balance': ['sum', 'count', 'mean', 'min',]})
        df_perct.columns = ["_".join(x) for x in df_perct.columns.ravel()]
    
        df_perct['percentage_owned'] = 100* df_perct['address_balance_sum'] / df_perct['address_balance_sum'].sum()
        df_perct['volume_owned'] = df_perct['address_balance_sum'] 
        
        df_grouped = df_total    
        labels = [ i for i in range(1, 100, 1)]
        df_grouped['address_balance_group'] = pd.qcut(df_total['address_balance'].rank(method='first'), q=99, labels=labels)
        df_grouped = df_total.groupby('address_balance_group', as_index=False).agg({'address_balance': ['sum', 'count', 'mean', 'min']})
        df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()]
    
        df_grouped["address_balance_group_list"] = df_grouped['address_balance_group_'].values.tolist() 
        df_grouped["address_balance_list"] = df_grouped['address_balance_min'].values.tolist()

        df_grouped = df_grouped[['address_balance_list', 'address_balance_group_list']]

        return Response({
        'distribution_percentage':{
        'percentage_owned':df_perct['percentage_owned'],
        'volume_owned':df_perct['volume_owned'],
        },
        'distribution_percentile': df_grouped,
        'distribution_wallets_holding_x':totalnumber_wallets_with_balance_x,
        
        }) 

        
class walletNumberOfWallets(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='walletNumberOfWallets')
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlWalletAddress.objects.all()        
        df = read_frame(qs)
        s = pd.to_datetime(df['address_first_found'])
                
        df['date']=s.dt.floor('d')
    
        df_grouped = df.groupby(['date']).agg({'wallet_address': ['count',]})
        df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()]
        
        s = pd.Series(df_grouped['wallet_address_count'])
        df_grouped["total_number_of_wallet"] = s.cumsum()
        df_grouped = df_grouped.rename(columns={"wallet_address_count":"daily_new_wallets_found"}) 
        
        df_grouped["date"] = df_grouped.index
        df_grouped["date"] = (df_grouped["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
                        
        
        chart_data_point_list = []
        for index, rows in df_grouped.iterrows(): 
            data_point_row = {
                "date":rows.date, 
                "total_number_of_wallet":rows.total_number_of_wallet, 
                "daily_new_wallets_found":rows.daily_new_wallets_found} 
            chart_data_point_list.append(data_point_row) 
            
        return Response({
        'chart_data_point_list': chart_data_point_list, 
        })



class blockBlockSize(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='blockBlockSize')
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlAggregatedBlockData.objects.values('date','block_size_mean', 'block_size_min', 'block_size_max', 'block_timestamp_seconds_max').order_by('date')
        df_total = read_frame(qs)
    
        df_total["block_size_mean"] = df_total['block_size_mean']
        df_total["block_size_min"] = df_total['block_size_min']
        df_total["block_size_max"] = df_total['block_size_max']
        
        #df_total["dateReadable"] = df_total["date"].dt.strftime("%d %B %Y")
                
        df_total["date"] = (df_total["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        chart_data_point_list = []
        for index, rows in df_total.iterrows(): 
            data_point_row = {
            "date":rows.date , 
            "block_size_mean":rows.block_size_mean,
            "block_size_min":rows.block_size_min,
            "block_size_max":rows.block_size_max
            } 
            chart_data_point_list.append(data_point_row) 
            
        return Response({
        'chart_data_point_list': chart_data_point_list, 
        })



class blockBlockTime(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='blockBlockTime')
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlAggregatedBlockData.objects.values('date','block_number_count', 'block_timestamp_seconds_mean', 'block_timestamp_seconds_min', 'block_timestamp_seconds_max').order_by('date')      
        df_total = read_frame(qs)
           
        df_total["date"] = (df_total["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        chart_data_point_list = []
        for index, rows in df_total.iterrows(): 
            data_point_row = {
            "date":rows.date , 
            "block_timestamp_seconds_mean":rows.block_timestamp_seconds_mean,
            "block_timestamp_seconds_min":rows.block_timestamp_seconds_min,
            "block_timestamp_seconds_max":rows.block_timestamp_seconds_max,
            "block_number_count":rows.block_number_count
            } 
            chart_data_point_list.append(data_point_row) 
            
        return Response({
        'chart_data_point_list': chart_data_point_list, 
        })



class networkTransactions(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='networkTransactions')
    def get(self, request, format=None, *args, **kwargs):

        qs = QrlAggregatedTransactionData.objects.values('date','total_number_of_transactions', 'total_amount_transfered', 'transaction_type', 'total_blocks_found').order_by('date')      
        df_total = read_frame(qs)

        df_total = df_total[1:] # remove first day because mainnet tokentransfers
        
        df_coinbase = df_total[df_total['transaction_type']=='coinbase']
        df_coinbase["epoch"] = (df_coinbase["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        df_coinbase["total_number_of_transactions"] = df_coinbase['total_number_of_transactions']
        df_coinbase["total_amount_transfered"] = df_coinbase['total_amount_transfered']
        
        df_transfer = df_total[df_total['transaction_type']=='transfer']
        df_transfer["epoch"] = (df_transfer["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        df_transfer["total_number_of_transactions"] = df_transfer['total_number_of_transactions']
        df_transfer["total_amount_transfered"] = df_transfer['total_amount_transfered']
        
        df_slave = df_total[df_total['transaction_type']=='slave']
        df_slave["epoch"] = (df_slave["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        df_slave["total_number_of_transactions"] = df_slave['total_number_of_transactions']
        df_slave["total_amount_transfered"] = df_slave['total_amount_transfered']    
        
        
        chart_data_point_list_coinbase = []
        for index, rows in df_coinbase.iterrows(): 
            data_point_row = {
            "date": rows.epoch ,
            "total_number_of_transactions":rows.total_number_of_transactions,
            "total_amount_transfered":rows.total_amount_transfered,  
            } 
            chart_data_point_list_coinbase.append(data_point_row) 
    

        chart_data_point_list_transfer = []
        for index, rows in df_transfer.iterrows(): 
            data_point_row = {
            "date": rows.epoch ,
            "total_number_of_transactions":rows.total_number_of_transactions,
            "total_amount_transfered":rows.total_amount_transfered,  
            } 
            chart_data_point_list_transfer.append(data_point_row) 

        chart_data_point_list_slave = []
        for index, rows in df_slave.iterrows(): 
            data_point_row = {
            "date": rows.epoch ,
            "total_number_of_transactions":rows.total_number_of_transactions,
            "total_amount_transfered":rows.total_amount_transfered,  
            } 
            chart_data_point_list_slave.append(data_point_row) 
        
        
        return Response({
        'chart_data_point_list_coinbase': chart_data_point_list_coinbase, 
        'chart_data_point_list_transfer': chart_data_point_list_transfer,
        'chart_data_point_list_slave': chart_data_point_list_slave
        })        
        
   
class networkTotalCirculatingQuanta(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='networkTotalCirculatingQuanta')        
    def get(self, request, format=None, *args, **kwargs):

        qs = QrlAggregatedBlockData.objects.values('date','block_reward_block_sum').order_by('date') 
        df_total = read_frame(qs)
    
        df_total["block_reward_block_added"] = df_total['block_reward_block_sum'] / 1000000000  # total daily mining rewards convert from shor to quanta
        s = pd.Series(df_total['block_reward_block_sum'])
        df_total["block_reward_block_sum"] = s.cumsum() / 1000000000   # total circulating quanta         
                
        df_total["date"] = (df_total["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        chart_data_point_list = []
        for index, rows in df_total.iterrows(): 
            data_point_row = {
            "date":rows.date , 
            "network_emission_total_circulating_quanta":rows.block_reward_block_sum, 
            "network_emission_added_quanta_daily":rows.block_reward_block_added
            } 
            chart_data_point_list.append(data_point_row) 
            
        return Response({
        'chart_data_point_list': chart_data_point_list, 
        })

class networkTransactionFee(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='networkTransactionFee')         
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlAggregatedTransactionData.objects.filter(transaction_type__exact='transfer').order_by('date') # in db there is a daily row for each transaction type. to calculate average transaction fee only type 'transfer' is selected      
        df_total = read_frame(qs)
    
        df_total["transaction_fee_mean"] = df_total["transaction_fee_mean"] / 1000000000  # average transaction fee from shor to quanta
        df_total["transaction_fee_min"] = df_total["transaction_fee_min"] / 1000000000  # average transaction fee from shor to quanta
        df_total["transaction_fee_max"] = df_total["transaction_fee_max"] / 1000000000  # average transaction fee from shor to quanta
                
        df_total["epoch"] = (df_total["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        chart_data_point_list = []
        for index, rows in df_total.iterrows(): 
            data_point_row = {
            "date": rows.epoch ,
            "transaction_fee_mean":rows.transaction_fee_mean,
            "transaction_fee_min":rows.transaction_fee_min,  
            "transaction_fee_max":rows.transaction_fee_max, 
            } 
            chart_data_point_list.append(data_point_row) 
            
        return Response({
        'chart_data_point_list': chart_data_point_list, 
        })

class networkUniqueWalletsUsed(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='networkUniqueWalletsUsed')        
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlAggregatedTransactionData.objects.all()      
        df_total = read_frame(qs)
    
        df_total["transaction_fee_mean"] = df_total["transaction_fee_mean"] / 1000000000  # average transaction fee from shor to quanta
            
        df_total["date"] = (df_total["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
        chart_data_point_list = []
        for index, rows in df_total.iterrows(): 
            data_point_row = {
            "date": rows.date ,
            "transaction_fee_paid_mean":rows.transaction_fee_mean, 
            } 
            chart_data_point_list.append(data_point_row) 
            
        return Response({
        'chart_data_point_list': chart_data_point_list, 
        })
    


class walletData(APIView):
    @method_decorator(cache_page(5*60))
    @silk_profile(name='walletData') 
    def get(self, request, format=None, *args, **kwargs):
 
        get_data = request.query_params
        wallet = get_data['wallet']
        
        qs_wallet = QrlWalletAddress.objects.values('address_balance', 'wallet_custom_name', 'wallet_address', 'address_first_found', 'wallet_type').annotate(rank=Window(expression=Rank(),order_by=F('address_balance').desc()))    
        df_wallet = read_frame(qs_wallet)

        df_wallet_filter = df_wallet[(df_wallet.wallet_address == wallet)]
        if not df_wallet_filter.empty:
            df_wallet_filter = df_wallet_filter[['wallet_address', 'address_balance', 'wallet_custom_name', 'rank', 'address_first_found', 'wallet_type']]
            df_wallet_filter = df_wallet_filter.to_dict(orient='records')[0]
                
            return Response({
            'general_wallet_data':df_wallet_filter,
            })
        else:
            raise Http404

class walletData2(APIView):
    @method_decorator(cache_page(60*60*4))
    @silk_profile(name='walletData2') 
    def get(self, request, format=None, *args, **kwargs):
        get_data = request.query_params
        wallet = get_data['wallet']

        qs_transaction = QrlBlockchainTransactions.objects.values('transaction_amount_send', 'transaction_sending_wallet_address', 'transaction_receiving_wallet_address', 'transaction_type', 'block_found_datetime', 'transaction_hash').filter(transaction_sending_wallet_address=wallet) | QrlBlockchainTransactions.objects.values('transaction_amount_send', 'transaction_sending_wallet_address', 'transaction_receiving_wallet_address', 'transaction_type', 'block_found_datetime', 'transaction_hash').filter(transaction_receiving_wallet_address=wallet)    

        if qs_transaction:
            df_transaction = read_frame(qs_transaction)
            # Creat Associated Addresses
            top_transactions_sending = df_transaction[df_transaction["transaction_sending_wallet_address"]== wallet].sort_values(by=['transaction_amount_send'], ascending=False).head(100)
            top_transactions_sending = top_transactions_sending[['transaction_amount_send', 'transaction_receiving_wallet_address','transaction_hash', 'transaction_type', 'block_found_datetime']]
            top_transactions_receiving = df_transaction[df_transaction["transaction_receiving_wallet_address"]== wallet].sort_values(by=['transaction_amount_send'], ascending=False).head(100)    
            top_transactions_receiving = top_transactions_receiving[['transaction_amount_send', 'transaction_sending_wallet_address','transaction_hash', 'transaction_type', 'block_found_datetime']]

            list_top_transactions_sending = []
            for index, rows in top_transactions_sending.iterrows(): 
                data_point_row = { 
                    "transaction_amount_send":rows.transaction_amount_send, 
                    "transaction_receiving_wallet_address":rows.transaction_receiving_wallet_address,
                    "transaction_hash":rows.transaction_hash,
                    "transaction_type":rows.transaction_type,
                    "block_found_datetime":rows.block_found_datetime} 
                list_top_transactions_sending.append(data_point_row) 

            list_top_transactions_receiving = []
            for index, rows in top_transactions_receiving.iterrows(): 
                data_point_row = { 
                    "transaction_amount_send":rows.transaction_amount_send, 
                    "transaction_sending_wallet_address":rows.transaction_sending_wallet_address,
                    "transaction_hash":rows.transaction_hash,
                    "transaction_type":rows.transaction_type,
                    "block_found_datetime":rows.block_found_datetime} 
                list_top_transactions_receiving.append(data_point_row) 


            df_transaction["number_of_transactions"] = ''
            most_sending_wallets = df_transaction.pivot_table(index=['transaction_sending_wallet_address'],aggfunc={'number_of_transactions':'size', 'transaction_amount_send':'sum'}).head(100).sort_values('number_of_transactions', ascending=False).reset_index('transaction_sending_wallet_address')
            most_receiving_wallets = df_transaction.pivot_table(index=['transaction_receiving_wallet_address'], aggfunc={'number_of_transactions':'size', 'transaction_amount_send':'sum'}).head(100).sort_values('number_of_transactions', ascending=False).reset_index('transaction_receiving_wallet_address')

            list_most_sending_wallets = []
            for index, rows in most_sending_wallets.iterrows(): 
                data_point_row = { 
                    "transaction_sending_wallet_address":rows.transaction_sending_wallet_address,  
                    "number_of_transactions":rows.number_of_transactions,
                    "transaction_amount_send":rows.transaction_amount_send} 
                list_most_sending_wallets.append(data_point_row) 

            list_most_receiving_wallets = []
            for index, rows in most_receiving_wallets.iterrows(): 
                data_point_row = { 
                    "transaction_receiving_wallet_address":rows.transaction_receiving_wallet_address, 
                    "number_of_transactions":rows.number_of_transactions,
                    "transaction_amount_send":rows.transaction_amount_send} 
   
                if (data_point_row["transaction_receiving_wallet_address"] != wallet):      
                    list_most_receiving_wallets.append(data_point_row)
                else:
                    pass     



            total_transaction_send_number =  df_transaction[df_transaction["transaction_sending_wallet_address"] == wallet].count()[0]
            total_transaction_receive_number =  df_transaction[df_transaction["transaction_receiving_wallet_address"] == wallet].count()[0]
            
            # Create Chart
            df_transaction.loc[df_transaction.transaction_sending_wallet_address == wallet, 'transaction_amount_send'] = df_transaction.transaction_amount_send* -1 # if wallet is sending then substract value of transaction amount
            s = pd.to_datetime(df_transaction['block_found_datetime'], unit='s')          
            df_transaction['date']=s.dt.floor('d')
             
            df_grouped = df_transaction.groupby(['date'],).agg({'transaction_amount_send': ['sum',]})
            df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()]
        
            df_grouped["date"] = df_grouped.index
            startDateRange = df_grouped.index[0] - timedelta(5)  
            idx = pd.date_range(startDateRange, dt.datetime.now())
        
            s = pd.Series(df_grouped["date"])        
            df_grouped.index = pd.DatetimeIndex(s.index)        
            df_grouped = df_grouped.reindex(idx, fill_value=0)
            
            s_transactions = pd.Series(df_grouped['transaction_amount_send_sum'])
            df_grouped["wallet_value_total_daily"] = s_transactions.cumsum()

            df_grouped["epoch"] = df_grouped.index 
            df_grouped["epoch"] = (df_grouped["epoch"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000
                    
            chart_data_point_list = []
            for index, rows in df_grouped.iterrows(): 
                data_point_row = {
                    "date":rows.epoch, 
                    "wallet_value_total_daily":rows.wallet_value_total_daily, 
                    "transaction_amount_send_sum":rows.transaction_amount_send_sum} 
                chart_data_point_list.append(data_point_row) 
    
            
            return Response({
            'general_wallet_transaction_data':{
                'total_transaction_send_number':total_transaction_send_number,
                'total_transaction_receive_number':total_transaction_receive_number,
                'list_most_sending_wallets':list_most_sending_wallets,
                'list_most_receiving_wallets':list_most_receiving_wallets,
                'total_transaction_receive_number':total_transaction_receive_number,
                'list_top_transactions_receiving':list_top_transactions_receiving,
                'list_top_transactions_sending':list_top_transactions_sending,
            },
            'chart_data_point_list': chart_data_point_list, 
            })

        else:
            raise Http404


class donationData(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='walletData') 
    def get(self, request, format=None, *args, **kwargs):

        get_data = request.query_params
        wallet = 'Q010500f5350cf6abf88d28ff2de85b0512ee57c31e8eb69fa39d6640fc0ceb37e4634554282609'
        qs_wallet = QrlWalletAddress.objects.get(pk=wallet)
       
        
        qs_transaction = QrlBlockchainTransactions.objects.filter(transaction_receiving_wallet_address=wallet).order_by('-block_timestamp_seconds').values('transaction_sending_wallet_address', 'transaction_amount_send', 'block_timestamp_seconds')
        df_transaction = read_frame(qs_transaction)        
        
        donation_latest_transactions = df_transaction.head(10)    
        donation_top_transactions = df_transaction.sort_values(by=['transaction_amount_send'],ascending=False).head(10)
        donation_top_donators = df_transaction.groupby('transaction_sending_wallet_address').sum().reset_index()
        donation_top_donators = donation_top_donators.sort_values(by=['transaction_amount_send'], ascending=False).head(10)
        
        donation_latest_transactions = donation_latest_transactions[['transaction_sending_wallet_address', 'transaction_amount_send', 'block_timestamp_seconds']].to_dict(orient='records')
        donation_top_transactions = donation_top_transactions[['transaction_sending_wallet_address', 'transaction_amount_send', 'block_timestamp_seconds']].to_dict(orient='records')      
        donation_top_donators = donation_top_donators[['transaction_sending_wallet_address', 'transaction_amount_send']].to_dict(orient='records')
            
        return Response({
        'donation_latest_transactions': donation_latest_transactions,
        'donation_top_transactions': donation_top_transactions,
        'donation_top_donators': donation_top_donators
        })



class blockRewardDecay(APIView):
    @method_decorator(cache_page(60*60*12))
    @silk_profile(name='blockRewardDecay')
    def get(self, request, format=None, *args, **kwargs):

        qs = QrlAggregatedBlockData.objects.values('date','block_reward_block_mean',)      
        df_total = read_frame(qs)

        df_total = df_total[1:] # remove first day because mainnet tokentransfers
        df_total["block_reward_block_mean"] = df_total['block_reward_block_mean'] / 1000000000  # total daily mining rewards convert from shor to quanta
        #df_total.index = df_total["date"] # set index to date
        
        df_total = df_total.iloc[:-1] # removes last row of df_total because last daily average will change during the day otherwhise will cause a wrongly made startpoint for prediction 
        

        df_prediction = pd.DataFrame({'date': pd.date_range(start= '26/06/2018', end='31/12/2020', freq='M', closed='right' )},)
        df_prediction = df_prediction.append(pd.DataFrame({'date': pd.date_range(start= '01/01/2021', end='30/12/2219', freq='Y', closed='right' )},)) # create daily rows between startdate and end (31/12/2219)
        df_prediction = df_prediction.reset_index(drop=True)
    
        list_with_estimates = [6.65409074166821, 6.60484405827424, 6.55596184779167, 6.50900096869821, 6.46082808943325, 6.41454866106017, 6.36707481988630, 6.31995233088309, 6.27768991409571, 6.23122895952696, 6.18659416808585, 6.14080740981777, 6.09682031516958, 6.05169796991948, 6.00690957350429, 5.96388159976742, 5.91974312911151, 5.87733953558782, 5.83384155959909, 5.79066551054748, 5.75056434492522, 5.70800462750842, 5.66711773380759, 5.62517560172251, 5.58354388183045, 5.54354851040589, 5.50252090964573, 5.46310591225154, 5.42267366423249, 5.38383061904377, 5.34398508458442, 4.89643410235329, 4.48636486427483, 4.11063832876382, 3.76547592056039, 3.45012278612224, 3.16118001826137, 2.89643578716996, 2.65322763516335, 2.43102367774111, 2.22742897873293, 2.04088503979910, 1.86951584142566, 1.71294660743953, 1.56948981919352, 1.43804732841860, 1.31729725524523, 1.20697552508621, 1.10589307945224, 1.01327614169558, 0.92819328952749, 0.85045845085312, 0.77923379191385, 0.71397409462077, 0.65402306069746, 0.59924958012370, 0.54906329892325, 0.50308004581706, 0.46083738026358, 0.42224291959026, 0.38688068889319, 0.35447999361059, 0.32471498913466, 0.29752058083596, 0.27260366469949, 0.24977350406749, 0.22880050248619, 0.20963879301247, 0.19208184885162, 0.17599527324154, 0.16121728805140, 0.14771557454023, 0.13534460990805, 0.12400969558002, 0.11359683953587, 0.10408326936156, 0.09536644685939, 0.08737964557004, 0.08004254449699, 0.07333909775398, 0.06719705493084, 0.06156939926540, 0.05639953501989, 0.05167615595052, 0.04734835301179, 0.04338299727782, 0.03974021029004, 0.03641202544897, 0.03336257125014, 0.03056850440743, 0.02800172578266, 0.02565662195474, 0.02350791716331, 0.02153916327457, 0.01973056108874, 0.01807815528007, 0.01656413605575, 0.01517691374053, 0.01390253743281, 0.01273822013306, 0.01167141271459, 0.01069394886659, 0.00979599851222, 0.00897559787736, 0.00822390460305, 0.00753516454771, 0.00690245124786, 0.00632438099001, 0.00579472327591, 0.00530942362540, 0.00486476711513, 0.00445734994120, 0.00408405336332, 0.00374201983117, 0.00342780961047, 0.00314073553864, 0.00287770350299, 0.00263670001796, 0.00241530137981, 0.00221302340040, 0.00202768590771, 0.00185787016061, 0.00170186837026, 0.00155933936827, 0.00142874696301, 0.00130909148184, 0.00119916958351, 0.00109874087414, 0.00100672292318, 0.00092241134184, 0.00084495823246, 0.00077419420882, 0.00070935656928, 0.00064994898780, 0.00059537401917, 0.00054551231057, 0.00049982644758, 0.00045796670919, 0.00041951212390, 0.00038437859337, 0.00035218744495, 0.00032269225842, 0.00029559640903, 0.00027084063949, 0.00024815812965, 0.00022737524705, 0.00020828298410, 0.00019083958697, 0.00017485704898, 0.00016021302532, 0.00014676024519, 0.00013446928800, 0.00012320768061, 0.00011288921647, 0.00010341012570, 0.00009474967800, 0.00008681453021, 0.00007954393951, 0.00007286478763, 0.00006676246759, 0.00006117120798, 0.00005604820824, 0.00005134194780, 0.00004704213431, 0.00004310242395, 0.00003949265860, 0.00003617653588, 0.00003314680359, 0.00003037080698, 0.00002782729604, 0.00002549069142, 0.00002335588307, 0.00002139986182, 0.00001960765450, 0.00001796123740, 0.00001645700987, 0.00001507875922, 0.00001381593506, 0.00001265583752, 0.00001159592952, 0.00001062478728, 0.00000973497679, 0.00000891754948, 0.00000817071768, 0.00000748643195, 0.00000685945416, 0.00000628347896, 0.00000575724673, 0.00000527508569, 0.00000483330493, 0.00000442746159, 0.00000405666812, 0.00000371692806, 0.00000340564074, 0.00000311967563, 0.00000285840733, 0.00000261901987, 0.00000239968076, 0.00000219818418, 0.00000201408945, 0.00000184541238, 0.00000169086178, 0.00000154888336, 0.00000141916663, 0.00000130031349, 0.00000119141413, 0.00000109137337, 0.00000099997244, 0.00000091622620, 0.00000083949359, 0.00000076918722, 0.00000070476891, 0.00000064574554, 0.00000059166529, 0.00000054198429, 0.00000049659389, 0.00000045500488, 0.00000041689888, 0.00000038189268, 0.00000034990972, 0.00000032060529, 0.00000029375507, 0.00000026908902, 0.00000024655320, 0.00000022590473, 0.00000020698554, 0.00000018960536, 0.00000017372619, 0.00000015917687, 0.00000014584604,]
        #number_of_dates = df_prediction['date'].count() # count the number of dates
        df_prediction["block_reward_block_mean"] = pd.Series(list_with_estimates) #addes predicted calculations list tot df based on number of dates

        df_total["date"] = (df_total["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000 # creat datetime      
        df_prediction["date"] = (df_prediction["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000 # creat datetime  

        chart_data_point_list = df_total.to_dict('records')
        chart_data_point_list_prediction = df_prediction.to_dict('records') # error -  dub here
        
        return Response({
        'chart_data_point_list': chart_data_point_list,
        'chart_data_point_list_prediction': chart_data_point_list_prediction, 
        })

class blockRewardPos(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='blockRewardPos') 
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlWalletAddress.objects.values('address_balance').filter(address_balance__gte=10000000000000).order_by('-address_balance') # get al values more then 10k quanta
        df = read_frame(qs)
    
        # calculate the number of dilithium keys for every qrl wallet
        df["number_of_quanta"] = df["address_balance"] / 1000000000  # remove shors, keaping whole quanta
        df["number_of_dilithium_public_key"] = df["number_of_quanta"] / 10000 # divide address balance to 10.000, currently for every whole 10k of quanta you get 1 dilithium public key
        df["number_of_dilithium_public_key"] = df["number_of_dilithium_public_key"].astype(int) # make float > int removing decimals after dot
        df["number_of_dilithium_public_key"] = df["number_of_dilithium_public_key"].where(df["number_of_dilithium_public_key"] <= 100, 100) # current max dilithium public keys per qrl address = 100
        total_dilithium_public_keys = df["number_of_dilithium_public_key"].sum() # total dilithium public keys
        total_wallet_staking = df["number_of_dilithium_public_key"].count() # total staking wallets
        
        # create dilithium bar chart
        df_grouped = df.groupby('number_of_dilithium_public_key').agg({'number_of_dilithium_public_key':['count', 'sum']}) # group wallets based on number of pos keys + get count and sum of those groups
        df_grouped.columns = ["_".join(x) for x in df_grouped.columns.ravel()] # name columns  number_of_dilithium_public_key_count / number_of_dilithium_public_key_sum
    

        return Response({
        'general_pos_data': {
            'total_dilithium_public_keys': total_dilithium_public_keys,
            'total_wallet_staking':total_wallet_staking    
        },
        'pos_distribution':{
            'count_of_pos_keys': df_grouped.index,
            'count_of_wallets':df_grouped["number_of_dilithium_public_key_count"],
            'sum_of_wallets':df_grouped["number_of_dilithium_public_key_sum"],
        
        
        } 
        })

class networkTransactionExchangVolume(APIView):
    @method_decorator(cache_page(60*60))
    @silk_profile(name='blockRewardPos') 
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlWalletAddress.objects.values('wallet_address').filter(wallet_type__exact='exchange')
        df = read_frame(qs)
    
        for wallet in df["wallet_address"]:
            qs_transaction = QrlBlockchainTransactions.objects.values('transaction_amount_send', 'transaction_sending_wallet_address', 'transaction_receiving_wallet_address', 'transaction_type', 'block_found_datetime', 'transaction_hash').filter(transaction_sending_wallet_address=wallet) | QrlBlockchainTransactions.objects.values('transaction_amount_send', 'transaction_sending_wallet_address', 'transaction_receiving_wallet_address', 'transaction_type', 'block_found_datetime', 'transaction_hash').filter(transaction_receiving_wallet_address=wallet)    
        
            ds_transaction = read_frame(qs_transaction)
            ds_sending = ds_transaction[ds_transaction["transaction_receiving_wallet_address"] == wallet]
            ds_receiving = ds_transaction[ds_transaction["transaction_sending_wallet_address"] == wallet]
        
        print(df)


        df_total["date"] = (df_total["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000 # creat datetime      
        df_prediction["date"] = (df_prediction["date"] - dt.datetime(1970,1,1)).dt.total_seconds() *1000 # creat datetime  

        chart_data_point_list = df_total.to_dict('records')
        chart_data_point_list_prediction = df_prediction.to_dict('records') # error -  dub here
        
        return Response({
        'chart_data_point_list': chart_data_point_list,
        'chart_data_point_list_prediction': chart_data_point_list_prediction, 
        })

        