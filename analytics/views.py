
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


from .models import *
from .serializers import *
import numpy as np 
from django.db.models import Max

class BlockStatisticsView(APIView):
    @method_decorator(cache_page(5 * 60))
    def get(self, request, format=None):
        # Existing logic for blocks
        highest_block_number = QrlBlockchainBlocks.objects.aggregate(
            max_block_number=Max('block_number')
        )['max_block_number']

        total_rows = QrlBlockchainBlocks.objects.count()
        adjusted_rows = total_rows - 1 if total_rows > 0 else 0  # block 0 vs row 1
        compliance_percentage = (
            round((adjusted_rows / highest_block_number * 100), 2)
            if highest_block_number > 0 else 0
        )
        missing_blocks = highest_block_number - adjusted_rows if highest_block_number and adjusted_rows >= 0 else 0

        # 1. Sum of all transactions recorded in blocks
        total_transactions_in_blocks = QrlBlockchainBlocks.objects.aggregate(
            total_tx_in_blocks=Sum('block_number_of_transactions')
        )['total_tx_in_blocks'] or 0

        # 2. Actual transactions in the transactions table (counting only unique transaction_hash)
        total_transactions_in_database = QrlBlockchainTransactions.objects.values(
            'transaction_hash'
        ).distinct().count()


        # 3. Missing transactions (if any)
        missing_transactions = total_transactions_in_blocks - total_transactions_in_database

        # 4. Compliance for transactions
        if total_transactions_in_blocks > 0:
            compliance_percentage_transactions = round(
                (total_transactions_in_database / total_transactions_in_blocks) * 100, 2
            )
        else:
            compliance_percentage_transactions = 0

        return Response({
            # Existing fields
            'highest_block_number': highest_block_number,
            'total_rows': adjusted_rows,
            'compliance_percentage': compliance_percentage,
            'missing_blocks': missing_blocks,
            # New transaction stats
            'total_transactions_in_blocks': total_transactions_in_blocks,
            'total_transactions_in_database': total_transactions_in_database,
            'missing_transactions': missing_transactions,
            'compliance_percentage_transactions': compliance_percentage_transactions,
        })





class walletRichList(APIView):
    @method_decorator(cache_page(5*60))
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
        totalnumber_wallets_with_balance_40000 =df_total[df_total['address_balance'] > 39999999999999].count()["address_balance"] # 40000 quanta
        totalnumber_wallets_with_balance_100000 =df_total[df_total['address_balance'] > 99999999999999].count()["address_balance"] # 100000 quanta

        totalnumber_wallets_with_balance_x = [
        {'id':1 ,'count':totalnumber_wallets_with_balance_1, 'amount':'1'},
        {'id':2,'count':totalnumber_wallets_with_balance_5, 'amount':'5'},
        {'id':3, 'count':totalnumber_wallets_with_balance_10, 'amount':'10'},
        {'id':4, 'count':totalnumber_wallets_with_balance_100, 'amount':'100'},
        {'id':5, 'count':totalnumber_wallets_with_balance_1000, 'amount':'1.000'},
        {'id':6, 'count':totalnumber_wallets_with_balance_10000, 'amount':'10.000'},
        {'id':7, 'count':totalnumber_wallets_with_balance_40000, 'amount':'40.000'},
        {'id':8, 'count':totalnumber_wallets_with_balance_100000, 'amount':'100.000'}
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
            df_grouped = df_grouped.reindex(idx)
            df_grouped.fillna(0, inplace=True)
            
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
    @method_decorator(cache_page(60 * 60 * 12))
    def get(self, request, format=None, *args, **kwargs):
        import datetime as dt
        import pandas as pd
        from django_pandas.io import read_frame

        # 1. Fetch DB rows for your data
        qs = QrlAggregatedBlockData.objects.values('date', 'block_reward_block_mean')
        df_total = read_frame(qs)

        # 2. Adjust your existing DataFrame
        # Remove first day
        df_total = df_total[1:]
        # Convert from 'shor' to 'quanta'
        df_total["block_reward_block_mean"] = df_total['block_reward_block_mean'] / 1_000_000_000
        # Remove last row (so partial day doesn't skew averages)
        df_total = df_total.iloc[:-1]

        # ----------------------------------------------------------------------
        # 3. Dynamically compute monthly decay predictions
        # ----------------------------------------------------------------------
        # For example: 120 months from 2018-06-26, 1% monthly decay
        months_count = 2400  # 10 years
        initial_reward = 6.654  # start value
        decay_rate = 0.982      # 1% monthly decay

        # Generate a monthly date range
        df_prediction = pd.DataFrame({
            'date': pd.date_range(
                start='2018-06-26',
                periods=months_count,
                freq='M'  # monthly intervals
            )
        })

        # Compute the decayed reward for each month: reward_i = initial_reward * decay_rate^i
        list_with_estimates = [
            initial_reward * (decay_rate ** i) 
            for i in range(months_count)
        ]
        df_prediction["block_reward_block_mean"] = list_with_estimates

        # ----------------------------------------------------------------------
        # 4. Convert `date` columns to milliseconds since epoch
        # ----------------------------------------------------------------------
        df_total["date"] = (
            df_total["date"] - dt.datetime(1970, 1, 1)
        ).dt.total_seconds() * 1000

        df_prediction["date"] = (
            df_prediction["date"] - dt.datetime(1970, 1, 1)
        ).dt.total_seconds() * 1000

        # ----------------------------------------------------------------------
        # 5. Convert to dictionaries for serialization
        # ----------------------------------------------------------------------
        chart_data_point_list = df_total.to_dict('records')
        chart_data_point_list_prediction = df_prediction.to_dict('records')

        # ----------------------------------------------------------------------
        # 6. Return both in the response
        # ----------------------------------------------------------------------
        return Response({
            'chart_data_point_list': chart_data_point_list,
            'chart_data_point_list_prediction': chart_data_point_list_prediction,
        })


class blockRewardPos(APIView):
    @method_decorator(cache_page(60*60))
    def get(self, request, format=None, *args, **kwargs):
        qs = QrlWalletAddress.objects.values('address_balance').filter(address_balance__gte=40000000000000).order_by('-address_balance') # get al values more then 40k quanta
        df = read_frame(qs)
    
        # calculate the number of dilithium keys for every qrl wallet
        df["number_of_quanta"] = df["address_balance"] / 1000000000  # remove shors, keaping whole quanta
        df["number_of_dilithium_public_key"] = df["number_of_quanta"] / 40000 # divide address balance to 40.000, currently for every whole 40k of quanta you get 1 dilithium public key
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

        
