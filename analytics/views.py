
from django.db.models import Count, Sum, F, Max
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

import requests


from .models import *
from .serializers import *
import numpy as np 
from django.db.models import Max

class BlockStatisticsView(APIView):
    @method_decorator(cache_page(5 * 60))
    def get(self, request, format=None):
        # Block statistics
        highest_block_number = QrlBlockchainBlocks.objects.aggregate(
            max_block_number=Max('block_number')
        )['max_block_number'] or 0

        total_rows = QrlBlockchainBlocks.objects.count()
        adjusted_rows = total_rows - 1 if total_rows > 0 else 0  # block 0 vs row 1
        compliance_percentage = (
            round((adjusted_rows / highest_block_number * 100), 2)
            if highest_block_number > 0 else 0
        )
        missing_blocks = highest_block_number - adjusted_rows if highest_block_number and adjusted_rows >= 0 else 0

        # Transaction statistics
        total_transactions_in_blocks = QrlBlockchainBlocks.objects.aggregate(
            total_tx_in_blocks=Sum('block_number_of_transactions')
        )['total_tx_in_blocks'] or 0

        total_transactions_in_database = QrlBlockchainTransactions.objects.values(
            'transaction_hash'
        ).distinct().count()

        missing_transactions = total_transactions_in_blocks - total_transactions_in_database

        if total_transactions_in_blocks > 0:
            compliance_percentage_transactions = round(
                (total_transactions_in_database / total_transactions_in_blocks) * 100, 2
            )
        else:
            compliance_percentage_transactions = 0

        # Wallet statistics: Sum of all wallet balances
        total_quanta_in_wallets = QrlWalletAddress.objects.aggregate(
            total_quanta=Sum('address_balance')
        )['total_quanta'] or 0

        # Fetch the emission from external API
        try:
            emission_response = requests.get("https://explorer.theqrl.org/api/emission", timeout=5)
            if emission_response.ok:
                emission_data = emission_response.json()
                emission = emission_data.get("emission", 0)
                emission_clean = int(float(emission) * 1e9)
                missing_quanta = emission_clean - total_quanta_in_wallets
            else:
                emission = 0
                missing_quanta = 1000000000
        except Exception as e:
            print("Error fetching emission:", e)
            emission = 0
            missing_quanta = 1000000000



        return Response({
            # Block stats
            'highest_block_number': highest_block_number,
            'total_rows': adjusted_rows,
            'compliance_percentage': compliance_percentage,
            'missing_blocks': missing_blocks,
            # Transaction stats
            'total_transactions_in_blocks': total_transactions_in_blocks,
            'total_transactions_in_database': total_transactions_in_database,
            'missing_transactions': missing_transactions,
            'compliance_percentage_transactions': compliance_percentage_transactions,
            # Wallet stats
            'total_quanta_in_wallets': total_quanta_in_wallets,
            'emission': emission_clean,
            'missing_quanta': missing_quanta,
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
        qs = QrlWalletAddress.objects.values('wallet_address','address_balance', 'wallet_type', 'wallet_custom_name').annotate(rank=Window(expression=Rank(),order_by=F('address_balance').desc()))                
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


        try:
            emission_response = requests.get("https://explorer.theqrl.org/api/emission", timeout=5)
            if emission_response.ok:
                emission_data = emission_response.json()
                emission = float(emission_data.get("emission", 0))
                emission_clean = int(emission * 1e9)  # Convert to atomic units
            else:
                emission_clean = 0
        except:
            emission_clean = 0

        # Ensure all wallets have a category, default to 'Private'
        df_total['wallet_type'] = df_total['wallet_type'].fillna('Private')
        df_total.loc[df_total['wallet_type'] == '', 'wallet_type'] = 'Private'

        # **Wallet Categories Statistics**
        wallet_category_types = ['Exchange', 'Private', 'Others']
        wallet_categories_stats = []

        # Extract only exchange wallets
        exchange_wallets = df_total[df_total['wallet_type'] == 'Exchange']

        # Format exchange wallet data
        exchange_addresses = exchange_wallets[['wallet_address', 'wallet_custom_name', 'address_balance']].to_dict(orient='records')


        # Compute known total balance
        total_known_balance = df_total['address_balance'].sum()
        unknown_balance = max(0, emission_clean - total_known_balance)  # Prevent negative values

        for category in wallet_category_types:
            df_category = df_total[df_total['wallet_type'] == category]
            total_value = df_category['address_balance'].sum()
            percentage_of_supply = (total_value / emission_clean) * 100 if emission_clean > 0 else 0
            wallet_categories_stats.append({
                "name": category,
                "count": len(df_category),
                "total_value": total_value,
                "percentage": round(percentage_of_supply, 2)
            })

        # Add 'Unknown' category
        wallet_categories_stats.append({
            "name": "Unknown",
            "count": "N/A",  # No specific count since it's inferred
            "total_value": unknown_balance,
            "percentage": round((unknown_balance / emission_clean) * 100, 2) if emission_clean > 0 else 0
        })

        return Response({
            'distribution_percentage': {
                'percentage_owned': df_perct['percentage_owned'],
                'volume_owned': df_perct['volume_owned'],
            },
            'distribution_percentile': df_grouped,
            'distribution_wallets_holding_x': totalnumber_wallets_with_balance_x,
            'wallet_categories_stats': wallet_categories_stats,
            'exchange_addresses': exchange_addresses, 
            'emission': emission_clean,
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
    @method_decorator(cache_page(60 * 60))  # Cache for 1 hour
    def get(self, request, format=None, *args, **kwargs):
        # Fetch only required fields directly from the database
        qs = QrlAggregatedBlockData.objects.values(
            'date', 'block_size_mean', 'block_size_min', 'block_size_max'
        ).order_by('date')

        # Convert QuerySet to list with epoch time conversion
        chart_data_point_list = [
            {
                "date": int((row["date"] - dt.datetime(1970, 1, 1)).total_seconds() * 1000),
                "block_size_mean": row["block_size_mean"],
                "block_size_min": row["block_size_min"],
                "block_size_max": row["block_size_max"],
            }
            for row in qs
        ]

        return Response({"chart_data_point_list": chart_data_point_list})



class blockBlockTime(APIView):
    @method_decorator(cache_page(60 * 60))  # Cache for 1 hour
    def get(self, request, format=None, *args, **kwargs):
        # Fetch only required fields directly from the database
        qs = QrlAggregatedBlockData.objects.values(
            'date', 'block_number_count', 'block_timestamp_seconds_mean', 
            'block_timestamp_seconds_min', 'block_timestamp_seconds_max'
        ).order_by('date')

        # Convert QuerySet to list with epoch time conversion
        chart_data_point_list = [
            {
                "date": int((row["date"] - dt.datetime(1970, 1, 1)).total_seconds() * 1000),
                "block_timestamp_seconds_mean": row["block_timestamp_seconds_mean"],
                "block_timestamp_seconds_min": row["block_timestamp_seconds_min"],
                "block_timestamp_seconds_max": row["block_timestamp_seconds_max"],
                "block_number_count": row["block_number_count"],
            }
            for row in qs
        ]

        return Response({"chart_data_point_list": chart_data_point_list})



class networkTransactions(APIView):
    @method_decorator(cache_page(60 * 60))  # Cache for 1 hour
    def get(self, request, format=None, *args, **kwargs):

        # Fetch data directly from the database with required fields
        qs = QrlAggregatedTransactionData.objects.values(
            'date', 'total_number_of_transactions', 'total_amount_transfered', 'transaction_type'
        ).order_by('date')

        # Convert QuerySet to list for easier processing
        data_list = list(qs)

        # Remove first day's data (mainnet token transfers)
        if data_list:
            data_list = data_list[1:]

        # Convert datetime to epoch (milliseconds) and structure data per transaction type
        transactions = {
            "coinbase": [],
            "transfer": [],
            "slave": []
        }

        for row in data_list:
            epoch_time = int((row["date"] - dt.datetime(1970, 1, 1)).total_seconds() * 1000)

            # Create data point
            data_point = {
                "date": epoch_time,
                "total_number_of_transactions": row["total_number_of_transactions"],
                "total_amount_transfered": row["total_amount_transfered"],
            }

            # Categorize based on transaction type
            tx_type = row["transaction_type"]
            if tx_type in transactions:
                transactions[tx_type].append(data_point)

        # Return structured response
        return Response({
            'chart_data_point_list_coinbase': transactions["coinbase"],
            'chart_data_point_list_transfer': transactions["transfer"],
            'chart_data_point_list_slave': transactions["slave"],
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
    @method_decorator(cache_page(60 * 60))  # Cache for 1 hour
    def get(self, request, format=None, *args, **kwargs):
        # Fetch only required fields
        qs = QrlAggregatedTransactionData.objects.filter(transaction_type='transfer').values(
            'date', 'transaction_fee_mean', 'transaction_fee_min', 'transaction_fee_max'
        ).order_by('date')

        # Process data
        chart_data_point_list = [
            {
                "date": int((row["date"] - dt.datetime(1970, 1, 1)).total_seconds() * 1000),  # Manual epoch conversion
                "transaction_fee_mean": (row["transaction_fee_mean"] ) if row["transaction_fee_mean"] else 0,
                "transaction_fee_min": (row["transaction_fee_min"] ) if row["transaction_fee_min"] else 0,
                "transaction_fee_max": (row["transaction_fee_max"]) if row["transaction_fee_max"] else 0,
            }
            for row in qs
        ]

        return Response({"chart_data_point_list": chart_data_point_list})
    
    
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
    @method_decorator(cache_page(5 * 60))  # Cache for 5 minutes
    def get(self, request, format=None, *args, **kwargs):
        wallet = request.query_params.get('wallet')
        if not wallet:
            raise Http404("Wallet parameter is missing.")

        # Fetch all wallets with ranks
        ranked_wallets = QrlWalletAddress.objects.annotate(
            rank=Window(expression=Rank(), order_by=F('address_balance').desc())
        ).values(
            'wallet_address', 'address_balance', 'wallet_custom_name', 'rank', 'address_first_found', 'wallet_type'
        )

        # Filter for the specific wallet after ranking
        wallet_data = [w for w in ranked_wallets if w['wallet_address'] == wallet]

        if not wallet_data:
            raise Http404("Wallet not found.")

        return Response({"general_wallet_data": wallet_data[0]})

class walletData2(APIView):
    @method_decorator(cache_page(60 * 60 * 4))  # Cache for 4 hours
    def get(self, request, format=None, *args, **kwargs):
        wallet = request.query_params.get('wallet')
        if not wallet:
            raise Http404("Wallet parameter is missing.")

        # Fetch transactions involving the given wallet
        qs_transaction = QrlBlockchainTransactions.objects.filter(
            transaction_sending_wallet_address=wallet
        ).union(
            QrlBlockchainTransactions.objects.filter(transaction_receiving_wallet_address=wallet)
        ).values(
            'transaction_amount_send', 'transaction_sending_wallet_address',
            'transaction_receiving_wallet_address', 'transaction_type',
            'block_found_datetime', 'transaction_hash'
        )

        transactions = list(qs_transaction)
        if not transactions:
            raise Http404("No transactions found.")

        # Create associated addresses
        top_transactions_sending = sorted(
            (tx for tx in transactions if tx["transaction_sending_wallet_address"] == wallet),
            key=lambda x: x["transaction_amount_send"],
            reverse=True
        )[:100]

        top_transactions_receiving = sorted(
            (tx for tx in transactions if tx["transaction_receiving_wallet_address"] == wallet),
            key=lambda x: x["transaction_amount_send"],
            reverse=True
        )[:100]

        list_top_transactions_sending = [
            {
                "transaction_amount_send": tx["transaction_amount_send"],
                "transaction_receiving_wallet_address": tx["transaction_receiving_wallet_address"],
                "transaction_hash": tx["transaction_hash"],
                "transaction_type": tx["transaction_type"],
                "block_found_datetime": tx["block_found_datetime"],
            }
            for tx in top_transactions_sending
        ]

        list_top_transactions_receiving = [
            {
                "transaction_amount_send": tx["transaction_amount_send"],
                "transaction_sending_wallet_address": tx["transaction_sending_wallet_address"],
                "transaction_hash": tx["transaction_hash"],
                "transaction_type": tx["transaction_type"],
                "block_found_datetime": tx["block_found_datetime"],
            }
            for tx in top_transactions_receiving
        ]

        # Aggregate most sending/receiving wallets
        df_transaction = pd.DataFrame(transactions)
        df_transaction["number_of_transactions"] = ''

        most_sending_wallets = df_transaction.groupby("transaction_sending_wallet_address").agg(
            number_of_transactions=('transaction_amount_send', 'size'),
            transaction_amount_send=('transaction_amount_send', 'sum')
        ).reset_index().nlargest(100, 'number_of_transactions').to_dict(orient="records")

        most_receiving_wallets = df_transaction.groupby("transaction_receiving_wallet_address").agg(
            number_of_transactions=('transaction_amount_send', 'size'),
            transaction_amount_send=('transaction_amount_send', 'sum')
        ).reset_index().nlargest(100, 'number_of_transactions').to_dict(orient="records")

        # Remove the wallet itself from receiving list
        list_most_receiving_wallets = [
            wallet_data for wallet_data in most_receiving_wallets if wallet_data["transaction_receiving_wallet_address"] != wallet
        ]

        # Total transaction counts
        total_transaction_send_number = sum(
            1 for tx in transactions if tx["transaction_sending_wallet_address"] == wallet
        )
        total_transaction_receive_number = sum(
            1 for tx in transactions if tx["transaction_receiving_wallet_address"] == wallet
        )

        # Create Chart
        df_transaction.loc[df_transaction.transaction_sending_wallet_address == wallet, 'transaction_amount_send'] *= -1
        df_transaction['date'] = pd.to_datetime(df_transaction['block_found_datetime'], unit='s').dt.floor('d')

        df_grouped = df_transaction.groupby(['date']).agg(transaction_amount_send_sum=('transaction_amount_send', 'sum'))
        df_grouped["date"] = df_grouped.index

        startDateRange = df_grouped.index.min() - timedelta(5)
        idx = pd.date_range(startDateRange, dt.datetime.now())

        df_grouped = df_grouped.reindex(idx, fill_value=0)
        df_grouped["wallet_value_total_daily"] = df_grouped['transaction_amount_send_sum'].cumsum()
        df_grouped["epoch"] = (df_grouped.index - dt.datetime(1970, 1, 1)).total_seconds() * 1000

        chart_data_point_list = [
            {
                "date": epoch,
                "wallet_value_total_daily": wallet_value_total_daily,
                "transaction_amount_send_sum": transaction_amount_send_sum
            }
            for epoch, wallet_value_total_daily, transaction_amount_send_sum in zip(
                df_grouped["epoch"], df_grouped["wallet_value_total_daily"], df_grouped["transaction_amount_send_sum"]
            )
        ]

        return Response({
            'general_wallet_transaction_data': {
                'total_transaction_send_number': total_transaction_send_number,
                'total_transaction_receive_number': total_transaction_receive_number,
                'list_most_sending_wallets': most_sending_wallets,
                'list_most_receiving_wallets': list_most_receiving_wallets,
                'list_top_transactions_receiving': list_top_transactions_receiving,
                'list_top_transactions_sending': list_top_transactions_sending,
            },
            'chart_data_point_list': chart_data_point_list,
        })


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
                freq='ME'  # monthly intervals
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

        
