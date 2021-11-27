"""qrlanalytics URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path
from .views import *

urlpatterns = [
    path('wallet-rich-list' , walletRichList.as_view(), name='walletRichList'),  #richlist
    path('wallet-distribution' , walletDistribution.as_view(), name='walletDistribution'), # wallet distribution
    path('wallet-number-of-wallets' , walletNumberOfWallets.as_view(), name='walletNumberOfWallets'),
    
    path('block-block-size' , blockBlockSize.as_view(), name='blockBlockSize'),
    path('block-block-time' , blockBlockTime.as_view(), name='blockBlockTime'),
    path('block-reward-pos' , blockRewardPos.as_view(), name='blockRewardPos'),
    path('block-reward-decay' , blockRewardDecay.as_view(), name='blockRewardDecay'), 
    
    path('network-transactions' , networkTransactions.as_view(), name='networkTransactions'), 
    path('network-total-circulating-quanta' , networkTotalCirculatingQuanta.as_view(), name='networkTotalCirculatingQuanta'), 
    path('network-transaction-fee' , networkTransactionFee.as_view(), name='networkTransactionFee'), 
    path('network-unique-wallets-used' , networkUniqueWalletsUsed.as_view(), name='networkUniqueWalletsUsed'), 
    path('network-transaction-exchange-volume' , networkTransactionExchangVolume.as_view(), name='networkTransactionExchangVolume'),
    
    
    path('wallet-data' , walletData.as_view(), name='walletData'),
    path('wallet-data-2' , walletData2.as_view(), name='walletData2'),     
    path('donation-data' , donationData.as_view(), name='donationData'),

    
]


