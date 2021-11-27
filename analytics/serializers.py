
from rest_framework import serializers

from .models import *
from django.conf import settings


class QrlWalletAddressSerializer(serializers.ModelSerializer):

    class Meta:
        model = QrlWalletAddress
        fields = (
            'wallet_address',
            'address_first_found',
        )
        #read_only_fields = ('__all__',)