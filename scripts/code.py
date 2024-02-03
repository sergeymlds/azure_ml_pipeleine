import os
import argparse
import pandas as pd
import time
import numpy as np
import io
from datetime import datetime, timedelta

from azure.storage.blob import BlockBlobService, PublicAccess
container_name = 'spardata'
key = os.environ['account_key']
blobstorageaccount = 'sparmlstorage'
block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)

counter = 0
indx = 0
def days_counter(x):
    global counter, indx
    if (indx == x):
        counter += 1
        return counter
    else:
        indx = x
        counter = 1
        return counter
    
    
parser = argparse.ArgumentParser(description="blob storage transfer")
parser.add_argument('--stores', dest="stores", required=True, nargs='+')
args = parser.parse_args()
stores = args.stores

stores = stores[0].split(',')

('akciya.csv')

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                        blob_name = 'akciya.csv',
                                        stream = input_blob)
    input_blob.seek(0)
    old = pd.read_csv(input_blob, header=None, sep=';')
    old.columns = ['PromoTypeCode', 'DateBegin', 'DateEnd', 'Item', 'store_id', 'SalePriceBeforePromo', 'SalePriceTimePromo']
    
old = old.drop_duplicates(['store_id', 'Item', 'DateBegin', 'DateEnd'], keep='last')
old = old.sort_values('DateBegin')

dics_map = {'1+1': 12,
            '2+1': 13,
            '3+1': 14,
            '4+1': 22,
            'лучшая цена': 20,
            'по карте': 1,
            'при покупке 2': 4,
            'при покупке 3': 17,
            'при покупке n шт': 16,
            'при покупке от 2-х шт': 18,
            'при покупке от n штук': 19,
            'скидка % на 2-ю шт': 15,
            'скидка 50% на 2-ую бутылк': 7,
            'удар': 2,
            'Хаммер': 5}

old['PromoTypeCode'] = old['PromoTypeCode'].map(dics_map)
old = old[['store_id', 'Item', 'PromoTypeCode', 'DateBegin', 'DateEnd',
       'SalePriceBeforePromo', 'SalePriceTimePromo']]

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='spardata',
                                        blob_name = 'pwc/discounts.csv',
                                        stream = input_blob)
    input_blob.seek(0)
    discounts = pd.read_csv(input_blob, header=None)
    discounts.columns = ['store_id', 'Item', 'PromoTypeCode', 'DateBegin', 'DateEnd', 'SalePriceBeforePromo', 'SalePriceTimePromo',
       'doc_id', 'uploading_date']
    
discounts = discounts[discounts['DateBegin']!='1969-12-31 23:59:59.999999999']
discounts = discounts.drop(['uploading_date'], axis=1)
old['doc_id'] = 0

discounts = pd.concat([discounts, old])

discounts['DateBegin'] = pd.to_datetime(discounts['DateBegin'])
discounts['DateEnd'] = pd.to_datetime(discounts['DateEnd'])

discounts = discounts.drop_duplicates(['store_id', 'Item', 'DateBegin', 'DateEnd'], keep='last')
discounts = discounts.sort_values(['DateBegin', 'Item'])
    
for store_id in stores:
    store_id = int(store_id)
    discount_for_store = discounts[discounts['store_id']==store_id]
    
    discount_for_store['DateBegin'] = pd.to_datetime(discount_for_store['DateBegin'])
    discount_for_store['DateEnd'] = pd.to_datetime(discount_for_store['DateEnd'])
    
    discount_for_store['doc_id'] = discount_for_store['doc_id'].astype(str)
    
    counter1 = 0
    def f(row):
        global counter1
        df = pd.DataFrame()
        df['date'] = pd.date_range(start = row[1]['DateBegin'], end =row[1]['DateEnd'])
        df['Item'] = row[1]['Item']
        df['SalePriceBeforePromo'] = row[1]['SalePriceBeforePromo']
        df['SalePriceTimePromo'] = row[1]['SalePriceTimePromo']
        df['PromoTypeCode'] = row[1]['PromoTypeCode']
        df.index = counter1* np.ones(len(df), dtype=np.int)
        df['doc_id'] = row[1]['doc_id'] 
        counter1 +=1
        return df
    
    result = pd.concat([f(row) for row in discount_for_store.iterrows()])
    result['number_disc_day'] = result.index.map(lambda x: days_counter(x))
    
    result = result.drop_duplicates(subset = ['date', 'Item'], keep = 'last')
    
    output = result.to_csv(index = False)
    block_blob_service.create_blob_from_text('planning', 
                            r'discount_history_new/{}/discount.csv'.format(store_id), output)