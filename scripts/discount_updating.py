import os
import argparse
import pandas as pd
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
    

# '1+1':1, '000000012'
# '2+1':2, '000000013'
# '3+1':3, '000000014'
# '4+1':4, '000000022'
# 'Хаммер':10, 
# 'лучшая цена':5, '000000020'
# 'по карте':6, '000000001'
# 'при покупке 2':7, '000000004'
# 'при покупке 3':8, '000000018'
# 'при покупке n шт':8, '000000018'
# 'при покупке от 2-х шт':8, '000000018'
# 'при покупке от n штук':8, '000000018'
# 'скидка % на 2-ю шт':9, '000000015'
# 'скидка 50% на 2-ую бутылк':9, '000000015'
# 'удар':5, '000000020'
# 'купон' :11, 

dics_map = {12: 1, 
            13: 2, 
            14: 3, 
            11:6, 
            9:5, 
            8:5,
            22: 4, 
            20:5, 
            1:6,
            4:7, 
            17:8, 
            16: 8,
            18:8, 
            19:8, 
            15:9,
            7:9, 
            2:5, 
            21:10, 
            23:23,
            27:27,
            28:28}


parser = argparse.ArgumentParser(description="blob storage transfer")
# parser.add_argument('--discount_history', dest="discount_history", required=True)
parser.add_argument('--stores', dest="stores", required=True, nargs='+')
args = parser.parse_args()

stores = args.stores
print(stores)
print(type(stores))
stores = stores[0].split(',')

#discount_history_dir = args.discount_history

# загружаем данные из временной папки
new_disc = list(block_blob_service.list_blob_names(container_name=container_name, prefix='pwc/promo'))

# если есть новые скидки, то обновляем данные
if len(new_disc) != 0:
    # загружаем временные файлы скидок
    frames = []
    for blob in new_disc:
        with io.BytesIO() as input_blob:
            block_blob_service.get_blob_to_stream(container_name=container_name,
                                                  blob_name=blob,
                                                  stream=input_blob)
            input_blob.seek(0)
            frames += [pd.read_csv(input_blob)]
    discount_temp = pd.concat(frames)

    # обновляем для каждого магазина
    for store_id in stores:

        ### updating actual file
        ########################
        try:
            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name=container_name,
                                                    blob_name = 'actual/{}/actual.csv'.format(store_id),
                                                    stream = input_blob)
                input_blob.seek(0)
                actual_df = pd.read_csv(input_blob)

            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name=container_name,
                                                    blob_name = 'actual/{}/actual1.csv'.format(store_id),
                                                    stream = input_blob)
                input_blob.seek(0)
                actual1_df = pd.read_csv(input_blob)

            actual_df['date'] = (datetime.now() - timedelta(1)).date()
            actual1_df['date'] = pd.to_datetime(actual1_df['date'])
            actual_df['date'] = pd.to_datetime(actual_df['date'])

            df = pd.concat([actual1_df, actual_df])
            df = df.drop_duplicates(subset='Item', keep='first')
            df = df.merge(actual_df[['Item']], on='Item')
            output = df.to_csv(index = False)
            block_blob_service.create_blob_from_text(container_name,
                            r'actual/{}/actual1.csv'.format(store_id), output)

        except Exception as inst:
            print(inst)

        #################
        #end

        try:

            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name=container_name,
                                                    blob_name = 'disc_history1/{}/discount.csv'.format(store_id),
                                                    stream = input_blob)
                input_blob.seek(0)
                discount_history = pd.read_csv(input_blob)


            if (store_id == '956') or (store_id == '1521'):
                store_id1 = '1463'
            else:
                store_id1 = store_id

            # копируем полные данные из temp
            discount_new = discount_temp.copy()

            discount_history['doc_id'] = discount_history['doc_id'].astype(str)

            # удаляем все акции, которые есть в обновлениях
            discount_history = discount_history[~discount_history['doc_id'].isin(set(discount_new['doc_id']))]

            # удаляем акции, которые передаются с -1
            discount_history = discount_history[~discount_history['doc_id'].isin(set(discount_new[discount_new['Item']==-1]['doc_id']))]

            discount_history['date'] = pd.to_datetime(discount_history['date'])
            discount_new['DateBegin'] = pd.to_datetime(discount_new['DateBegin'])
            discount_new['DateEnd'] = pd.to_datetime(discount_new['DateEnd'])

            discount_new = discount_new[discount_new['store_id'] == int(store_id1)]

            # снова проверяем есть ли новые скидки уже по данному магазину
            if discount_new.shape[0] != 0:
                discount_new['PromoTypeCode'] = discount_new['PromoTypeCode'].astype(int)
                discount_new['PromoTypeCode'] = discount_new['PromoTypeCode'].map(dics_map)

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

                result = pd.concat([f(row) for row in discount_new.iterrows()])
                result['number_disc_day'] = result.index.map(lambda x: days_counter(x))


                discount_history['Item'] = discount_history['Item'].astype(int)
                discount_history = pd.concat([discount_history, result])
                discount_history = discount_history.drop_duplicates(subset = ['date', 'Item'], keep = 'last')

                output = discount_history.to_csv(index = False)
                block_blob_service.create_blob_from_text(container_name,
                                        r'disc_history1/{}/discount.csv'.format(store_id), output)

            counter = 0

        except Exception as inst:
            print(inst)