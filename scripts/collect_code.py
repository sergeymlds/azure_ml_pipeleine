import pandas as pd
from azure.storage.blob import AppendBlobService, BlockBlobService
from datetime import datetime, timedelta, date
import io
import numpy as np
container_name = 'spardata'
key = os.environ['account_key']
blobstorageaccount = 'sparmlstorage'
block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)

from sklearn.linear_model import Ridge
from sklearn.linear_model import RidgeCV
from sklearn.model_selection import RepeatedKFold
from numpy import arange

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                        blob_name = r'planning_stores.csv',
                                        stream = input_blob)
    input_blob.seek(0)
    check_ids = pd.read_csv(input_blob)
check_ids = check_ids['ObjCode'].values


format_map = {'MaxiEuro':'MaxiEuro',
 'MaxiEuro Москва':'MaxiEuro',
 'Premium1':'Premium1',
 'Premium1 Москва':'MaxiEuro''Premium1',
 'Premium2':'Premium2',
 'Premium2 Москва':'Premium2',
 'Гурме':'Гурме',
 'Москва Лубянка':'Москва Лубянка',
 'Формат-1':'Формат-1',
 'Формат-2':'Формат-2',
 'Формат-2 Мини':'Формат-2',
 'Формат-3':'Формат-3',
 'Формат-4':'Формат-4',
 'Формат-4 Москва':'Формат-4',
 'Формат-5':'Формат-5',
 'Формат-5 Москва':'Формат-5',
 'Формат-6':'Формат-6',
 'Формат-6 Москва':'Формат-6',
 'Формат-7':'Формат-7'}              


#читаем все средние продажи на акцию
frames = []
for store_id in check_ids:
    with io.BytesIO() as input_blob:
        block_blob_service.get_blob_to_stream(container_name='planning',
                                                  blob_name = r'average_sales/{}/sales.csv'.format(store_id),
                                                           stream = input_blob)
        input_blob.seek(0)
        df = pd.read_csv(input_blob)
    frames.append(df)
aver_disc = pd.concat(frames)

#выбираем последнюю акцию
last_discount = aver_disc.sort_values(['ObjCode', 'Item', 'DateBegin'])
last_discount['DateBegin'] = pd.to_datetime(last_discount['DateBegin'])
last_discount['begin_month'] = last_discount['DateBegin'].dt.month
last_discount['begin_day'] = last_discount['DateBegin'].dt.day
last_discount['season'] = 2*last_discount['begin_month'] - 1 + last_discount['begin_day']//16
last_discount = last_discount[(last_discount['season']!=24)&(last_discount['season']!=8)&(last_discount['season']!=9)]
last_discount = last_discount[['ObjCode', 'Item', 'DateBegin', 'DateEnd', 'Qnty_mean', 'discount', 'PromoTypeCode', 'SalePriceBeforePromo', 'SalePriceTimePromo']]
last_discount = last_discount[(last_discount['discount']>=0.05)&(last_discount['discount']<=0.6)]
last_discount = last_discount.drop_duplicates(['ObjCode', 'Item', 'PromoTypeCode'], keep = 'last')

output1 = last_discount.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'last_discount.csv', output1)

all_aver = aver_disc.groupby(['Item', 'DateBegin'], as_index=False)['Qnty_mean'].sum().merge(aver_disc.groupby(['Item', 'DateBegin'], as_index=False)['ObjCode'].count()).rename(columns={'ObjCode':'Количество магазинов', 'fact_promo':'Фактические продажи', 'Qnty_mean':'qnty'})
aver_disc = aver_disc.merge(all_aver[(all_aver['qnty']==0)], on = ['Item', 'DateBegin'], how='left')
aver_disc = aver_disc[aver_disc['qnty']!=0]
aver_disc = aver_disc[['ObjCode', 'Item', 'DateBegin', 'DateEnd', 'Qnty_mean', 'PromoTypeCode']]

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                              blob_name = r'items.csv',
                                                       stream = input_blob)
    input_blob.seek(0)
    item_class = pd.read_csv(input_blob)
    
aver_disc['DateBegin'] = pd.to_datetime(aver_disc['DateBegin'])
aver_disc['DateEnd'] = pd.to_datetime(aver_disc['DateEnd'])

aver_disc = aver_disc.merge(item_class)

aver_disc['begin_month'] = aver_disc['DateBegin'].dt.month
aver_disc['begin_day'] = aver_disc['DateBegin'].dt.day

aver_disc['end_month'] = aver_disc['DateEnd'].dt.month
aver_disc['end_day'] = aver_disc['DateEnd'].dt.day

# разбил год на 24 периода , по полмесяца каждый
aver_disc['season'] = 2*aver_disc['begin_month'] - 1 + aver_disc['begin_day']//16

disc_season_mean = aver_disc.groupby(['ObjCode', 'Item', 
                               'season', 'PromoTypeCode'], as_index = False)['Qnty_mean'].mean()

#нормализованные продажи товаров в разрезе сезона
disc_season_mean_max = disc_season_mean.groupby(['ObjCode', 'Item', 'PromoTypeCode'],
                                                as_index = False)['Qnty_mean'].mean().rename(columns = {'Qnty_mean':'Qnty_max'})

disc_season_mean = disc_season_mean.merge(disc_season_mean_max)
disc_season_mean['normalized'] = disc_season_mean['Qnty_mean']/disc_season_mean['Qnty_max']

disc_season_mean_normalized = disc_season_mean.groupby(['Item',
                               'season', 'PromoTypeCode'], as_index = False)['normalized'].mean()

disc_season_mean_normalized_max = disc_season_mean_normalized.groupby(['Item', 'PromoTypeCode'], 
                                                    as_index = False)['normalized'].mean().rename(columns = {'normalized':'normalized_max'})

disc_season_mean_normalized = disc_season_mean_normalized.merge(disc_season_mean_normalized_max)
disc_season_mean_normalized['normalized'] = disc_season_mean_normalized['normalized']/disc_season_mean_normalized['normalized_max']
disc_season_mean_normalized = disc_season_mean_normalized[['Item', 'season', 'normalized', 'PromoTypeCode']]

disc_season_mean_normalized = disc_season_mean_normalized.merge(item_class[['Item', 'CLASS']])

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name=container_name,
                                              blob_name = r'season/{}/season.csv'.format(1354),
                                                       stream = input_blob)
    input_blob.seek(0)
    season = pd.read_csv(input_blob)
    
season['date'] = season['dayofyear'].map(lambda x: date(2019, 1, 1) + timedelta(x - 1))
season['date'] = pd.to_datetime(season['date'])
season['begin_month'] = season['date'].dt.month
season['begin_day'] = season['date'].dt.day
season['season'] = 2*season['begin_month'] - 1 + season['begin_day']//16
season = season.groupby(['CLASS', 'season'], as_index = False)['y'].sum().rename(columns = {'y':'class_coeff'})
season_max = season.groupby(['CLASS'], as_index = False)['class_coeff'].mean().rename(columns = {'class_coeff':'coeff_max'})

season = season.merge(season_max)
season['class_coeff'] = season['class_coeff']/season['coeff_max']
season = season[['CLASS', 'season', 'class_coeff']]

all_items = disc_season_mean_normalized[['Item', 'CLASS']].drop_duplicates()
all_items['ones'] = 1

#сезоны
season_df = pd.DataFrame()
season_df['season'] = range(1, 25)
season_df['ones'] = 1

types = disc_season_mean_normalized.drop_duplicates(['Item', 'PromoTypeCode'], keep='last')[['Item', 'PromoTypeCode']]

all_items_season = all_items.merge(season_df)
all_items_season = all_items_season.merge(types, how='left')
all_items_season = all_items_season.merge(disc_season_mean_normalized, how = 'left')
all_items_season = all_items_season.merge(season, how = 'left')

all_items_season.loc[all_items_season['normalized'].isnull(), 'normalized'] = all_items_season.loc[all_items_season['normalized'].isnull(), 'class_coeff']
all_items_season = all_items_season.drop(['class_coeff', 'ones'], axis = 1)

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                              blob_name = r'stores_formats.xlsx',
                                                       stream = input_blob)
    input_blob.seek(0)
    stores_formates = pd.read_excel(input_blob)
    
stores_formates = stores_formates[['Код объекта', 'Формат SPAR']].drop_duplicates()
stores_formates = stores_formates[stores_formates['Формат SPAR'] != 'Все']
stores_formates = stores_formates.rename(columns = {'Код объекта': 'ObjCode'})
stores = disc_season_mean_max[['ObjCode']].drop_duplicates()
stores = stores.merge(stores_formates)

stores['Формат SPAR'] = stores['Формат SPAR'].map(format_map)
disc_season_mean_max_format = disc_season_mean_max.merge(stores)
disc_season_mean_max_format = disc_season_mean_max_format.groupby(['Формат SPAR', 'Item'], as_index = False)['Qnty_max'].mean()

all_items_season = all_items_season[['Item', 'PromoTypeCode', 'season', 'normalized']]

output1 = all_items_season.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'code_tables/all_items_season.csv', output1)

output1 = disc_season_mean_max.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'code_tables/disc_season_mean_max.csv', output1)