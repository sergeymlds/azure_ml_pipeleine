#!/usr/bin/env python
# coding: utf-8

# In[1]:


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


#читаем актуальные товары по всем магазинам, объединяем в один справочник
frames = []
for store_id in check_ids:    
    with io.BytesIO() as input_blob:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        block_blob_service.get_blob_to_stream(container_name=container_name,
                                                  blob_name = r'actual/{}/actual.csv'.format(store_id),
                                                  stream = input_blob)
        input_blob.seek(0)
        df = pd.read_csv(input_blob)
        frames.append(df)
items = pd.concat(frames)
items = items.drop_duplicates('Item')
    
output = items.to_csv(index = False)    
block_blob_service.create_blob_from_text('planning',
                        r'items.csv', output)                    

#читаем все неакционные продажи
frames = []
for store_id in check_ids:    
    with io.BytesIO() as input_blob:
        block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)
        block_blob_service.get_blob_to_stream(container_name='prepared',
                                                  blob_name = r'rolling_median/{}.csv'.format(store_id),
                                                  stream = input_blob)
        input_blob.seek(0)
        df = pd.read_csv(input_blob)
        frames.append(df)
no_disc = pd.concat(frames)
    
output = no_disc.to_csv(index = False)    
block_blob_service.create_blob_from_text('planning',
                    r'no_disc_rolling_median.csv', output)

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
last_discount = last_discount.drop_duplicates(['ObjCode', 'Item'], keep = 'last')

output1 = last_discount.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'last_discount.csv', output1)

all_aver = aver_disc.groupby(['Item', 'DateBegin'], as_index=False)['Qnty_mean'].sum().merge(aver_disc.groupby(['Item', 'DateBegin'], as_index=False)['ObjCode'].count()).rename(columns={'ObjCode':'Количество магазинов', 'fact_promo':'Фактические продажи', 'Qnty_mean':'qnty'})
aver_disc = aver_disc.merge(all_aver[(all_aver['qnty']==0)], on = ['Item', 'DateBegin'], how='left')
aver_disc = aver_disc[aver_disc['qnty']!=0]
aver_disc = aver_disc[['ObjCode', 'Item', 'DateBegin', 'DateEnd', 'Qnty_mean']]

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                              blob_name = r'items.csv',
                                                       stream = input_blob)
    input_blob.seek(0)
    item_class = pd.read_csv(input_blob)
    
aver_disc['DateBegin'] = pd.to_datetime(aver_disc['DateBegin'])
aver_disc['DateEnd'] = pd.to_datetime(aver_disc['DateEnd'])

aver_disc = aver_disc.merge(item_class)

aver_disc['begin_year'] = aver_disc['DateBegin'].dt.year
aver_disc['begin_month'] = aver_disc['DateBegin'].dt.month
aver_disc['begin_day'] = aver_disc['DateBegin'].dt.day

aver_disc['end_month'] = aver_disc['DateEnd'].dt.month
aver_disc['end_day'] = aver_disc['DateEnd'].dt.day

# разбил год на 24 периода , по полмесяца каждый
aver_disc['season'] = 2*aver_disc['begin_month'] - 1 + aver_disc['begin_day']//16

disc_season_mean = aver_disc.groupby(['ObjCode', 'Item', 
                               'season'], as_index = False)['Qnty_mean'].mean()


# ## Корректировка на медленные изменения

# In[22]:


#######Корректировка на медленные изменения

disc_season_mean = aver_disc.groupby(['ObjCode', 'Item', 'begin_year', 
                              'season'], as_index = False)['Qnty_mean'].mean()
#нормализованные продажи товаров в разрезе сезона
disc_season_norm = disc_season_mean.groupby(['ObjCode', 'Item'], as_index = False)['Qnty_mean'].mean().rename(columns = {'Qnty_mean':'Qnty_max'})
disc_season_mean = disc_season_mean.merge(disc_season_norm)
disc_season_mean['normalized'] = disc_season_mean['Qnty_mean']/disc_season_mean['Qnty_max']
disc_season_mean_normalized = disc_season_mean.groupby(['Item', 'begin_year', 'season'], as_index = False)['normalized'].mean()
disc_season_mean_year_correction = disc_season_mean_normalized[disc_season_mean_normalized['season'] != 24].\
            groupby(['Item', 'begin_year'], as_index = False).agg({'normalized':'mean', 
            'season':'count'}).rename(columns = {'season':'count_per_year'})

#disc_season_mean_year_correction = disc_season_mean_year_correction[disc_season_mean_year_correction['count_per_year'] > 2]
disc_season_mean_year_correction_last = disc_season_mean_year_correction.groupby(['Item'], as_index = False)['normalized'].last()

disc_season_mean_year_correction = disc_season_mean_year_correction.merge(disc_season_mean_year_correction_last. \
                                                                rename(columns = {'normalized':'normalized_last'}), how = 'left')


disc_season_mean_year_correction['coef'] = disc_season_mean_year_correction['normalized_last']/disc_season_mean_year_correction['normalized']
disc_season_mean_year_correction = disc_season_mean_year_correction[['begin_year', 'Item', 'coef']]


disc_season_mean = aver_disc.groupby(['ObjCode', 'Item', 'begin_year', 'season'], as_index = False)['Qnty_mean'].mean()
disc_season_mean = disc_season_mean.merge(disc_season_mean_year_correction, how = 'left')
disc_season_mean['coef'] = disc_season_mean['coef'].fillna(1)
disc_season_mean['Qnty_mean'] = disc_season_mean['Qnty_mean']*disc_season_mean['coef']
#######Звершение корректировки 
disc_season_mean_max = disc_season_mean.groupby(['ObjCode', 'Item'], as_index = False)['Qnty_mean'].mean().rename(columns = {'Qnty_mean':'Qnty_max'})
disc_season_mean = disc_season_mean.merge(disc_season_mean_max)
disc_season_mean['normalized'] = disc_season_mean['Qnty_mean']/disc_season_mean['Qnty_max']


# In[39]:


#нормализованные продажи товаров в разрезе сезона

disc_season_mean_normalized = disc_season_mean.groupby(['Item',
                               'season'], as_index = False)['normalized'].mean()

disc_season_mean_normalized_max = disc_season_mean_normalized.groupby('Item', 
                                                    as_index = False)['normalized'].mean().rename(columns = {'normalized':'normalized_max'})

disc_season_mean_normalized = disc_season_mean_normalized.merge(disc_season_mean_normalized_max)
disc_season_mean_normalized['normalized'] = disc_season_mean_normalized['normalized']/disc_season_mean_normalized['normalized_max']
disc_season_mean_normalized = disc_season_mean_normalized[['Item', 'season', 'normalized']]

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

all_items_season = all_items.merge(season_df)
all_items_season = all_items_season.merge(disc_season_mean_normalized, how = 'left')
all_items_season = all_items_season.merge(season, how = 'left')

all_items_season.loc[all_items_season['normalized'].isnull(), 'normalized'] = all_items_season.loc[all_items_season['normalized'].isnull(), 'class_coeff']
all_items_season = all_items_season.drop(['class_coeff', 'ones'], axis = 1)


# In[41]:


with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                          blob_name='new_tables/stores.csv',
                                          stream=input_blob)
    input_blob.seek(0)
    stores_formates = pd.read_csv(input_blob)
    
    
stores = disc_season_mean_max[['ObjCode']].drop_duplicates()
stores = stores.merge(stores_formates)


disc_season_mean_max_format = disc_season_mean_max.merge(stores)
# слабое место из-за времени 
disc_season_mean_max_format = disc_season_mean_max_format.groupby(['Формат SPAR', 'Item'], as_index = False)['Qnty_max'].mean()

all_items_season = all_items_season[['Item', 'season', 'normalized']]

output1 = disc_season_mean_max_format.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'new_tables/disc_season_mean_max_format.csv', output1)


output1 = all_items_season.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'new_tables/all_items_season.csv', output1)

output1 = disc_season_mean_max.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'new_tables/disc_season_mean_max.csv', output1)

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                              blob_name = r'no_disc_rolling_median.csv',
                                                       stream = input_blob)
    input_blob.seek(0)
    no_disc_rolling_median = pd.read_csv(input_blob)
    

with io.BytesIO() as input_blob:
    block_blob_service.get_blob_to_stream(container_name='planning',
                                          blob_name='new_tables/stores.csv',
                                          stream=input_blob)
    input_blob.seek(0)
    stores_formates = pd.read_csv(input_blob)



no_disc_rolling_median = no_disc_rolling_median.merge(stores_formates, how = 'left')
no_disc_rolling_median = no_disc_rolling_median.groupby(['Item', 'Формат SPAR'], as_index = False)['no_disc_rolling_median14'].mean()

output1 = no_disc_rolling_median.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', 
                        r'new_tables/no_disc_rolling_median_formats.csv', output1)



### linear_model ###

frames = []
count=0
for st in check_ids:
    if count%20 == 0:
        print(str(st) + ' ' + str(count))
    with io.BytesIO() as input_blob:
        block_blob_service.get_blob_to_stream(container_name='planning',
                                                  blob_name = r'average_sales/{}/sales.csv'.format(st),
                                                           stream = input_blob)
        input_blob.seek(0)
        discounts = pd.read_csv(input_blob)
    frames.append(discounts)
    count=count+1
    
average = pd.concat(frames)

average['DateBegin'] = pd.to_datetime(average['DateBegin'])
average['begin_month'] = average['DateBegin'].dt.month
average['begin_day'] = average['DateBegin'].dt.day
average['season'] = 2*average['begin_month'] - 1 + average['begin_day']//16

average = average[(average['season']!=24)&(average['season']!=8)&(average['season']!=9)]

all_aver = average.groupby(['Item', 'DateBegin'], as_index=False)['Qnty_mean'].sum().merge(average.groupby(['Item', 'DateBegin'], as_index=False)['ObjCode'].count(), on=['Item', 'DateBegin']).rename(columns={'ObjCode':'Store counts'})

all_aver = all_aver[all_aver['Store counts']>10]
all_aver = all_aver[all_aver['Qnty_mean']>0]
all_aver['Qnty'] = all_aver['Qnty_mean']/all_aver['Store counts']

all_aver = all_aver.merge(average.groupby(['Item', 'DateBegin'], as_index=False)['SalePriceBeforePromo'].mean(), on=['Item', 'DateBegin']).merge(average.groupby(['Item', 'DateBegin'], as_index=False)['SalePriceTimePromo'].mean(), on=['Item', 'DateBegin'])

all_aver['disc'] = (all_aver['SalePriceBeforePromo'] - all_aver['SalePriceTimePromo'])/(all_aver['SalePriceBeforePromo']+0.00001)

to_lm = all_aver[['Item', 'Qnty', 'disc']]
to_lm['disc_2'] = to_lm['disc']*to_lm['disc']
to_lm = to_lm[(to_lm['disc']>=0.05)&(to_lm['disc']<=0.6)]
to_lm['disc'] = to_lm['disc'].round(2)

frames = []
for item in set(to_lm['Item']):#
    temp = pd.DataFrame(columns=['score', 'item', 'a', 'b', 'c', 'alpha'])
    
    x = to_lm[to_lm['Item']==item].sort_values('disc')[['disc', 'disc_2']]
    y = to_lm[to_lm['Item']==item].sort_values('disc')[['Qnty']]
    
    if len(x)>1:
    
        # define model evaluation method
        cv = RepeatedKFold(n_splits=len(x) if len(x)<10 else 10, n_repeats=3, random_state=1)
        # define model
        model = RidgeCV(alphas=arange(0.1, 1, 0.1))
        # fit model
        model.fit(x, y)
        
        reg = Ridge(alpha=model.alpha_).fit(x, y)

        temp.loc[0] = [reg.score(x, y), item,reg.intercept_,reg.coef_[0][0],reg.coef_[0][1],model.alpha_]

        frames.append(temp)        
        
result = pd.concat(frames)

result.loc[result['b']<0, 'b'] = 0
result.loc[result['c']<0, 'c'] = 0

output = result.to_csv(index = False)
block_blob_service.create_blob_from_text('planning', r'linear_model.csv', output)