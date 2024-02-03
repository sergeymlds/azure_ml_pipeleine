import pandas as pd
from azure.storage.blob import AppendBlobService, BlockBlobService
from datetime import datetime, timedelta
import io
import numpy as np
import argparse
import os
container_name = 'spardata'
key = os.environ['account_key']
blobstorageaccount = 'sparmlstorage'

block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                      account_key=key)


parser = argparse.ArgumentParser(description="Start a average.py")
parser.add_argument('--stores', dest="stores", required=True, nargs='+')

args = parser.parse_args()
stores = args.stores

stores = stores[0].split(',')

def drange(x): 
    e = x.max()
    s = x.min()
    return pd.Series(pd.date_range(s,e))

# sales reading
def read_sales(check_files):
    frames = []
    for ch in check_files:
        with io.BytesIO() as input_blob:
            block_blob_service.get_blob_to_stream(container_name=container_name,
                                                      blob_name = ch,
                                                      stream = input_blob)
            input_blob.seek(0)
            frame = pd.read_csv(input_blob)
            frames.append(frame)
    sales = pd.concat(frames)
    sales['DateFact'] = pd.to_datetime(sales['DateFact'])
    return sales

# discounts reading
def read_discount(st):
    with io.BytesIO() as input_blob:
        block_blob_service.get_blob_to_stream(container_name='spardata',
                                                  blob_name = f'disc_history1/{st}/discount.csv',
                                                  stream = input_blob)
        input_blob.seek(0)
        disc = pd.read_csv(input_blob)
    disc['date'] = pd.to_datetime(disc['date'])
    return disc
    
for st in stores:
    print(st)
    check_files = list(block_blob_service.list_blob_names(container_name, prefix = f'check/{st}'))

    #get sales history
    sales =  read_sales(check_files)
    disc = read_discount(st)
    
    date_max = sales['DateFact'].max()
    def drange(x): 
        e = date_max
        s = x.min()
        return pd.Series(pd.date_range(s,e))

    res = sales.groupby('Item').DateFact.apply(drange).reset_index(level=0)
    sales = sales[['DateFact', 'Item', 'Qnty', 'PriceBase']]
    
    sales['Item'] = sales['Item'].astype(np.int32)
    sales['Qnty'] = sales['Qnty'].astype(np.float32)
    sales['PriceBase'] = sales['PriceBase'].astype(np.float32)

    res['Item'] = res['Item'].astype(np.int32)
    
    sales = sales.merge(res, how = 'outer').fillna(0)
    def drange_loyalty(x): 
        e = date_max
        s = '2021-03-09'
        return pd.Series(pd.date_range(s,e))
    
    with io.BytesIO() as input_blob:
        block_blob_service.get_blob_to_stream(container_name='spardata',
                                            blob_name = r'alternative/bokaly.csv',
                                            stream = input_blob)
        input_blob.seek(0)
        loyalty = pd.read_csv(input_blob)
        
    res = sales[sales['Item'].isin(loyalty['new_id'])].groupby('Item').DateFact.apply(drange_loyalty).reset_index(level=0)
    sales = sales.merge(res, how = 'outer')
    sales['Qnty'] = sales['Qnty'].fillna(0)
    
    del res
    
    sales = sales.merge(loyalty[['new_id', 'parent_id']], left_on='Item', right_on='new_id', how='left')
    sales.loc[~sales['parent_id'].isna(), 'Item'] = sales.loc[~sales['parent_id'].isna(), 'parent_id']
    sales = sales.groupby(['DateFact', 'Item'], as_index=False).agg({'Qnty':'sum', 'PriceBase':'max'})
    
    sales['Item'] = sales['Item'].astype(np.int32)
    
    #markdown correction
    try:        
        markdown_files = list(block_blob_service.list_blob_names(container_name, prefix = f'markdown/{st}'))

        frames = []
        for ch in markdown_files:    
            with io.BytesIO() as input_blob:
                block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                              account_key=key)
                block_blob_service.get_blob_to_stream(container_name='spardata',
                                                          blob_name = ch,
                                                          stream = input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob, header=None)
                frames.append(df)

        markdowns = pd.concat(frames)
        markdowns.columns=["DateFact", "Item", "NormalPrice", "Price", "Qnty_mark"]
        markdowns['DateFact'] = pd.to_datetime(markdowns['DateFact'])
        sales = sales.merge(markdowns, how='left', on=['DateFact', 'Item'])
        sales['new_qnty'] = sales['Qnty']-sales['Qnty_mark']            
        sales.loc[sales['new_qnty']<0, 'new_qnty'] = 0
        sales.loc[~sales['new_qnty'].isna(), 'Qnty'] = sales.loc[~sales['new_qnty'].isna(), 'new_qnty']

        sales = sales[['DateFact', 'Item', 'Qnty', 'PriceBase']]
    except Exception as e:
        print(e)
        
    try:    
        online_files = list(block_blob_service.list_blob_names(container_name, prefix = f'online/{st}'))
        frames = []
        for ch in online_files:
            with io.BytesIO() as input_blob:
                block_blob_service.get_blob_to_stream(container_name=container_name,
                                                    blob_name = ch,
                                                    stream = input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob)
                frames.append(df)
        online = pd.concat(frames)
        online = online[['DateFact', 'Item', 'Qnty']]
        online = online.rename(columns = {'Qnty':'new_qnty'})
        online = online[online['new_qnty']>0]
        online['DateFact'] = pd.to_datetime(online['DateFact'])

        sales = sales.merge(online, on = ['DateFact', 'Item'], how = 'left')
        sales['new_qnty'] = sales['new_qnty'].fillna(0)
        sales['Qnty'] = sales['Qnty'] + sales['new_qnty']

        sales = sales[['DateFact', 'Item', 'Qnty', 'PriceBase']]
    except Exception as e:
        print(e)
    
    #assign IDs to discounts
    disc = disc.sort_values(['Item', 'date'])
    disc_id = disc[disc['number_disc_day']==1]
    disc_id['disc_id'] = disc_id.reset_index().index+1
    disc = disc.merge(disc_id[['Item', 'date', 'disc_id']], how='left', on=['Item', 'date'])
    disc['disc_id'] = disc['disc_id'].fillna(method='ffill')
    disc['number_disc_day'] = disc.groupby('disc_id', as_index=False).cumcount() + 1

    sales = sales.merge(disc, left_on=['Item', 'DateFact'], right_on=['Item', 'date'], how='left')
    
    # last 180 days of regular sales
    sales = sales[(sales['DateFact'] > datetime.now() - timedelta(180))|sales['PromoTypeCode'].notnull()]
    
    sales['PromoTypeCode'] = sales['PromoTypeCode'].fillna(0)
    
    #nodiscount rm
    sales['no_disc_rolling_median14'] = sales[sales['PromoTypeCode'] == 0].\
                        groupby('Item')['Qnty'].transform(
        lambda x: x.rolling(center=False, window=14, min_periods=1).mean())
    
    sales['no_disc_rolling_median14'] = sales.\
                        groupby('Item')['no_disc_rolling_median14'].transform(
        lambda x: x.fillna(method='ffill').fillna(method='bfill'))
        
    #discount rm
    sales['disc_rolling_median14'] = sales[sales['PromoTypeCode'] != 0].\
                        groupby('Item')['Qnty'].transform(
        lambda x: x.rolling(center=False, window=14, min_periods=1).mean())
    
    sales['disc_rolling_median14'] = sales.\
                        groupby('Item')['disc_rolling_median14'].transform(
        lambda x: x.fillna(method='ffill').fillna(method='bfill'))
    
    #discount rolling std
    sales['disc_rolling_std14'] = sales[sales['PromoTypeCode'] != 0].\
                        groupby('Item')['Qnty'].transform(
        lambda x: x.rolling(center=False, window=14, min_periods=1).std())
    
    sales['disc_rolling_std14'] = sales.\
                        groupby('Item')['disc_rolling_std14'].transform(
        lambda x: x.fillna(method='ffill').fillna(method='bfill'))
    
    
    # взять максимальную дату
    sales_to_output = sales[sales['DateFact'] == sales['DateFact'].max()]
    sales_to_output['ObjCode'] = int(st)
    
    output = sales_to_output[['ObjCode', 'Item', 'no_disc_rolling_median14', 'disc_rolling_median14', 'disc_rolling_std14']].to_csv(index = False)
    

    block_blob_service.create_blob_from_text('prepared',
                        r'rolling_median/{}.csv'.format(st), output)
    
    sales.loc[sales['PriceBase']==0, 'PriceBase'] = np.nan
    sales.loc[~sales['disc_id'].isna(), 'PriceBase'] = np.nan
    
    sales['PriceBase'] = sales.groupby(['Item'])['PriceBase']\
                .transform(lambda x: x.fillna(method = 'ffill').fillna(method = 'bfill'))
    
    # filter only sales with discounts
    sales = sales[~sales['number_disc_day'].isna()]

    sales.loc[sales['SalePriceBeforePromo'].isna(), 'SalePriceBeforePromo'] = sales.loc[sales['SalePriceBeforePromo'].isna(), 'PriceBase']
    sales.loc[sales['SalePriceBeforePromo']==0, 'SalePriceBeforePromo'] = sales.loc[sales['SalePriceBeforePromo']==0, 'PriceBase']
    sales.loc[sales['SalePriceBeforePromo']<sales['SalePriceTimePromo'], 'SalePriceBeforePromo'] = sales.loc[sales['SalePriceBeforePromo']<sales['SalePriceTimePromo'], 'PriceBase']
        
    
    stock_files = list(block_blob_service.list_blob_names(container_name, prefix = f'stock/{st}'))
    
    frames = []
    for ch in stock_files:    
        with io.BytesIO() as input_blob:
            block_blob_service = BlockBlobService(account_name=blobstorageaccount,
                                          account_key=key)
            block_blob_service.get_blob_to_stream(container_name='spardata',
                                                      blob_name = ch,
                                                      stream = input_blob)
            input_blob.seek(0)
            df = pd.read_csv(input_blob)
            frames.append(df)

    stock = pd.concat(frames)    
    stock = stock[['DateEnd', 'Item', 'StockQuantity']].rename(columns={'DateEnd':'DateFact', 'StockQuantity':'Stock'})
    stock['DateFact'] = pd.to_datetime(stock['DateFact'])
    
    #gathering data
    res = sales.merge(stock, on=['DateFact', 'Item'], how='left')
        
    res['discount'] = (res['SalePriceBeforePromo'] - res['SalePriceTimePromo'])/(res['SalePriceBeforePromo']+0.00001)
    res = res[['Item', 'DateFact', 'Qnty', 'disc_id', 'number_disc_day', 'Stock', 'discount', 'PromoTypeCode', 'SalePriceBeforePromo', 'SalePriceTimePromo']]
    
    #получаем таблицу, в которой есть товар-количество строк с нулевыми или отрицательными остатками-общее количество строк
    cd = res[(res['Stock'].isna())|(res['Stock']<=0)].groupby('Item', as_index=False)['DateFact'].count().sort_values('DateFact').merge(res.groupby('Item', as_index=False)['DateFact'].count().rename(columns={'DateFact':'AllCount'}))
    cd['diff'] = (cd['AllCount']-cd['DateFact'])/cd['AllCount']
    
    #выбираем те товары, у которых строк с нулевыми или отрицательными остатками меньше 20% от общего количества
    items_with_stock = cd[(cd['diff']>0.8)]['Item'].values
    
    #убираем такие строки из продаж
    res.loc[((res['Stock'].isna())|(res['Stock']<=0))&(res['Item'].isin(items_with_stock)), 'to_delete'] = 1
    sales = res[res['to_delete']!=1]
    sales = sales.drop(['to_delete', 'Stock'], axis=1)
    
    #выбираем товары, у которых средние продажи больше 5шт в день, убираем у них строки с нулевыми продажами
    cd = sales.groupby('Item', as_index=False)['Qnty'].mean()
    items_with_zero_sales = cd[cd['Qnty']>5]['Item'].values
    sales.loc[(sales['Qnty']==0)&(sales['Item'].isin(items_with_zero_sales)), 'to_delete'] = 1
    sales = sales[sales['to_delete']!=1]
    
    #считаем среднюю суточную продажу на акцию
    fact = sales.groupby('disc_id', as_index=False)['Qnty'].mean().rename(columns={'Qnty':'Qnty_mean'})
    sales = sales.merge(fact, how='left', on='disc_id')
    
    #считаем даты начала акции 
    res = sales.drop_duplicates('disc_id', keep='last')
    res = res[['Item', 'DateFact', 'Qnty_mean', 'number_disc_day', 'discount', 'PromoTypeCode', 'SalePriceBeforePromo', 'SalePriceTimePromo']].rename(columns={'DateFact':'DateEnd'})
    res['DateBegin'] = res['DateEnd']-pd.to_timedelta(res['number_disc_day'].astype(int), unit='d')
    res['ObjCode'] = st
    
    #убираем акции-однодневки
    res['DateBegin'] = res['DateBegin']+timedelta(days=1)
    res = res[res['DateEnd']!=res['DateBegin']]
    
    output = res[['ObjCode', 'Item', 'DateBegin', 'DateEnd', 'Qnty_mean', 
                  'discount', 'PromoTypeCode', 'SalePriceBeforePromo', 'SalePriceTimePromo']].to_csv(index = False)
    
    #записываем среднюю продажу на акцию
    block_blob_service.create_blob_from_text('planning',
                        r'average_sales/{}/sales.csv'.format(st), output)
    
    del res, sales, disc, fact, stock, disc_id, sales_to_output, cd, items_with_stock