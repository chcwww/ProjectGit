import dask.dataframe as pd
import sys
sys.path.append(r'C:\\vs_code_python\\Project')

dirThis = 'C:\\Users\\chcww\\Downloads\\'
import pandas

tranDtype = {
    'id': 'uint64',
    'chain': 'uint16',
    'dept': 'uint8',
    'category': 'uint16',
    'company': 'uint64',
    'brand': 'uint32',
    'date' : 'object',
    'productsize': 'float32',
    'productmeasure': 'category',
    'purchasequantity': 'int64',
    'purchaseamount': 'float32'
    }

offers = pd.read_csv(dirThis + 'offers.csv')
transactions = pd.read_csv(dirThis + 'transactions.csv', dtype = tranDtype)











# import useful package
from datetime import timedelta
import pandas as pd
import numpy as np
import time
import gc
ts = time.time()
tk = 1

use = pd.concat([trainHistory.drop(columns = ['repeater', 'repeattrips']), testHistory], axis=0, ignore_index=True)
offers.rename(columns={'category' : 'offercategory', 'company' : 'offercompany', 'brand' : 'offerbrand'}, inplace = True)
usf = use.set_index('offer').join(offers.set_index('offer'))[['id', 'offerdate', 'offercategory', 'offercompany', 'offerbrand']]
nu = transactions.set_index('id').join(usf.set_index('id'))

del usf, trainHistory, testHistory, transactions
gc.collect()

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(put offer information) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1

# generate time index
day = np.linspace(30, 180, 6, endpoint = True).astype(int).astype(str)
mea = ['total', 'company', 'brand', 'category']
date_format = '%Y-%m-%d'
nu['daydiff'] = pd.to_datetime(nu['offerdate'], format = date_format) - pd.to_datetime(nu['date'], format = date_format)
for i in day :
    nu['diff_' + i] = nu['daydiff'] <= timedelta(days = int(i))
for i in mea[1:] :
    nu['in_' + i] = nu['offer' + i] == nu[i]

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate time index) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1

# put offervalue
of1 = offers[['offer', 'offervalue']]
use = pd.merge(use, of1, on='offer')

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(put offervalue) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1

# generate total
new1 = nu.groupby(['id']).agg({'purchaseamount': ['sum', 'mean', 'count'], 'daydiff': ['min']})
new2 = nu.groupby(['id', 'productmeasure'])['purchasequantity'].sum().reset_index('productmeasure')
new = pd.concat([new1, new2.loc[(new2['productmeasure'].values == 'CT'), 'purchasequantity'],
                new2.loc[(new2['productmeasure'].values == 'OZ'), 'purchasequantity'],
                new2.loc[(new2['productmeasure'].values == 'LT'), 'purchasequantity']], axis = 1)
new.columns = ['buy_total_amount', 'buy_total_avgamount', 'buy_total_freq',
            'buy_total_daydiff', 'buy_total_CT', 'buy_total_OZ', 'buy_total_LT']
use = use.set_index('id').join(new)

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate total) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1


for i in day :
    filter = nu['diff_' + i]
    new1 = nu[filter].groupby('id').agg({'purchaseamount' : ['sum', 'mean', 'count']})
    new2 = nu[filter].groupby(['id', 'productmeasure'])['purchasequantity'].sum().reset_index('productmeasure')
    new = pd.concat([new1, new2[(new2['productmeasure'].values == 'CT')]['purchasequantity'],
                    new2[(new2['productmeasure'].values == 'OZ')]['purchasequantity'],
                    new2[(new2['productmeasure'].values == 'LT')]['purchasequantity']], axis = 1)
    new.columns = ['buy_total_amount_' + i, 'buy_total_avgamount_' + i, 'buy_total_freq_' + i,
                    'buy_total_CT_' + i, 'buy_total_OZ_' + i, 'buy_total_LT_' + i]
    use = use.join(new)

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate total day) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1

# generate company & brand & category
mea = ['company', 'brand', 'category']
for m in mea :
    filter = nu['in_' + m]
    new1 = nu[filter].groupby(['id']).agg({'purchaseamount': ['sum', 'mean', 'count'], 'daydiff': ['min']})
    new2 = nu[filter].groupby(['id', 'productmeasure'])['purchasequantity'].sum().reset_index('productmeasure')
    new = pd.concat([new1, new2.loc[(new2['productmeasure'].values == 'CT'), 'purchasequantity'],
                    new2.loc[(new2['productmeasure'].values == 'OZ'), 'purchasequantity'],
                    new2.loc[(new2['productmeasure'].values == 'LT'), 'purchasequantity']], axis = 1)
    new.columns = ['buy_'+m+'_amount', 'buy_'+m+'_avgamount', 'buy_'+m+'_freq',
                    'buy_'+m+'_daydiff', 'buy_'+m+'_CT', 'buy_'+m+'_OZ', 'buy_'+m+'_LT']
    use = use.join(new)

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1


for i in day :
    for m in mea :
        filter = nu['diff_'+i] & nu['in_' + m]
        new1 = nu[nu['diff_'+i]].groupby('id').agg({'purchaseamount' : ['sum', 'mean', 'count']})
        new2 = nu[nu['diff_'+i]].groupby(['id', 'productmeasure'])['purchasequantity'].sum().reset_index('productmeasure')
        new = pd.concat([new1, new2[(new2['productmeasure'].values == 'CT')]['purchasequantity'],
                        new2[(new2['productmeasure'].values == 'OZ')]['purchasequantity'],
                        new2[(new2['productmeasure'].values == 'LT')]['purchasequantity']], axis = 1)
        new.columns = ['buy_'+m+'_amount_' + i, 'buy_'+m+'_avgamount_' + i, 'buy_'+m+'_freq_' + i,
                    'buy_'+m+'_CT_' + i, 'buy_'+m+'_OZ_' + i, 'buy_'+m+'_LT_' + i]
        use = use.join(new)

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other day) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1


# generate not buy index
for m in mea :
    f = (use['buy_'+m+'_freq']>0) 
    use['not_buy_'+m] = True!=f

it = np.repeat(range(3), 2)
for m1, m2 in zip(it[:3], it[3:]) :
    n = use['not_buy_'+mea[m1]]
    t = use['not_buy_'+mea[m2]]
    use['not_buy_'+mea[m1]+'_'+mea[m2]] = (n & t)

del new1, new2, new
gc.collect()

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate not buy) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1

# handle na problem
dayVar = ['buy_company_daydiff', 'buy_brand_daydiff', 'buy_category_daydiff', 'buy_total_daydiff']
use[dayVar] = use[dayVar].fillna(timedelta(0)).astype('timedelta64[D]').astype(int)
use = use.fillna(0)

te = time.time()
print('Inner -', str(tidx) + ' of ' + str(tk) + '(handle na problem) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
tk+=1

# transform bool type into int
use[use.columns[use.dtypes == 'bool']] = use[use.columns[use.dtypes == 'bool']].astype(int)

te = time.time()
print('Inner -', str(tidx) + ' (generate feature) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')