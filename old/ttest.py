import pandas as pd
import gc

dirThis = 'C:\\Users\\chcww\\Downloads\\'

offers = pd.read_csv(dirThis + 'offers.csv')
trainHistory = pd.read_csv(dirThis + 'trainHistory.csv')
testHistory = pd.read_csv(dirThis + 'testHistory.csv')

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
transactions = pd.read_csv(
    dirThis + 'transactions.csv', 
    names=['id', 'chain', 'dept', 'category', 'company', 'brand', 'date', \
            'productsize', 'productmeasure', 'purchasequantity', 'purchaseamount'],
    dtype = tranDtype,
    nrows = 100000,
    parse_dates=['date'],
    infer_datetime_format=True,
    skiprows = 1,
    # engine = "pyarrow",
    # engine = "python-fwf",
    engine = "c"
    # blocksize=64000000 # = 64 Mb chunks
)

# import useful package
from datetime import timedelta
from itertools import cycle
import pandas as pd
import numpy as np
import time
import gc
ts = time.time()
# tk = 1

# get all data & delete those not in transactions
train = trainHistory.drop(columns = ['repeater', 'repeattrips'])
data = pd.concat([train, testHistory], axis=0, ignore_index=True)
use = data[data['id'].isin(transactions['id'])] # (310665, )

del trainHistory, train, testHistory
gc.collect()

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(get all data) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

# put offer information into transactions
of = offers[['offer', 'category', 'company', 'offervalue', 'brand']]
usf = pd.merge(use, of, on='offer')
usf.columns = ['id', 'chain', 'offer', 'market', 'offerdate', 'offercategory', 'offercompany',
        'offervalue', 'offerbrand']
tu = usf[['id', 'offer', 'offerdate', 'offercategory', 'offercompany', 'offerbrand']]
nu = pd.merge(tu, transactions, on='id')

del tu, usf, of, transactions
gc.collect()

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(put offer information) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

# generate time index
date_format = '%Y-%m-%d'
nu['offerdate'] = pd.to_datetime(nu['offerdate'], format = date_format)
# nu['date'] = pd.to_datetime(nu['date'], format = date_format)
nu['daydiff'] = nu['offerdate'] - nu['date']
nu['diff_180'] = nu['offerdate'] - timedelta(days = 180)
nu['diff_150'] = nu['offerdate'] - timedelta(days = 150)
nu['diff_120'] = nu['offerdate'] - timedelta(days = 120)
nu['diff_90'] = nu['offerdate'] - timedelta(days = 90)
nu['diff_60'] = nu['offerdate'] - timedelta(days = 60)
nu['diff_30'] = nu['offerdate'] - timedelta(days = 30)

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate time index) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

# put offervalue
of1 = offers[['offer', 'offervalue']]
use = pd.merge(use, of1, on='offer')

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(put offervalue) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

# generate total
group = nu.groupby(['id'])

test = group['chain'].count().reset_index()
test.columns = ['id', 'buy_total_freq']
use = pd.merge(use, test, on='id')

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate total freq) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

test = group['purchaseamount'].sum().reset_index()
test.columns = ['id', 'buy_total_amount']
use = pd.merge(use, test, on='id')

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate total amount) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

test = group['purchaseamount'].mean().reset_index()
test.columns = ['id', 'buy_total_avgamount']
use = pd.merge(use, test, on='id')

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate total avg) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

test = group['purchasequantity'].sum().reset_index()
test.columns = ['id', 'buy_total_quantity']
use = pd.merge(use, test, on='id')

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate total quan) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

test = group['daydiff'].min().reset_index()
test.columns = ['id', 'buy_total_daydiff']
use = pd.merge(use, test, on='id')

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate total) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

day = np.linspace(30, 180, 6, endpoint=True).astype(int).astype(str)

for i in day :
    daa = 'diff_' + i
    nu['ascom'] = nu['date'] >= nu[daa]

    name = 'buy_total_amount_' + i
    var = 'purchaseamount'

    group = nu.groupby(['id', 'ascom'])
    test = group[var].sum().reset_index()
    test.columns = ['id', 'ascom', name]
    use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate day amount) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    name = 'buy_total_quantity_' + i
    var = 'purchasequantity'

    group = nu.groupby(['id', 'ascom'])
    test = group[var].sum().reset_index()
    test.columns = ['id', 'ascom', name]
    use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate day quan) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    name = 'buy_total_freq_' + i
    var = 'purchaseamount'

    group = nu.groupby(['id', 'ascom'])
    test = group['chain'].count().reset_index()
    test.columns = ['id', 'ascom', name]
    use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate day) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

# generate company & brand & category
day = np.repeat(np.linspace(30, 180, 6, endpoint=True), 3).astype(int).astype(str)
mea = ['company', 'brand', 'category']


for i in mea :
    nu['ascom'] = nu[i] == nu['offer' + i]
    group = nu.groupby(['id', 'ascom'])

    test = group['chain'].count().reset_index()
    test.columns = ['id', 'ascom', 'buy_'+i+'_freq']
    use = pd.merge(use, test[test['ascom']][['id', 'buy_'+i+'_freq']], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other freq) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    test = group['purchaseamount'].sum().reset_index()
    test.columns = ['id', 'ascom', 'buy_'+i+'_amount']
    use = pd.merge(use, test[test['ascom']][['id', 'buy_'+i+'_amount']], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other amount) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    test = group['purchaseamount'].mean().reset_index()
    test.columns = ['id', 'ascom', 'buy_'+i+'_avgamount']
    use = pd.merge(use, test[test['ascom']][['id', 'buy_'+i+'_avgamount']], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other avg) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    test = group['purchasequantity'].sum().reset_index()
    test.columns = ['id', 'ascom', 'buy_'+i+'_quantity']
    use = pd.merge(use, test[test['ascom']][['id', 'buy_'+i+'_quantity']], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other quan) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    test = group['daydiff'].min().reset_index()
    test.columns = ['id', 'ascom', 'buy_'+i+'_daydiff']
    use = pd.merge(use, test[test['ascom']][['id', 'buy_'+i+'_daydiff']], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1


for i, j in zip(day, cycle(mea)) :
    daa = 'diff_' + i
    nu['ascom'] = ((nu['date'] >= nu[daa]) & (nu[j] == nu['offer' + j]))

    name = 'buy_'+j+'_amount_' + i
    var = 'purchaseamount'

    group = nu.groupby(['id', 'ascom'])
    test = group[var].sum().reset_index()
    test.columns = ['id', 'ascom', name]
    use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other day amount) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    name = 'buy_'+j+'_quantity_' + i
    var = 'purchasequantity'

    group = nu.groupby(['id', 'ascom'])
    test = group[var].sum().reset_index()
    test.columns = ['id', 'ascom', name]
    use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')

    # te = time.time()
    # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other day quan) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # tk+=1

    name = 'buy_'+j+'_freq_' + i
    var = 'purchaseamount'

    group = nu.groupby(['id', 'ascom'])
    test = group['chain'].count().reset_index()
    test.columns = ['id', 'ascom', name]
    use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')

    # te = time.time()
    # # print('Inner -', str(tidx) + ' of ' + str(tk) + '(generate other day) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
    # # tk+=1
use.columns

nu['ascom'] = (nu['company'] == nu['offercompany']) & (nu['brand'] == nu['offerbrand']) & (nu['category'] == nu['offercategory'])
group = nu.groupby(['id', 'ascom'])

name1 = 'buy_company_brand_category'
name = name1 + '_freq'

test = group['chain'].count().reset_index()
test.columns = ['id', 'ascom', name]
use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')
new = pd.DataFrame((use[name] > 0) != True)
new.columns = ['not_' + name1]
use = pd.concat([use, new], axis = 1)



nu['ascom'] = (nu['company'] == nu['offercompany']) & (nu['brand'] == nu['offerbrand'])
group = nu.groupby(['id', 'ascom'])

name1 = 'buy_company_brand'
name = name1 + '_freq'

test = group['chain'].count().reset_index()
test.columns = ['id', 'ascom', name]
use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')
new = pd.DataFrame((use[name] > 0) != True)
new.columns = ['not_' + name1]
use = pd.concat([use, new], axis = 1)



nu['ascom'] = (nu['company'] == nu['offercompany']) & (nu['category'] == nu['offercategory'])
group = nu.groupby(['id', 'ascom'])

name1 = 'buy_company_category'
name = name1 + '_freq'

test = group['chain'].count().reset_index()
test.columns = ['id', 'ascom', name]
use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')
new = pd.DataFrame((use[name] > 0) != True)
new.columns = ['not_' + name1]
use = pd.concat([use, new], axis = 1)



nu['ascom'] = (nu['brand'] == nu['offerbrand']) & (nu['category'] == nu['offercategory'])
group = nu.groupby(['id', 'ascom'])

name1 = 'buy_brand_category'
name = name1 + '_freq'

test = group['chain'].count().reset_index()
test.columns = ['id', 'ascom', name]
use = pd.merge(use, test[test['ascom']][['id', name]], on='id', how = 'outer')
new = pd.DataFrame((use[name] > 0) != True)
new.columns = ['not_' + name1]
use = pd.concat([use, new], axis = 1)

del new
gc.collect()

new1 = pd.DataFrame((use['buy_company_freq'] > 0) != True)
new1.columns = ['not_buy_company']
new2 = pd.DataFrame((use['buy_brand_freq'] > 0) != True)
new2.columns = ['not_buy_brand']
new3 = pd.DataFrame((use['buy_category_freq'] > 0) != True)
new3.columns = ['not_buy_category']
use = pd.concat([use, new1, new2, new3], axis = 1)

del new1, new2, new3
gc.collect()

# handle na problem
dayVar = ['buy_company_daydiff', 'buy_brand_daydiff', 'buy_category_daydiff', 'buy_total_daydiff']
use1 = use[dayVar]
use.drop(columns = dayVar, inplace = True)
use = use.fillna(0)
use1 = use1.fillna(timedelta(0))
use = pd.concat([use, use1], axis = 1)

del use1
gc.collect()

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(handle na problem) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

# transform date type into int
for i in use[dayVar] :
    use[i] = use[i].astype('str').apply(lambda x:x[:-5]).astype('int32')

# te = time.time()
# print('Inner -', str(tidx) + ' of ' + str(tk) + '(transform date type) -> time elapsed: ' + str(round(te-ts, 2)) + ' seconds')
# tk+=1

# transform bool type into int
for i in use.columns :
    if(use[i].dtypes == 'bool') :
        use[i] = use[i].apply(int)

use.isna().sum().sum()









use = generateFeature(offers, transactions, trainHistory, testHistory, to)