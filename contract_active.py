import os
import sys
import pandas as pd
os.chdir('Z:\Documents\workspace\PyWind2')
mpath = os.path.join(os.path.abspath('..'), 'PyShare\\PyShare')
sys.path.append(mpath)
import Wind
import Mysql

# ------------- connect to rds -------------    
fpath = os.path.join(os.path.abspath('..'), 'PyShare', 'config', 'mysql_connection.ini')
rds = Mysql.MySqlDB(fpath)
rtn = rds.connect(rds.connection['rds_prod'])   

# ------------- get active contract data from wind -------------    
# create Wind object, set rootdir, wind sector config file, and timeframe
rootdir = 'C:\\wind_data_cn_futures'
wd = Wind.Wind(rootdir, 'wind_sector.ini', timeframe=[])
cnac = wd.get_sector_constituent(sector='active')

# set active contract for equity options
cnac_equity = pd.DataFrame()
cnac_equity = cnac_equity.append(pd.Series({'contract':'510050','desc_zh':'50ETF', 'exchange':'SSE','w_symbol':'510050.SSE','symbol':'SSE.510050'}), ignore_index=True)
cnac_equity = cnac_equity.append(pd.Series({'contract':'510180','desc_zh':'180ETF','exchange':'SSE','w_symbol':'510180.SSE','symbol':'SSE.510180'}), ignore_index=True)
cnac_equity = cnac_equity.append(pd.Series({'contract':'510300','desc_zh':'300ETF','exchange':'SSE','w_symbol':'510300.SSE','symbol':'SSE.510300'}), ignore_index=True)

# ------------------------ insert active contract data to rds ------------------------
tmp = pd.concat([cnac, cnac_equity]).reset_index(drop=True).loc[:, ['contract','exchange']]
tmp['underlying'] = ''

mm, result = rds.execute('TRUNCATE contract_active', [])
rtn = rds.upsert('contract_active', tmp, False)
mm, result = rds.execute('UPDATE contract_active a JOIN contractinfo b ON a.contract=b.contract_symbol SET a.contract=b.contract_symbol, a.underlying=b.underlying_symbol', [])

# ------------------------ underlying charting hour, similar to trading hour ------------------------
# get trading hours of active contracts, each underlying should have at least one active contract
# trading hour does not include equity options, or INE contracts
idx = [ii not in ['INE'] for ii in cnac['exchange']]
ath = wd.get_trading_hours(cnac.iloc[idx])
ath.rename(columns={'symbol':'Symbol'}, inplace=True)
# all contracts from ctp api
cctp = pd.read_csv(''.join([os.path.dirname(os.getcwd()), '\PyCtp2\Contract.Future.csv']))

# active contract (Wind) --> underlying (CTP) --> trading hour
gmc = pd.merge(ath, cctp, on='Symbol', how='left')
gmc['thi'] = -1
for index, row in gmc.iterrows():
#     print(row['ProductID2'])
    if row['ProductID2'] in ['ZC','BU','HC','RB','RU']:
        gmc.set_value(index, 'thi', 0)
    elif row['ProductID2'] in ['CF','CY','OI','FG','MA','RM','SR','TA','A','B','I','J','JM','M','P','Y','M_O','SR_O']: 
        gmc.set_value(index, 'thi', 1)
    elif row['ProductID2'] in ['AL','CU','NI','PB','SN','ZN']:
        gmc.set_value(index, 'thi', 2)
    elif row['ProductID2'] in ['AG','AU']:
        gmc.set_value(index, 'thi', 3)
    elif row['ProductID2'] in ['AP','FU','WR','WH','JR','LR','PM','RI','RS','SF','SM','BB','C','CS','FB','JD','L','PP','V']:
        gmc.set_value(index, 'thi', 4)
    elif row['ProductID2'] in ['T','TF']:
        gmc.set_value(index, 'thi', 5)
    elif row['ProductID2'] in ['IC','IF','IH']:
        gmc.set_value(index, 'thi', 6)

print('trading hour by underlying')
print(gmc[['trading_hour','exchange','ProductID2','thi']].sort_values(by=['trading_hour','exchange']))

# trading hour dict
thdict = dict()
# ['ZC','BU','HC','RB','RU']
th = {}
th.update({'session':[1,2,3,4]})
th.update({'normal_start':  ['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'normal_end':    ['23:00:00','10:15:00','11:30:00','15:00:00']})
th.update({'last_day_start':['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'last_day_end':  ['23:00:00','10:15:00','11:30:00','15:00:00']})
thdict.update({0:pd.DataFrame(th)})

# ['CF','CY','OI','FG','MA','RM','SR','TA','A','B','I','J','JM','M','P','Y']: 
th = {}
th.update({'session':[1,2,3,4]})
th.update({'normal_start':  ['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'normal_end':    ['23:30:00','10:15:00','11:30:00','15:00:00']})
th.update({'last_day_start':['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'last_day_end':  ['23:30:00','10:15:00','11:30:00','15:00:00']})
thdict.update({1:pd.DataFrame(th)})

# ['AL','CU','NI','PB','SN','ZN']:
th = {}
th.update({'session':[1,2,3,4]})
th.update({'normal_start':  ['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'normal_end':    ['01:00:00','10:15:00','11:30:00','15:00:00']})
th.update({'last_day_start':['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'last_day_end':  ['01:00:00','10:15:00','11:30:00','15:00:00']})
thdict.update({2:pd.DataFrame(th)})

# ['AG','AU']
th = {}
th.update({'session':[1,2,3,4]})
th.update({'normal_start':  ['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'normal_end':    ['02:30:00','10:15:00','11:30:00','15:00:00']})
th.update({'last_day_start':['21:00:00','09:00:00','10:30:00','13:30:00']})
th.update({'last_day_end':  ['02:30:00','10:15:00','11:30:00','15:00:00']})
thdict.update({3:pd.DataFrame(th)})

# ['AP','FU','WR','WH','JR','LR','PM','RI','RS','SF','SM','BB','C','CS','FB','JD','L','PP','V']:
th = {}
th.update({'session':[1,2,3]})
th.update({'normal_start':  ['09:00:00','10:30:00','13:30:00']})
th.update({'normal_end':    ['10:15:00','11:30:00','15:00:00']})
th.update({'last_day_start':['09:00:00','10:30:00','13:30:00']})
th.update({'last_day_end':  ['10:15:00','11:30:00','15:00:00']})
thdict.update({4:pd.DataFrame(th)})

# ['T','TF']:
th = {}
th.update({'session':[1,2]})
th.update({'normal_start':  ['09:15:00','13:00:00']})
th.update({'normal_end':    ['11:30:00','15:15:00']})
th.update({'last_day_start':['09:15:00','13:00:00']})
th.update({'last_day_end':  ['11:30:00','15:15:00']})
thdict.update({5:pd.DataFrame(th)})

# ['IC','IF','IH']:
th = {}
th.update({'session':[1,2]})
th.update({'normal_start':  ['09:30:00','13:00:00']})
th.update({'normal_end':    ['11:30:00','15:00:00']})
th.update({'last_day_start':['09:30:00','13:00:00']})
th.update({'last_day_end':  ['11:30:00','15:00:00']})
thdict.update({6:pd.DataFrame(th)})

# ['510050','510180','510300']:
th = {}
th.update({'session':[1,2]})
th.update({'normal_start':  ['09:30:00','13:00:00']})
th.update({'normal_end':    ['11:30:00','15:00:00']})
th.update({'last_day_start':['09:30:00','13:00:00']})
th.update({'last_day_end':  ['11:30:00','15:00:00']})
thdict.update({7:pd.DataFrame(th)})

# trading hour by underlying
trading_hour = pd.DataFrame()
for index, row in gmc.iterrows():    
    xg = thdict[row['thi']].copy()
    xg['exchange_symbol'] = row['ExchangeID2']
    xg['underlying_symbol'] = row['ProductID2']
    trading_hour = trading_hour.append(xg, True)

# ------------------------ insert active contract data to rds ------------------------
mm, result = rds.execute('TRUNCATE underlying_charting_hour', [])
rtn = rds.upsert('underlying_charting_hour', trading_hour[['exchange_symbol','underlying_symbol','session','normal_start','normal_end','last_day_start','last_day_end']], False)
mm, result = rds.execute('UPDATE underlying_charting_hour a JOIN underlying b ON a.underlying_symbol = b.underlying_symbol SET a.underlying_symbol = b.underlying_symbol', [])
