import datetime as dt
import os
import argparse
import platform
import pandas as pd
from PyShare.PyUtils import Mysql
from PyShare.PyUtils import Wind


def func(args):
    # ------------- get input parameters -------------
    config_file_prefix = 'default' if args.config_file_prefix is None else args.config_file_prefix[0]
    mysql = '' if args.mysql is None else args.mysql[0]

    # ------------- connect to rds -------------
    fpath = os.path.join(os.path.abspath('../../python_packages'), 'PyConfig', 'config', '_'.join([config_file_prefix, 'mysql_connection.ini']))
    rds = Mysql.MySqlDB(fpath)
    rtn = rds.connect(mysql)

    # ------------- get active contract data from wind -------------
    # create Wind object, set rootdir, wind sector config file, and timeframe
    rootdir = 'C:\\wind_data_cn_futures' if 'Windows' in platform.system() else '/usr/local/share/wind_data_cn_futures'

    wd = Wind.Wind(rootdir, 'wind_sector.ini', timeframe=[])
    cnac = wd.get_sector_constituent(sector='active')

    # set active contract for equity options
    cnac_equity = pd.DataFrame()
    cnac_equity = cnac_equity.append(pd.Series({'contract': '510050', 'desc_zh': '50ETF', 'exchange': 'SSE', 'w_symbol': '510050.SSE', 'symbol': 'SSE.510050'}), ignore_index=True)
    cnac_equity = cnac_equity.append(pd.Series({'contract': '510180', 'desc_zh': '180ETF', 'exchange': 'SSE', 'w_symbol': '510180.SSE', 'symbol': 'SSE.510180'}), ignore_index=True)
    cnac_equity = cnac_equity.append(pd.Series({'contract': '510300', 'desc_zh': '300ETF', 'exchange': 'SSE', 'w_symbol': '510300.SSE', 'symbol': 'SSE.510300'}), ignore_index=True)

    print(dt.datetime.today(), '---- upsert to contract_active table ----')
    tmp = pd.concat([cnac, cnac_equity], sort=True, ignore_index=True).loc[:, ['contract', 'exchange']]
    # tmp contains symbols from Wind, which are all upper case
    mm, result = rds.execute('TRUNCATE contract_active', [])
    sql = '''SELECT exchange_symbol, underlying_symbol, contract_symbol FROM futurexdb.contractinfo WHERE exchange_symbol IN %s AND contract_symbol IN %s'''
    mm, result = rds.execute(sql, (set(tmp['exchange']), set(tmp['contract'])))
    df = result.rename(index=str, columns={'exchange_symbol': 'exchange', 'underlying_symbol': 'underlying', 'contract_symbol': 'contract'})
    df.drop_duplicates(inplace=True)
    df.to_csv('contract_active.csv', index=False)
    rtn = rds.upsert('contract_active', df, is_on_duplicate_key_update=False)

    # ------------------------ underlying charting hour, similar to trading hour ------------------------
    # get trading hours of active contracts, each underlying should have at least one active contract
    # trading hour does not include options, e.g., m_o or 510050_O
    idx = [ii not in ['INE'] for ii in cnac['exchange']]
    ath = wd.get_trading_hours(cnac.iloc[idx])
    ath.rename(columns={'symbol': 'Symbol'}, inplace=True)
    # all contracts from ctp api
    cctp = pd.read_csv(os.path.join(os.path.abspath('..'), 'PyCtpSE', 'Contract.Future.csv'))

    # active contract (Wind) --> underlying (CTP) --> trading hour
    gmc = pd.merge(ath, cctp, on='Symbol', how='left')
    gmc['thi'] = -1
    for index, row in gmc.iterrows():
        # print(row['ProductID2'])
        if row['ProductID2'] in ['SP', 'BU', 'HC', 'RB', 'RU', 'FU', 'RU_O']:
            gmc.loc[index, 'thi'] = 0
        elif row['ProductID2'] in ['CF', 'CY', 'OI', 'FG', 'MA', 'RM', 'SR', 'TA', 'ZC', 'A', 'B', 'I', 'J', 'JM', 'M', 'P', 'Y', 'M_O', 'SR_O', 'CF_O']:
            gmc.loc[index, 'thi'] = 1
        elif row['ProductID2'] in ['AL', 'CU', 'NI', 'PB', 'SN', 'ZN', 'CU_O']:
            gmc.loc[index, 'thi'] = 2
        elif row['ProductID2'] in ['AG', 'AU']:
            gmc.loc[index, 'thi'] = 3
        elif row['ProductID2'] in ['AP', 'CJ', 'WR', 'WH', 'JR', 'LR', 'PM', 'RI', 'RS', 'SF', 'SM', 'BB', 'C', 'CS', 'FB', 'JD', 'L', 'PP', 'V', 'EG', 'C_O']:
            gmc.loc[index, 'thi'] = 4
        elif row['ProductID2'] in ['T', 'TF', 'TS']:
            gmc.loc[index, 'thi'] = 5
        elif row['ProductID2'] in ['IC', 'IF', 'IH']:
            gmc.loc[index, 'thi'] = 6

    print(dt.datetime.today(), '---- extract trading hour by underlying ----')
    # print(gmc[['trading_hour', 'exchange', 'ProductID2', 'thi']].sort_values(by=['trading_hour', 'exchange']))

    # trading hour dict
    thdict = dict()
    # ['SP', 'BU', 'HC', 'RB', 'RU', 'FU', 'RU_O']
    th = {}
    th.update({'session': [1, 2, 3, 4]})
    th.update({'normal_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'normal_end': ['23:00:00', '10:15:00', '11:30:00', '15:00:00']})
    th.update({'last_day_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'last_day_end': ['23:00:00', '10:15:00', '11:30:00', '15:00:00']})
    thdict.update({0: pd.DataFrame(th)})

    # ['CF', 'CY', 'OI', 'FG', 'MA', 'RM', 'SR', 'TA', 'ZC', 'A', 'B', 'I', 'J', 'JM', 'M', 'P', 'Y', 'M_O', 'SR_O', 'CF_O']
    th = {}
    th.update({'session': [1, 2, 3, 4]})
    th.update({'normal_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'normal_end': ['23:30:00', '10:15:00', '11:30:00', '15:00:00']})
    th.update({'last_day_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'last_day_end': ['23:30:00', '10:15:00', '11:30:00', '15:00:00']})
    thdict.update({1: pd.DataFrame(th)})

    # ['AL', 'CU', 'NI', 'PB', 'SN', 'ZN', 'CU_O']
    th = {}
    th.update({'session': [1, 2, 3, 4]})
    th.update({'normal_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'normal_end': ['01:00:00', '10:15:00', '11:30:00', '15:00:00']})
    th.update({'last_day_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'last_day_end': ['01:00:00', '10:15:00', '11:30:00', '15:00:00']})
    thdict.update({2: pd.DataFrame(th)})

    # ['AG', 'AU']
    th = {}
    th.update({'session': [1, 2, 3, 4]})
    th.update({'normal_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'normal_end': ['02:30:00', '10:15:00', '11:30:00', '15:00:00']})
    th.update({'last_day_start': ['21:00:00', '09:00:00', '10:30:00', '13:30:00']})
    th.update({'last_day_end': ['02:30:00', '10:15:00', '11:30:00', '15:00:00']})
    thdict.update({3: pd.DataFrame(th)})

    # ['AP', 'CJ', 'WR', 'WH', 'JR', 'LR', 'PM', 'RI', 'RS', 'SF', 'SM', 'BB', 'C', 'CS', 'FB', 'JD', 'L', 'PP', 'V', 'EG', 'C_O']
    th = {}
    th.update({'session': [1, 2, 3]})
    th.update({'normal_start': ['09:00:00', '10:30:00', '13:30:00']})
    th.update({'normal_end': ['10:15:00', '11:30:00', '15:00:00']})
    th.update({'last_day_start': ['09:00:00', '10:30:00', '13:30:00']})
    th.update({'last_day_end': ['10:15:00', '11:30:00', '15:00:00']})
    thdict.update({4: pd.DataFrame(th)})

    # ['T', 'TF', 'TS']
    th = {}
    th.update({'session': [1, 2]})
    th.update({'normal_start': ['09:15:00', '13:00:00']})
    th.update({'normal_end': ['11:30:00', '15:15:00']})
    th.update({'last_day_start': ['09:15:00', '13:00:00']})
    th.update({'last_day_end': ['11:30:00', '15:15:00']})
    thdict.update({5: pd.DataFrame(th)})

    # ['IC', 'IF', 'IH']
    th = {}
    th.update({'session': [1, 2]})
    th.update({'normal_start': ['09:30:00', '13:00:00']})
    th.update({'normal_end': ['11:30:00', '15:00:00']})
    th.update({'last_day_start': ['09:30:00', '13:00:00']})
    th.update({'last_day_end': ['11:30:00', '15:00:00']})
    thdict.update({6: pd.DataFrame(th)})

    # ['510050', '510180', '510300']
    th = {}
    th.update({'session': [1, 2]})
    th.update({'normal_start': ['09:30:00', '13:00:00']})
    th.update({'normal_end': ['11:30:00', '15:00:00']})
    th.update({'last_day_start': ['09:30:00', '13:00:00']})
    th.update({'last_day_end': ['11:30:00', '15:00:00']})
    thdict.update({7: pd.DataFrame(th)})

    # trading hour by underlying
    trading_hour = pd.DataFrame()
    for index, row in gmc.iterrows():
        if row['thi'] == -1:
            print('unspecified trading hour:', row['Symbol'], row['desc_zh'])
        else:
            xg = thdict[row['thi']].copy()
            xg['exchange_symbol'] = row['ExchangeID2']
            xg['underlying_symbol'] = row['ProductID2']
            trading_hour = trading_hour.append(xg, True)

    print(dt.datetime.today(), '---- upsert to underlying_charting_hour table ----')
    mm, result = rds.execute('TRUNCATE underlying_charting_hour', [])
    rtn = rds.upsert('underlying_charting_hour', trading_hour[['exchange_symbol', 'underlying_symbol', 'session', 'normal_start', 'normal_end', 'last_day_start', 'last_day_end']], False)
    mm, result = rds.execute('UPDATE underlying_charting_hour a JOIN underlying b ON a.underlying_symbol = b.underlying_symbol SET a.underlying_symbol = b.underlying_symbol', [])


def main():
    parser = argparse.ArgumentParser(usage='Get active contract data from Wind')
    parser.add_argument('-id', '--config_file_prefix', nargs='*', action='store', help='config file prefix, e.g., gxjy, default')
    parser.add_argument('-r', '--mysql', nargs='*', action='store', help='mysql profile, saved in ../PyConfig/config')
    args = parser.parse_args()
    # print(args)
    try:
        func(args)
    except Exception as e:
        print(__file__, '\n', e)


if __name__ == '__main__':
    main()

# cd Z:\Documents\workspace\PyWind2\
# python .\contract_active.py -id default -r PyOption
