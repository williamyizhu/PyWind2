import argparse
import datetime as dt
import os
import pandas as pd
import platform
from PyShare.PyUtils import Mysql
from PyShare.PyUtils import Wind


def func(args):
    print(dt.datetime.today(), '---- get input parameters ----')
    filename = 'contract_otc_pricing.csv' if args.filename is None else args.filename[0]
    expiration = dt.datetime.now().date() if args.expiration is None else args.expiration[0]
    days_to_expiration = 30 if args.days_to_expiration is None else int(args.days_to_expiration[0])
    config_file_prefix = 'default' if args.config_file_prefix is None else args.config_file_prefix[0]
    mysql = '' if args.mysql is None else args.mysql[0]

    print(dt.datetime.today(), '---- connect to rds ----')
    fpath = os.path.join(os.path.abspath('..'), 'PyConfig', 'config', '_'.join([config_file_prefix, 'mysql_connection.ini']))
    rds = Mysql.MySqlDB(fpath)
    rtn = rds.connect(mysql)
    if rtn is False:
        print(dt.datetime.today(), '---- MySQL is not connected ----', mysql)

    print(dt.datetime.today(), '---- get pricing contract spec ----')
    # get from csv file
    # spec = pd.read_csv(os.path.join(os.getcwd(), filename), sep=',')
    # get from rds contract_otc_pricing_spec table
    sql = '''SELECT * FROM futurexdb.contract_otc_pricing_spec'''
    mm, spec = rds.execute(sql, ())
    print(spec)

    print(dt.datetime.today(), '---- get underlying from underlying table ----')
    sql = '''SELECT * FROM futurexdb.underlying WHERE exchange_symbol IN %s AND underlying_symbol IN %s'''
    mm, result_underlying = rds.execute(sql, (set(spec['exchange']), set(spec['underlying'])))
    # print(result_underlying)

    print(dt.datetime.today(), '---- get active contracts from contract_active table ----')
    sql = '''SELECT * FROM futurexdb.contract_active WHERE exchange IN %s AND underlying IN %s'''
    mm, result_contract_active = rds.execute(sql, (set(spec['exchange']), set(spec['underlying'])))
    # print(result_contract_active)

    print(dt.datetime.today(), '---- get contracts from contractinfo table, expiration after:', expiration, '----')
    sql = '''SELECT * FROM futurexdb.contractinfo WHERE exchange_symbol IN %s AND underlying_symbol IN %s AND expiration > %s AND contract_type IN %s AND product_type IN %s'''
    mm, result_contractinfo = rds.execute(sql, (set(spec['exchange']), set(spec['underlying']), expiration, set([1]), set([1])))
    result_contractinfo.drop_duplicates(subset=['exchange_symbol', 'underlying_symbol', 'contract_symbol'], inplace=True)
    # print(result_contractinfo)

    print(dt.datetime.today(), '---- filter days to expiration:', days_to_expiration, '----')
    current_date = dt.datetime.now().date()
    dte_idx = [(i - current_date).days >= days_to_expiration for i in result_contractinfo['expiration']]
    contract_ft1 = result_contractinfo[dte_idx]
    # print(contract_ft1)

    print(dt.datetime.today(), '---- filter active month ----')
    contract_ft2 = pd.merge(contract_ft1, spec, how='left', left_on=['exchange_symbol', 'underlying_symbol'], right_on=['exchange', 'underlying'])
    active_month_idx = [a[-2:] in b.split(';') for a, b in zip(contract_ft2['contract_symbol'], contract_ft2['active_month'])]
    contract_ft3 = contract_ft2[active_month_idx]
    # print(contract_ft3)

    print(dt.datetime.today(), '---- filter number of active month selected for OTC pricing----')
    contract_gp = contract_ft3.groupby(['exchange_symbol', 'underlying_symbol'])
    spec2 = spec.set_index(['exchange', 'underlying'])
    tmp = pd.DataFrame()
    for k, gp in contract_gp:
        nn = spec2.loc[k]['active_contract_number']
        jj = gp.sort_values(by='expiration', ascending=True)[0:nn]
        jj['daily_limit_multiplier'] = spec2.loc[k]['daily_limit_multiplier'].split(';')
        tmp = tmp.append(jj)
        print(k, 'active contract number:', nn)
        print(jj)
    # upper case and sort contract_symbol, for wind wsq
    tmp['contract_symbol_upper'] = [i.upper() for i in tmp['contract_symbol']]
    tmp.sort_values(by=['contract_symbol_upper'], ascending=True, inplace=True)

    print(dt.datetime.today(), '---- get pre settlement price from wind ----')
    rootdir = 'C:\\wind_data_cn_futures' if 'Windows' in platform.system() else '/usr/local/share/wind_data_cn_futures'
    wd = Wind.Wind(rootdir, 'wind_sector.ini', timeframe=[])
    contract_info = wd.get_contract_info(tmp['contract_symbol_upper'])
    wind_wsq = wd.wsq(contract_info['wind_code'], ['rt_pre_settle', 'rt_high_limit', 'rt_low_limit'])

    print(dt.datetime.today(), '---- truncate contract_otc_pricing table ----')
    sql = '''TRUNCATE futurexdb.contract_otc_pricing'''
    mm, result = rds.execute(sql, ())

    print(dt.datetime.today(), '---- upsert to contract_otc_pricing table ----')
    contract_otc_pricing_df = tmp[['exchange_symbol', 'underlying_symbol', 'contract_symbol', 'daily_limit_multiplier', 'tick_size_multiplier', 'least_days', 'exclude_days', 'max_days']].reset_index(drop=True)
    contract_otc_pricing_df['pre_settlement'] = wind_wsq['RT_PRE_SETTLE']
    contract_otc_pricing_df['upper_limit'] = wind_wsq['RT_HIGH_LIMIT']
    contract_otc_pricing_df['lower_limit'] = wind_wsq['RT_LOW_LIMIT']
    contract_otc_pricing_df['update_time'] = f'{dt.datetime.now():%Y-%m-%d %H:%M:%S}'
    rtn = rds.upsert('contract_otc_pricing', contract_otc_pricing_df, is_on_duplicate_key_update=True)
    print(dt.datetime.today(), '---- number of contracts upsert:', rtn, '----')


def main():
    parser = argparse.ArgumentParser(usage='Get exchange-traded contracts for OTC pricing')
    parser.add_argument('-f', '--filename', nargs='*', action='store')
    parser.add_argument('-exp', '--expiration', nargs='*', action='store', help='expiration date of OTC pricing contracts')
    parser.add_argument('-dte', '--days_to_expiration', nargs='*', action='store', help='minimum days to expiration')
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
# python .\contract_otc_pricing.py -id default -r PyOption
# python .\contract_otc_pricing.py -exp 2019-01-01 -id default -r PyOption
# python .\contract_otc_pricing.py -dte 10 -id default -r PyOption

# python .\contract_otc_pricing.py -id gxjy -r dev_001
# python .\contract_otc_pricing.py -exp 2019-01-01 -id gxjy -r dev_001
# python .\contract_otc_pricing.py -dte 10 -id gxjy -r dev_001
