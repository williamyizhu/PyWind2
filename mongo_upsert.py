import argparse
import platform
import os
import pandas as pd
from PyShare.PyUtils import DataBase


def func(args):
    # ------------- parse command line input args -------------
    # default use wind datasource
    mode = [] if args.mode is None else [x.lower() for x in args.mode]
    timeframe = ['eod'] if args.timeframe is None else [x.lower() for x in args.timeframe]
    datasource = ['wind'] if args.datasource is None else [x.lower() for x in args.datasource]
    if datasource[0] == 'wind':
        rootdir = 'C:\\wind_data_cn_futures' if 'Windows' in platform.system() else '/usr/local/share/wind_data_cn_futures'
        sector_ini = 'wind_sector.ini'
    elif datasource[0] == 'quandl':
        rootdir = 'C:\\quandl_data_cn_futures' if 'Windows' in platform.system() else '/usr/local/share/quandl_data_cn_futures'
        sector_ini = 'quandl_sector.ini'
    else:
        rootdir = ''
        sector_ini = ''

    # mongodb connection ini file
    config_file_papth = os.path.join(os.path.abspath('../../python_packages'), 'PyConfig', 'config')
    config_file_prefix = 'default' if args.config_file_prefix is None else args.config_file_prefix[0]
    mongodb = '' if args.mongodb is None else args.mongodb[0]

    # ------------- create mongodb handler, upsert into mongodb -------------
    if 'um' in mode:
        db = DataBase.DataBase(rootdir, sector_ini, timeframe, datasource[0])
        mdb = db.mongo_connect(config_file_papth, config_file_prefix, mongodb)

        for tf in timeframe:
            # convert csv file into dataframe
            filelist = os.listdir(os.path.join(rootdir, tf))
            filedf = pd.DataFrame([ff.split('.') for ff in filelist], columns=['exchange', 'contract', 'ext'])
            filedf['symbol'] = ['.'.join([i[0], i[1]]) for i in zip(filedf['exchange'], filedf['contract'])]

            # upsert all csv file data into mongodb
            idx = [j == 'csv' for j in filedf['ext']]
            contract_spec_update = filedf[idx]
            # print(contract_spec_update)
            db.mongo_upsert(contract_spec_update, mdb)


def main():
    parser = argparse.ArgumentParser(usage='Get Wind data')
    parser.add_argument('-m', '--mode', nargs='*', action='store', help='-s cfe: only apply to sector=cfe, in wind_section.ini')
    parser.add_argument('-tf', '--timeframe', nargs='*', action='store')
    parser.add_argument('-ds', '--datasource', nargs='*', action='store')
    parser.add_argument('-id', '--config_file_prefix', nargs='*', action='store', help='config file prefix, e.g., gxjy, default')
    parser.add_argument('-d', '--mongodb', nargs='*', action='store')
    args = parser.parse_args()
    # print(args)
    try:
        func(args)
    except Exception as e:
        print(__file__, '\n', e)


if __name__ == '__main__':
    main()

# -------------------------------------------------------------------------------------------
# default
# -------------------------------------------------------------------------------------------
# python .\mongo_upsert.py -m um -tf eod -ds wind -id default -d wind_mongodb2
# python .\mongo_upsert.py -m um -tf eod -ds quandl -id default -d quandl_mongodb2

# -------------------------------------------------------------------------------------------
# gxjy mongodb_market_data_unicom
# -------------------------------------------------------------------------------------------
# python .\mongo_upsert.py -m um -tf eod -ds wind -id gxjy -d mongodb_market_data_unicom
# python .\mongo_upsert.py -m um -tf eod -ds quandl -id gxjy -d mongodb_market_data_unicom
