import argparse
import platform
import os
from PyShare.PyUtils import Wind


def func(args):
    # ------------- parse command line input args -------------
    # default use wind datasource
    rootdir = 'C:\\wind_data_cn_futures' if 'Windows' in platform.system() else '/usr/local/share/wind_data_cn_futures'
    mode = [] if args.mode is None else [x.lower() for x in args.mode]
    sector = 'trading' if args.sector is None else args.sector[0].lower()
    timeframe = ['eod'] if args.timeframe is None else [x.lower() for x in args.timeframe]
    # cleancopy = False if args.cleancopy is None else args.cleancopy[0].lower() == 'y'

    # mongodb connection ini file
    config_file_papth = os.path.join(os.path.abspath('../../python_packages'), 'PyConfig', 'config')
    config_file_prefix = 'default' if args.config_file_prefix is None else args.config_file_prefix[0]
    mongodb = '' if args.mongodb is None else args.mongodb[0]

    # ------------- get data from wind -------------
    # create Wind object, set rootdir, wind sector config file, and timeframe
    wd = Wind.Wind(rootdir, 'wind_sector.ini', timeframe)
    # get contract specifications from Wind terminal
    spec_field = ['ipo_date', 'lasttrade_date', 'lastdelivery_date', 'contractmultiplier']
    contract_spec_update, contract_spec_all = wd.get_contract_spec(sector, spec_field)

    # ------------- get data from wind -------------
    if 'dd' in mode:
        wd.get_data_procedure(contract_spec=contract_spec_update)

    # ------------- stop wind terminal -------------
    wd.stop()

    # ------------- create mongodb handler, upsert into mongodb -------------
    if 'um' in mode:
        mdb = wd.mongo_connect(config_file_papth, config_file_prefix, mongodb)
        wd.mongo_upsert(contract_spec_update, mdb)

    # # ------------- clean copy dir, rename file name and skip old contracts -------------
    # if cleancopy:
    #     wd.cleancopy()


def main():
    parser = argparse.ArgumentParser(usage='Get Wind data')
    parser.add_argument('-m', '--mode', nargs='*', action='store', help='-s cfe: only apply to sector=cfe, in wind_section.ini')
    parser.add_argument('-s', '--sector', nargs='*', action='store')
    parser.add_argument('-tf', '--timeframe', nargs='*', action='store')
    # parser.add_argument('-cc', '--cleancopy', nargs='*', action='store', help='-cc y: e.g., rename CZCE.ZC705.csv to CZCE.ZC1705')
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
# daily routine, trading section contains only non-expired contracts
# historial section contains all expired contracts
# -------------------------------------------------------------------------------------------
# python .\wind_dr.py -m dd -s trading -tf eod
# python .\wind_dr.py -m dd -s historical -tf eod

# python .\wind_dr.py -m dd -s dce-t -tf eod
# python .\wind_dr.py -m dd -s czc-t -tf eod
# python .\wind_dr.py -m dd -s cfe-t -tf eod
# python .\wind_dr.py -m dd -s shf-t -tf eod

# -------------------------------------------------------------------------------------------
# collect wind data daily, only non-expired contracts, and save in mongodb
# -------------------------------------------------------------------------------------------
# python .\wind_dr.py -m dd um -s dce-t -tf eod -id default -d ctp_mongodb2
# python .\wind_dr.py -m dd um -s czc-t -tf eod -id default -d ctp_mongodb2
# python .\wind_dr.py -m dd um -s cfe-t -tf eod -id default -d ctp_mongodb2
# python .\wind_dr.py -m dd um -s shf-t -tf eod -id default -d ctp_mongodb2

# -------------------------------------------------------------------------------------------
# collect wind data daily and hourly, only non-expired contracts
# -------------------------------------------------------------------------------------------
# python .\wind_dr.py -m dd um -s trading -tf eod -id gxjy -d mongodb_market_data
# python .\wind_dr.py -m dd um -s trading -tf 60 -id gxjy -d mongodb_market_data

# -------------------------------------------------------------------------------------------
# collect wind historical data, daily and hourly
# -------------------------------------------------------------------------------------------
# python .\wind_dr.py -m dd um -s historical -tf eod -id gxjy -d mongodb_market_data
#
# python .\wind_dr.py -m dd um -s dce-h -tf 60 -id gxjy -d mongodb_market_data
# python .\wind_dr.py -m dd um -s czc-h -tf 60 -id gxjy -d mongodb_market_data
# python .\wind_dr.py -m dd um -s cfe-h -tf 60 -id gxjy -d mongodb_market_data
# python .\wind_dr.py -m dd um -s shf-h -tf 60 -id gxjy -d mongodb_market_data
