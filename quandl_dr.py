import argparse
import platform
from PyShare.PyUtils import Quandl


def func(args):
    # ------------- parse command line input args -------------
    # arguments for both wind and quandl
    rootdir = 'C:\\quandl_data_cn_futures' if 'Windows' in platform.system() else '/usr/local/share/quandl_data_cn_futures'
    mode = [] if args.mode is None else [x.lower() for x in args.mode]
    timeframe = ['eod'] if args.timeframe is None else [x.lower() for x in args.timeframe]

    # quandl specific argument
    get_quandl_dataset = False if args.get_quandl_dataset is None else args.get_quandl_dataset[0].lower() == 'y'
    exchange = ['CFFEX', 'SHFE', 'DCE', 'ZCE'] if args.exchange is None else [x.upper() for x in args.exchange]
    underlying = [] if args.underlying is None else [x.upper() for x in args.underlying]
    check_expiration = True if args.check_expiration is None else args.check_expiration[0].lower() == 'y'

    # mongodb connection ini file
    config_file_prefix = 'default' if args.config_file_prefix is None else args.config_file_prefix[0]
    mongodb = '' if args.mongodb is None else args.mongodb[0]

    # ------------- get data from quandl -------------
    # create Quandl object
    qd = Quandl.Quandl(rootdir, 'quandl_sector.ini', timeframe)

    # refresh quandl dataset file, and get contract specification
    if get_quandl_dataset:
        qd.get_datasets(exchange)
    contract_spec_update, contract_spec_all = qd.get_contract_spec(exchange, underlying, check_expiration)

    # ------------- get data from quandl -------------
    if 'dd' in mode:
        qd.get_data_procedure(contract_spec=contract_spec_update)

    # ------------- create mongodb handler, upsert into mongodb -------------
    if 'um' in mode:
        mdb = qd.mongo_connect(config_file_prefix, mongodb)
        qd.mongo_upsert(contract_spec_update, mdb)


def main():
    parser = argparse.ArgumentParser(usage='Get Quandl data')
    parser.add_argument('-m', '--mode', nargs='*', action='store', help='-m dd: download data from quandl, -m um: upsert to mongodb')
    parser.add_argument('-tf', '--timeframe', nargs='*', action='store', help='-tf eod: download end of day data')
    parser.add_argument('-g', '--get_quandl_dataset', nargs='*', action='store', help='whether download dataset csv file, n: no, y: yes')
    parser.add_argument('-e', '--exchange', nargs='*', action='store', help='-e SHFE: only apply to exchange=SHFE')
    parser.add_argument('-u', '--underlying', nargs='*', action='store', help='apply to all underlying')
    parser.add_argument('-exp', '--check_expiration', nargs='*', action='store', help='-exp y: check for expiration, i.e., exclude expired contracts')
    parser.add_argument('-id', '--config_file_prefix', nargs='*', action='store', help='config file prefix, e.g., gxjy, default')
    parser.add_argument('-d', '--mongodb', nargs='*', action='store', help='-d quandl_mongodb2: use quandl_mongodb2 section in <config_file_prefix>_mongodb_connection.ini')
    args = parser.parse_args()
    # print(args)
    try:
        func(args)
    except Exception as e:
        print(__file__, '\n', e)


if __name__ == '__main__':
    main()

# cd 'Z:\Documents\workspace\PyWind2'

# python .\quandl_dr.py -m dd -tf eod -g y -e CFFEX -exp n
# python .\quandl_dr.py -m dd -tf eod -g y -e SHFE -exp n
# python .\quandl_dr.py -m dd -tf eod -g y -e DCE -exp n
# python .\quandl_dr.py -m dd -tf eod -g y -e ZCE -exp n

# python .\quandl_dr.py -m dd um -tf eod -g y -e CFFEX -exp y -id default -d quandl_mongodb2
# daily routine, download from quandl and upsert to quandl_mongodb2, non-expired contracts only
