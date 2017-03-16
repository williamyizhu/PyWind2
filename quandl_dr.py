import os
import sys
import argparse
# os.chdir('Z:\williamyizhu On My Mac\Documents\workspace\PyWind2')
mpath = os.path.join(os.path.abspath('..'), 'PyShare\\Pyshare')
sys.path.append(mpath)
import Quandl
import Mongo

def func(args):
#     ------------- parse command line input args -------------
#     arguments for both wind and quandl
    mode = [] if args.mode is None else [x.lower() for x in args.mode]
    timeframe = ['eod'] if args.timeframe is None else [x.lower() for x in args.timeframe]
    mongodb = '' if args.mongodb is None else args.mongodb[0]
#     quandl specific argument
    get_quandl_dataset = False if args.get_quandl_dataset is None else args.get_quandl_dataset[0].lower()=='y'
    exchange = ['CFFEX','SHFE','DCE','ZCE'] if args.exchange is None else [x.upper() for x in args.exchange]
    underlying = [] if args.underlying is None else [x.upper() for x in args.underlying]
    check_expiration = True if args.check_expiration is None else args.check_expiration[0].lower()=='y'
     
#     ------------- get data from quandl ------------- 
#     create Quandl object
    qd = Quandl.Quandl('C:\\quandl_data_cn_futures', 'quandl_sector.ini', timeframe)
#     refresh quandl dataset file, and get contract specification
    if get_quandl_dataset:
        qd.get_datasets(exchange)         
    contract_spec_update, contract_spec_all = qd.get_contract_spec(exchange, underlying, check_expiration)
    
#     ------------- get data from quandl -------------
    if 'dd' in mode:
        success, failed = qd.get_quandl_data(contract_spec_update)
     
#     ------------- create mongodb handler, upsert into mongodb -------------
    if 'um' in mode:
        mdb = Mongo.MongoDB('mongodb_connection.ini')
        mdb.connect(mdb.connection['_'.join(['quandl', mongodb])])
        qd.mongo_upsert(contract_spec_update, mdb)                 

def main():
    parser = argparse.ArgumentParser(usage='Get Quandl data')
    parser.add_argument('-m', '--mode', nargs='*', action='store')
    parser.add_argument('-tf', '--timeframe', nargs='*', action='store')
    parser.add_argument('-d', '--mongodb', nargs='*', action='store')
    parser.add_argument('-g', '--get_quandl_dataset', nargs='*', action='store')
    parser.add_argument('-e', '--exchange', nargs='*', action='store')
    parser.add_argument('-u', '--underlying', nargs='*', action='store')
    parser.add_argument('-exp', '--check_expiration', nargs='*', action='store')    
    args = parser.parse_args()    
#     print(args)
    try:
        func(args)
    except Exception as e: 
        print(__file__, '\n', e)  

if __name__ == '__main__':
    main()
 
# quandl_dr.py -m 'dd' -g 'n' -e 'SHFE' -u '' -exp 'y' -d 'mongodb1'
# -m 'dd': download data from quandl
# -g 'n': do not download dataset csv file
# -e 'SHFE': only apply to exchange='SHFE'
# -u '': apply to all underlying
# -exp 'y': check for expiration, i.e., exclude expired contracts
# -d 'mongodb1': use mongodb connection section quandl_mongodb1 in mongodb_connection.ini

# quandl_dr.py -m 'dd' 'um' -g 'n' -u '' -exp 'y' -d 'mongodb1'
# daily routine, download from quandl and upsert to mongodb, non-expired contracts only