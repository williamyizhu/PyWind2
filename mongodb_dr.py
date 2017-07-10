import os
import sys
import argparse
import numpy as np
# os.chdir('Z:\williamyizhu On My Mac\Documents\workspace\PyWind2')
mpath = os.path.join(os.path.abspath('..'), 'PyShare\\PyShare')
sys.path.append(mpath)
import Mongo

def func(args):
#     ------------- parse command line input args -------------
#     default use wind datasource
    mode = [] if args.mode is None else [x.lower() for x in args.mode]
#     sector = 'trading' if args.sector is None else args.sector[0].lower()
#     mongodb connection string, 'wind_' or 'quandl_'
    mongodb = '' if args.mongodb is None else args.mongodb[0]
     
#     ------------- create mongodb handler, upsert into mongodb -------------
    if 'um' in mode:
        mongo_path = os.path.join(os.path.abspath('..'), 'PyShare', 'config', 'mongodb_connection.ini')
        mdb = Mongo.MongoDB(mongo_path)
        mdb_connection_result = mdb.connect(mongodb)

#     ------------- if connection to mongodb is successful -------------
    if mdb_connection_result:
        if mdb.connection[mongodb]['db']=='WindDataCnFutures':
            print('db=WindDataCnFutures: remove nan value')
            cond_dict = {'$or':[{'DATETIME':np.nan}, 
                                {'OPEN':np.nan}, 
                                {'HIGH':np.nan},
                                {'LOW':np.nan},
                                {'CLOSE':np.nan},
                                {'SETTLE':np.nan},
                                {'VOLUME':np.nan},
                                {'OI':np.nan}
                                ]}
#             mdb.find(cond_dict)
            mdb.delete_many(cond_dict)
        elif mdb.connection[mongodb]['db']=='CtpData':
            print('db=CtpData: remove pre-market open data')
            cond_dict = {'$and':[{'BID':{'$gt':1e32}}, 
                                 {'ASK':{'$gt':1e32}}, 
                                 {'BVOL':0},
                                 {'AVOL':0}
                                 ]}
#             mdb.find(cond_dict)
            mdb.delete_many(cond_dict)
#             each index on a collection adds some amount of overhead to the performance of write operation
#             print('db=CtpData: create index for TradingDay')
#             mdb.create_index('TradingDay', True)

def main():
    parser = argparse.ArgumentParser(usage='MongoDB Daily Routine')
    parser.add_argument('-m', '--mode', nargs='*', action='store')
    parser.add_argument('-d', '--mongodb', nargs='*', action='store') 
    args = parser.parse_args()    
#     print(args)
    try:
        func(args)
    except Exception as e: 
        print(__file__, '\n', e)  

if __name__ == '__main__':
    main()  


# cd 'Z:\williamyizhu On My Mac\Documents\workspace\PyWind2'
# .\mongodb_dr.py -m um -d wind_mongodb2
# .\mongodb_dr.py -m um -d ctp_mongodb2