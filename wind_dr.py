import os
import sys
import shutil
import argparse
# os.chdir('Z:\williamyizhu On My Mac\Documents\workspace\PyWind2')
mpath = os.path.join(os.path.abspath('..'), 'PyShare\\Pyshare')
sys.path.append(mpath)
import Wind
import Mongo

def func(args):
#     ------------- parse command line input args -------------
#     default use wind datasource
    mode = [] if args.mode is None else [x.lower() for x in args.mode]
    sector = 'trading' if args.sector is None else args.sector[0].lower()
    timeframe = ['eod'] if args.timeframe is None else [x.lower() for x in args.timeframe]    
    cleancopy = False if args.cleancopy is None else args.cleancopy[0].lower()=='y'    
#     mongodb connection string, 'wind_' or 'quandl_'
    mongodb = '' if args.mongodb is None else args.mongodb[0]
        
#     ------------- get data from wind -------------    
#     create Wind object, set rootdir, wind sector config file, and timeframe
    rootdir = 'C:\\wind_data_cn_futures'
    wd = Wind.Wind(rootdir, 'wind_sector.ini', timeframe)           
#     get contract specifications from Wind terminal
    spec_field = ['ipo_date', 'lasttrade_date', 'lastdelivery_date', 'contractmultiplier']
    contract_spec_update, contract_spec_all = wd.get_contract_spec(sector, spec_field)

#     ------------- get data from wind -------------
    if 'dd' in mode:
        success, failed = wd.get_wind_data(contract_spec_update)
        wd.stop()

#     ------------- create mongodb handler, upsert into mongodb -------------
    if 'um' in mode:        
        mdb = Mongo.MongoDB('mongodb_connection.ini')
        mdb.connect(mdb.connection['_'.join(['wind', mongodb])])
        wd.mongo_upsert(contract_spec_update, mdb)

#     ------------- clean copy dir, rename file name and skip old contracts -------------
    if cleancopy:
        for tf in timeframe:
#             source and destination directory, create if not exist
            src = os.path.join(rootdir, tf)
            dst = os.path.join(rootdir, '_'.join([tf,'c']))
            if not(os.path.isdir(dst)):
                os.mkdir(dst)
#             rename or remove certain contract files
            for ff in os.listdir(src):
                yymm = [j for j in ff.split('.')[1] if j.isdigit()]
                if ff.split('.')[0]=='CZCE' and len(yymm)==3:
                    gg = ff.replace(''.join(yymm), ''.join(['1']+yymm))                
                    shutil.copyfile(os.path.join(src,ff), os.path.join(dst,gg))
                    print('rename', ff, 'to', gg)                
                else:
                    if yymm[0]=='9':
                        print('skip', ff)
                        continue
                    else:
                        shutil.copyfile(os.path.join(src,ff), os.path.join(dst,ff))
                                    
def main():
    parser = argparse.ArgumentParser(usage='Get Wind data')
    parser.add_argument('-m', '--mode', nargs='*', action='store')
    parser.add_argument('-tf', '--timeframe', nargs='*', action='store')
    parser.add_argument('-s', '--sector', nargs='*', action='store')
    parser.add_argument('-cc', '--cleancopy', nargs='*', action='store')
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
# .\wind_dr.py -m dd -s cfe -tf eod -d mongodb1
# -m dd: download data from wind, um: update to mongodb
# -s cfe: only apply to sector=cfe, in wind_section.ini
# -tf eod: only download end of day data
# -d mongodb1: use mongodb connection section wind_mongodb1 in mongodb_connection.ini

# cd 'Z:\williamyizhu On My Mac\Documents\workspace\PyWind2'
# .\wind_dr.py -m dd um -s trading -tf eod -cc y -d mongodb1
# .\wind_dr.py -m dd -s historical -tf eod -cc y
# daily routine, trading section contains only non-expired contracts
# -cc y: e.g., rename CZCE.ZC705.csv to CZCE.ZC1705
