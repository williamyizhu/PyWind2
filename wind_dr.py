import os
import sys
import shutil
import argparse
# os.chdir('Z:\williamyizhu On My Mac\Documents\workspace\PyWind2')
mpath = os.path.join(os.path.abspath('..'), 'PyShare\\PyShare')
sys.path.append(mpath)
import Wind
import Mongo

def func(args):
#     ------------- parse command line input args -------------
#     default use wind datasource
    rootdir = 'C:\\wind_data_cn_futures' if args.rootdir is None else args.rootdir[0].lower()
    mode = [] if args.mode is None else [x.lower() for x in args.mode]
    sector = 'trading' if args.sector is None else args.sector[0].lower()
    timeframe = ['eod'] if args.timeframe is None else [x.lower() for x in args.timeframe]
    cleancopy = False if args.cleancopy is None else args.cleancopy[0].lower()=='y'
#     mongodb connection string, 'wind_' or 'quandl_'
    mongodb = '' if args.mongodb is None else args.mongodb[0]

#     ------------- get data from wind -------------    
#     create Wind object, set rootdir, wind sector config file, and timeframe
    wd = Wind.Wind(rootdir, 'wind_sector.ini', timeframe)
#     get contract specifications from Wind terminal
    spec_field = ['ipo_date', 'lasttrade_date', 'lastdelivery_date', 'contractmultiplier']
    contract_spec_update, contract_spec_all = wd.get_contract_spec(sector, spec_field)

#     ------------- get data from wind -------------
    if 'dd' in mode:
        success, failed = wd.get_wind_data(contract_spec_update)
        print('\n***********************************************\ndownload results', 
              [':'.join([a,str(abs(b))]) for a,b in zip(['success','failed'], [len(success), len(failed)])], 
              '\n***********************************************\n')
        wd.stop()

#     ------------- create mongodb handler, upsert into mongodb -------------
    if 'um' in mode:
        mongo_path = os.path.join(os.path.abspath('..'), 'PyShare', 'config', 'mongodb_connection.ini')
        mdb = Mongo.MongoDB(mongo_path)
        mdb_connection_result = mdb.connect(mongodb)
        if mdb_connection_result:
            wd.mongo_upsert(contract_spec_update, mdb)

#     ------------- clean copy dir, rename file name and skip old contracts -------------
    if cleancopy:
        for tf in timeframe:
#             source and destination directory, create if not exist
            src = os.path.join(rootdir, tf)
            dst = os.path.join(rootdir, '_'.join([tf,'c']))
            if not(os.path.isdir(dst)):
                os.mkdir(dst)
#             rename or remove certain contract files, skip contracts before 2000 Jan, e.g., 9912
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
    parser.add_argument('-dir', '--rootdir', nargs='*', action='store')
    parser.add_argument('-m', '--mode', nargs='*', action='store')
    parser.add_argument('-s', '--sector', nargs='*', action='store')
    parser.add_argument('-tf', '--timeframe', nargs='*', action='store')
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
# cd 'Z:\Documents\workspace\PyWind2'

# daily routine, trading section contains only non-expired contracts, historial section contains all expired contracts
# python .\wind_dr.py -m dd -s trading -tf eod -cc y
# python .\wind_dr.py -m dd -s historical -tf eod -cc y
# python .\wind_dr.py -m dd -s shf-t -tf eod -cc y

# -dir root directory, by default, 'C:\\wind_data_cn_futures'
# -m dd: download data from wind, um: update to mongodb
# -s cfe: only apply to sector=cfe, in wind_section.ini
# -tf eod: only download end of day data
# -cc y: e.g., rename CZCE.ZC705.csv to CZCE.ZC1705
# -d mongodb1: use mongodb connection section wind_mongodb2 in mongodb_connection.ini

# python .\wind_dr.py -dir 'c:\wind_data_cn_continuous' -m dd -s continuous -tf eod
# python .\wind_dr.py -dir 'c:\wind_data_cn_continuous2' -m dd -s continuous2 -tf eod
# python .\wind_dr.py -dir 'c:\test' -m dd -s cfe-t -tf eod
# python .\wind_dr.py -m dd -s cfe-t -tf eod
# python .\wind_dr.py -m dd um -s cfe-t -tf eod -cc y -d wind_mongodb2
# python .\wind_dr.py -m dd um -s trading -tf eod -cc y -d wind_mongodb2
