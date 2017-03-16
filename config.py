import os
import configparser

os.chdir('Z:\williamyizhu On My Mac\Documents\workspace\PyWind2')
os.getcwd()

# -------------- Wind sector id --------------
wind_sector = configparser.SafeConfigParser()

# Wind, WSET, future exchange all trading contracts
wind_sector.add_section('trading')
wind_sector.set('trading', 'CFE', 'a599010101000000')
wind_sector.set('trading', 'SHF', 'a599010201000000')
wind_sector.set('trading', 'DCE', 'a599010301000000')
wind_sector.set('trading', 'CZC', 'a599010401000000')

# Wind, WSET, future exchange all historical contracts
wind_sector.add_section('historical')
wind_sector.set('historical', 'CFE', '1000009643000000')
wind_sector.set('historical', 'SHF', '1000009644000000')
wind_sector.set('historical', 'DCE', '1000009645000000')
wind_sector.set('historical', 'CZC', '1000009646000000')

# 
wind_sector.add_section('cfe')
wind_sector.set('cfe', 'trading', 'a599010101000000')
wind_sector.set('cfe', 'historical', '1000009643000000')

# 
wind_sector.add_section('shf')
wind_sector.set('shf', 'trading', 'a599010201000000')
wind_sector.set('shf', 'historical', '1000009644000000')

# 
wind_sector.add_section('dce')
wind_sector.set('dce', 'trading', 'a599010301000000')
wind_sector.set('dce', 'historical', '1000009645000000')

# 
wind_sector.add_section('czc')
wind_sector.set('czc', 'trading', 'a599010401000000')
wind_sector.set('czc', 'historical', '1000009646000000')                        

with open('wind_sector.ini', 'w') as f:
    wind_sector.write(f)

# -------------- Quandl sector id --------------
quandl_sector = configparser.SafeConfigParser()

quandl_sector.add_section('account')
quandl_sector.set('account', 'id', 'william.yizhu@live.com')
quandl_sector.set('account', 'api_key', 'zbJDcWPAfgVRVFe_H2uN')

with open('quandl_sector.ini', 'w') as f:
    quandl_sector.write(f)
    
# -------------- mysql connection --------------
mysql_connection = configparser.SafeConfigParser()

mysql_connection.add_section('rds_test')
mysql_connection.set('rds_test', 'id', 'rds_test')
mysql_connection.set('rds_test', 'host', 'rm-uf61c095vt8jo2589o.mysql.rds.aliyuncs.com')
mysql_connection.set('rds_test', 'port', '3306')
mysql_connection.set('rds_test', 'user', 'zhuyi')
mysql_connection.set('rds_test', 'password', 'Zhuyi@2701')
mysql_connection.set('rds_test', 'db', 'futurexdb')
mysql_connection.set('rds_test', 'charset', 'utf8')

with open('mysql_connection.ini', 'w') as f:
    mysql_connection.write(f)
    
# -------------- mongodb connection --------------
mongodb_connection = configparser.SafeConfigParser()

mongodb_connection.add_section('wind_mongodb1')
mongodb_connection.set('wind_mongodb1', 'id', 'wind_mongodb1')
mongodb_connection.set('wind_mongodb1', 'ip1', 'mongodb://root:Xhmz372701@114.55.54.144:3717')
mongodb_connection.set('wind_mongodb1', 'ip2', 'mongodb://root:Xhmz372701@114.55.54.144:3718')
mongodb_connection.set('wind_mongodb1', 'db', 'WindDataCnFutures')

mongodb_connection.add_section('wind_mongodb2')
mongodb_connection.set('wind_mongodb2', 'id', 'wind_mongodb2')
mongodb_connection.set('wind_mongodb2', 'ip1', 'mongodb://root:Xhmz372701@114.215.252.135:3717')
mongodb_connection.set('wind_mongodb2', 'ip2', 'mongodb://root:Xhmz372701@114.215.252.135:3718')
mongodb_connection.set('wind_mongodb2', 'db', 'WindDataCnFutures')

mongodb_connection.add_section('wind_mongodbi')
mongodb_connection.set('wind_mongodbi', 'id', 'wind_mongodbi')
mongodb_connection.set('wind_mongodbi', 'ip1', 'mongodb://root:Xhmz372701@dds-bp1affea778ad1842.mongodb.rds.aliyuncs.com:3717,dds-bp1affea778ad1841.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-1401299')
mongodb_connection.set('wind_mongodbi', 'ip2', 'mongodb://root:Xhmz372701@dds-bp1affea778ad1842.mongodb.rds.aliyuncs.com:3717,dds-bp1affea778ad1841.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-1401299')
mongodb_connection.set('wind_mongodbi', 'db', 'WindDataCnFutures')

mongodb_connection.add_section('quandl_mongodb1')
mongodb_connection.set('quandl_mongodb1', 'id', 'quandl_mongodb1')
mongodb_connection.set('quandl_mongodb1', 'ip1', 'mongodb://root:Xhmz372701@114.55.54.144:3717')
mongodb_connection.set('quandl_mongodb1', 'ip2', 'mongodb://root:Xhmz372701@114.55.54.144:3718')
mongodb_connection.set('quandl_mongodb1', 'db', 'QuandlDataCnFutures')

mongodb_connection.add_section('quandl_mongodb2')
mongodb_connection.set('quandl_mongodb2', 'id', 'quandl_mongodb2')
mongodb_connection.set('quandl_mongodb2', 'ip1', 'mongodb://root:Xhmz372701@114.215.252.135:3717')
mongodb_connection.set('quandl_mongodb2', 'ip2', 'mongodb://root:Xhmz372701@114.215.252.135:3718')
mongodb_connection.set('quandl_mongodb2', 'db', 'QuandlDataCnFutures')

mongodb_connection.add_section('quandl_mongodbi')
mongodb_connection.set('quandl_mongodbi', 'id', 'quandl_mongodbi')
mongodb_connection.set('quandl_mongodbi', 'ip1', 'mongodb://root:Xhmz372701@dds-bp1affea778ad1842.mongodb.rds.aliyuncs.com:3717,dds-bp1affea778ad1841.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-1401299')
mongodb_connection.set('quandl_mongodbi', 'ip2', 'mongodb://root:Xhmz372701@dds-bp1affea778ad1842.mongodb.rds.aliyuncs.com:3717,dds-bp1affea778ad1841.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-1401299')
mongodb_connection.set('quandl_mongodbi', 'db', 'QuandlDataCnFutures')

with open('mongodb_connection.ini', 'w') as f:
    mongodb_connection.write(f)