import configparser
import os

print('config file location', os.path.abspath(os.path.dirname(__file__)))

# -------------- Wind sector id --------------
wind_sector = configparser.ConfigParser()

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

# both trading and historical
wind_sector.add_section('cfe')
wind_sector.set('cfe', 'trading', 'a599010101000000')
wind_sector.set('cfe', 'historical', '1000009643000000')
wind_sector.add_section('shf')
wind_sector.set('shf', 'trading', 'a599010201000000')
wind_sector.set('shf', 'historical', '1000009644000000')
wind_sector.add_section('dce')
wind_sector.set('dce', 'trading', 'a599010301000000')
wind_sector.set('dce', 'historical', '1000009645000000')
wind_sector.add_section('czc')
wind_sector.set('czc', 'trading', 'a599010401000000')
wind_sector.set('czc', 'historical', '1000009646000000')

# trading only
wind_sector.add_section('cfe-t')
wind_sector.set('cfe-t', 'trading', 'a599010101000000')
wind_sector.add_section('shf-t')
wind_sector.set('shf-t', 'trading', 'a599010201000000')
wind_sector.add_section('dce-t')
wind_sector.set('dce-t', 'trading', 'a599010301000000')
wind_sector.add_section('czc-t')
wind_sector.set('czc-t', 'trading', 'a599010401000000')

# historical only
wind_sector.add_section('cfe-h')
wind_sector.set('cfe-h', 'historical', '1000009643000000')
wind_sector.add_section('shf-h')
wind_sector.set('shf-h', 'historical', '1000009644000000')
wind_sector.add_section('dce-h')
wind_sector.set('dce-h', 'historical', '1000009645000000')
wind_sector.add_section('czc-h')
wind_sector.set('czc-h', 'historical', '1000009646000000')

# active contract only
wind_sector.add_section('active')
wind_sector.set('active', 'cn', '1000015510000000')

# continuous contract only
wind_sector.add_section('continuous')
wind_sector.set('continuous', 'cn', 'a599020600000000')

# continuous contract only, arranged by same months
wind_sector.add_section('continuous2')
wind_sector.set('continuous2', 'cn', '1000019227000000')

with open('wind_sector.ini', 'w') as f:
    wind_sector.write(f)

# -------------- Quandl sector id --------------
quandl_sector = configparser.ConfigParser()

quandl_sector.add_section('account')
quandl_sector.set('account', 'id', 'william.yizhu@live.com')
quandl_sector.set('account', 'api_key', 'zbJDcWPAfgVRVFe_H2uN')

with open('quandl_sector.ini', 'w') as f:
    quandl_sector.write(f)
