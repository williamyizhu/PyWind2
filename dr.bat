echo wind_dr.py, change wind_mongodb2 to wind_mongodb2i

taskkill /f /im WBox.exe
taskkill /f /im wim.exe

python .\contract_active.py > log_contract_active.txt

python .\wind_dr.py -m dd um -s trading -tf eod -cc y -d wind_mongodb2 > log_wind_dr.txt

rem python .\mongodb_dr.py > log_mongodb_dr.txt

taskkill /f /im WBox.exe
taskkill /f /im wim.exe