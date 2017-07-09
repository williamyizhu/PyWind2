ECHO "Daily Routine"

taskkill /f /im WBox.exe
taskkill /f /im wim.exe

$dd = Get-Date -format yyyyMMdd

$ff1 = "log\contract_active_" + $dd + ".txt"
$ff2 = "log\wind_dr_" + $dd + ".txt"
$ff3 = "log\mongodb_dr_" + $dd + ".txt"

ECHO "Run active_contract.py"
python .\contract_active.py > $ff1

ECHO "Run wind_dr.py, change wind_mongodb2 to wind_mongodb2i"
python .\wind_dr.py -m dd um -s trading -tf eod -cc y -d wind_mongodb2 > $ff2

ECHO "Run mongodb_dr.py, change wind_mongodb2 to wind_mongodb2i, change ctp_mongodb2 to ctp_mongodb2i"
python .\mongodb_dr.py -m um -d wind_mongodb2 > $ff3
python .\mongodb_dr.py -m um -d ctp_mongodb2 > $ff3

taskkill /f /im WBox.exe
taskkill /f /im wim.exe
