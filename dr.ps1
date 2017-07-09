$d0 = Get-Date
Write-Host $d0 "Daily Routine"

taskkill /f /im WBox.exe
taskkill /f /im wim.exe

$dd = Get-Date -format yyyyMMdd

$d0 = Get-Date
Write-Host $d0 "Run active_contract.py"
$ff = "log\contract_active_" + $dd + ".txt"
python .\contract_active.py > $ff

$d0 = Get-Date
Write-Host $d0 "Run wind_dr.py, change wind_mongodb2 to wind_mongodb2i"
$ff = "log\wind_dr_" + $dd + ".txt"
python .\wind_dr.py -m dd um -s trading -tf eod -cc y -d wind_mongodb2 > $ff

$d0 = Get-Date
Write-Host $d0 "Run mongodb_dr.py for db=WindDataCnFutures, change wind_mongodb2 to wind_mongodb2i"
$ff = "log\mongodb_dr_wind_" + $dd + ".txt"
python .\mongodb_dr.py -m um -d wind_mongodb2 > $ff

$d0 = Get-Date
Write-Host $d0 "Run mongodb_dr.py for db=CtpData, change ctp_mongodb2 to ctp_mongodb2i"
$ff = "log\mongodb_dr_ctp_" + $dd + ".txt"
python .\mongodb_dr.py -m um -d ctp_mongodb2 > $ff

taskkill /f /im WBox.exe
taskkill /f /im wim.exe

$d0 = Get-Date
Write-Host $d0 "Daily Routine Finished"