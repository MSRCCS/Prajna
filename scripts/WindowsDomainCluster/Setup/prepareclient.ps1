##############################################################################
## Copyright 2016, Microsoft.	All rights reserved   
## Author: Jin Li
## Date: Feb. 2016                                                 
##############################################################################

# Enable Remote Exuectuion of Powershell script 
Set-ExecutionPolicy RemoteSigned -Force
Enable-PSRemoting -Force
Set-Item wsman:\localhost\client\trustedhosts * -Force
Enable-WSManCredSSP Client –DelegateComputer * -Force
Enable-WSManCredSSP Server -Force
Restart-Service WinRM -Force

# Openning firewalls used by daemon and containers 

New-NetFirewallRule -DisplayName "Allow Port 1000-1500" -Direction Inbound -LocalPort 1000-1500 -Protocol TCP -Action Allow

New-NetFirewallRule -Program ..\..\..\..\bin\Releasex64\Client\PrajnaClient.exe -Action Allow -Profile Private, Public, Domain -DisplayName "Allow PrajnaClient" -Description "Allow PrajnaClient" -Direction Outbound
