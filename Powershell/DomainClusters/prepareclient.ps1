##############################################################################
## Copyright 2013, Microsoft.	All rights reserved   
## Author: Jin Li
## Date: Aug. 2013                                                 
## Start a default SkyNet client
##############################################################################
Set-ExecutionPolicy RemoteSigned -Force
Enable-PSRemoting -Force
Set-Item wsman:\localhost\client\trustedhosts * -Force
Enable-WSManCredSSP Client –DelegateComputer * -Force
Enable-WSManCredSSP Server -Force
Restart-Service WinRM -Force
"select disk=1`r`n import" | diskpart
