#######################################################################
## Copyright 2013, Microsoft.	All rights reserved    
## Author: Jin Li                                                  
## Enable firewall on a particular client
##############################################################################
param(
	## Machine name to deploy
	[string] $machineName, 
    [string] $clusterLst, 
    [string] $cluster
)

Invoke-Expression .\config.ps1
Invoke-Expression .\parsecluster.ps1
Invoke-Expression .\getcred.ps1


## $cmd1 = 'New-NetFirewallRule -Enabled true -Program "C:\PrajnaSource\bin\Debugx64\Client\PrajnaClient.exe” -Action Allow -Profile Private, Public, Domain -DisplayName "Allow PrajnaClient" -Description "Allow PrajnaClient" -Direction Inbound -LocalPort 1082 -Protocol TCP'
## $cmd1 = 'Get-NetFirewallProfile | Set-NetFirewallProfile –Enabled False'
$cmd1 = 'New-NetFirewallRule -DisplayName "Allow Port 1000-1500" -Direction Inbound -LocalPort 1000-1500 -Protocol TCP -Action Allow'
$sb1=$executioncontext.InvokeCommand.NewScriptBlock( $cmd1 )
## $cmd2 = 'New-NetFirewallRule -Program "C:\PrajnaSource\bin\Debugx64\Client\PrajnaClient.exe”” -Action Allow -Profile Private, Public, Domain -DisplayName "Allow PrajnaClient" -Description "Allow PrajnaClient" -Direction Outbound'
## $sb2=$executioncontext.InvokeCommand.NewScriptBlock( $cmd2 )
foreach ($mach in $machines ) 
{
	Invoke-Command -ComputerName $mach -ScriptBlock $sb1 -Credential $cred -Authentication CredSSP
	write-host $cmd1 "to machine " $mach
}


