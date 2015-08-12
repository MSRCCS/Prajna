##############################################################################
## Increase the memory limit of the cluster
## Author: Jin Li
## Date: May. 2014                                                      
## Initialization Script
##############################################################################
param (
    [string] $machineName, 
    [string] $cluster, 
    [string] $clusterLst, 
    [string] $MaxMemInMB = 192000
)

Invoke-Expression .\parsecluster.ps1 

$cmd1 = 'cd WSMan:\localhost\ ;' + 
	'set-item Shell\MaxMemoryPerShellMB ' + $MaxMemInMB + ';' + 
	'set-item Plugin\Microsoft.PowerShell\Quotas\MaxMemoryPerShellMB ' + $MaxMemInMB + '; ' + 
	'Restart-Service winrm -Force'

$sb1=$executioncontext.InvokeCommand.NewScriptBlock( $cmd1 )
##$sb2=$executioncontext.InvokeCommand.NewScriptBlock( $cmd2 )
##$sb3=$executioncontext.InvokeCommand.NewScriptBlock( $cmd3 )

Invoke-Expression .\getcred.ps1

foreach ($mach in $machines  )
{
    write-host "Increase memory limit to " $MaxMemInMB " MB for machine " $mach
    Invoke-Command -ComputerName $mach -ScriptBlock $sb1 -Authentication CredSSP -Cred $cred
##    Invoke-Command -ComputerName $mach -ScriptBlock $sb2 -Authentication CredSSP -Cred $cred
##    Invoke-Command -ComputerName $mach -ScriptBlock $sb3 -Authentication CredSSP -Cred $cred
}