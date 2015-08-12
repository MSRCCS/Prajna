##############################################################################
## Copyright 2013, Microsoft.	All rights reserved     
## Author: Jin Li
## Date: Aug. 2013                                                 
## Kill a SkyNet client
##############################################################################
param(
	## Machine name to deploy
    [string] $machineName, 
    [string] $clusterlst, 
    [string] $cluster,
    [string] $killpattern,
    [PSCredential] $cred
)

Invoke-Expression ./config.ps1
Invoke-Expression ./parsecluster.ps1 

if ( !$targetExe )
{
	$targetExe = 'PrajnaClient.exe'
}

$filter = "name='$targetExe'"
if ( -Not $killpattern ) {
$killpattern = "*$targetSrcDir*"
}

Invoke-Expression ./getcred.ps1

##$machineName
##$filter
foreach ( $mach in $machines )
{
	write-host "Kill Process on machine $mach with Path " $killpattern
	$Processes = Get-WmiObject -Class Win32_Process -ComputerName $mach -Credential $cred | Where {$_.ExecutablePath -like $killpattern}
	foreach ($process in $Processes) 
	{
		$ret = $process.Terminate()
		$processid = $process.handle
		write-host "The process $targetExe on machine $mach ($processid) terminates with value $ret"
	}
	write-host "The process on machine $mach terminates"
}
