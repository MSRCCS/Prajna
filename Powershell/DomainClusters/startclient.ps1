##############################################################################
## Copyright 2013, Microsoft.	All rights reserved
## Author: Jin Li
## Date: Aug. 2013                                                      
## Start a default PrajnaClient
## If a large # of PrajnaClient couldn't be started, you may want to Remote 
## Desktop to a Prajna machine and PrajnaClient.exe
##############################################################################
param(
	## Machine name to deploy
    [string] $machineName,
    [string] $clusterLst, 
    [string] $cluster, 
    [string] $clientverbose,
    [PSCredential] $cred,
    [bool] $copyFiles = $true
)

Invoke-Expression .\config.ps1
Invoke-Expression .\parsecluster.ps1

Function Copy-FolderPermission{
param([Parameter(Mandatory=$true)][string]$srcdir,
[Parameter(Mandatory=$true)][string]$dstdir,
[Parameter(Mandatory=$true)]$cred )

if (-not (Test-Path $srcdir)) {
	Write-Error "Source folder is not valid"
	return $false
}

if (-not (Test-Path $dstdir)) {
	New-Item $dstdir -type directory -Credential $cred
}
 
}

if (-Not $clientAdditionalParam) {
	# Log folder at remote machine
	$clientAdditionalParam = ''
}


$targetExe1 = 'c:'+$targetSrcDir + '\bin\Debugx64\Client\PrajnaClient.exe'
$cmd2 = '$env:username; $env:userdomain; $env:computername; robocopy ' + $rootSrcFolder +' ' + $targetSrcDir + ' /s /mir; '
$cmd3 = $targetExe1 + ' -mem ' +$memsize+ ' -verbose '+$clientverbose + ' -dirlog '+$logdir + ' -homein ' + $homein + ' -port ' + $port + ' -jobport '+$jobport + $clientAdditionalParam 2>PrajnaClient_err.log


#$sb1=$executioncontext.InvokeCommand.NewScriptBlock( $cmd1 )
$sb2=$executioncontext.InvokeCommand.NewScriptBlock( $cmd2 )
$sb3=$executioncontext.InvokeCommand.NewScriptBlock( $cmd3 )

Invoke-Expression .\getcred.ps1 

if ( $targetSrcDir ) {
	$jobstate = @{}
	foreach ($mach in $machines ) 
	{
	##	Invoke-Command -ComputerName $mach -ScriptBlock $sb1 
	##	write-host "Deploy folder " $rootFolder "to machine " $mach
	##	Invoke-Command -ComputerName $mach -ScriptBlock $sb2 
	##	write-host "Deploy folder " $rootSrcFolder "to machine " $mach
		if ($copyFiles) {
			robocopy $rootSrcFolder \\$mach\c$\$targetSrcDir /s /mir /R:1
		}
		write-host "Launch job" $targetExe1 "on machine " $mach "with command " $cmd3
	## -Authentication CredSSP -Credential ${cred} -Authentication Negotiate -Authentication NegotiateWithImplicitCredential
		$state = Invoke-Command -ComputerName $mach -ScriptBlock $sb3 -AsJob -EnableNetworkAccess -Authentication CredSSP -Cred $cred
		$jobstate.Set_Item($mach, $state)
	}
	# start-sleep -s 5
	foreach ($mach in $machines ) 
	{
		$state = $jobstate.Get_Item( $mach )
		write-host "Machine " $mach " state " $state
	}
	Write-Output $jobstate
}
else
{
	write-host "Variable targetSrcDir hasn't been set, deployment failed..........."
}
