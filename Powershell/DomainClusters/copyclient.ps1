##############################################################################
## Copyright 2015, Microsoft.	All rights reserved     
## Author: Sanjeev Mehrotra
## copy files to cluster
##############################################################################
param(
	## Machine name to deploy
    [string] $machineName, 
    [string] $clusterlst, 
    [string] $cluster
)

Invoke-Expression ./config.ps1
Invoke-Expression ./parsecluster.ps1 

Invoke-Expression ./deployroot.ps1
if ( $targetSrcDir ) 
{
	foreach ($mach in $machines ) 
	{
		robocopy $rootSrcFolder \\$mach\c$\$targetSrcDir /s /mir /R:1
	}
}
else
{
	write-host "Variable targetSrcDir hasn't been set, deployment failed..........."
}
