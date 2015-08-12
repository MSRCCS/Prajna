##############################################################################
## Copyright 2013, Microsoft.	All rights reserved
## Author: Jin Li
## Date: Apr. 2014                                                      
##############################################################################
param(
	## Machine name to deploy
	[string] $cluster,
	[string] $clusterLst, 
	[string] $machineName, 
	[string] $verbose,
	[string] $clientverbose
)

Invoke-Expression ./config.ps1
Invoke-Expression ./parsecluster.ps1

write-host "#################################################################################################################################################"
write-host "#                                                    RESTART CLUSTER                                                                            #"
write-host "#################################################################################################################################################"

./killclient.ps1 -machineName $machineName -clusterLst $clusterLst -cluster $cluster
./deployroot.ps1
./startclient.ps1 -machineName $machineName -clusterLst $clusterLst -cluster $cluster -clientverbose $clientverbose
