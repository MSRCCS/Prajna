##############################################################################
## Copyright 2013, Microsoft.	All rights reserved
## Author: Jin Li
## Date: Oct. 2014                                                      
## Join related unittest 
##############################################################################
param(
	## Cluster name 
	[string] $cluster, 
	[string] $remoteDKVName, 
	[string] $localSaveDir, 
	[string] $verbose,
	[bool] $startcluster = $true
)

./config.ps1


if ($startcluster ) {
	$clusterLst = $cluster + ".lst"
	write-host "#############################################################################################################################"
	write-host "#                                                    RESTART CLUSTER                                                        #"
	write-host "#############################################################################################################################"
	write-host "Cluster LST=" $clusterLst
	./killclient.ps1 $clusterLst
	./deployroot.ps1
	./startclient.ps1 $clusterLst
	## Wait for the cluster to start
	start-sleep -s 10
}

$clusterINF = $cluster + ".inf"

write-host "LocalDir=" $localSaveDir "RemoteDKV=" $remoteDKVName 
write-host "#################################################################################################################################################"
write-host "#                                                    Cross Join Unittest																		#"
write-host "#################################################################################################################################################"
remove-item c:\OneNet\joinVector.log
c:\OneNet\DistributedKMeans.exe -dist 2 -remote $remoteVector -verbose $verbose -log c:\OneNet\joinVector.log -con
c:\OneNet\DistributedKMeans.exe -dist 1 -remote $remoteVector -verbose $verbose -log c:\OneNet\joinVector.log -con
