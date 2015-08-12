##############################################################################
## Copyright 2014, Microsoft.	All rights reserved
## Author: Jin Li
## Date: June. 2014                                                      
## Test to download remote image
##############################################################################
param(
	## Cluster name 
	[string] $cluster, 
	[string] $remoteDKVName, 
	[string] $localDirName, 
	[string] $uploadfile,
	[string] $uploadkey,
	[string] $uploadRemote,
	[string] $verbose,
	[string] $clientverbose,
	[bool] $startcluster = $true
)

Invoke-Expression ./config.ps1

if ($startcluster ) {
	$clusterLst = $cluster + ".lst"
	write-host "#################################################################################################################################################"
	write-host "#                                                    RESTART CLUSTER                                                                            #"
	write-host "#################################################################################################################################################"
	./killclient.ps1 $clusterLst
	./deployroot.ps1
	./startclient.ps1 $clusterLst
	## Wait for the cluster to start
	start-sleep -s 10
}

$clusterINF = $cluster + ".inf"
write-host "LocalDir=" $localDirName "RemoteDKV=" $remoteDKVName "Cluster=" $clusterINF
write-host "#################################################################################################################################################"
write-host "#                                                             Generate Random Vector Class 1000                                                 #"
write-host "#################################################################################################################################################"
$remote1000 = $remoteVector + "1000"
c:\OneNet\DistributedKMeans.exe -local vector1000.data -cluster $clusterINF -gen -in -num 100000 -class 1000 -var 0.1 -dim 80 -verbose $verbose -mapreduce -remote $remote1000
