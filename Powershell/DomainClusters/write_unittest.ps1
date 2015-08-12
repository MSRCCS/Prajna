##############################################################################
## Copyright 2013, Microsoft.	All rights reserved
## Author: Jin Li
## Date: Apr. 2014                                                      
## A series of test to verify the write path of the code
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

#remove-item c:\OneNet\writeDKV.log
#remove-item c:\OneNet\writeURL.log
#remove-item c:\OneNet\writeVector.log

$clusterINF = $cluster + ".inf"
write-host "LocalDir=" $localDirName "RemoteDKV=" $remoteDKVName "Cluster=" $clusterINF
write-host "#################################################################################################################################################"
write-host "#                                                    WRITE FILE                                                                                 #"
write-host "#################################################################################################################################################"
c:\OneNet\DKVCopy.exe -cluster $clusterINF -localdir $localDirName -remote $remoteDKVName -in -rep 3 -balancer 1 -verbose $verbose -rec -log c:\OneNet\writeDKV.log -con

write-host "#################################################################################################################################################"
write-host "#                                                                          Write URL                                                            #"
write-host "#################################################################################################################################################"
c:\OneNet\DistributedWebCrawler.exe -cluster $clusterINF -upload $uploadfile -uploadKey $uploadkey -remote $uploadRemote -verbose $verbose -rep 3 -slimit 100 -log c:\OneNet\writeURL.log -con

write-host "#################################################################################################################################################"
write-host "#                                                             Generate Random Vector                                                            #"
write-host "#################################################################################################################################################"
#remove-item c:\OneNet\writeVector.log
c:\OneNet\DistributedKMeans.exe -cluster $clusterINF -local vector.data -gen -in -remote $remoteVector -num 100000 -class 3 -var 0.1 -dim 80 -noise 0.01 -verbose $verbose -log c:\OneNet\writeVector.log -con
#type c:\OneNet\writeVector.log
write-host "#################################################################################################################################################"
write-host "#                                                             Generate Random Vector Class 1000                                                 #"
write-host "#################################################################################################################################################"
$remote1000 = $remoteVector + "1000"
write-host "MapReduce Remove DKV:" $remote1000
#remove-item c:\OneNet\writeVector1000.log
c:\OneNet\DistributedKMeans.exe -local vector1000.data -cluster $clusterINF -gen -in -num 100000 -class 1000 -var 0.1 -dim 80 -verbose $verbose -mapreduce -remote $remote1000 -log c:\OneNet\writeVector1000.log -con
#type c:\OneNet\writeVector1000.log
