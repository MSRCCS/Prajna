##############################################################################
## Copyright 2013, Microsoft.	All rights reserved
## Author: Jin Li
## Date: Apr. 2014                                                      
## A series of test to verify the read path of the code
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

if ((Test-Path $localSaveDir)) {
	write-host "Remove Directory " $localSaveDir
	remove-item -recurse -force $localSaveDir
}

$clusterINF = $cluster + ".inf"
#remove-item c:\OneNet\readDKV.log
#remove-item c:\OneNet\foldDKV.log
#remove-item c:\OneNet\readVector.log
#remove-item c:\OneNet\sortVector.log
#remove-item c:\OneNet\AnalysisLog.log

write-host "LocalDir=" $localSaveDir "RemoteDKV=" $remoteDKVName 
write-host "#################################################################################################################################################"
write-host "#                                                    TESTING READ                                                                               #"
write-host "#################################################################################################################################################"
c:\OneNet\DKVCopy.exe -cluster $clusterINF -localdir $localSaveDir -remote $remoteDKVName -out -verbose $verbose -log c:\OneNet\readDKV.log -con
write-host "#################################################################################################################################################"
write-host "#                                                    TESTING FOLD                                                                               #"
write-host "#################################################################################################################################################"
c:\OneNet\DKVCopy.exe -cluster $clusterINF -localdir $localSaveDir -remote $remoteDKVName -out -verbose $verbose -spattern  ".*" -hash -log c:\OneNet\foldDKV.log -con
write-host "#################################################################################################################################################"
write-host "#                                                    TESTING Random Vector                                                                      #"
write-host "#################################################################################################################################################"
c:\OneNet\DistributedKMeans.exe -cluster $clusterINF -local vector.data -gen -out -remote $remoteVector -verbose $verbose -log c:\OneNet\readVector.log -con
write-host "#################################################################################################################################################"
write-host "#                                                    TESTING Random Vector (additional noise)                                                   #"
write-host "#################################################################################################################################################"
$remoteVector1 = $remoteVector + "_NOISE"
$remoteVectors = $remoteVector + "," + $remoteVector1
c:\OneNet\DistributedKMeans.exe -cluster $clusterINF -local vector.data -gen -out -remote $remoteVector1 -verbose $verbose -log c:\OneNet\readVector.log -con
write-host "#################################################################################################################################################"
write-host "#                                                    TESTING DKV Union						                                                   #"
write-host "#################################################################################################################################################"
c:\OneNet\DistributedKMeans.exe -cluster $clusterINF -local vector.data -gen -out -remotes "$remoteVectors" -verbose $verbose -log c:\OneNet\readVector.log -con
write-host "#################################################################################################################################################"
write-host "#                                                    TESTING read of MapReduce Result (1000  noise)                                             #"
write-host "#################################################################################################################################################"
$remote1000 = $remoteVector + "1000"
$remote1000MR = $remote1000 + "_MapReduce"
write-host "Remove DKV " $remote1000MR
c:\OneNet\DistributedKMeans.exe -local vector1000.data -cluster $clusterINF -verify -out -verbose $verbose -remote $remote1000MR -log c:\OneNet\readVector.log -con
write-host "#################################################################################################################################################"
write-host "#                                                    TESTING Sort							                                                    #"
write-host "#################################################################################################################################################"
c:\OneNet\DistributedSort.exe -sort -num 1000 -dim 100000 -cluster $clusterINF  -verbose $verbose -nump 80 -log c:\OneNet\sortVector.log -con
#write-host "#################################################################################################################################################"
#write-host "#                                                    TESTING Distributed Log Analysis                                                           #"
#write-host "#################################################################################################################################################"
c:\OneNet\DistributedLogAnalysis.exe -num 4 -reg "!!! Error !!!,0,0" -reg "!!! Warning !!!,0,0" -reg "!!! Exception !!!,0,0" -dir "C:\OneNet\Log" -rec -cluster $clusterINF -verbose $verbose -log c:\OneNet\AnalysisLog.log -con
#write-host "#################################################################################################################################################"
#write-host "#                                                    TESTING CountTag                                                                          #"
#write-host "#################################################################################################################################################"
#remove-item c:\OneNet\CountTag.log
$tagKey = [int] $uploadkey + 1
c:\OneNet\DistributedWebCrawler.exe -remote $uploadRemote -counttag $tagKey -verbose $verbose -con -log c:\OneNet\CountTag.log 
