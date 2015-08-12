h:##############################################################################
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
write-host "#                                                    Distributed Crawling                                                                       #"
write-host "#################################################################################################################################################"
remove-item c:\OneNet\DownloadImageTest.log
# c:\OneNet\DistributedWebCrawler.exe -download $ParallelDownload -remote $uploadRemote -verbose $verbose -log c:\OneNet\DownloadImageTest.log -con -task 
c:\OneNet\DistributedWebCrawler.exe -download $ParallelDownload -remote $uploadRemote -verbose $verbose -log c:\OneNet\DownloadImage.log -con -task 
# c:\OneNet\DistributedWebCrawler.exe -download $ParallelDownload -remote $uploadRemote -verbose $verbose -log c:\OneNet\DownloadImage.log -con  
# c:\OneNet\Releasex64\DistributedWebCrawler.exe -download $ParallelDownload -remote $uploadRemote -verbose $verbose -log c:\OneNet\DownloadImage.log -con  
