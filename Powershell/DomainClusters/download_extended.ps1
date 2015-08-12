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
	[string] $verbse, 
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
write-host "#                                                                          Write URL                                                            #"
write-host "#################################################################################################################################################"
c:\OneNet\DKVCopy.exe -cluster $clusterINF -upload \\research\root\share\jinl\Seattle.FlickrImageList.txt -uploadKey $uploadkey -remote ImBase\Seattle.FlickrImageList.Timing -verbose $verbose -rep 3 -slimit 100 -log c:\OneNet\writeURL.log -con

write-host "#################################################################################################################################################"
write-host "#                                                    Download                                                                                 #"
write-host "#################################################################################################################################################"
# Crashed, failed to complete
c:\OneNet\DistributedWebCrawler.exe -download $ParallelDownload -remote ImBase\Seattle.FlickrImageList.Timing -verbose $verbose -log c:\OneNet\ExtendedDownload1.log -con -task 
c:\OneNet\DistributedWebCrawler.exe -download $ParallelDownload -remote ImBase\Seattle.FlickrImageList.Timing -verbose $verbose -log c:\OneNet\ExtendedDownload2.log -con
# The debug version failed repeatedly, move to download_extendedasync for test. 
#c:\OneNet\DistributedWebCrawler.exe -download $ParallelDownload -remote ImBase\Seattle.FlickrImageList.Timing -verbose $verbose -log c:\OneNet\ExtendedDownload.log -con -task 
# The debug version fails
#c:\OneNet\DistributedWebCrawler.exe -download $ParallelDownload -remote ImBase\Seattle.FlickrImageList.Timing -verbose $verbose -log c:\OneNet\ExtendedDownload.log -con 
