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
write-host "#                                                    Download Images                                                                            #"
write-host "#################################################################################################################################################"
$downloadDKV = "ImBase\Seattle.FlickrImageList.download"
$localDownloadDir = $downloadDir+"\\"+$uploadRemote+"1"
$exe = $LocalExeFolder+"\\PrajnaCopy.exe"
write-host $exe
write-host $downloadDKV 
write-host $localDownloadDir
remove-item c:\OneNet\VerifyDownloadExtended.log
c:\OneNet\DKVCopy.exe -remote $downloadDKV -local $localDownloadDir -verbose $verbose -out -log c:\OneNet\VerifyDownloadExtended.log -con





