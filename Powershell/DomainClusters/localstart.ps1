##############################################################################
## Copyright 2013, Microsoft.	All rights reserved
## Author: Jin Li
## Date: Jun. 2014                                                      
## Start a default PrajnaClient Locally
## You should:
##		 Kill all active client by runnling .\killclient.ps1 _Cluster_
##		 Deploy current executable by .\deployroot.ps1
##############################################################################
Invoke-Expression ./config.ps1

robocopy $rootSrcFolder c:$targetSrcDir /s /mir /R:1
$targetExe = 'c:'+$targetSrcDir + '\bin\Debugx64\Client\PrajnaClient.exe'
&$targetExe -mem $memsize -verbose 6 -dirlog $logdir -homein $homein -port $port -jobport $jobport -con
