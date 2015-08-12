##############################################################################
## Copyright 2013, Microsoft.	All rights reserved  
## Author: Jin Li
## Date: Aug. 2013                                                    
## Copy files to a root folder to be deployed to client
##############################################################################
Invoke-Expression ./config.ps1

write-host $localSrcFolder
write-host $rootSrcFolder

robocopy $localSrcFolder'\src\Client\ ' $rootSrcFolder'\src\Client' /s /mir /XD Debug /XD Release 
robocopy $localSrcFolder'\src\CoreLib\ ' $rootSrcFolder'\src\CoreLib' /s /mir /XD bin /XD obj
robocopy $localSrcFolder'\src\ULib\ ' $rootSrcFolder'\src\ULib' /s /mir /XD bin /XD obj
robocopy $localSrcFolder'\src\Tools\ ' $rootSrcFolder'\src\Tools' /s /mir /XD bin /XD obj

write-host "Additional Deploy" $AdditionalDeploy
Invoke-Expression $AdditionalDeploy
