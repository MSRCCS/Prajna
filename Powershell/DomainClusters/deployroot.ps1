##############################################################################
## Copyright 2013, Microsoft.	All rights reserved  
## Author: Jin Li
## Date: Aug. 2013                                                    
## Copy files to a root folder to be deployed to client
##############################################################################
Invoke-Expression ./config.ps1

write-host $localSrcFolder
write-host $rootSrcFolder

##############################################################################
## Force a build in $localSrcFolder

Push-Location -Path $localSrcFolder

$tmpfile = [System.IO.Path]::GetTempFileName()
& .\build.cmd D > $tmpfile 2>&1

if ($LASTEXITCODE -ne 0)
{
    Pop-Location
    throw "Fail to build! Please check build logs at $localSrcFolder"
}

Pop-Location


##############################################################################
write-host "Additional Deploy" $AdditionalDeploy
Invoke-Expression $AdditionalDeploy

robocopy $localSrcFolder'\bin\Debugx64\Client' c:\OneNet /s /R:0
write-host "Robocopy " $localSrcFolder'\samples\bin\Debugx64' c:\OneNet /s /R:0
robocopy $localSrcFolder'\samples\bin\Debugx64' c:\OneNet /s /R:0
# robocopy $localSrcFolder'\bin\Releasex64\Client' c:\OneNet\Releasex64 /s /R:0
# robocopy $localSrcFolder'\samples\bin\Releasex64' c:\OneNet\Releasex64 /s /R:0
robocopy $localSrcFolder'\bin\Debugx64\ ' $rootSrcFolder'\bin\Debugx64\' /s /mir
robocopy $localSrcFolder'\src\ ' $rootSrcFolder'\src\' /s /mir
robocopy $localSrcFolder $rootSrcFolder Prajna.sln

