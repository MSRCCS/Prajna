##############################################################################
## Copyright 2015, Microsoft.	All rights reserved
## Author: Weirong Zhu
## Date: March. 2015                                                     
## Batch execution of a command against multiple VMs in Azure
##############################################################################

param
(
    [Parameter(Mandatory = $true)]
    [string] $Csv,

    # Source directory of Prajna
    # Assume the client binary is located at bin\Debugx64\Client
    # Assume the remote copy is located at bin\Debugx64\RemoteCopy
    [Parameter(Mandatory = $true)]
    [string] $Source,

    # dest directory, the prajna client will be copied to bin\Debugx64\Client under $dest
    [Parameter(Mandatory = $true)]
    [string] $Dest,       

    [Parameter(Mandatory = $false)]
    [string] $User = "imhub",

    [Parameter(Mandatory = $false)]
    [string] $Password,

    [Parameter(Mandatory = $false)]
    [string] $Subscription = "Windows Azure Internal Consumption",

    # The directory where the cluster INF files locates
    # The script assume a VM's Inf file is located under the specified folder, and the file name is <VM's Name>.inf
    [Parameter(Mandatory = $false)]
    [string] $InfDir,

    # delete all the previous copies
    [Parameter(Mandatory = $false)]
    [string[]] $Delete = $false,   

    # Local folder for dropping the binary
    [Parameter(Mandatory = $false)]
    [string] $Bin = $env:LOCALAPPDATA + "\Prajna",

    [Parameter(Mandatory = $false)]
    [string] $Config = ".\config.ps1"
)

##############################################################################
## Force a build in $source

Push-Location -Path $source

$tmpfile = [System.IO.Path]::GetTempFileName()
& .\build.cmd D > $tmpfile 2>&1

if ($LASTEXITCODE -ne 0)
{
    Pop-Location
    throw "Fail to build! Please check build logs at $source"
}

Pop-Location
##############################################################################    
## Get the to be deployed copy ready for all nodes

$clientBinPath = "\bin\Debugx64\Client";
$remoteCopyBinPath = "\bin\Debugx64\RemoteCopy";

$sourceClientDir = $source + $clientBinPath
$sourceRemoteCopyDir = $source + $remoteCopyBinPath

$localDeploy = $Bin + "\_deploy"
$tag = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd-HH-mm-ss")
$localDeployRootDir = $localDeploy + "\" + $tag

$localDeployClientDir = $localDeployRootDir + $clientBinPath
$localDeployRemoteCopyDir = $localDeployRootDir + $remoteCopyBinPath

if (!(Copy-Item $sourceClientDir $localDeployClientDir -Recurse -PassThru))
{
    throw ("Local copy failed: from '" + $sourceClientDir + "' to '" + $localDeployClientDir + "'")
}
if (!(Copy-Item $sourceRemoteCopyDir $localDeployRemoteCopyDir -Recurse -PassThru))
{
    throw ("Local copy failed: from '" + $sourceRemoteCopyDir + "' to '" + $localDeployRemoteCopyDir + "'")
}
##############################################################################

if ($delete -eq $true)
{
    $cmd = "$config ; .\deployvm.ps1 -Name `$Name -source $Source -dest $Dest -ClusterInfo `$Inf -session `$Session -background `$true -tag $tag -delete `$true; Start-Sleep 2"
}
else
{
    $cmd = "$config ; .\deployvm.ps1 -Name `$Name -source $Source -dest $Dest -ClusterInfo `$Inf -session `$Session -background `$true -tag $tag -delete `$false; Start-Sleep 2"
}

.\Batch-Remote-Exe.ps1 -Csv $Csv -InfDir $InfDir -Cmd $cmd -User $User -Password $Password -Subscription $Subscription
