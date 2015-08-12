##############################################################################
## Copyright 2015, Microsoft.	All rights reserved
## Author: Weirong Zhu
## Date: March. 2015                                                     
## Deploy PrajnaClient to an Azure VM
##############################################################################

param
(
    [Parameter(Mandatory = $true)]
    [string] $Name,

    [Parameter(Mandatory = $false)]
    [string] $ServiceName,

    [Parameter(Mandatory = $true)]
    [string] $ClusterInfo,

    # Source directory of Prajna
    # Assume the client binary is located at bin\Debugx64\Client
    # Assume the remote copy is located at bin\Debugx64\RemoteCopy
    [Parameter(Mandatory = $true)]
    [string] $source,

    # Local folder for dropping the binary
    [Parameter(Mandatory = $false)]
    [string] $bin = $env:LOCALAPPDATA + "\Prajna",

    # dest directory, the prajna client will be copied to bin\Debugx64\Client under $dest
    [Parameter(Mandatory = $true)]
    [string] $dest,            

    [Parameter(Mandatory = $false)]
    [PSCredential] $creds,

    [Parameter(Mandatory = $false)]
    [System.Management.Automation.Runspaces.PSSession] $session,

    # extra folders (other than "bin\Debugx64\Client". If it's a relative path, it needs to be under $source.
    [Parameter(Mandatory = $false)]
    [string[]] $extra,        
    
    # delete all the previous copies
    [Parameter(Mandatory = $false)]
    [string[]] $delete = $false,        

    # verbose level
    [Parameter(Mandatory = $false)]
    [string] $copyVerbose = "3",
	
	# When set to $false, when the $session is remove, the client is closed
	# When set to $true, when the $session is removed, the client remains running
    [Parameter(Mandatory = $false)]
    [bool] $background = $false,
    
    # the tag of the version deploy
    [Parameter(Mandatory = $false)]
    [string] $tag = ""
)

Set-StrictMode -Version Latest

function Test-Remote-Path
{
    Param
    (
        [Parameter(Mandatory = $true)]
        [System.Management.Automation.Runspaces.PSSession] $session,

        [Parameter(Mandatory = $true)]
        [string] $path
    )

    Invoke-Command -Session $session { Param($p) Test-Path $p } -Args $path
}

function Get-Remote-Current-Dir
{
    $result = Invoke-Command -Session $remoteSession {
        Param
        (
            $wildcard
        )

        $date = Get-Item $wildcard | 
                Select -ExpandProperty Name | 
                Select-String -Pattern "current-(.*).txt" -AllMatches | 
                % {$_.Matches} | 
                % {$_.Groups[1].Value }

        $date
    } -ArgumentList @($currentFileWildcard)

    $result
}

function Prajna-Remote-Copy
{
    Param
    (
        $copySourceDir, $copyDstDir
    )

    $remoteCurrentTarget = Get-Remote-Current-Dir
    Write-Host ("Date: " +  $remoteCurrentTarget)
    $remoteCopyPath = $localDeploy + "\" + $remoteCurrentTarget + $remoteCopyBinPath + "\" + $remoteCopyExe
    if (!(Test-Path $remoteCopyPath))
    {
        Write-Host ("Cannot find matching PrajnaRemoteCopy.exe: Directory '" + $remoteCopyPath + "' does not exist on local machine!")
        return $false
    }

    Write-Host ("Copy: start copy from '" + $copySourceDir + "' to '" + $copyDstDir + "'")
    # note: append ".cloudapp.net" to the end $Name", otherwise, it won't resolve
    $remoteCpCmd = $remoteCopyPath + " -s -in -cluster " + $ClusterInfo + " -node " + $Name + ".cloudapp.net -local " + $copySourceDir + " -remote " + $copyDstDir + " -hashdir " + $hashDir + " -con -verbose " + $copyVerbose
    Write-Host ("Copy Cmd: " + $remoteCpCmd)
    Invoke-Command {cmd /c $remoteCpCmd}
    Write-Host "Copy: completed"        

    if (!(Test-Remote-Path -path $copyDstDir -session $remoteSession))
    {
        Write-Host ("'" + $copyDstDir + "' does not exist, the copy failed!")            
        return $false
    }

    return $true
}

if (-not $ServiceName)
{
    $ServiceName = $Name
}

if (!(Test-Path $source))
{
    throw ("Directory '" + $source + "' does not exist!")
}

$clientBinPath = "\bin\Debugx64\Client";
$remoteCopyBinPath = "\bin\Debugx64\RemoteCopy";

$sourceClientDir = $source + $clientBinPath
$sourceRemoteCopyDir = $source + $remoteCopyBinPath

$localDeploy = $bin + "\_deploy"

# located under $dest
$hashDir = $dest + "\" + "_hash"

# the sym link points to the current deployment
$current = $dest + "\" + "_current"
$currentFileWildcard = $dest + "\current-*.txt"

# the dir to deploy the client
if ($tag -eq "")
{
    $tag = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd-HH-mm-ss")
}

$deployRootDir = $dest + "\" + $tag
$currentFile = $dest + "\current-" + $tag + ".txt"

# track the deployed version of PrajnaClient (does not track folders specified by -extra) and the corresponding PrajnaRemoteCopy
# In case the deployed PrajnaClient does not work, so that we need to use a prev version, we will need to have matching PrajnaRemoteCopy.exe
# Note: this script does not support rollback yet!
$localDeployRootDir = $localDeploy + "\" + $tag
$localDeployClientDir = $localDeployRootDir + $clientBinPath
$localDeployRemoteCopyDir = $localDeployRootDir + $remoteCopyBinPath


$deployClientDir = $deployRootDir + $clientBinPath

$clientCurrentPath = $current + "\bin\Debugx64\Client\PrajnaClient.exe" 
#$clientExtCurrentPath = $current + "\bin\Debugx64\Client\PrajnaClientExt.exe" 

$remoteCopyExe = "PrajnaRemoteCopy.exe"
$remoteCopyPath = $source + $remoteCopyBinPath + "\" + $remoteCopyExe

if (!(Test-Path $localDeployRootDir))
{
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
}


if (!(Test-Path $sourceClientDir))
{
    throw ("Directory '" + $sourceClientDir + "' does not exist!")
}

if (!(Test-Path $remoteCopyPath))
{
    throw ("Directory '" + $remoteCopyPath + "' does not exist!")
}

# get session
if (!$session)
{
    $remoteSession = .\getremotesession.ps1 -Name $Name -ServiceName $ServiceName -creds $creds
    Write-Host "Remote Session Created:"
    Write-Host ($remoteSession | Format-Table | Out-String)
}
else
{
    $remoteSession = $session
}

# perform local copy to snapshot the client and remotecopy
if (!(Test-Path $localDeployRootDir))
{
    if (!(Copy-Item $sourceClientDir $localDeployClientDir -Recurse -PassThru))
    {
        throw ("Local copy failed: from '" + $sourceClientDir + "' to '" + $localDeployClientDir + "'")
    }
    if (!(Copy-Item $sourceRemoteCopyDir $localDeployRemoteCopyDir -Recurse -PassThru))
    {
        throw ("Local copy failed: from '" + $sourceRemoteCopyDir + "' to '" + $localDeployRemoteCopyDir + "'")
    }
}

# check whether $dest, $current, PrajnaClient.exe
Write-Host ("Test if client '" + $clientCurrentPath + "' has not yet been deployed")
$firstDeploy = !(Test-Remote-Path -path $clientCurrentPath -session $remoteSession)
Write-Host ("Result: " + $firstDeploy)

if ($firstDeploy)
{
    # the first deployment, use vmcopy.ps1
    Write-Host ("The first deployment: start copy from '" + $localDeployClientDir + "' to '" + $deployClientDir + "'")
    .\vmcopy.ps1 -session $remoteSession -source $localDeployClientDir -dest $deployClientDir -Name $Name -ServiceName $ServiceName    
    Write-Host ("The first deployment: completed copy")
}
else
{
    # a new deployment, use PrajnaRemoteCopy.exe

    $isClientRunning = .\vmlaunch.ps1 -exe $clientCurrentPath -session $remoteSession -list $true

    if (!$isClientRunning)
    {
        Write-Host ("Client is not running: start '" + $clientCurrentPath + "'!")

        # make sure all client-ext was killed before launch the new client
        # .\vmlaunch.ps1 -exe $clientExtCurrentPath -session $remoteSession -kill $true

        $launchSuccess = .\vmlaunch.ps1 -background $background -exe $clientCurrentPath -session $remoteSession -argumentList @("-verbose", $clientverbose, "-mem", $memsize, "-homein", $homein, "-port", $port, "-jobport", $jobport)
        if ($launchSuccess)
        {
            Write-Host "Client is launched!"
        }
        else
        {
            throw "Failed to launch the client!"
        }
    }

    if ((Prajna-Remote-Copy -copySourceDir $localDeployClientDir -copyDstDir $deployClientDir) -eq $false)
    {
        Write-Host ("PrajnaRemoteCopy failed. Let's use vmcopy instead: start copy from '" + $localDeployClientDir + "' to '" + $deployClientDir + "'")
        .\vmcopy.ps1 -session $remoteSession -source $localDeployClientDir -dest $deployClientDir -Name $Name -ServiceName $ServiceName    
        Write-Host ("vmcopy: completed copy")
    }
}

if (!(Test-Remote-Path -path $deployClientDir -session $remoteSession))
{
    throw ("'" + $deployClientDir + "' does not exist, the copy failed!")
}

# link $current to $deployRootdir
if (Test-Remote-Path -path $current -session $remoteSession)
{
    Write-Host ("Symlink '" + $current + "' exists, remove it!")
    $rmdirCmd = "rmdir " + $current;    
    $res = Invoke-Command -Session $remoteSession {param($c, $d) cmd /c $c; Remove-Item $d} -ArgumentList @($rmdirCmd, $currentFileWildcard)
    Write-Host ("Remove symlink '" + $current + "': " + $res)
}

Write-Host ("Create Symlink from '" + $current + "' to '" + $deployRootDir + "'")
$mklinkCmd = "mklink /d " + $current + " " + $deployRootDir
$res = Invoke-Command -Session $remoteSession {param($mklink, $c) cmd /c $mklink; New-Item $c -type file} -ArgumentList @($mklinkCmd, $currentFile)
Write-Host ("Symlink created: " + $res)

# kill the client process if any
$killed = .\vmlaunch.ps1 -exe $clientCurrentPath -session $remoteSession -kill $true
if (!$killed)
{
     throw ("Failed to restart the client: cannot kill the original one")
}

# kill the ClientExt process if any
# $killed = .\vmlaunch.ps1 -exe $clientExtCurrentPath -session $remoteSession -kill $true
# if (!$killed)
#{
#     throw ("Failed to restart the client ext: cannot kill the original one")
#}

# launch the new client
$launched = .\vmlaunch.ps1  -background $background -exe $clientCurrentPath -session $remoteSession -argumentList @("-verbose", $clientverbose, "-mem", $memsize, "-homein", $homein, "-port", $port, "-jobport", $jobport)
if (!$launched)
{
    throw ("Failed to restart the client: cannot launch the new one")
}

Write-Host ("Success: the new client has started!")

# copy extra folders under $source other than the client
if ($extra)
{
    ForEach ( $extraDir in $extra ) 
    {        
        $isAbsolute = [System.IO.Path]::IsPathRooted($extraDir)
        $extraSourceDir = $extraDir
     
        if (!$isAbsolute)
        {
            $extraSourceDir = $source + "\" + $extraDir
        }

        Write-Host ("Extra dir to copy: " + $extraSourceDir)

        if (!(Test-Path $extraSourceDir))
        {
            Write-Host ("'" + $extraSourceDir + "' does not exist");
            continue
        }        

        if (!$isAbsolute)
        {
            $extraDeployDir = $deployRootDir + "\" + $extraDir
        }
        else
        {
            # if given absolute path, 
            $extraDirInfo = New-Object System.IO.DirectoryInfo($extraDir)
            $extraDeployDir = $deployRootDir + "\" + $extraDirInfo.Name
        }
        
        Prajna-Remote-Copy -copySourceDir $extraSourceDir -copyDstDir $extraDeployDir
    }
}

if ($delete -eq $true)
{
    #delete old versions

    # kill the client process if any
    $killed = .\vmlaunch.ps1 -exe $clientCurrentPath -session $remoteSession -kill $true
    if (!$killed)
    {
        throw ("Failed to restart the client: cannot kill the original one")
    }

    # kill the client ext process if any
    # $killed = .\vmlaunch.ps1 -exe $clientExtCurrentPath -session $remoteSession -kill $true
    #if (!$killed)
    #{
    #    throw ("Failed to restart the client ext: cannot kill the original one")
    #}


    Start-Sleep -Seconds 2

    #note: this will delete _hash directory too
    Write-Host ("Delete old folders!")    
    Invoke-Command -Session $remoteSession { 
        Param($destRoot, $curLink, $curDir) 

        Get-ChildItem -Path $destRoot | 
        Select -ExpandProperty FullName | 
        Where {$_ -ne $curLink -and $_ -ne $curDir -and $_ -notlike  "*.txt" } | 
        Remove-Item -Recurse -Force 
   } -ArgumentList @($dest, $current, $deployRootDir)      

   # launch the new client
   $launched = .\vmlaunch.ps1 -background $background -exe $clientCurrentPath -session $remoteSession -argumentList @("-verbose", $clientverbose, "-mem", $memsize, "-homein", $homein, "-port", $port, "-jobport", $jobport)
   if (!$launched)
   {
       throw ("Failed to restart the client: cannot launch the new one")
   }

   # delete previous versions in local
   # $curDate = Get-Remote-Current-Dir
   # $folderToKeep = $localDeploy + "\" + $curDate
   
   # Get-ChildItem -Path $localDeploy |
   # Select -ExpandProperty FullName | 
   # Where {$_ -ne $folderToKeep} |
   # Remove-Item -Recurse -Force
}

# end remote session
if (!$session)
{
    Remove-PSSession -session $remoteSession
}

return $true
