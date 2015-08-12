# Copy files from local directory to azure VM using PowerShell
# Usage
#     pscopyvm credential soruce destURI destination [options]

param
(
    [Parameter(Mandatory = $true)]
    [string] $source,

    [Parameter(Mandatory = $true)]
    [string] $dest,

    [Parameter(Mandatory = $false)]
    [string] $Name,

    [Parameter(Mandatory = $false)]
    [string] $ServiceName,

    [Parameter(Mandatory = $false)]
    [PSCredential] $creds,

    [Parameter(Mandatory = $false)]
    [System.Management.Automation.Runspaces.PSSession] $session,

    [Parameter(Mandatory = $false)]
    [bool] $overwriteOnlyNew = $true,

    # reverse direction of copy to receive data from remote end instead
    [Parameter(Mandatory = $false)]
    [bool] $reverse = $false
)

Set-StrictMode -Version Latest

# Remote script blocks
# delete destination and create directory if needed
$scriptDelete =
{
    param([string] $destR, [bool] $overwriteR, [bool] $overwriteOnlyNewR,
          [System.DateTime] $srcModTime)
    Set-StrictMode -Version Latest
    #Write-Host "overwrite: $overwriteR"
    #Write-Host "overwriteOnlyNew: $overwriteOnlyNewR"
    if (Test-Path $destR)
    {
        if ((-not $overwriteR) -and (-not $overwriteOnlyNewR))
        {
            Write-Host "$destR exists, not overwriting"
            return $true
        }
        if ((-not $overwriteR) -and $overwriteOnlyNewR)
        {
            $destModTime = [IO.File]::GetLastWriteTimeUtc($destR)
            if ($destModTime.Equals($srcModTime))
            {
                Write-Host "$destR exists with same mod time, not overwriting"
                return $true
            }
            else
            {
                Write-Host "$destR exists but with differing mod time:"
                Write-Host "$srcModTime"
                Write-Host "$destModTime"
            }
        }
        Write-Host "$destR exists, deleting"
        Remove-Item $destR
    }
        
    # create destination directory if does not exist
    $destDir = Split-Path -Path $destR -Parent
    if (-not (Test-Path $destDir))
    {
        New-Item -ItemType Directory -Force -Path $destDir
    }
    
    return $false
}
    
function Receive-File
{
    param
    (
        [Parameter(Mandatory = $true)]
        [string] $source, # remote
        
        [Parameter(Mandatory = $true)]
        [string] $dest, # local
        
        [Parameter(Mandatory = $true)]
        [System.Management.Automation.Runspaces.PSSession] $session,
        
        # ovewrite regardless of mod times
        [Parameter(Mandatory = $true)]
        [bool] $overwrite,
        
        # overwrite only if source newer than dest
        [Parameter(Mandatory = $true)]
        [bool] $overwriteOnlyNew
    )
    Write-Host "Sending $source to $dest, overwrite: $overwrite, overwriteOnlyNew: $overwriteOnlyNew"

    $readFileLen =
    {
        param ([string] $source)
        (Get-Item $source).Length
    }

    $srcModTime = Invoke-Command -Session $session `
                  { [IO.File]::GetLastWriteTimeUtc($args[0]) } -ArgumentList $source
    $abort = Invoke-Command $scriptDelete `
             -ArgumentList $dest, $overwrite, $overwriteOnlyNew, $srcModTime
    if ($abort -eq $true)
    {
        Write-Host "Ignored file $source"
        return
    }

    # no chunking, chunking adds to complexity
    $readFile =
    {
        param ([string] $source)
        $contents = [IO.File]::ReadAllBytes((Get-Item $source).FullName)
        return $contents
    }

    $fileContents = Invoke-Command -Session $session $readFile -ArgumentList $source
    #Write-Host "FileContents: $fileContents"
    [IO.File]::WriteAllBytes($dest, $fileContents)

    # chunking-way
    #
    # Removing seek makes it work okay
    # So chunking is not an option on file receive
    #
    #$readFileChunk =
    #{
    #    param ([string] $source, [int] $offset, [int] $len)
    #    $sourceItem = Get-Item $source
    #    $file = [IO.File]::OpenRead($sourceItem.FullName)
    #    # need to assign output of seek, otherwise gets returned in $contents
    #    $position = $file.Seek($offset, "Begin")
    #    # implement seek
    #    #$toSeek = $offset
    #    #$contents = New-Object byte[] $len
    #    #while ($toSeek -gt 0)
    #    #{
    #    #    $toRead = $len
    #    #    if ($toSeek -lt $toRead) { $toRead = $toSeek }
    #    #    $read = $file.Read($contents, 0, $toRead)
    #    #    $toSeek -= $read
    #    #}
    #    # now read actual contents
    #    $read = $file.Read($contents, 0, $len)
    #    Write-Host "Read: $read\nLen:$len"
    #    if ($read -lt $len) { [Array]::Resize([ref]$contents, $read) }
    #    $file.Close()
    #    Write-Host "BBBContents: $contents"
    #    Write-Host ("BBBLen: "+$contents.Length)
    #    return $contents
    #}
    #
    #$fileLen = Invoke-Command -Session $session $readFileLen -ArgumentList $source
    #Write-Host "Len: $fileLen Dest: $dest"
    #$file = [IO.File]::Open($dest, "OpenOrCreate")
    #$pos = 0
    #while ($true)
    #{
    #    $bufferSize = 5MB
    #    if ($fileLen -lt $bufferSize)
    #    {
    #        $bufferSize = $fileLen;
    #    }
    #    $contents = Invoke-Command -Session $session $readFileChunk -ArgumentList $source, $pos, $bufferSize
    #    if ($contents.GetType().BaseType.Name -ne "Array")
    #    {
    #        $contents = @($contents)
    #    }
    #    Write-Host "Contents: $contents"
    #    Write-Host ("Len: "+$contents.Length)
    #    $file.Write($contents, 0, $contents.Length)
    #    $pos += $contents.Length
    #    #Write-Progress -Activity "Receiving $source" -Status "Receiving file" `
    #    #               -PercentComplete ($pos / $fileLen * 100)
    #    if (($contents.Length -ne $bufferSize) -or ($pos -ge $fileLen))
    #    {
    #        break
    #    }
    #}
    #$file.Close()
    
    # set file time
    [IO.File]::SetLastWriteTimeUtc($dest, $srcModTime)
}
    
function Receive-Folder 
{
    param
    (
        [Parameter(Mandatory = $true)]
        [string] $source,
        
        [Parameter(Mandatory = $true)]
        [string] $dest,
            
        [Parameter(Mandatory = $true)]
        [System.Management.Automation.Runspaces.PSSession] $session,
        
        [Parameter(Mandatory = $true)]
        [bool] $overwrite,
            
        [Parameter(Mandatory = $true)]
        [bool] $overwriteOnlyNew
    )

    $items = Invoke-Session -Session $session { Get-ChildItem $source }
    foreach ($item in Get-ChildItem $source)
    {
        if (Test-Path $item.FullName -PathType container) 
        {
            Receive-Folder $item.FullName "$dest\$item" $session $overwrite $overwriteOnlyNew
        }
        else
        {
            Receive-File $item.FullName "$dest\$item" $session $overwrite $overwriteOnlyNew
        }
    }
}

function Send-File
{
    param
    (
        [Parameter(Mandatory = $true)]
        [string] $source, # local
        
        [Parameter(Mandatory = $true)]
        [string] $dest, # remote
        
        [Parameter(Mandatory = $true)]
        [System.Management.Automation.Runspaces.PSSession] $session,
        
        # ovewrite regardless of mod times
        [Parameter(Mandatory = $true)]
        [bool] $overwrite,
        
        # overwrite only if source newer than dest
        [Parameter(Mandatory = $true)]
        [bool] $overwriteOnlyNew
    )
    Write-Host "Sending $source to $dest, overwrite: $overwrite, overwriteOnlyNew: $overwriteOnlyNew"
    
    # perform transfer in chunks
    $scriptTransfer = 
    {
        param ([string] $dest, [byte[]] $bytes, [int] $length)
        
        $state = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($dest)
    
        $file = [IO.File]::Open($state, "OpenOrCreate")
        $file.Seek(0, "End")
        $file.Write($bytes, 0, $length)
        $file.Close()
    }
    
    # set modification time of destination file to match
    $scriptSetTime =
    {
        param ([string] $dest, [System.DateTime] $time)        
        $state = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($dest)    
        [IO.File]::SetLastWriteTimeUtc($state, $time)
    }

    $sourceFile = Get-Item $source
    $file = [IO.File]::OpenRead($sourceFile.FullName) # Locks file from modification
    # must use UTC time since source and dest machines may be in differing timezones
    $modTime = [IO.File]::GetLastWriteTimeUtc($source)
    
    # check to see if file exists on dest and needs deletion
    $abort = Invoke-Command -Session $session $scriptDelete `
                            -ArgumentList $dest, $overwrite, $overwriteOnlyNew, $modTime
    # abort if exists and no deletion
    if ($abort -eq $true)
    {
        $file.Close()
        Write-Host "Ignored file $source"
        return
    }
    # perform transfer
    Write-Progress -Activity "Sending Source $source" -Status "Preparing file"
    $bufferSize = 5MB
    $pos = 0
    $rawBytes = New-Object byte[] $bufferSize
    while (($read = $file.Read($rawBytes, 0, $bufferSize)) -gt 0)
    {
        Write-Progress -Activity "Writing $dest" -Status "Sending file" `
                       -PercentComplete ($pos / $sourceFile.Length * 100)
         
        Invoke-Command -Session $session $scriptTransfer `
                       -ArgumentList $dest, $rawBytes, $read

        $pos += $read
    }
    $file.Close()
    # change modifiction time of dest
    Invoke-Command -Session $session $scriptSetTime -ArgumentList $dest, $modTime
    
    # show destination file
    Invoke-Command -Session $session { Get-Item $args[0] } -ArgumentList $dest
}
    
function Send-Folder 
{
    param
    (
        [Parameter(Mandatory = $true)]
        [string] $source,
        
        [Parameter(Mandatory = $true)]
        [string] $dest,
        
        [Parameter(Mandatory = $true)]
        [System.Management.Automation.Runspaces.PSSession] $session,
        
        [Parameter(Mandatory = $true)]
        [bool] $overwrite,
        
        [Parameter(Mandatory = $true)]
        [bool] $overwriteOnlyNew
    )
    
    foreach ($item in Get-ChildItem $source)
    {
        if (Test-Path $item.FullName -PathType container) 
        {
            Send-Folder $item.FullName "$dest\$item" $session $overwrite $overwriteOnlyNew
        }
        else
        {
            Send-File $item.FullName "$dest\$item" $session $overwrite $overwriteOnlyNew
        }
    }
}

if (!$session)
{
	if (!$Name)
	{
		Write-Host "Neither -Name or -Session is supplied, cannot continue ..."
		exit
	}
	$remoteSession = .\getremotesession.ps1 -Name $Name -ServiceName $ServiceName -creds $creds
}
else
{
	$remoteSession = $session
}

Invoke-Command -Session $remoteSession { Set-StrictMode -Version Latest }

Write-Host "Source: $source"
Write-Host "dest: $dest"
Write-Host "overwriteOnlyNew: $overwriteOnlyNew"
Write-Host "reverse: $reverse"

if (-not $reverse)
{
    $isDir = Test-Path $source -pathtype container
    $isLeaf = Test-Path $source -pathtype leaf

    if (-not $isDir -and -not $isLeaf)
    {
        Write-Host "Source does not exist"
        if (!$session)
        {
            Remove-PSSession -session $remoteSession
        }
        exit
    }

    if ($isDir)
    {
        Send-Folder $source $dest $remoteSession (!$overwriteOnlyNew) $overwriteOnlyNew
    }
    if ($isLeaf)
    {
        Send-File $source $dest $remoteSession (!$overwriteOnlyNew) $overwriteOnlyNew
    }
}
else
{
    # copy from remote (source) to local (destinatin)
    #$remoteCopy =
    #{
    #    param ([string] $source, [ref] $isDir, [ref] $isLeaf)
    #
    #   $isDir.Value = Test-Path $source -pathtype container
    #   $isLeaf.Value = Test-Path $source -pathtype leaf
    #
    #
    #$isDir = $true
    #$isLeaf = $false
    #Invoke-Command -Session $session $remoteCopy -ArgumentList $source, [ref]$isDir, [ref]$isLeaf
    $isDir = Invoke-Command -session $session { Test-Path $args[0] -pathtype container } -ArgumentList $source
    $isLeaf = Invoke-Command -session $session { Test-Path $args[0] -pathtype leaf } -ArgumentList $source

    if (-not $isDir -and -not $isLeaf)
    {
        Write-Host "Source does not exist"
        return
    }

    if ($isDir)
    {
        Receive-Folder $source $dest $remoteSession (!$overwriteOnlyNew) $overwriteOnlyNew
    }
    else
    {
        Receive-File $source $dest $remoteSession (!$overwriteOnlyNew) $overwriteOnlyNew
    }
}

# end remote session
if (!$session)
{
    Remove-PSSession -session $remoteSession
}

