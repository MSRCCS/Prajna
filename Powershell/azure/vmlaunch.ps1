# Launch exe on remote side, session must exist
# otherwise when session termiantes, so does program

param
(
    [Parameter(Mandatory = $true)]
    [string] $exe,

    [Parameter(Mandatory = $false)]
    [string[]] $argumentList = $null,

    [Parameter(Mandatory = $false)]
    [bool] $kill = $false,

    [Parameter(Mandatory = $false)]
    [bool] $list = $false,

    [Parameter(Mandatory = $true)]
    [System.Management.Automation.Runspaces.PSSession] $session,
	
	# When set to $false, when the $session is remove, the process is closed
	# When set to $true, when the $session is removed, the process remains running
    [Parameter(Mandatory = $false)]
    [bool] $background = $false
)

$getItem =
{
    param([string] $exe)

    if (Test-Path -PathType Leaf $exe)
    {
        $item = Get-ChildItem $exe
        $dir = $item.DirectoryName
        $exeName = $item.Name
        ($dir, $exeName)
    }
    else
    {
        write-host "$exe does not exist"
        ($null, $null)
    }
}

$startExe =
{
    param ([string] $dir, [string] $exeName, [string[]] $argumentList, [bool]$background)
    
    Set-StrictMode -Version Latest

    if ($argumentList -eq $null)
    {
        if ($background -eq $false)
		{
			Start-Process $exeName -WorkingDirectory $dir
		}
		else
		{
            $cmd = $dir + "\" + $exeName

			# start the process via WinRM, so it won't be terminated when the $session is removed
			# see: http://www.drawbackz.com/stack/463450/launching-background-tasks-in-a-remote-session-that-dont-get-killed-when-the-session-is-removed.html
			Invoke-WmiMethod -path win32_process -name create -argumentlist @($cmd, $dir)
		}
    }
    else
    {
		if ($background -eq $false)
		{
			Start-Process $exeName -WorkingDirectory $dir -ArgumentList $argumentList
		}
		else
		{
			$cmd = $dir + "\" + $exeName
			
			foreach($arg in $argumentList)
			{
				$cmd += (" " + $arg)
			}
			
			# start the process via WinRM, so it won't be terminated when the $session is removed
			# see: http://www.drawbackz.com/stack/463450/launching-background-tasks-in-a-remote-session-that-dont-get-killed-when-the-session-is-removed.html
			Invoke-WmiMethod -path win32_process -name create -argumentlist @($cmd, $dir)		
		}        
    }
    $filter = "name='$exeName'"
    $path = $dir + '\*'
    write-host filter by "name='$exeName'" and "executablepath -like $path"
    $Processes = Get-WmiObject -Class Win32_Process -Filter $filter | where { $_.ExecutablePath -like $path }
	$started = $false
    foreach ($process in $Processes)
    {
        $processid = $process.handle
        write-host "The process $item.Name ($processid) has started"
		$started = $true
    }
	return $started
}

$listExe =
{
    param ([string] $dir, [string] $exeName)

    Set-StrictMode -Version Latest

    $filter = "name='$exeName'"
    $path = $dir + '\*'
    write-host filter by "name='$exeName'" and "executablepath -like $path"
    $Processes = Get-WmiObject -Class Win32_Process -Filter $filter | where { $_.ExecutablePath -like $path }
	$found = $false
    foreach ($process in $Processes)
    {
        $processid = $process.handle
        write-host "The process $item.Name ($processid) is running"
		$found = $true
    }
	return $found
}

$killExe =
{
    param ([string] $dir, [string] $exeName)

    Set-StrictMode -Version Latest

    $filter = "name='$exeName'"
    $path = $dir + '\*'
    write-host filter by "name='$exeName'" and "executablepath -like $path"
    $Processes = Get-WmiObject -Class Win32_Process -Filter $filter | where { $_.ExecutablePath -like $path }
	$killed = $true
    foreach ($process in $Processes)
    {
        $processid = $process.handle
        $ret = $process.Terminate()
        write-host "The process $exeName ($processid) terminates with value $ret"
		if ($ret -eq $null)
		{
			$killed = $false
		}		
    }
	return $killed
}

($dir, $exeName) = Invoke-Command -Session $session $getItem -ArgumentList $exe
$name = $session.ComputerName

$ret = $false
if (($dir -ne $null) -and ($exeName -ne $null))
{
    if ($list)
    {
        Write-Host "List $exe processes on $name"
        $ret = Invoke-Command -Session $session $listExe -ArgumentList $dir, $exeName
    }
    elseif ($kill)
    {
        Write-Host "Kill $exe processes on $name"
        $ret = Invoke-Command -Session $session $killExe -ArgumentList $dir, $exeName
    }
    else
    {
        Write-Host "Launch $exe process on $name"
        $ret = Invoke-Command -Session $session $startExe -ArgumentList $dir, $exeName, $argumentList, $background
    }
}

$ret

