##############################################################################
## Start a Prajna client on a Windows domain machine
##############################################################################

param(
    [Parameter(Mandatory=$true)]
    [string] $ComputerName,
    [Parameter(Mandatory=$true)] 
    [string] $ClientLocation,
    [Parameter(Mandatory=$true)] 
    [int] $Port,
    [Parameter(Mandatory=$true)] 
    [ValidatePattern("[0-9]+-[0-9]+")] # Example: 11000-12000
    [string] $JobPortRange,
    [int] $MemorySize = 1024, # in MB
    [int] $LogLevel = 4,
    [string] $LogDir,
    [string] $Password,
    [bool] $ShutdownRunningClients = $false,
    [PSCredential] $Cred
)

$JobRangeArray = $JobPortRange -split "-"
if ([convert]::ToInt32($JobRangeArray[0]) -gt [convert]::ToInt32($JobRangeArray[1]))
{
    Write-Error "Invalid job port range: $JobPortRange."
    Exit
}


$Sb = {
    Param([string]$ClientLocation,
          [int] $Port,
          [string] $JobPortRange,
          [int] $MemorySize,
          [int] $LogLevel,
          [string] $LogDir,
          [string] $Password,
          [bool] $ShutdownRunningClients) 

    $ClientName = "PrajnaClient.exe"    

    if (!(Test-Path $ClientLocation))
    {
        Write-Error "$ClientLocation (specified by -ClientLocation) does not exist."
        return $false
    }

    $ClientPath = Join-Path $ClientLocation $ClientName
    if (!(Test-Path $ClientPath))
    {
        Write-Error "Cannot find the client at location specified by -ClientLocation."
        return $false
    }

    $ClientNameWithoutExtension = ([System.IO.Path]::GetFileNameWithoutExtension($ClientName))
   
    $Procs = Get-Process -Name $ClientNameWithoutExtension -ErrorAction SilentlyContinue
    if ($Procs)
    {
        if ($ShutdownRunningClients)
        {
            $Procs | Where-Object { $_.Path -eq $ClientPath } | Stop-Process -Force -Verbose
        }
        else
        {
            $P = $Proc.Path
            Write-Error "There are one or more clients are still running ."
            return $false
        }
    }

    
    $Cmd = "$ClientPath -mem $MemorySize -verbose $LogLevel -port $Port -jobport $JobPortRange"
    if ($LogDir)
    {
        $Cmd = $Cmd + " -dirlog $LogDir"
    }
    if ($Password)
    {
        $Cmd = $Cmd + " -pwd $Password"
    }

    $result = Invoke-WmiMethod -path win32_process -name create -argumentlist @($Cmd, $ClientLocation)

    $Procs = Get-Process -Name $ClientNameWithoutExtension -ErrorAction SilentlyContinue
    if ($Procs)
    {
        $Proc = $Procs | Where-Object { $_.Path -eq $ClientPath }
        if ($Proc -and !$Proc.HasExited)
        {
            return $true;
        }
        else
        {
            return $false;
        }
    }
    else
    {
        return $false;
    }
}

if ($Cred)
{
    $State = Invoke-Command -ComputerName $ComputerName -EnableNetworkAccess -Authentication CredSSP -Credential $Cred -ScriptBlock $Sb -ArgumentList @($ClientLocation, $Port, $JobPortRange, $MemorySize, $LogLevel, $LogDir, $Password, $ShutdownRunningClients)
    Write-Output "Machine $ComputerName : Start-Client : $State"
}
else
{
    $State = Invoke-Command -ComputerName $ComputerName -EnableNetworkAccess -ScriptBlock $Sb -ArgumentList @($ClientLocation, $Port, $JobPortRange, $MemorySize, $LogLevel, $LogDir, $Password, $ShutdownRunningClients)
    Write-Output "Machine $ComputerName : Start-Client : $State"
}
