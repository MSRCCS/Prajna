##############################################################################
## Deploy and start Prajna clients on Windows domain joined machines
##############################################################################

param(
    [Parameter(Mandatory=$true)]
    [string] $SourceLocation,
    [Parameter(Mandatory=$true)]
    [string[]] $ComputerNames,
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
    [PSCredential] $Cred
)

$ClientName = "PrajnaClient.exe"

if ($ComputerNames.Count -eq 0)
{
    Write-Error "No computer specified by -ComputerNames"
    Exit
}

if (!(Test-Path $SourceLocation))
{
    Write-Error “Invalid location '$SourceLocation' specified by -SourceLocation."
    exit
}

if (!(Test-Path (Join-Path $SourceLocation $ClientName)))
{
    Write-Error “'$SourceLocation' (specified by -SourceLocation) does not contain the Prajna client."
    exit
}

# stop the clients if any to allow the copy
.\Stop-Clients.ps1 -ComputerNames $ComputerNames -ClientLocation $ClientLocation -Cred $Cred


# copy files
$ClientLocationDrive = (Split-Path -Path $ClientLocation -Qualifier).Replace(':', '$')
$ClientLocationFolder = Split-Path -Path $ClientLocation -NoQualifier

Foreach ($ComputerName in $ComputerNames)
{
    $Root = "\\$ComputerName\$ClientLocationDrive"
    # Write-Output "Root: $Root"
    if ($Cred)
    {
        New-PSDrive -Name $ComputerName -Root $Root -PSProvider FileSystem -Credential $Cred | Out-Null
    }
    else
    {
        New-PSDrive -Name $ComputerName -Root $Root -PSProvider FileSystem | Out-Null
    }

    $Dest = "$ComputerName" +":\" + $ClientLocationFolder

    if (!(Test-Path $Dest))
    {
        New-Item -ItemType directory -Path $Dest -Force | Out-Null
    }

    Copy-Item -Path $SourceLocation\* -Destination $Dest -Force -Recurse 
    Remove-PSDrive -Name $ComputerName
    Write-Output "The client is copied over to machine $ComputerName"
}

# start the clients
.\Start-Clients.ps1 -ComputerNames $ComputerNames -ClientLocation $ClientLocation -Port $Port -JobPortRange $JobPortRange -MemorySize $MemorySize -LogLevel $LogLevel -LogDir $LogDir -Password $Password -Cred $Cred -ShutdownRunningClients $true
