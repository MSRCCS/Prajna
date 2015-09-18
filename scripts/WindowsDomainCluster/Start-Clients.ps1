##############################################################################
## Start Prajna clients on Windows domain joined machines
##############################################################################

param(
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
    [bool] $ShutdownRunningClients = $false,
    [PSCredential] $Cred
)

if ($ComputerNames.Count -eq 0)
{
    Write-Error "No computer specified by -ComputerNames"
    Exit
}

$JobRangeArray = $JobPortRange -split "-"
if ([convert]::ToInt32($JobRangeArray[0]) -gt [convert]::ToInt32($JobRangeArray[1]))
{
    Write-Error "Invalid job port range: $JobPortRange."
    Exit
}

# start the clients in sequence 
Foreach ($ComputerName in $ComputerNames)
{
    .\Start-Client.ps1 -ComputerName $ComputerName -ClientLocation $ClientLocation -Port $Port -JobPortRange $JobPortRange -MemorySize $MemorySize -LogLevel $LogLevel -LogDir $LogDir -Password $Password -Cred $Cred -ShutdownRunningClients $ShutdownRunningClients
}
