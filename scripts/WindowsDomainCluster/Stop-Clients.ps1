##############################################################################
## Stop Prajna clients on Windows domain joined machines
##############################################################################

param(
    [Parameter(Mandatory=$true)]
    [string[]] $ComputerNames,
    [Parameter(Mandatory=$true)] 
    [string] $ClientLocation,
    [PSCredential] $Cred
)

if ($ComputerNames.Count -eq 0)
{
    Write-Error "No computer specified by -ComputerNames"
    Exit
}

# start the clients in sequence 
Foreach ($ComputerName in $ComputerNames)
{
    .\Stop-Client.ps1 -ComputerName $ComputerName -ClientLocation $ClientLocation -Cred $Cred
}
