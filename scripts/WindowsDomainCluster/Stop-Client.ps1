##############################################################################
## Stop a Prajna client on a Windows domain joined machine
##############################################################################

param(
    [Parameter(Mandatory=$true)]
    [string] $ComputerName,
    [Parameter(Mandatory=$true)] 
    [string] $ClientLocation,
    [PSCredential] $Cred
)

$Sb = {
    param(
        [string] $ClientLocation
    )

    $ClientName = "PrajnaClient.exe"


    if (!(Test-Path $ClientLocation))
    {
        # If it does not exist, there's nothing to stop
        return $true
    }

    $ClientNameWithoutExtension = ([System.IO.Path]::GetFileNameWithoutExtension($ClientName))
    $ClientPath = Join-Path $ClientLocation $ClientName
    
    $Procs = Get-Process -Name $ClientNameWithoutExtension -ErrorAction SilentlyContinue
    $Procs | Where-Object { $_.Path -eq $ClientPath } | Stop-Process -Force
    return $true
}

if ($Cred)
{
    $State = Invoke-Command -ComputerName $ComputerName -EnableNetworkAccess -Authentication CredSSP -Credential $Cred -ScriptBlock $Sb -ArgumentList @($ClientLocation)
    Write-Output "Machine $ComputerName : Stop-Client : $State" 
}
else
{
    $State = Invoke-Command -ComputerName $ComputerName -EnableNetworkAccess -ScriptBlock $Sb -ArgumentList @($ClientLocation)
    Write-Output "Machine $ComputerName : Stop-Client : $State"
}
