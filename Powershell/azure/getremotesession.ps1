# Get a remote session to work on
param
(
    [Parameter(Mandatory = $true)]
    [string] $Name,

    [Parameter(Mandatory = $false)]
    [string] $ServiceName,

    [Parameter(Mandatory = $false)]
    [PSCredential] $creds,

    [Parameter(Mandatory = $false)]
    [System.Management.Automation.Runspaces.PSSession] $session
)

Set-StrictMode -Version Latest

if (-not $ServiceName)
{
    $ServiceName = $Name
}

# start remote session, if needed
if (!$session)
{
    $destURI = Get-AzureWinRMUri -ServiceName $ServiceName -Name $Name
    if (-not $creds) 
    {
        $creds = Get-Credential
    }
    $remoteSession = New-PSSession -ConnectionUri $destURI -Credential $creds
}
else 
{ 
    $remoteSession = $session 
}

$remoteSession


