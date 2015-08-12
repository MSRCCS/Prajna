param
(
    [Parameter(Mandatory = $true)]
    [string] $Name,

    [Parameter(Mandatory = $false)]
    [string] $ServiceName,

    [Parameter(Mandatory = $true)]
    [string] $PrivatePort,

    [Parameter(Mandatory = $false)]
    [string] $PublicPort,

    [Parameter(Mandatory = $false)]
    [string] $PortName
)

if (-not $ServiceName)
{
    $ServiceName = $Name
}
if (-not $PublicPort)
{
    $PublicPort = $PrivatePort
}
if (-not $PortName)
{
    $PortName = "Endpoint" + $PrivatePort
}

$point = Get-AzureVM -Name $Name -ServiceName $ServiceName | Get-AzureEndPoint -Name $PortName
if ($point)
{
    Write-Host "Endpoint on port $PrivatePort already exists"
}
else
{
    Write-Host "Adding endpoint on port $PrivatePort"
    Get-AzureVM -ServiceName $ServiceName -Name $Name | `
        Add-AzureEndPoint -Name $PortName -Protocol tcp -LocalPort $PrivatePort -PublicPort $PublicPort | `
        Update-AzureVM 
}

