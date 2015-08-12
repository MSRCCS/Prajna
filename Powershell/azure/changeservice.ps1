param
(
    [Parameter(Mandatory = $true)]
    [string] $Name,

    [Parameter(Mandatory = $true)]
    [string] $OldServiceName,

    [Parameter(Mandatory = $true)]
    [string] $NewServiceName
)

$VM = Get-AzureVM -Name $Name -ServiceName $OldServiceName
if ($VM)
{
    $disk = Get-AzureDisk | Where-Object { $_.AttachedTo.RoleName -eq $VM.InstanceName }
    if (-not $disk)
    {
        $disk = Get-AzureDisk | Where-Object { $_.DiskName -like "*$Name*" }
    }
    $instanceSize = $VM.InstanceSize
}
else
{
    $disk = Get-AzureDisk | Where-Object { $_.DiskName -like "*$Name*" }
    $instanceSize = "Medium"
}

$dname = $disk.DiskName
Write-Host "Disk: $dname"

$newService = Get-AzureService -ServiceName $NewServiceName
$oldService = Get-AzureService -ServiceName $OldServiceName
if ($newService)
{
    $location = $newService.Location
}
elseif ($oldService)
{
    $location = $oldService.Location
}
if ($disk)
{
    Remove-AzureVM -Name $Name -ServiceName $OldServiceName
    Start-Sleep -s 60
    New-AzureVMConfig -Name $Name -InstanceSize $instanceSize -DiskName $disk.DiskName | New-AzureVM -ServiceName $NewServiceName -Location $location
}
