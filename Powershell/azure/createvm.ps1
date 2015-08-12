param
(
    [Parameter(Mandatory = $false)]
    [string] $Subscription,

    [Parameter(Mandatory = $false)]
    [string] $StorageAccount,

    [Parameter(Mandatory = $false)]
    [string] $Image,

    [Parameter(Mandatory = $true)]
    [string] $Name,

    [Parameter(Mandatory = $false)]
    [string] $ServiceName,

    [Parameter(Mandatory = $true)]
    [string] $User,

    [Parameter(Mandatory = $true)]
    [string] $Password,

    [Parameter(Mandatory = $true)]
    [string] $InstanceSize,

    [Parameter(Mandatory = $false)]
    [string] $Location = "West US"
)

if (-not $Subscription)
{
    $Subscription = 'Windows Azure Internal Consumption'
}

if (-not $StorageAccount)
{
    $StorageAccount = 'skynetvm'
}

if (-not $Image)
{
    #$Image = "a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-R2-201401.01-en.us-127GB.vhd"
    $Image = "03f55de797f546a1b29d1b8d66be687a__Visual-Studio-2013-Community-12.0.31101.0-AzureSDK-2.6-WS2012R2"
}

if (-not $ServiceName)
{
    $ServiceName = $Name
}

# set subcription information and storage account
Select-AzureSubscription -SubscriptionName $Subscription
Set-AzureSubscription -SubscriptionName $Subscription -CurrentStorageAccountName $StorageAccount

$ThisVM = Get-AzureVM -Name $Name -ServiceName $ServiceName
if ($ThisVM) 
{
    Write-Host "VM $Name already exists on service $ServiceName"
	$ret = "Existed"
}
else
{
	Write-Host "Creating $Name on service $ServiceName"
	Try
	{    
		New-AzureVMConfig -Name $Name -InstanceSize $InstanceSize -ImageName $image | `
		  Add-AzureProvisioningConfig -Windows -AdminUsername $User -Password $Password | `
		  New-AzureVM -ServiceName $ServiceName -Location $Location -WaitForBoot
	}
	Catch
	{
		$ret = "Failed: " + $_
	}
	$ret = "Created"
}
$ret
