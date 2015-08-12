##############################################################################
## Copyright 2015, Microsoft.	All rights reserved
## Author: Weirong Zhu
## Date: March. 2015                                                     
## Create VMs
##
## Create VMs according to an CSV file. The CSV file has format
##    Name,ServiceName,Location,SourceImage,User,Password,InstanceSize
## Some fields, if not specified, can be inferred
##############################################################################

param
(
    [Parameter(Mandatory = $false)]
    [string] $Subscription = "Windows Azure Internal Consumption",

    [Parameter(Mandatory = $false)]
    [string] $Image = "03f55de797f546a1b29d1b8d66be687a__Visual-Studio-2013-Community-12.0.31101.0-AzureSDK-2.6-WS2012R2",

    [Parameter(Mandatory = $false)]
    [string] $User = "imhub",

    [Parameter(Mandatory = $false)]
    [string] $Password,

    [Parameter(Mandatory = $false)]
    [string] $InstanceSize = "Medium",

    [Parameter(Mandatory = $false)]
    [string] $Location = "West US",

    [Parameter(Mandatory = $true)]
    [string] $Csv,

    [Parameter(Mandatory = $false)]
    [string] $Ports = "1082:80:81",

    [Parameter(Mandatory = $true)]
    [string] $SourceRoot
)

$PrajnaClusterPath = "C:\OneNet\Cluster"
$PrajnaClusterInfoDir = "\bin\Debugx64\ClusterInfo"
$PrajnaClusterInfo = "\PrajnaClusterInfo.exe"
$LocalPrajnaClusterInfoPath = $SourceRoot + $PrajnaClusterInfoDir
$LocalPrajnaClusterInfo = $LocalPrajnaClusterInfoPath + $PrajnaClusterInfo
$RemoteDir = "C:\" + "f7ccbce8-afa5-4596-af56-bc2e97b51267"
$RemoteOnetClusterInfoPath = $RemoteDir + $PrajnaClusterInfoDir
$RemotePrajnaClusterInfo = $RemoteOnetClusterInfoPath + $PrajnaClusterInfo

if (!(Test-Path -Path $Csv))
{
    Write-Host ("CSV file '" + $Csv + "' does not exist")
    exit $false
}

if (!(Test-Path -Path $SourceRoot))
{
    Write-Host ("Source Root '" + $SourceRoot + "' does not exist")
    exit $false
}

if (!(Test-Path -Path $LocalPrajnaClusterInfoPath))
{
    Write-Host ("Prajna ClusterInfo '" + $LocalPrajnaClusterInfoPath + "' does not exist")
    exit $false
}

if (!(Test-Path -Path $LocalPrajnaClusterInfo))
{
    Write-Host ("Prajna Cluster Info '" + $LocalPrajnaClusterInfo + "' does not exist")
    exit $false
}

$vms = Import-CSV $Csv | Foreach {

    if ($_.Name.StartsWith("#"))
    {
        Write-Host ("Skip: " + $_)
    }
    elseif ($_.Name -ne $_.ServiceName)
    {
        Write-Host ("Name != ServiceName -- Skip :" + $_)
    }
    else
    {
        New-Object PSObject -prop @{
            Name = $_.Name
            ServiceName = $_.ServiceName
            Location = @{$true = $Location; $false = $_.Location }[!$_.Location]
            Image = @{$true = $Image; $false = $_.SourceImage }[!$_.SourceImage]
            User = @{$true = $User; $false = $_.User }[!$_.User]
            Password = @{$true = $Password; $false = $_.Password }[!$_.Password]
            InstanceSize = @{$true = $InstanceSize; $false = $_.InstanceSize }[!$_.InstanceSize]
            StorageAccountName = ($_.Name -replace '[-]','').ToLower()
            Ports = @{$true = $Ports; $false = $_.Ports }[!$_.Ports]
            Result = $false
            JobName = ""
        }
    }
} 

Write-Host ($vms | Format-Table | Out-String)

$currentLocation = (Get-Location).Path

Foreach ($vm in $vms)
{
    $job = Start-Job -Name ([guid]::NewGuid()).ToString() { 
        Param([string]$subscription,
              [string]$location,
              [string]$prajnaClusterPath,
              [string]$localPrajnaClusterInfoPath,
              [string]$remoteOnetClusterInfoPath,
              [string]$remotePrajnaClusterInfo,
              [PSObject]$vm)

        function Test-Remote-Path
        {
            Param
            (
                [Parameter(Mandatory = $true)]
                [System.Management.Automation.Runspaces.PSSession] $session,

                [Parameter(Mandatory = $true)]
                [string] $path
            )

            Invoke-Command -Session $session { Param($p) Test-Path $p } -Args $path
        }
        
        Function Create-VM
        {
            Param([string]$subscription,
                  [string]$location,
                  [string]$prajnaClusterPath,
                  [string]$localPrajnaClusterInfoPath,
                  [string]$remoteOnetClusterInfoPath,
                  [string]$remotePrajnaClusterInfo,
                  [PSObject]$vm)
    
            # set the location
            Set-Location -Path $location -PassThru

            # select the Azure subscription
            Select-AzureSubscription -Current -SubscriptionName $subscription

            Write-Host ("Current Path: " + ((Get-Item -Path ".\").FullName))

            Write-Host ($vm | Format-Table | Out-String)
  
            #########################################
            # Create Storage Account
            #########################################
            $sa = Get-AzureStorageAccount -StorageAccountName $vm.StorageAccountName -ErrorAction SilentlyContinue -WarningAction Ignore

            Write-Host ($sa | Format-Table | Out-String)

            if (!$sa)
            {
                Write-Host ("Create Storage Account '" + $vm.StorageAccountName + "' at '" + $vm.Location + "'")
                $sa = New-AzureStorageAccount -StorageAccountName $vm.StorageAccountName -Location $vm.Location -ErrorAction SilentlyContinue -WarningAction Ignore -ErrorVariable "ErrorMsg"
                if (!$sa)
                {
                    Write-Host ("Failed to create Storage Account '" + $vm.StorageAccountName + "' at '" + $vm.Location + "': " + $ErrorMsg)                        
                    throw
                }
            }
            else
            {
                Write-Host ("Storage Account '" + $vm.StorageAccountName + "' at '" + $vm.Location + "' already exists")
            }

            #########################################
            # Create the VM
            #########################################

            Write-Host ("Start creating VM " + $vm.Name)        
            $createVMRes = .\createvm.ps1 -Subscription $subscription -StorageAccount $vm.StorageAccountName -Image $vm.Image -Name $vm.Name -ServiceName $vm.ServiceName -User $vm.User -Password $vm.Password -Location $vm.Location -InstanceSize $vm.InstanceSize

            Write-Host ("Create VM Result: " + $createVMRes)

            if ($createVMRes -eq "Existed")
            {
                Write-Host ("VM '" + $vm.Name + "'")
            }
            elseif ($createVMRes -eq "Created")
            {
                Write-Host ("Complete creating VM")

                Start-Sleep 30
                ################################################
                # Install the cert for the new VM
                ################################################
                 Write-Host ("Install the cert")
                .\Install-AzureCert.ps1 -Name $vm.Name -ServiceName $vm.ServiceName

                ################################################
                # Open Endpoint
                ################################################
                Write-Host ("Open endpoints")
                $endpoints = $vm.Ports.Split(':')
                foreach ($port in $endpoints)
                {
                    .\openendpt.ps1 -Name $vm.Name -ServiceName $vm.ServiceName -PrivatePort $port -PublicPort $port
                }
            }
            else
            {
                Write-Host ("Failed to create VM: " + $createVMRes) 
                throw
            }
         

            ################################################
            # Get Remote Session
            ################################################
            Write-Host("Get remote session")
            
            $passwd = ConvertTo-SecureString $vm.Password -AsPlainText -Force
            $cred = New-Object PSCredential($vm.User, $passwd)
            
            $remoteSession = .\getremotesession.ps1 -Name $vm.Name -ServiceName $vm.ServiceName -creds $cred

            if (!$remoteSession)
            {
                Write-Host ("Failed to get remote session to the vm")
                throw        
            }
            Write-Host("Get remote session: complete")

            ################################################
            # Update Firewall
            ################################################
            Write-Host("Update Firewall to allow port 1000 - 1500")
            Invoke-Command -Session $remoteSession -ScriptBlock { 
                $rule = Get-NetFirewallRule -Name "AllowPort1000to1500" -ErrorAction SilentlyContinue -WarningAction Ignore
                if (!$rule)
                {
                    Write-Host("Firewall rule (AllowPort1000to1500) does not exist: try to update")
                    New-NetFirewallRule -Name "AllowPort1000to1500" -DisplayName "Allow Port 1000-1500" -Direction Inbound -LocalPort 1000-1500 -Protocol TCP -Action Allow 
                }
                $rule = Get-NetFirewallRule -Name "AllowPort80to81" -ErrorAction SilentlyContinue -WarningAction Ignore
                if (!$rule)
                {
                    Write-Host("Firewall rule (AllowPort80to81) does not exist: try to update")
                    New-NetFirewallRule -Name "AllowPort80to81" -DisplayName "Allow Port 80-81" -Direction Inbound -LocalPort 80-81 -Protocol TCP -Action Allow 
                }
            }
            Write-Host("Firewall: updated")
   

            ################################################
            # Get Inf file
            ################################################
            $infFileName = $vm.Name + ".inf"
            $infFilePath = $prajnaClusterPath + "\" + $infFileName
            $remoteInfFilePath = $remoteOnetClusterInfoPath + "\" + $infFileName

            if (!(Test-Path -Path $infFilePath))
            {
                Write-Host("Generating Inf file '" + $infFilePath + "'")
                if (!(Test-Remote-Path -path $remoteOnetClusterInfoPath -session $remoteSession))
                {
                    Write-Host("Start to copy PrajnaClusterInfo")
                    .\vmcopy.ps1 -session $remoteSession -source $localPrajnaClusterInfoPath -dest $remoteOnetClusterInfoPath -Name $vm.Name -ServiceName $vm.ServiceName
                    Write-Host("PrajnaClusterInfo is copied")
                }
                Start-Sleep -Seconds 5
                
                Write-Host("Try to run PrajnaClunsterInfo remotely")
                $launchRes = .\vmlaunch.ps1 -session $remoteSession -exe $remotePrajnaClusterInfo -ArgumentList @("-outcluster", $infFileName, "-nameext", ".cloudapp.net") -background $true
                if ($launchRes)
                {
                    Write-Host("PrajnaClusterInfo launched")

                    Start-Sleep -Seconds 5
                    .\vmcopy.ps1 -session $remoteSession -source $remoteInfFilePath -dest $infFilePath -Name $vm.Name -ServiceName $vm.ServiceName -reverse $true
                    if (!(Test-Path -Path $infFilePath))
                    {
                        Write-Host("Failed to generate inf file '" + $infFilePath + "'")
                        throw
                    }

                    Write-Host("Inf file '" + $infFilePath + "' is generated")
                }
                else
                {
                    Write-Host("PrajnaClusterInfo failed to launch")
                    throw
                }
                                }
            else
            {
                Write-Host("Inf file '" + $infFilePath + "' is already generated")
            }

            Remove-PSSession $remoteSession  
        }

        [Console]::Out.Flush() 
        $start = Get-Date | Out-String
        Write-Host ("Job starts at " + $start)
        [Console]::Out.Flush() 

        Create-VM -subscription $subscription -location $location -vm $vm -prajnaClusterPath $prajnaClusterPath -localPrajnaClusterInfoPath $localPrajnaClusterInfoPath -remoteOnetClusterInfoPath $remoteOnetClusterInfoPath -remotePrajnaClusterInfo $remotePrajnaClusterInfo

        [Console]::Out.Flush() 
        $end = Get-Date | Out-String
        Write-Host ("Job ends at " + $end)        

    } -ArgumentList @($Subscription, $currentLocation, $PrajnaClusterPath, $LocalPrajnaClusterInfoPath, $RemoteOnetClusterInfoPath, $RemotePrajnaClusterInfo, $vm)

    $vm.JobName = $job.Name
}

Write-Host "================================================"
Write-Host "Wait for jobs"
Write-Host "================================================"

Foreach ($vm in $vms)
{
    $job = Wait-Job -Name $vm.JobName
    $output = Receive-Job -Job $job

    Write-Host "================================================"
    if (($job.State -ne "Completed"))
    {
        Write-Host ("Job '" + $job.Name + "' for VM '" + $vm.Name + "' failed")
        $vm.Result = $false
    }
    else
    {
        Write-Host ("Job '" + $job.Name + "' for VM '" + $vm.Name + "' completed.")
        $vm.Result = $true
    }
    [Console]::Out.Flush() 
    Write-Host "================================================"
    Write-Host $output
    Write-Host "================================================"


    $vm.PSObject.Properties.Remove("JobName")

    Remove-Job $job
}

[Console]::Out.Flush() 
Write-Host ($vms | Format-Table | Out-String)

$resultCsvFile = "result-vms-" + ((Get-Date).ToUniversalTime().ToString("yyyy-MM-dd-HH-mm-ss")) + ".csv"

$vms | 
    Select-object -Property Name,ServiceName,InstanceSize,Location,Port,User,Password,Result  |
    Export-Csv -path $resultCsvFile -NoTypeInformation

exit $true
