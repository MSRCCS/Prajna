##############################################################################
## Copyright 2015, Microsoft.	All rights reserved
## Author: Weirong Zhu
## Date: March. 2015                                                     
## Batch execution of a command against multiple VMs in Azure
##############################################################################

param
(
    [Parameter(Mandatory = $true)]
    [string] $Csv,

    [Parameter(Mandatory = $false)]
    [string] $User = "imhub",

    [Parameter(Mandatory = $false)]
    [string] $Password,

    [Parameter(Mandatory = $false)]
    [string] $Subscription = "Windows Azure Internal Consumption",

    [Parameter(Mandatory = $true)]
    [string] $Cmd,

    # The directory where the cluster INF files locates
    # if $Cmd refers to variable $Inf, this parameter must be specified
    # The script assume a VM's Inf file is located under the specified folder, and the file name is <VM's Name>.inf
    [Parameter(Mandatory = $false)]
    [string] $InfDir,

    # if $true, store the result to a file
    [Parameter(Mandatory = $false)]
    [bool] $out = $false
)

if (!(Test-Path -Path $Csv))
{
    Write-Host ("CSV file '" + $Csv + "' does not exist")
    throw
}

if (($Cmd.IndexOf('$Inf', [System.StringComparison]::OrdinalIgnoreCase) -ne -1) -and !$InfDir)
{
    Write-Host('$Inf is referenced in the command, argument -InfDir must be provided!')
    throw
}

Write-Host $Cmd

$vms = Import-CSV $Csv | Foreach {

    if (!$_.SerivceName)
    {
        $_.ServiceName = $_.Name
    }

    if (!$_.Name -or $_.Name.StartsWith("#"))
    {
        Write-Host ("Skip: " + $_)
    }
    elseif ($_.Name -ne $_.ServiceName)
    {
        Write-Host ("Name != ServiceName -- Skip :" + $_)
    }
    elseif (!$_.User -and !$User)
    {
        Write-Host ("No 'User' specified, Skip: " + $_)
    }
    elseif (!$_.Password -and !$Password)
    {
        Write-Host ("No 'Password' specified, Skip: " + $_)
    }
    else
    {
        New-Object PSObject -prop @{
            Name = $_.Name
            ServiceName = $_.ServiceName
            User = @{$true = $User; $false = $_.User }[!$_.User]
            Password = @{$true = $Password; $false = $_.Password }[!$_.Password]
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
        Param([string]$command,
              [string]$location,
              [string]$subscription,
              [string]$infDir,
              [PSObject]$vm)

        [Console]::Out.Flush() 
        $start = Get-Date | Out-String
        Write-Host ("Job starts at " + $start)
        [Console]::Out.Flush() 

        $sb = {
           Param([string]$command,
                 [string]$location,
                 [string]$subscription,
                 [string]$infDir,
                 [PSObject]$vm) 

            # set the location
            Set-Location -Path $location -PassThru

            # select the Azure subscription
            Select-AzureSubscription -Current -SubscriptionName $subscription

            $passwd = ConvertTo-SecureString $vm.Password -AsPlainText -Force
            $cred = New-Object PSCredential($vm.User, $passwd)

            $Name = $vm.Name
            $ServiceName = $vm.ServiceName            

            if ($infDir)
            {
                # $infDir is specified, initialize $Inf
                $Inf = $infDir + "\" + $vm.Name + ".inf"
                if (!(Test-Path -Path $Inf) -and ($cmd.IndexOf('$Inf', [System.StringComparison]::OrdinalIgnoreCase) -ne -1))
                {
                    Write-Host ("Failed: Inf file '" + $Inf + "' does not exist")
                    throw                            
                }

                Write-Host ('$Inf: ' + $Inf)
            }

            $Session = .\getremotesession.ps1 -Name $Name -ServiceName $ServiceName -creds $cred

            if (!$Session)
            {
                Write-Host ("Failed to get remote session to the vm")
                throw        
            }
            Write-Host("Get remote session: complete")

            # execute the command, the command may reference $Name, $ServiceName, and $Session
            $output = Invoke-Expression $command -ErrorVariable "Errors"

            Write-Host "================================================"
            Write-Host $output
            Write-Host "================================================"

            if ($Errors)
            {
                throw $Errors
            }

            Remove-PSSession $Session
        }
    
        Invoke-Command -ScriptBlock $sb -ArgumentList @($command, $location, $subscription, $infDir, $vm)

        [Console]::Out.Flush() 
        $end = Get-Date | Out-String
        Write-Host ("Job ends at " + $end)        

    } -ArgumentList @($Cmd, $currentLocation, $Subscription, $InfDir, $vm)

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

if ($out -eq $true)
{
    $resultCsvFile = "batch-result-" + ((Get-Date).ToUniversalTime().ToString("yyyy-MM-dd-HH-mm-ss")) + ".csv"

    $vms | 
        Select-object -Property Name,Result  |
        Export-Csv -path $resultCsvFile -NoTypeInformation
}

$vms

[Console]::Out.Flush() 
Write-Host ($vms | Format-Table | Out-String)
