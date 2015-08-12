param
(
    [Parameter(Mandatory = $true)]
    [string] $VMName,

    [Parameter(Mandatory = $true)]
    [string] $NumVM,

    [Parameter(Mandatory = $true)]
    [string] $User,

    [Parameter(Mandatory = $true)]
    [string] $Password,

    [Parameter(Mandatory = $false)]
    [ref] $sessionExe,

    [Parameter(Mandatory = $false)]
    [string] $srcFileDir = 'e:\ccs\skynet',

    [Parameter(Mandatory = $false)]
    [string] $dstFileDir = 'c:\skynetsrc',

    [Parameter(Mandatory = $false)]
    [string] $InstanceSize = "Small",

    [Parameter(Mandatory = $false)]
    [string] $Location = "West US",

    [Parameter(Mandatory = $false)]
    [bool] $Copy = $false,

    [Parameter(Mandatory = $false)]
    [bool] $CopyExe = $true,

    [Parameter(Mandatory = $false)]
    [bool] $StartClient = $false,

    [Parameter(Mandatory = $false)]
    [bool] $KillClient = $false,

    [Parameter(Mandatory = $false)]
    [bool] $GetSession = $false
)

Write-Host "VMName  : $VMName"
Write-Host "NumVM   : $NumVM"
Write-Host "User    : $User"
Write-Host "srcDir  : $srcFileDir"
Write-Host "dstDir  : $dstFileDir"
Write-Host "Size    : $InstanceSize"
Write-Host "Loc     : $Location"
Write-Host "Copy    : $Copy"
Write-Host "CopyExe : $CopyExe"

# Common Images
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-Datacenter-201310.01-en.us-127GB.vhd
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-Datace#nter-201311.01-en.us-127GB.vhd
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-Datacenter-201312.01-en.us-127GB.vhd
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-Datacenter-201401.01-en.us-127GB.vhd
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-R2-201310.01-en.us-127GB.vhd
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-R2-201311.01-en.us-127GB.vhd
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-R2-201312.01-en.us-127GB.vhd
#ImageName            : a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-R2-201401.01-en.us-127GB.vhd

$image = "a699494373c04fc0bc8f2bb1389d6106__Windows-Server-2012-R2-201401.01-en.us-127GB.vhd"

$subscription = 'Windows Azure Internal Consumption'
$storageaccount = 'skynetvm'

$srcFileBinD = $srcFileDir+'\skynetcontroller\bin\debug'
$srcFileBinR = $srcFileDir+'\skynetcontroller\bin\release'
$dstFileBinD = $dstFileDir+'\skynetcontroller\bin\debug'
$dstFileBinR = $dstFileDir+'\skynetcontroller\bin\release'

# set subcription information and storage account
Select-AzureSubscription -SubscriptionName $subscription
Set-AzureSubscription -SubscriptionName $subscription -CurrentStorageAccountName $storageaccount

function NewVM
{
    param
    (
        [Parameter(Mandatory = $true)]
        [string] $Extension,
        
        [Parameter(Mandatory = $true)]
        [bool] $controller,

        [Parameter(Mandatory = $true)]
        [ref] $rsession
    )

    $ThisVMName = $VMName + "-" + $Extension

    $ThisVM = Get-AzureVM | Where-Object { $_.ServiceName -eq $ThisVMName }
    if ($ThisVM)
    {
        Write-Host "VM $ThisVMName already exists"
        # Public URI
        $uri = Get-AzureWinRMUri -ServiceName $ThisVMName -Name $ThisVMName
    }
    else
    {
        Write-Host "Creating VM $ThisVMName"
        New-AzureVMConfig -Name $ThisVMName -InstanceSize $InstanceSize -ImageName $image | `
          Add-AzureProvisioningConfig -Windows -AdminUsername $User -Password $Password | `
          New-AzureVM -ServiceName $ThisVMName -Location $Location -WaitForBoot
        $uri = Get-AzureWinRMUri -ServiceName $ThisVMName -Name $ThisVMName
        # install certificate for VM
        Write-Host (Invoke-Expression ".\Install-AzureCert.ps1 $ThisVMName $ThisVMName")
    }

    # add endpoints
    $endpt = Get-AzureVM -ServiceName $ThisVMName | `
             Get-AzureEndPoint -Name "SkynetController"
    if (!$endpt)
    {
        Write-Host "Adding endpoint SkynetController on port 1080 on VM $ThisVMName"
        Get-AzureVM -ServiceName $ThisVMName | `
          Add-AzureEndPoint -Name "SkynetController" -Protocol tcp -LocalPort 1080 -PublicPort 1080 | `
          Update-AzureVM 
    }
    else
    {
        Write-Host "Endpoint SkynetController already exists on $ThisVMName"
    }
    $endpt = Get-AzureVM -ServiceName $ThisVMName | `
             Get-AzureEndPoint -Name "SkynetData"
    if (!$endpt)
    {
        Write-Host "Adding endpoint SkynetData on port 1081 on VM $ThisVMName"
        Get-AzureVM -ServiceName $ThisVMName | `
          Add-AzureEndPoint -Name "SkynetData" -Protocol tcp -LocalPort 1081 -PublicPort 1081 | `
          Update-AzureVM
    }
    else
    {
        Write-Host "Endpoint SkynetData already exists on $ThisVMName"
    }
    $endpt = Get-AzureVM -ServiceName $ThisVMName | `
             Get-AzureEndPoint -Name "SkynetDataListen"
    if (!$endpt)
    {
        Write-Host "Adding endpoint SkynetDataListen on port 1082 on VM $ThisVMName"
        Get-AzureVM -ServiceName $ThisVMName | `
          Add-AzureEndPoint -Name "SkynetDataListen" -Protocol tcp -LocalPort 1082 -PublicPort 1082 | `
          Update-AzureVM
    }
    else
    {
        Write-Host "Endpoint SkynetDataListen already exists on $ThisVMName"
    }

    # Get credentials
    $spwd = ConvertTo-SecureString $Password -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential("$thisVMName\$User", $spwd)
    # create session
    $session = New-PSSession -ConnectionUri $uri -Credential $creds
    # copy skynet files over
    if ($Copy)
    {
        Write-Host "Starting File Copy to $ThisVMName"
        Write-Host (Invoke-Expression ".\pscopyvm.ps1 -session `$`session -source $srcFileDir -dest $dstFileDir")
    }
    elseif ($CopyExe)
    {
        Write-Host "Starting File Copy Exe to $ThisVMName"
        Write-Host (Invoke-Expression ".\pscopyvm.ps1 -session `$`session -source $srcFileDir\skynetcontroller\bin\debug -dest $dstFileDir\skynetcontroller\bin\debug")
        Write-Host (Invoke-Expression ".\pscopyvm.ps1 -session `$`session -source $srcFileDir\skynetcontroller\bin\release -dest $dstFileDir\skynetcontroller\bin\release")
    }

    # Get session manager & add firewall rules
    $setFirewall =
    {
        param ([string] $dest, [string] $ThisVMName)

        Set-StrictMode -Version Latest

        function AllowProgramFirewall
        {
            param ([string] $prog, [string] $ruleName)
            
            $progMatch = (($prog -replace "\\", "/") -replace "/", "[\\\/]").ToLower()
            $rules = Invoke-Command { netsh advfirewall firewall show rule name=$ruleName verbose }
            #$rules
            #Write-Host $progMatch
            $ruleMatch = $rules | Where-Object { $_.ToLower() -match "program:\s+($progMatch)" }
            #$ruleMatch
            if ($ruleMatch -and $ruleMatch.Length -gt 0)
            {
                Write-Host "Rule for $progMatch already exists, updating"
                # following fails because Where-Object returns array if more than one elem
                # but returns string object if only one exists
                #$doesMatch = $ruleMatch[0].ToLower() -match "program:\s+($progMatch)"
                #$program = $matches[1]
                #netsh advfirewall firewall set rule name=$ruleName program=$program new dir=in action=allow enable=yes profile=any
                netsh advfirewall firewall set rule name=$ruleName program=$prog new dir=in action=allow enable=yes profile=any
            }
            else
            {
                # create new rule
                Write-Host "Adding rule for $prog"
                netsh advfirewall firewall add rule name=$ruleName program=$prog dir=in action=allow enable=yes profile=any
            }
        }

        Write-Host "Setting firewall rules on $ThisVMName"

        $destProg = $dest+'\skynetcontroller\bin\debug\skynetclient.exe'
        AllowProgramFirewall $destProg "SkyNetClient"

        $destProg = $dest+'\skynetcontroller\bin\debug\skynetcontroller.exe'
        AllowProgramFirewall $destProg "SkyNetController"

        $destProg = $dest+'\skynetcontroller\bin\release\skynetclient.exe'
        AllowProgramFirewall $destProg "SkyNetClient"

        $destProg = $dest+'\skynetcontroller\bin\release\skynetcontroller.exe'
        AllowProgramFirewall $destProg "SkyNetController"
    }
    $res = Invoke-Command -Session $session $setFirewall -ArgumentList $dstFileDir, $ThisVMName
    Write-Host $res

    #create the master.info file on remote side
    $remoteScriptMasterInfo = 
    {
        param ([string] $destDir, [string] $destMaster, [string] $controlVM)

        Write-Host "creating $destMaster in $destDir with Controller $controlVM"
        
        $file = [IO.File]::CreateText($destMaster)
        $file.WriteLine("v0.0.0.5  $controlVM  1080  1081 $destDir")
        $file.WriteLine("skynetclient.exe")
        $file.Close()
    }

    #$endPt = Get-AzureVM -ServiceName ($VMName+"-Cont") | Get-AzureEndpoint -Name "SkynetController"
    #$controlVM = $endPt.Vip
    $contVM = Get-AzureVM -ServiceName ($VMName+"-Cont")
    $doesMatch = $contVM.DNSName -match "http://(.*)/"
    $controlVM = $matches[1]
    Write-Host $controlVM
    $destMaster = $dstFileDir+'\skynetcontroller\bin\debug\master.info'
    $res = Invoke-Command -Session $session $remoteScriptMasterInfo `
                          -ArgumentList  $dstFileDir, $destMaster, $controlVM
    Write-Host $res
    $destMaster = $dstFileDir+'\skynetcontroller\bin\release\master.info'
    $res = Invoke-Command -Session $session $remoteScriptMasterInfo `
                          -ArgumentList  $dstFileDir, $destMaster, $controlVM
    Write-Host $res

    # create the cluster info file
    $createInfo =
    {
        param([string] $dstDir, [string] $ext, [string] $ThisVMName, [string] $extIP, [string] $intIP)
        Write-Host "creating cluster file cluster_$ThisVMName.inf machinenameextension: $ext"
        Set-Location ($dstDir+'\skynetcontroller\bin\debug')
        Write-Host (Get-Location)
        Invoke-Command { .\skynetclusterinfo -outcluster cluster_$ThisVMName.inf -nameext $ext -extip $extIP -intip $intIP }
        #Set-Location ($dstDir+'\skynetcontroller\bin\release')
        #Write-Host (Get-Location)
        Invoke-Command { .\skynetclusterinfo -outcluster ..\release\cluster_$ThisVMName.inf -nameext $ext -extip $extIP -intip $intIP }
    }
    $doesMatch = $controlVM -match ".*?(\..*)"
    $extension = $matches[1]
    $dataEP = Get-AzureVM -ServiceName $ThisVMName | Get-AzureEndpoint -Name "skynetdata"
    $VMIP = Get-AzureVM -ServiceName $ThisVMName
    $res = Invoke-Command -Session $session $createInfo -ArgumentList $dstFileDir, $extension, $ThisVMName, $dataEP.Vip, $VMIP.IpAddress
    Write-Host $res
    # copy back
    .\pscopyvm.ps1 -session $session -source ($dstFileBinD+"\cluster_$ThisVMName.inf") `
                                     -dest ($srcFileBinD+"\cluster_$ThisVMName.inf") -reverse $true
    .\pscopyvm.ps1 -session $session -source ($dstFileBinR+"\cluster_$ThisVMName.inf") `
                                     -dest ($srcFileBinR+"\cluster_$ThisVMName.inf") -reverse $true

    #Write-Host $session
    # attempting to return values is tricky in powershell, following is easier
    $rsession.Value = $session
}

function StartClient
{
    param ([string] $VMName, [System.Management.Automation.Runspaces.PSSession] $session)

    $destDir = $dstFileDir+'\skynetcontroller\bin\debug'
    $exe = $destDir+'\skynetclient.exe'
    Write-Host "starting $exe on $VMName"

    # Three ways to invoke executable on remote end - neither will create interactive console
    # on remote end due to security considerations.  Perhaps psexec can do that?
    # 
    # 1) Direct Process Invoke, no job created which can monitor progress or kill underlying job
    #    Invoke-Command -Session $session { Start-Process $args[0] } -ArgumentList $exe
    # 2) Start BackgroundJob on remote side (underlying job and monitoring happens at remote end)
    #    Invoke-Command -Session $session { Start-Job -ScriptBlock $ExecutionContext.InvokeCommand.NewScriptBlock($args[0]) } -ArgumentList $exe
    #    Use following to monitor BackgrounJob on remote end and control job
    #    Invoke-Command -Session $session { Get-Job -Id <JobID> }
    # 3) Start RemoteJob on local side (underlying job on remote side, but monitoring local)
    #    Invoke-Command -Session $session $ExecutionContext.InvokeCommand.NewScriptBlock($exe) -AsJob
    #    Use following to monitor RemoteJob on local end
    #    Get-Job -Id <JobID>
    #
    # To get interactive console on local end, use
    #    Invoke-Command -Session $session $ExecutionContext.InvokeCommand.NewScriptBlock($exe)
    #
    # No interactive console displays at local end, but no way to run other jobs

    $startExe =
    {
        param ([string] $destDir, [string] $exe)

        Set-StrictMode -Version Latest

        Start-Process $exe -WorkingDirectory $destDir

        $filter = "name='skynetclient.exe'"
        $Processes = Get-WmiObject -Class Win32_Process -Filter $filter
        foreach ($process in $Processes)
        {
            $processid = $process.handle
            write-host "The process skynetclient ($processid) has started"
        }
    }
    Invoke-Command -Session $session $startExe -ArgumentList $destDir, $exe
}

function KillClient
{
    param ([string] $VMName, [System.Management.Automation.Runspaces.PSSession] $session)

    Write-Host "Kill skynetclient.exe on $VMName"
    $killExe =
    {
        $filter = "name='skynetclient.exe'"
        $Processes = Get-WmiObject -Class Win32_Process -Filter $filter
        foreach ($process in $Processes)
        {
            $ret = $process.Terminate()
            $processid = $process.handle
            write-host "The process skynetclient ($processid) terminates with value $ret"
        }
    }
    Invoke-Command -Session $session $killExe
}

function RemoveGlobalSessions
{
    if ($global:g_ContSession)
    {
        Remove-PSSession $global:g_ContSession
    }
    if ($global:g_ClientSession)
    {
        for ($i=0; $i -lt $global:g_ClientSession.Length; $i++)
        {
            Remove-PSSession $global:g_ClientSession[$i]
        }
    }
}

function CreateSession
{
    param ([string] $VMName)
    $uri = Get-AzureWinRMUri -ServiceName $VMName -Name $VMName
    $spwd = ConvertTo-SecureString $Password -AsPlainText -Force
    $creds = New-Object System.Management.Automation.PSCredential("$VMName\$User", $spwd)

    New-PSSession -ConnectionUri $uri -Credential $creds
}

function CreateClusterLst
{
    param ([string] $list)
    Write-Host "Create cluster lst file $list"
    $file = [IO.File]::CreateText($list)
    for ($i=0; $i -lt $NumVM; $i++)
    {
        $ThisVMName = $VMName+("-{0:0000}" -f $i)
        $ThisVMName
        $VM = Get-AzureVM -ServiceName $ThisVMName
        [void]($VM.DNSName -match "http://(.*)/")
        $VMNameFull = $matches[1]
        Write-Host "Write $VMNameFull to lst file"
        $file.WriteLine($VMNameFull)
    }
    $file.Close()
}

if (-not $StartClient -and -not $KillClient -and -not $GetSession)
{
    #RemoveGlobalSessions

    $global:g_ContSession = $null
    NewVM "Cont" $true ([ref]$global:g_ContSession) # the controller
    $global:g_ClientSession = New-Object System.Management.Automation.Runspaces.PSSession[] $NumVM
    $infoIn = ""
    $retSession = $null
    for ($i=0; $i -lt $NumVM; $i++)
    {
        $ThisVMName = $VMName+("-{0:0000}" -f $i)
        NewVM ("{0:0000}" -f $i) $false ([ref]$retSession)
        $global:g_ClientSession[$i] = $retSession
        $infoIn = $infoIn + "-incluster $srcFileBinD\\cluster_$ThisVMName.inf "
    }
    # now create cluster file for VMs
    # Debug
    Write-Host "Creating cluster information files using $infoIn -outcluster $srcFileBinD\\cluster_$VMName.inf"
    Invoke-Expression "$srcFileBinD\skynetclusterinfo $infoIn -outcluster $srcFileBinD\\cluster_$VMName.inf" 
    CreateClusterLst "$srcFileBinD\\cluster_$VMName.lst"
    # Release
    Invoke-Expression "$srcFileBinD\skynetclusterinfo $infoIn -outcluster $srcFileBinR\\cluster_$VMName.inf" 
    CreateClusterLst "$srcFileBinR\\cluster_$VMName.lst"

    # copy to controller
    .\pscopyvm.ps1 -session $global:g_ContSession `
                   -source "$srcFileBinD\\cluster_$VMName.inf" `
                   -dest "$dstFileBinD\\cluster_$VMName.inf"
    .\pscopyvm.ps1 -session $global:g_ContSession `
                   -source "$srcFileBinD\\cluster_$VMName.lst" `
                   -dest "$dstFileBinD\\cluster_$VMName.lst"
    .\pscopyvm.ps1 -session $global:g_ContSession `
                   -source "$srcFileBinR\\cluster_$VMName.inf" `
                   -dest "$dstFileBinR\\cluster_$VMName.inf"
    .\pscopyvm.ps1 -session $global:g_ContSession `
                   -source "$srcFileBinR\\cluster_$VMName.lst" `
                   -dest "$dstFileBinR\\cluster_$VMName.lst"
}
elseif ($GetSession)
{
    if ($sessionExe)
    {
        Remove-PSSession $sessionExe.Value
        $sessionExe.Value = CreateSession $VMName
        $sessionExe.Value
        Write-Host "Get Session for $VMName returns with $sessionExe.Value"
    }
    else
    {
        RemoveGlobalSessions

        $global:g_ContSession = CreateSession ($VMName+"-Cont")
        $global:g_ClientSession = @()
        for ($i=0; $i -lt $NumVM; $i++)
        {
            $ThisVMName = $VMName+("-{0:0000}" -f $i)
            if (-not (Get-AzureVM -ServiceName $ThisVMName))
            {
                break
            }
            $global:g_ClientSession = $global:g_ClientSession + (CreateSession $ThisVMName)
        }
    }
}
elseif ($StartClient)
{
    if ($sessionExe)
    {
        Remove-PSSession $sessionExe.Value
        $sessionExe.Value = CreateSession $VMName
        StartClient $VMName $sessionExe.Value
    }
    else
    {
        RemoveGlobalSessions

        $global:g_ClientSession = @()
        $NumVM = 10000 # max
        for ($i=0; $i -lt $NumVM; $i++)
        {
            $ThisVMName = $VMName+("-{0:0000}" -f $i)
            if (-not (Get-AzureVM -ServiceName $ThisVMName))
            {
                break
            }
            # append to end of array
            # to append to beginning use  $a = @(StartClient(.)) + $a
            $global:g_ClientSession = $global:g_ClientSession + (CreateSession $ThisVMName)
            StartClient $ThisVMName $global:g_ClientSession[$i]
        }
    }
}
elseif ($KillClient)
{
    if ($sessionExe)
    {
        KillClient $VMName $sessionExe.Value
        Remove-PSSession $sessionExe.Value
    }
    else
    {
        $NumVM = 10000 # max
        for ($i=0; $i -lt $NumVM; $i++)
        {
            $ThisVMName = $VMName+("-{0:0000}" -f $i)
            if (-not (Get-AzureVM -ServiceName $ThisVMName))
            {
                break
            }
            KillClient $ThisVMName $global:g_ClientSession[$i]
        }

        RemoveGlobalSessions
    }
}

