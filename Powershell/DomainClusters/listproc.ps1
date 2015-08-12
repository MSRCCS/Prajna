##############################################################################
## Copyright 2015, Microsoft.    All rights reserved     
## Author: Sanjeev Mehrotra
## List processes
##############################################################################
param(
    ## Machine name to deploy
    [string] $machineName, 
    [string] $clusterlst, 
    [string] $cluster
)

Invoke-Expression ./config.ps1
Invoke-Expression ./parsecluster.ps1 

#$findpattern = "*$targetSrcDir*"
$findpattern = "*Prajna*"

foreach ($mach in $machines)
{
    #$Processes = Get-WmiObject -Class Win32_Process -ComputerName $mach | Where {$_.ExecutablePath -like $findpattern}
    #$Processes = .\getuserproc.ps1 -mach $mach | Where {$_.Name -like $findpattern -and $_.user -like "*$user*" }
    $Processes = Get-WmiObject -Class Win32_Process -ComputerName $mach | Where { $_.Name -like $findpattern }
    foreach ($process in $Processes) 
    {
        $process = .\addusertoproc.ps1 $process
        if ($process.user -like "*$user*")
        {
            $name = $process.name
            $id = $process.handle
            write-host "Process $name on $mach with ID:$id"
        }
    }
}
