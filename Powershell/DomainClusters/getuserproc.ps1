##############################################################################
## Copyright 2015, Microsoft.	All rights reserved     
## Author: Sanjeev Mehrotra
## Get processes on machine with user attached
##############################################################################
param (
    [Parameter(Mandatory=$true)]
    [string] $mach
)

$Processes = Get-WmiObject -Class Win32_Process -ComputerName $mach
foreach ($proc in $Processes)
{
	$proc = .\addusertoproc.ps1 $proc
}

$Processes

