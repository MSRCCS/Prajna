##############################################################################
## Parse machine/cluster information
## Author: Jin Li
## Date: Apr. 2015                                                      
## Configuration specific to each user (please edit this file for your own usage)
##############################################################################


if ( -not $clusterLst ) {
    $clusterLst1 = $cluster+".lst"
    Write-Host "Set Cluster = " $cluster
    $clusterLst = $clusterLst1
    Set-Variable -Name clusterLst -Value $cluster1 -Scope 1
}

if ( -not $machineName ) 
{
    $machineName = $clusterLst
}

if ($machineName -ilike "*.lst")  {
	$machines1 = (type $machineName)
    Set-Variable -Name machines -Value $machines1 -Scope 1
	Write-Host "To execute on multiple machine" 
}
else
{
	Write-Host "To execute on a single machine" 
	Set-Variable -Name machines -Value $machineName -Scope 1
}