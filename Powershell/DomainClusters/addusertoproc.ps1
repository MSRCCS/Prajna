##############################################################################
## Copyright 2015, Microsoft.	All rights reserved     
## Author: Sanjeev Mehrotra
## Add user to process
##############################################################################
param (
    [System.Management.ManagementBaseObject] $proc
)

$user = $proc.getowner()
if ($user.domain -eq "")
{
    $u = ""
}
else
{
    $u = $user.domain + '\'
}
$u = $u + $user.user
$proc | add-member -membertype noteproperty -name user -value $u

$proc
