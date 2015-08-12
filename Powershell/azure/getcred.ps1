##############################################################################
## Copyright 2015, Microsoft.	
## All rights reserved
## Author: Jin Li
## Date: Jan. 2015
## Get the credential for remote execution. If both the credential file and username information 
## exists, it will be used to construct the credential. If not, a prompt will be popped up to ask the 
## user for his/her credential to run on the remote machine. 
##############################################################################

Invoke-Expression ./config.ps1

if ( (-Not $User) -Or (-Not $CredentialFile ) -Or (-Not (Test-Path $CredentialFile)) ) {
    write-host "Please input remote credential for execution"
    $cred1 = Get-Credential 
	Set-Variable -Name cred -Value $cred1 -Scope 1
}
else
{
    $password = Get-Content $CredentialFile | ConvertTo-SecureString
    $cred1 = New-Object System.Management.Automation.PsCredential( $User, $password )
	Set-Variable -Name cred -Value $cred1 -Scope 1

}
