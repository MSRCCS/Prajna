##############################################################################
## Store Credential
## Author: Jin Li
## Date: Apr. 2015                                                     
##############################################################################
Invoke-Expression ./config.ps1

write-host "Credential File = " $CredentialFile

$cred = Get-Credential 
$cred.Password | ConvertFrom-SecureString | Set-Content $CredentialFile