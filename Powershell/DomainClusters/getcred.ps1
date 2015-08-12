##############################################################################
## Copyright 2015, Microsoft.	
## All rights reserved
## Author: Jin Li
## Date: Jan. 2015
## Get the credential for remote execution. If both the credential file and username information 
## exists, it will be used to construct the credential. If not, a prompt will be popped up to ask the 
## user for his/her credential to run on the remote machine. 
##############################################################################
if ($GlobalCred)
{
    Set-Variable -Name cred -Value $GlobalCred -Scope 1
}
if (-not $cred) 
{
    Invoke-Expression ./config.ps1

    if ( (-Not $User) -Or (-Not $CredentialFile ) -Or (-Not (Test-Path $CredentialFile)) ) {
        write-host "Please input remote credential for execution"
        $cred1 = Get-Credential 
        Set-Variable -Name cred -Value $cred1 -Scope 1
    }
    else
    {
        write-host "Get credential for user $User from file $password"
        $password = Get-Content $CredentialFile | ConvertTo-SecureString
        $cred1 = New-Object System.Management.Automation.PsCredential( $User, $password )
        Set-Variable -Name cred -Value $cred1 -Scope 1
    }
}
