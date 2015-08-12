param
(
    [Parameter(Mandatory = $true)]
    [string] $program,

    [Parameter(Mandatory = $true)]
    [string] $Name,

    [Parameter(Mandatory = $false)]
    [string] $ServiceName,

    [Parameter(Mandatory = $false)]
    [PSCredential] $creds,

    [Parameter(Mandatory = $false)]
    [System.Management.Automation.Runspaces.PSSession] $session
)

$remoteSession = .\getremotesession.ps1 -Name $Name -ServiceName $ServiceName -creds $creds -session $session

Invoke-Command -Session $remoteSession { Set-StrictMode -Version Latest }
 
# Get session manager & add firewall rules
$setFirewall =
{
    param ([string] $Name, [string] $program)
    
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
    
    Write-Host "Setting firewall rules on $Name for program $program"
    AllowProgramFirewall $program "$program"
}

$res = Invoke-Command -Session $remoteSession $setFirewall -ArgumentList $Name, $program
Write-Host $res

# remove session if locally created
if (-not $session)
{
    Remove-PSSession -session $remoteSession
}

