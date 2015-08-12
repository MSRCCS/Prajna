param($user="yuxhu",
      $domain="redmond",
      $group="administrators",
      $machineName,
      $cluster,
      $clusterLst)

function AddLocalUser
{
    Param(
        $computer=$env:computername,
        $group="LogReaders",
        $userdomain=$env:userdomain,
        $username=$env:username
    )
    ([ADSI]"WinNT://$computer/$Group,group").psbase.Invoke("Add",([ADSI]"WinNT://$domain/$user").path)
}
 
Invoke-Expression .\parsecluster.ps1
 
#$user = "t-mohsha"
#$user = "t-hongzl"
#$user = "yuxhu"
#$domain = "redmond"
#$Group = "ad

#$computers = @();
#for ($i=1;$i -le 20; $i++) {
#    $computers += [string]::Format("onenet{0:00}",$i)
#}
#write-host $computers
#$computers = @("VERARI-G12-401",
#               "VERARI-G12-402",
#               "VERARI-G12-406",
#               "VERARI-G12-408",
#               "VERARI-G12-504")

 
#foreach ($Computer in $Computers)
foreach ($Computer in $machines) 
{
    write-host $computer -foregroundcolor green
    AddLocalUser -computer $Computer -group $group -userdomain $domain -username $user
}
 

