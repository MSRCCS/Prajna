# Deployment of Prajna

This document describes the basics on **Prajna** deployment.

# Introduction 

## Machine provision, deployment, and launch the client

  The essential of Prajna deployment to a cluster (or just a set of machines) is to copy and launch the Prajna daemon, which will in turn launch Prajna container 
  to each properly provisioned machine. 

  A Prajna daemon is going to use 
  * a port for daemon to accept job requests
  * a range of ports ( one port for each container) to communicate between containers 

  The most basic requirement for a machine is to launch the Prajna daemon, and open these ports. Thereafter, the Prajna container can be launched using
  these ports. Of course, these machines need to be accessible from the machine where user runs Prajna applications.

  The daemon is called "PrajnaClient.exe"
  * For user of the Nuget package, when the Prajna package is installed, the daemon and its dependencies are located in subfolder tools\Client
  * For user of the GitHub source code, after the source tree is built, the daemon and its dependencies are located in subfolder bin\ReleaseX64\Client or bin\DebugX64\Client

  To launch the daemon, use the following arguments
```
      -port 1005 -jobports 1250-1300
```
  An extra argument can be provided it request authentication is desired.
```
      -pwd somepasswd  
```
Depending on the environment and user preference, the way to provision, deploy, and launch the client can vary. Please refer to the subfolders for additional steps in each environment. 

## Cluster List File

  Once the client is deployed to a set of machines. User needs to construct a cluster list file that the Prajna application will use.

  It is a plain-text file with an ".lst" extension. An example looks like
 ```
      MyCluster,1005,somepasswd
      Machine0, port0
      Machine1, port1
      Machine2, port2
 ```
  The first line specifies "ClusterName,Port,Password". 
  ClusterName is a unique identifier of the cluster that the runtime will use. 
  Port is what specified by "-port" argument during client launch. If the daemon is launched with a "-pwd" argument, please also specify the password when using the cluster.

## Verify the deployment

  Use a tool called ClusterStatus.exe
  * For user of the Nuget package, it is located in subfolder tools\ClusterStatus
  * For user of the GitHub source code, after the source tree is built, it is located in subfolder bin\ReleaseX64\ClusterStatus or bin\DebugX64\ClusterStatus

  If the tool can show the disk usages etc on the target machines, it means the deployment is successful.

## Use the Cluster

  Prajna applications will construct the cluster object by (in C#) supplying the path to this list file:
```
      var cluster = new Cluster(pathToLstFile);
```

# Deployment to Windows Cluster


This section describes how to use provided Powershell scripts to deploy and launch the Prajna client on a cluster of windows domain-joined machines.


## Scripts


  For user of Nuget package, the scripts are located in tools\Scripts\WindowsDomainCLuster
  For user of GitHub source, the scripts are located at src\Scripts\WindowsDomainCluster


## Prerequisites


To deploy Prajna daemon and container on all target machines, the requirements are:

1. User needs to have a credential with admin privilege, 
2. PowerShell remote execution is enabled,
3. The ports that will be used by the client are opened. 

Steps 2 and 3 can be completed via script in the Setup subfolder. 

## Deployment


  In PowerShell, cd to the folder where the scripts locate, and invoke "Deploy-Clients.ps1"
```
    PS>  .\Deploy-Clients.ps1 -ComputerNames @("Machine1", "Machine2", "Machine3") -SourceLocation ..\..\Client -ClientLocation C:\Deployment\PrajnaClient -Port 1005 -JobPortRange 1250-1299
```
    -ComputerNames specifies an array of target machines
    -SourceLocation specifies the absolute or relative path to the folder that contains the client
    -ClientLocation specifies the path to put the client on target machines
    -Port specifies the port for the client to accept job requests
    -JobPortRange specifies the range of ports to be used by the client

To enable authentication, add an extra "-Password" parameter to pass in a string of desired password. If an explict credential is required for executing powershell command on remote machines, use:

```
	PS> $Cred = Get-Credential
```

It will prompt a dialog for input the credential. Here also assumes that the credential is the same for all remote machines. Then
	
```
    PS> .\Deploy-Clients.ps1 -ComputerNames @("Machine1", "Machine2", "Machine3") -SourceLocation ..\..\Client -ClientLocation C:\Deployment\PrajnaClient -Port 1005 -JobPortRange 1250-1299 -Cred $Cred
```

## Other scripts


  * Start-Clients.ps1 : start clients on machines by specify the location of the clients on the machines, for example
```
     PS> .\Start-Clients.ps1 -ComputerNames @("Machine1", "Machine2", "Machine3") -ClientLocation C:\Deployment\PrajnaClient -Port 1005 -JobPortRange 1250-1299 -ShutdownRunningClients $true
```
  * Stop-Clients.ps1: stop clients on machines by specify the location of the clients on the machines
```
     pS> .\Stop-Clients.ps1 -ComputerNames @("Machine1", "Machine2", "Machine3") -ClientLocation C:\Deployment\PrajnaClient
```
  * Start-Client.ps1 : start the client on a single machine
  * Stop-Client.ps1: stop the client on a single machine
  


