# Setup PowerShell script for **Prajna** 

The script prepareclient.ps1 is designed to prepare a windows domain-joined machine to be ready for Prajna deployment. It should be executed once with **Administrative** privilege on each of the machine.

Sometime, the following warning may appear, 

```
WARNING: Waiting for service 'Windows Remote Management (WS-Management) (winrm)' to stop...
```

Please simply wait, and the script should proceed. 