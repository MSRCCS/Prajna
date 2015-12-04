##############################################################################
# Configuration file for the tests
##############################################################################

# the name of the user
$UserName = "SomeOne"

##############################################################################
# Configuration for the cluster
##############################################################################


# Populate this variable with an array of machine names. 
# It can be literls, or it can be code that generates an array of machine names from an external source (e.g. a CSV file)
$ComputerNames =  @("Machine1", "Machine2")

# The location of Prajna client binary
$SourceLocation = Join-Path (Get-Location) "..\..\..\bin\Debugx64\Client"

# The location where the Prajna client should be deployed on remote machines
$ClientLocation = "C:\SomeOneDeployment\PrajnaClient"

# The port that the Prajna daemon uses
$Port = 1005

# The range of job ports
$JobPortRange = "1350-1399"

# If credential is needed to access remote machines, provide the credential
[PSCredential] $Cred # = Get-Credential

# Verbosity Level
$Verbose = 4

##############################################################################
# Configuration for the samples
##############################################################################

# The directory that contains samples
$SamplesDir = "..\..\..\samples"

# The build flavor of samples
$BuildFlavor = "Debugx64"

# The files under this directory will be uploaded to the cluster during tests
$DKVCopyLocalUploadDir = "C:\GitHub\Prajna\bin"

# The url list for web crawler
$WebCrawlerUrlList = "image.list"

# $WebCrawlerUrlList contains a list of URLs to be distributed crawled by DistributedWebCrawler.exe
# each line of the file contains tab separated information. The (WebCrawlerKey)th column contaisn the URL to be crawled. 
$WebCrawlerKey = 4

# The directory that contains log for DistributedLogAnalysis to analyze
$LogDirForAnalysis = "C:\Prajna\Log"

##############################################################################

Set-Variable -Name UserName -Value $UserName -Scope 1
Set-Variable -Name ComputerNames -Value $ComputerNames -Scope 1
Set-Variable -Name SourceLocation -Value $SourceLocation -Scope 1
Set-Variable -Name ClientLocation -Value $ClientLocation -Scope 1
Set-Variable -Name Port -Value $Port -Scope 1
Set-Variable -Name JobPortRange -Value $JobPortRange -Scope 1
Set-Variable -Name Cred -Value $Cred -Scope 1

Set-Variable -Name Verbose -Value $Verbose -Scope 1
Set-Variable -Name SamplesDir -Value $SamplesDir -Scope 1
Set-Variable -Name BuildFlavor -Value $BuildFlavor -Scope 1
Set-Variable -Name DKVCopyLocalUploadDir -Value $DKVCopyLocalUploadDir -Scope 1
Set-Variable -Name WebCrawlerUrlList -Value $WebCrawlerUrlList -Scope 1
Set-Variable -Name WebCrawlerKey -Value $WebCrawlerKey -Scope 1
Set-Variable -Name LogDirForAnalysis -Value $LogDirForAnalysis -Scope 1
