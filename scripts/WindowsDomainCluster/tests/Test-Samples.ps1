##############################################################################
# Script that runs Prajna samples
##############################################################################


$curLocation = Get-Location

# Get cluster configuration
.\Config.ps1

# Deploy to the cluster
Write-Progress -Activity "Deploy Prajna" -Status "In progress"
Set-Location -Path ..
.\Deploy-Clients.ps1 -ComputerNames $ComputerNames -SourceLocation $SourceLocation -ClientLocation $ClientLocation -Port $Port -JobPortRange $JobPortRange -Cred $Cred -LogLevel $Verbose
Set-Location -Path $curLocation
Write-Progress -Activity "Deploy Prajna" -Status "Completed"

# Generate cluster list file
$clusterName = $UserName + "-" + [Guid]::NewGuid().ToString()
$clusteFileName = $clusterName + ".lst"
$clusterFilePath = Join-Path $env:TEMP $clusteFileName
 
$clusterName + "," + $Port | Out-File $clusterFilePath
Foreach ($ComputerName in $ComputerNames) 
{
    $ComputerName | Out-File $clusterFilePath -Append
}

##############################################################################
# Run Samples
##############################################################################

##############################################################################
# DKVCopy Test
##############################################################################

$dkvCopyGuid = [Guid]::NewGuid().ToString()
$dkvCopy = [IO.Path]::Combine($SamplesDir, "DKVCopy", "bin", $BuildFlavor, "DKVCopy.exe")
$dkvCopyDKVName = $UserName + "-" + $dkvCopyGuid
$dkvCopyLog = [IO.Path]::Combine($env:TEMP, $dkvCopyDKVName, "Log")

Write-Progress -Activity "DKVCopy Tests" -Status "Start"

# Test Write
Write-Progress -Activity "DKVCopy Test: Write" -Status "Start"
$writeLog = Join-Path $dkvCopyLog "write"
& $dkvCopy -clusterlst $clusterFilePath -localdir $DKVCopyLocalUploadDir -remoteDKVName $dkvCopyDKVName -in -rep 3 -balancer 1 -verbose $Verbose -rec -log $writeLog -con

# Test Read
$localSaveDir = Join-Path $env:TEMP ([Guid]::NewGuid().ToString())
Write-Progress -Activity "DKVCopy Test: Read" -Status "Start"
$readLog = Join-Path $dkvCopyLog "read"
& $dkvCopy -clusterlst $clusterFilePath -localdir $localSaveDir -remoteDKVName $dkvCopyDKVName -out -verbose $Verbose -log $readLog -con

# Test fold
$localSaveFoldDir = Join-Path $env:TEMP ([Guid]::NewGuid().ToString())
Write-Progress -Activity "DKVCopy Test: Fold" -Status "Start"
$foldLog = Join-Path $dkvCopyLog "fold"
& $dkvCopy -clusterlst $clusterFilePath -localdir $localSaveFoldDir -remoteDKVName $dkvCopyDKVName -out -verbose $Verbose -spattern  ".*" -hash -log $foldLog -con

Write-Progress -Activity "DKVCopy Tests" -Status "Completed"

##############################################################################
# DistributedWebCrawler Test
##############################################################################

$webCrawlerGuid = [Guid]::NewGuid().ToString()
$webCrawler = [IO.Path]::Combine($SamplesDir, "DistributedWebCrawler", "bin", $BuildFlavor, "DistributedWebCrawler.exe")
$uploadRemoteDKV = $UserName + "-" + $webCrawlerGuid
$webCrawlerLog = [IO.Path]::Combine($env:TEMP, $uploadRemoteDKV, "Log")

Write-Progress -Activity "DistributedWebCrawler Tests" -Status "Start"

Write-Progress -Activity "DistributedWebCrawler Test: Upload" -Status "Start"
$uploadLog = Join-Path $webCrawlerLog "upload"
& $webCrawler -clusterlst $clusterFilePath -upload $WebCrawlerUrlList -uploadKey $WebCrawlerKey -remote $uploadRemoteDKV -verbose $Verbose -rep 3 -slimit 100 -log $uploadLog -con

Write-Progress -Activity "DistributedWebCrawler Test: Download" -Status "Start"
$downloadLog = Join-Path $webCrawlerLog "download"
& $webCrawler -clusterlst $clusterFilePath -download 4 -remote $uploadRemoteDKV -verbose $verbose -log $downloadLog -con -task 

<#  Note: CountTag test hits a Null-Reference Exception: to be investigated

Write-Progress -Activity "DistributedWebCrawler Test: Count Tag" -Status "Start"
$countTagLog = Join-Path $webCrawlerLog "countTag"
$tagKey = [int] $WebCrawlerKey + 1
& $webCrawler -clusterlst $clusterFilePath -remote $uploadRemoteDKV -counttag $tagKey  -verbose $Verbose -log $countTagLog -con

#>
Write-Progress -Activity "DistributedWebCrawler Tests" -Status "Completed"

##############################################################################
# DistributedKMeans Test
##############################################################################

$kMeansGuid = [Guid]::NewGuid().ToString()
$kMeans = [IO.Path]::Combine($SamplesDir, "DistributedKMeans", "bin", $BuildFlavor, "DistributedKMeans.exe")
$kMeansDKV = $UserName + "-" + $kMeansGuid
$kMeansLog = [IO.Path]::Combine($env:TEMP, $kMeansDKV, "Log")

Write-Progress -Activity "DistributedKMeans Tests" -Status "Start"

Write-Progress -Activity "DistributedKMeans Test: Generate Remote Random Vector" -Status "Start"
$genVecLog = Join-Path $kMeansLog "GenVector"
& $kMeans -clusterlst $clusterFilePath -local vector.data -gen -in -remote $kMeansDKV -num 100000 -class 3 -var 0.1 -dim 80 -noise 0.01 -verbose $Verbose -log $genVecLog -con

Write-Progress -Activity "DistributedKMeans Test: Generate Remote Random Vector Class 1000" -Status "Start"
$kMeans1000DKV = $kMeansDKV + "-1000" 
$genVecLog = Join-Path $kMeansLog "GenVector1000"
& $kMeans -clusterlst $clusterFilePath -local vector1000.data -gen -in -remote $kMeans1000DKV -mapreduce -num 100000 -class 1000 -var 0.1 -dim 80 -noise 0.01 -verbose $Verbose -log $genVecLog -con

Write-Progress -Activity "DistributedKMeans Test: Read Remote Random Vector" -Status "Start"
$readVecLog = Join-Path $kMeansLog "ReadVector"
& $kMeans -clusterlst $clusterFilePath -local vector.data -gen -out -remote $kMeansDKV -verbose $Verbose -log $readVecLog -con

Write-Progress -Activity "DistributedKMeans Test: Read Remote Random Vector with additonal noise" -Status "Start"
$kMeansDKVNoise = $kMeansDKV + "_NOISE"
$readVecLog = Join-Path $kMeansLog "ReadVectorNoise"
& $kMeans -clusterlst $clusterFilePath -local vector.data -gen -out -remote $kMeansDKVNoise -verbose $Verbose -log $readVecLog -con

Write-Progress -Activity "DistributedKMeans Test: DKV Union" -Status "Start"
$kMeansDKVUnion = $kMeansDKV + "," + $kMeansDKVNoise
$readVecLog = Join-Path $kMeansLog "DKVUnion"
& $kMeans -clusterlst $clusterFilePath -local vector.data -gen -out -remotes $kMeansDKVUnion -verbose $Verbose -log $readVecLog -con

Write-Progress -Activity "DistributedKMeans Test: Read MapReduce Result" -Status "Start"
$kMeans1000DKVMapReduce = $kMeans1000DKV + "_MapReduce"
$readVecLog = Join-Path $kMeansLog "ReadMapReduce"
& $kMeans -clusterlst $clusterFilePath -local vector1000.data -verify -out -remote $kMeans1000DKVMapReduce -verbose $Verbose -log $readVecLog -con

<# Non-Reference Exception: investigations needed

Write-Progress -Activity "DistributedKMeans Test: Cross Join 1" -Status "Start"
$readVecLog = Join-Path $kMeansLog "CrossJoin1"
& $kMeans -clusterlst $clusterFilePath -dist 1 -remote $kMeansDKV -verbose $Verbose -log $readVecLog -con

Write-Progress -Activity "DistributedKMeans Test: Cross Join 2" -Status "Start"
$readVecLog = Join-Path $kMeansLog "CrossJoin2"
& $kMeans -clusterlst $clusterFilePath -dist 2 -remote $kMeansDKV -verbose $Verbose -log $readVecLog -con

#>

Remove-Item vector.data
Remove-Item vector1000.data

Write-Progress -Activity "DistributedKMeans Tests" -Status "Completed"

##############################################################################
# DistributedSort Test
##############################################################################

$sortGuid = [Guid]::NewGuid().ToString()
$sort = [IO.Path]::Combine($SamplesDir, "DistributedSort", "bin", $BuildFlavor, "DistributedSort.exe")
$sortLog = [IO.Path]::Combine($env:TEMP, $sortGuid, "Log")

Write-Progress -Activity "DistributedSort Test" -Status "Start"

& $sort -sort -num 1000 -dim 100000 -clusterlst $clusterFilePath -verbose $Verbose -nump 80 -log $sortLog -con

Write-Progress -Activity "DistributedSort Test" -Status "Completed"


##############################################################################
# DistributedLogAnalysis Test
##############################################################################

$logGuid = [Guid]::NewGuid().ToString()
$logAnalysis = [IO.Path]::Combine($SamplesDir, "DistributedLogAnalysis", "bin", $BuildFlavor, "DistributedLogAnalysis.exe")
$logLog = [IO.Path]::Combine($env:TEMP, $logGuid, "Log")

Write-Progress -Activity "DistributedLogAnalysis Test" -Status "Start"

& $logAnalysis -clusterlst $clusterFilePath -num 4 -reg "!!! Error !!!,0,0" -reg "!!! Warning !!!,0,0" -reg "!!! Exception !!!,0,0" -dir $LogDirForAnalysis -rec -verbose $Verbose -log $logLog -con

Write-Progress -Activity "DistributedLogAnalysis Test" -Status "Completed"

##############################################################################

# Delete cluster list file
Remove-Item $clusterFilePath

# Stop Prajna on the cluster
Set-Location -Path ..
.\Stop-Clients.ps1 -ComputerNames $ComputerNames -ClientLocation $ClientLocation -Cred $Cred
Set-Location -Path $curLocation