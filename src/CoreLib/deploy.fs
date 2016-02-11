(*---------------------------------------------------------------------------
    Copyright 2013 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    File: 
        deploy.fs
  
    Description: 
        Module to aid deployment of Prajna program

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.IO

open Prajna
open Prajna.Tools

type internal GCMode = 
    | NoGarbageCollection
    | GarbageCollectionWhenNoIO

/// Enumeration Class that controls the behavior of the remote container launch
type TaskLaunchMode = 
    /// Launch a remote container, if there are containers of different signature exist, terminate those containers (with an outdated execution roster). 
    | LaunchAndTerminateDifferentVersion = 0
    /// Launch a remote container, don't check whether there are containers of different signatures. 
    | LaunchAndDisregardDifferentVersion = 1
    /// Launch a remote container, fail if there are containers of different signatures. 
    | LaunchAndFailWhenDifferentVersionExist = 2
    /// Do not launch a remote container, usually used to close a service in remote container. 
    | DonotLaunch = 3

/// Control the launch of container in Debug or Release mode. 
type DebugMode = 
    /// Always launch container in Debug Mode. 
    | LaunchInDebug = 1
    /// Always launch container in Release Mode. 
    | LaunchInRelease = 2
    /// Launch container based on the Debug flag in the main module. 
    | LaunchInEither = 3

/// A set of default parameter that controls Prajna execution behavior
type DeploymentSettings() = 
    // system drive   
    // Mono: use user's home directory for now, more investigations needed
    static let systemDrive = if Runtime.RunningOnMono then Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) else Path.GetPathRoot(Environment.SystemDirectory)
    // the drives that are excluded for data storage
    static let excludedDrivesForDataStorage = 
        if Runtime.RunningOnMono then
            Array.empty
        else
            let drivesInfo = DriveInfo.GetDrives() |> Array.filter ( fun dInfo -> dInfo.IsReady && dInfo.DriveType = DriveType.Fixed )
            if Utils.IsNull drivesInfo || Array.isEmpty drivesInfo then
                failwith("There is no driver")
            else
                if not (drivesInfo |> Array.exists(fun f -> f.Name = systemDrive)) then
                    failwith(sprintf "System drive '%s' does not exist!" systemDrive)
                if drivesInfo.Length = 1 then
                    // If there is only one drive, do not exclude any drive
                    Array.empty
                else 
                    // exclude system drive for data storage
                    [| KeyValuePair(systemDrive, true) |]

    // Somehow when Prajna is running in Mono on Linux. the "GetCurrentProcess().MainModule" takes seconds to return, cache the result here
    static let mainModule = System.Diagnostics.Process.GetCurrentProcess().MainModule
    static member val internal MainModuleFileName = mainModule.FileName
    static member val internal MainModuleModuleName = mainModule.ModuleName

    /// Running on Mono
    static member val internal RunningOnMono = Runtime.RunningOnMono
    /// Control the launch of container in Debug or Release mode. 
    static member val LaunchDebugMode = DebugMode.LaunchInDebug with get, set
    /// Control whether determiate remote container of different versions. 
    static member val DefaultLaunchBehavior = TaskLaunchMode.LaunchAndTerminateDifferentVersion with get, set
    /// Whether daemon is allowed to relay for job, if false, each job must secure a job port. 
    static member val internal bDaemonRelayForJobAllowed = false with get, set
    /// Whether to launch remote container in App Domain 
    static member val internal AllowAppDomain = false with get, set
    static member val internal LibVersion = "v0.0.0.6" with get, set
    static member val internal RegisterKeyForMasterInfo = "PRAJNA_MASTER_INFO" with get, set
    static member val internal RegisterKeyForDataDir = "PRAJNA_DIR" with get, set
    
    // Mono Note: For Mono, use user's home directory for now, more investigations needed
    static member val internal RootFolder = if DeploymentSettings.RunningOnMono then Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) else systemDrive with get
    /// A set of drives to be excluded for data storage
    static member val internal ExcludedDrives = ConcurrentDictionary<_,_>( excludedDrivesForDataStorage, StringComparer.OrdinalIgnoreCase ) with get
    static member private GetAllDataDrivesInfoImpl() = 
        let drivesInfo = DriveInfo.GetDrives() 
        if DeploymentSettings.RunningOnMono then 
            // Note: Environment.SpecialFolder.UserProfile is not necessarily be on the same drive mounted with "/"
            //       .Net API seems unable to get such information, and will need to use something in Mono.Unix namespace in the future
            //       to make it right.
            let drive = drivesInfo |> Seq.find(fun dInfo -> dInfo.Name = "/")
            seq { yield ((Environment.GetFolderPath(Environment.SpecialFolder.UserProfile)), drive.TotalFreeSpace) }
        else            
            drivesInfo |> Seq.filter ( fun dInfo -> dInfo.IsReady && dInfo.DriveType=DriveType.Fixed ) 
                       |> Seq.filter( fun dInfo -> not (DeploymentSettings.ExcludedDrives.ContainsKey( dInfo.Name )) )
                       |> Seq.map (fun dInfo -> (dInfo.Name, dInfo.TotalFreeSpace))
    static member val GetAllDataDrivesInfo = DeploymentSettings.GetAllDataDrivesInfoImpl with get, set
    static member private  GetAllDrivesImpl() = 
        DeploymentSettings.GetAllDataDrivesInfoImpl() |> Seq.map( fun dInfo -> dInfo |> fst )
    /// Is data drive 
    static member private IsDataDriveImpl(driveName:string) = 
        if driveName.EndsWith( """:\""", StringComparison.Ordinal) then 
            not (DeploymentSettings.ExcludedDrives.ContainsKey( driveName ))
        elif driveName.EndsWith( """:""", StringComparison.Ordinal) then 
            not (DeploymentSettings.ExcludedDrives.ContainsKey( driveName+"""\""" ))
        else
            not (DeploymentSettings.ExcludedDrives.ContainsKey( driveName+""":\""" )) 
    /// Is data drive 
    static member val IsDataDrive = DeploymentSettings.IsDataDriveImpl with get, set
    /// Get all data drives
    static member val internal GetAllDrives = DeploymentSettings.GetAllDrivesImpl with get, set
    static member private GetAllDataDrivesImpl() = 
        if DeploymentSettings.RunningOnMono then 
            // Mono Note: use home directory as StorageDrive for now, more investigations are needed
            [| Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) |]
        else
            let drives = DeploymentSettings.GetAllDrives()
            drives |> Seq.filter( fun drive -> not (DeploymentSettings.ExcludedDrives.ContainsKey( drive )) ) |> Seq.toArray
    static member val internal GetAllDataDrives = DeploymentSettings.GetAllDataDrivesImpl with get, set
    /// Get first data drives
    static member private GetFirstDataDriveImpl() = 
        if DeploymentSettings.RunningOnMono then 
            // Mono Note: use home directory as DataDrive for now, more investigations are needed
            Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) 
        else 
            let drives = DeploymentSettings.GetAllDrives()
            drives |> Seq.find( fun drive -> not (DeploymentSettings.ExcludedDrives.ContainsKey( drive )) )
    static member val internal GetFirstDataDrive = DeploymentSettings.GetFirstDataDriveImpl with get, set
    /// Directory at remote node that holds Prajna operating state. 
    static member val LocalFolder = Path.Combine(DeploymentSettings.RootFolder, "Prajna") with get
    /// Directory at remote node that holds strong hashed file of the remote execution roster. 
    static member val HashFolder = @"HashFolder" with get
    static member val internal LogFolder = Path.Combine(DeploymentSettings.LocalFolder, "Log") with get, set
    static member val internal ServiceFolder = Path.Combine(DeploymentSettings.LocalFolder, "Service") with get, set
    static member val internal KeyFile = Path.Combine( [| DeploymentSettings.LocalFolder; "Keys"; "ClientKeys" |] ) with get
    static member val internal MachineIdFile = @"MachineId" with get
    static member val internal DataDrive = DeploymentSettings.GetFirstDataDrive() with get, set
    static member val internal StorageDrives = DeploymentSettings.GetAllDataDrives() with get, set
    /// Are we using all drives 
    static member val internal StatusUseAllDrivesForData = false with get, set
    // Use all drives for data. 
    static member internal UseAllDrivesForData() = 
        DeploymentSettings.StatusUseAllDrivesForData <- true
        DeploymentSettings.ExcludedDrives.Clear()
        DeploymentSettings.DataDrive <- DeploymentSettings.GetFirstDataDrive()       
        DeploymentSettings.StorageDrives <- DeploymentSettings.GetAllDataDrives()
    static member val internal MasterPort = 1080 with get, set
    static member val internal ControlPort = 1081 with get, set
    /// Directory at remote node that holds Prajna data files
    static member val DataFolder = @"PrajnaData" with get
    static member val internal ClusterFolder = "Cluster" with get
    static member val internal AssemblyFolder = Path.Combine(DeploymentSettings.LocalFolder, "Assembly") with get
    static member val internal JobFolder = "Job" with get
    static member val internal JobDependencyFolder = Path.Combine(DeploymentSettings.LocalFolder, "JobDependency") with get
    static member val internal ClusterInfoPlainGuid = Guid ( "EB72D75C-3D00-4E73-990D-312EA0EC12BC" )
    static member val internal ClusterInfoPlainV2Guid = Guid ( "CD6A5FF2-A2B4-4A5E-B0BD-9DA21581F15C" )
    static member val internal ClusterInfoEndV2Guid = Guid ( "024FAE18-7352-48F2-AD1F-F651D800500D" )
    static member val internal ClusterInfoPlainV3Guid = Guid ( "429C2A41-517C-43E6-AB0B-9935D21424F9" )
    static member val internal ClusterInfoEndV3Guid = Guid ( "649EB25C-1475-492E-A19A-B752E2D8E678" )
    static member val internal BlobCloseMarker = Guid ( "2F6BF526-925D-4034-8563-639B188F974C" )
    static member val internal BlobBeginMarker = Guid ( "D859802F-CFB3-4B8B-9A99-D4AFE3DFF784" )
    static member internal ClusterInfoName( cur ) = "Cluster_" + StringTools.VersionToString(cur) + ".inf"
    /// Number of log files to be kept at the remote node. 
    static member val internal LogFileRetained = 100 with get, set
    // static member ClusterLstName( cur ) = "Cluster_" + StringTools.VersionToString(cur)  + ".lst"
    static member internal  ClusterLstName( name, cur ) = name  + ".lst"
    /// remove inactive DSet Peer after 600 seconds 
    static member val internal DSetPeerTimeout = 600 with get, set
    static member val internal ClientIP = "" with get, set
    static member val internal ClientPort = 1082 with get, set
    /// Port range for JobPort
    static member val internal JobIP = "" with get, set
    static member val internal JobPortMin = 1100 with get, set
    static member val internal JobPortMax = 1150 with get, set
    //500ms: 10GB*0.5/8
    static member val internal SendTokenBucketSize = 655360 with get, set
    static member val internal DefaultRcvdSpeed = 40000000000L with get, set
    static member val internal SIO_LOOPBACK_FAST_PATH = 0x98000010 with get
    /// Interval that we will process acknowledgement command (in microsecond). 
    static member val internal ClusterProcessingAcknoledgementInterval = 100 with get
    /// Second after CloseAfterFlush 
    static member val internal SecondsAfterCloseAfterFlush = 3L with get
    /// Remapping interval (in  sec ), that the host will seek to find a new peer to assign a partition. 
    static member val RemappingIntervalInMillisecond = 1000 with get
    /// Inactive time for monitoring of main thread. 
    static member val internal ClientInactiveTimeSpan = 10. with get
    /// Inactive time for monitoring of main thread. 
    static member val internal IOThreadMonitorTimeSpan = 1L with get
    /// Sleep time for the main loop in Job. 
    static member val internal SleepTimeJobLoop = 5 with get
    /// Timeout for an action (second)
    static member val internal TimeOutAction = 300000L with get, set
    /// Final Timeout before disconnect
    static member val internal TimeoutBeforeDisconnect = 1000. with get, set
    /// Whether this is a separate process (AppDomain/Application) 
    static member val internal LoadCustomAssebly = false with get, set
    /// Whether x86 or x64 bit 
    static member val internal PlatformFlag = if IntPtr.Size=8 then "x64" else "x86" with get
    /// Maximum working set
    static member val internal MaximumWorkingSet = System.Diagnostics.Process.GetCurrentProcess().MaxWorkingSet with get
    /// Maximum Memory 
    static member val internal MaxMemoryLimitInMB = 900L with get, set
    /// WaitAny time for the execution engine
    static member val internal TimeOutForWaitAny = 3 with get, set
    /// Default SerializationLimit for a DSet
    static member val DefaultSerializationLimit = 1000 with get, set
    /// ReSerialization For Cache
    static member val internal DefaultCacheSerializationLimit = 1 with get, set
    /// ReSerialization For Cache
    static member val internal DefaultIOMaxQueue = 100 with get, set
    /// ReSerialization For Cache
    static member val internal DefaultIOQueueReadUnblocking = 2 with get, set
    //static member val internal DefaultIOQueueWriteUnblocking = 100 with get, set
    /// Track seen partitions
    static member val internal TrackSeenKeyValue = false with get, set
    /// Save Initial Metadata?
    static member val internal bSaveInitialMetadata = false with get, set

    /// Client Extension Process that host unsafe unmanaged code
    static member val internal  ClientExtensionExcutable = "PrajnaClientExt.exe" with get, set
    static member val internal  ClientExtensionConfig = DeploymentSettings.ClientExtensionExcutable + ".config" with get, set

    /// Control Default Number of Jobs to be executed related to core
    static member val internal ParallelJobsPerCore = 0.5 with get, set
    static member internal NumParallelJobs( numCores: int ) = 
        let ret = 
            if DeploymentSettings.ParallelJobsPerCore > 0.0 then 
                int (Math.Round( float numCores * DeploymentSettings.ParallelJobsPerCore ))
            else
                int (Math.Round( -DeploymentSettings.ParallelJobsPerCore ))
        // Remove Cap 
        ret

    /// default group by size used in DSet
    static member val DefaultGroupBySerialization = 10000 with get, set
    /// Short Garbage Collect Interval (ms)
    /// If there is always activity for this duration, at least one shallow GC should be exected 
    static member val internal ShortGCInterval = 10. with get, set
    /// Long Garbage Collect Interval (ms)
    /// If there is always activity for this duration, a deep GC should be exected 
    static member val internal LongGCInterval = 1000. with get, set
    /// Maximum Asynchronous Tasks that are allowed to pending in downstream. 
    /// This is to avoid memory exhaustion if too many tasks are pending 
    static member val internal MaxDownStreamAsyncTasks = 8 with get, set
    /// JinL to SanjeevM: can you please provide explanation for the following three parameters? 
    /// Maximum combined buffer devoted to sending queue (in bytes) 
    static member val MaxSendingQueueLimit = 1024 * 1024 * 50 with get, set
    /// Maximum network stack memory (total across all connections), 0 means unbounded (in bytes)
    static member val MaxNetworkStackMemory = 1024 * 1024 * 1024 * 4 with get, set
    /// Maximum network stack memory (total across all connections) as percentage of total (as percent)
    static member val MaxNetworkStackMemoryPercentage = 0.5 with get, set
    /// The buffer size used by SocketAsyncEventArgs
    static member val NetworkSocketAsyncEventArgBufferSize = 128000 with get, set
    //static member val NetworkSocketAsyncEventArgBufferSize = 256000 with get, set // for sort benchmark
    /// The initial # of buffers in SocketAsyncEventArg stack
    static member val InitNetworkSocketAsyncEventArgBuffers = 128 with get, set
    //static member val InitNetworkSocketAsyncEventArgBuffers = 8*1024*8 // for sort benchmark
    /// The size of network command queue for sending
    static member val NetworkCmdSendQSize = 100 with get, set
    /// The size of network command queue for receiving
    static member val NetworkCmdRecvQSize = 100 with get, set
    /// The size of network socket async event args queue for sending
    static member val NetworkSASendQSize = 100 with get, set
    /// The size of network socket async event args queue for receiving
    static member val NetworkSARecvQSize = 100 with get, set
    /// The initial # of buffers for shared memory pool used by BufferListStream
    static member val InitBufferListNumBuffers = 128 with get, set
    //static member val InitBufferListNumBuffers = 8*1024*8 with get, set // for sort benchmark
    /// The buffer size of buffers in shared memory pool used by BufferListStream
    static member val BufferListBufferSize = 64000 with get, set
    /// Number of threads for network processing
    static member val NumNetworkThreads = DeploymentSettings.NumParallelJobs(Environment.ProcessorCount) with get, set
    /// Monitor Flow Control 
    /// --------------------------
    /// Flow Control Parameter
    /// --------------------------
    // Block at 4MB per partition
    static member val internal MaxBytesPerPartitionBeforeBlocking = 1L<<<22 with get, set
    // Block on deposit (1MB default)
    static member val internal BlockOnMixDeposit = 1L<<<20 with get, set
    // Get a random name, 1000000 System.Path.GetRandomFileName -> 2000ms
    //                    1000000 System.Guid.NewGuid() -> 1500ms
    static member val internal GetRandomName = fun () -> System.IO.Path.GetRandomFileName() with get, set

    static member val internal PartitionExecutionMode = ExecutionMode.ByThread with get, set
    /// --------------------------
    /// IO Thread parameter
    /// --------------------------
    static member val internal IOThreadPiority = System.Threading.ThreadPriority.AboveNormal with get, set
    /// --------------------------
    /// Job control parameter
    /// --------------------------
    static member val internal NumObjectsPerJob = 50 with get, set
    /// Garbage Collection Level 
    /// 0: no garbage collection
    /// 1: GC.Collect(1) when no IO activity 
    static member val internal GCMode = GCMode.NoGarbageCollection with get, set
    /// --------------------------
    /// Various Timeout value
    /// --------------------------
    static member val internal TimeOutGetRemoteStorageInfo = 45. with get, set // in second
    /// Timeout when trying to setup a remote container (in seconds). The default value is 30 seconds. If the remote execution roster is particularly large, 
    /// e.g., including data files of multi-gigabyte in size, the program should enlarge this timeout value so that there is time to send the entire remote execution roster 
    /// to the remote node. 
    static member val RemoteContainerEstablishmentTimeoutLimit = 30L with get, set // in seconds
    /// Timeout for reserve job port
    static member val internal TimeOutJobPortReservation = 1800. with get, set // in second
    /// Timeout to wait for DefaultJobInfo in DStream to be filled (in milliseconds) 
    static member val internal TimeOutWaitDefaultJobInfo = 1000. with get, set 
    /// Maximum time to wait before the a job start in the current peer (in second)
    static member val internal WarningJobStartTimeSkewAmongPeers = 30. with get, set
    /// Maximum time to wait before the a job start in the current peer (in second)
    static member val internal TimeOutJobStartTimeSkewAmongPeers = 3000. with get, set
    /// Maximum timeout to Flush the task queue and wait for job to end
    static member val internal TimeOutJobFlushMilliseconds = 300000000 with get, set
    /// Timeout for waiting a service to stop
    static member val internal SleepIntervalServiceStopInMs = 10 with get, set
    /// Timeout for waiting a service to stop
    static member val internal TimeOutServiceStopInMs = 120000 with get, set
    /// Monitor Interval for Task Queue Status 
    static member val internal MonitorIntervalTaskQueueInMs = 30000 with get, set
    /// Gap to reconnect to a certain daemon, <0: do not reconnect, 
    static member val internal IntervalToReconnectDaemonInMs = 2000 with get, set
    /// Travel Level for Blob Availability 
    static member val internal TraceLevelBlobAvailability = LogLevel.WildVerbose with get, set
    // JinL: use LogLevel.MildVerbose for debugging, turn up level when finished debugging. 
    /// Trace Level for Blob Send
    static member val internal TraceLevelBlobSend = LogLevel.MildVerbose with get, set
    /// Trace Level for Blob Rcvd
    static member val internal TraceLevelBlobRcvd = LogLevel.MildVerbose with get, set
    /// Trace Level for Blob Rcvd
    static member val internal TraceLevelEveryJobBlob = LogLevel.WildVerbose with get, set
    /// Trace level for network IO
    static member val internal TraceLevelEveryNetworkIO = LogLevel.WildVerbose with get, set



    /// Trace Level for Starting job
    static member val internal TraceLevelStartJob = LogLevel.MildVerbose with get, set
    /// Whether we will touch file when dependency/assembly is copied as a SHA256 named file in the remote node. 
    /// set the parameter to LogLevel.Info will touch dependecy/assembly when they are written. 
    static member val ExecutionLevelTouchAssembly = LogLevel.ExtremeVerbose with get, set
    // JinL: use LogLevel.MildVerbose for debugging. 

    /// Travel Level for Blob Send/Receive 
    static member val internal TraceLevelBlobIO = LogLevel.MildVerbose with get, set
    /// Validate Hash 
    static member val internal TraceLevelBlobValidateHash = LogLevel.MediumVerbose with get, set
    /// Trace Level to Monitor Seq Function
    static member val internal TraceLevelSeqFunction = LogLevel.MildVerbose with get, set
    /// Trace level for WaitHandle
    static member val internal TraceLevelWaitHandle = LogLevel.WildVerbose with get, set

    /// Total Job allowed for Prajna
    static member val internal TotalJobLimit = 80 with get, set 
    /// Light CPU load jobs that can be executed in parallel 
    static member val internal LightJobLimit = 4 with get, set 
    /// Separate Exe Jobs that can be executed in parallel
    static member val internal ExeJobLimit = 80 with get, set    
    /// App domain jobs that can be executed in parallel. 
    static member val internal AppDomainJobLimit = 80 with get, set 


    /// Monitor Interval for StandardError 
    static member val internal StandardErrorMonitorIntervalInMs = 1000 with get, set
    /// Monitor Interval for StandardError 
    static member val internal StandardOutputMonitorIntervalInMs = 1000 with get, set
    /// Monitor Interval for Network Status
    static member val internal NetworkActivityMonitorIntervalInMs = 60000L with get, set

    /// Warning on long blocking send socket (in milliseconds)
    static member val internal WarningLongBlockingSendSocketInMilliseconds = 100. with get, set
    /// Warning on long blocking send socket (in seconds)
    static member val internal MonitorDownstreamTaskQueues = 5. with get, set
    /// Monitor Blocking on the peer connecting socket
    static member val internal MonitorPeerBlockingTime = 1. with get, set
    /// Monitor Blocking on the client connecting socket
    static member val internal MonitorClientBlockingTime = 1. with get, set
    /// Single Task timeout (max time a single task can execute)
    static member val internal SingleTaskTimeout = 300000 with get, set
    /// Single Task timeout (max time a single task can execute)
    static member val internal CollectionTaskTimeout = 3000000 with get, set
    /// Monitoring for RcvdPeerClose & SendPeerConfirmed (in seconds)
    static member val internal PeerMonitorInterval = 10. with get, set
    /// Timer to show sync status
    static member val internal CheckSyncStatusInterval = 5. with get, set
    /// Sleep Time before unloading appdomain
    static member val internal SleepMSBeforeUnloadAppDomain = 0 with get, set
    /// Wait for all jobs done (10hours)
    static member val internal MaxWaitForAllJobsDone = 36000000 with get, set
    /// One Wait for all jobs done 
    static member val internal OneWaitForAllJobsDone = 1000 with get, set
    static member val internal MonitorForLiveJobs = 10000 with get, set
    static member val internal TraceLevelMonitorForLiveJobs = Tools.LogLevel.MildVerbose with get, set
    static member val internal TraceLevelMonitorRemotingMapping = Tools.LogLevel.MildVerbose with get, set
    static member val internal TraceLevelServiceTracking = Tools.LogLevel.MildVerbose with get, set
    /// Use curJob.EnvVars.Add(DeploymentSettings.EnvStringSetJobDirectory, depDirname ) to set the JobDirectory of remote container, where curJob is the current JobDependency object. 
    static member val EnvStringSetJobDirectory = """${PrajnaSetJobDirectory}""" with get, set
    /// Use curJob.EnvVars.Add( envvar, DeploymentSettings.EnvStringGetJobDirectory) 
    /// to setup environment string in relationship to the JobDirectory of the remote container. 
    static member val EnvStringGetJobDirectory = """${PrajnaGetJobDirectory}""" with get, set
    /// JinL to SanjeevM: Please review the following parameter 
    /// should this parameter be scaled according to number of connections? If there are thousands of connections, will the TCP Send/Receive buffer take too large a size?
    /// TCP sending buffer size
    static member val  TCPSendBufSize = 1 <<< 23 with get, set
    /// TCP receiving buffer size
    static member val  TCPRcvBufSize = 1 <<< 23 with get, set

    /// Maximum size of content each async send & recevie can get
    static member val internal MaxSendRcvdSegSize = 1 <<< 19 with get, set

    /// receive buffer size of each NetworkCommandQueue
    static member val internal CmdQueueRcvdBufSize = 1 <<< 25 with get, set
    /// Waiting for thread to terminate
    
    /// Default values for local cluster
    static member val internal LocalClusterNumJobPortsPerClient = 5
    static member val internal LocalClusterStartingPort = 21000
    /// Default Trace Level to start Local Cluster
    static member val LocalClusterTraceLevel = LogLevel.MildVerbose with get, set
    
    /// The time interval that the container will check the liveness of the host client
    static member val internal ContainerCheckClientLivenessInterval = TimeSpan.FromSeconds(30.0) with get

    /// The time interval that the container will wait for a analytical job to complete
    static member val internal ContainerMaxWaitForJobToFinish = TimeSpan.FromSeconds(5.0) with get

    /// The wait time for unload an appdomain that is hosting a daemon or container.
    static member val internal AppDomainUnloadWaitTime = TimeSpan.FromSeconds(1.0) with get
    /// If a job is idle for this much time, it is considered to have problems, and may be cancelled. 
    static member val MaxWaitToEndJobInMilliseconds = 30000 with get, set 


(*---------------------------------------------------------------------------
    05/31/2014, Jin Li
    The following is a set of system bugs we have found out during Prajna
    We use this class to capture those bugs, to be rewritten at a later time. 
 ---------------------------------------------------------------------------*)
type internal SystemBug() = 
    // UTC 140601_001147.765605 wake up after 213187.620400ms to retry send "partition:5 serial:0 numElems:10" to peer 0
    /// Sleep is used to replace Async.Sleep, which sometime takes a super long time to wake up. 
    /// A sleep of 1ms can turn into !!!213187.620400ms!!! in one of the instance. 
    static member Sleep( milliseconds:int ) = 
        // System.Threading.Thread.Sleep( milliseconds )
        // It seems that Task.Delay has a delay distribution larger than Thread.Sleep. It doesn't have the horrible behavior of Async.Sleep which hibernates though. A Task.Delay(1) leads to a delay from 
        // 1 ms to 70ms. Use this call with caution. 
        System.Threading.Tasks.Task.Delay( milliseconds ).Wait()
