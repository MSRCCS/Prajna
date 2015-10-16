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
        task.fs
  
    Description: 
        Task for a Prajna Client. You may consider it as a job to be executed. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Net.Sockets
open System.Reflection
open System.Threading
open System.Threading.Tasks

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

open Prajna.Tools.StringTools
open Prajna.Tools.FileTools
open Prajna.Tools.BytesTools

open Prajna.Service

type TPLTask = System.Threading.Tasks.Task

type internal TaskState = 
    | NotReady = 0
    | ReadyToExecute = 1
    | InExecution = 2
    | Terminated = 3
    | PendingTermination = 4

type internal ExecutionKind = 
    | NotStarted 
    | Thread of Thread
    | Process of Process
    | Terminated 

type internal TaskConnectedQueueStatus = 
    | NewConnectionPending = 0 
    | ConnectionRegistered = 1
    | ConnectionDeregistered = 2

[<AllowNullLiteral>]
type internal AsyncExecutionEngine( ta:Task, dset:DSet ) = 
    static member val NumParallelExecution = DeploymentSettings.NumParallelJobs( Environment.ProcessorCount ) with get, set
    static member val TaskList = List<_>() with get, set
    member val ExecutedPartitions = ConcurrentDictionary<int,DateTime>() with get
    /// To identify current task
    member val CurTask = ta with get
    /// To identify current DSet
    member val CurDSet = dset with get
    /// A unique token that identify the job related to this instance of the AsyncExecutionEngine
    member val Token = (PerfDateTime.UtcNow()) with get 
    member x.Reset() = 
        x.ExecutedPartitions.Clear()
    /// Add a task to the execution queue
    member x.AddTask( cts, parti:int, job:Async<_> ) = 
        let curTime = (PerfDateTime.UtcNow())
        let inTime = x.ExecutedPartitions.GetOrAdd( parti, curTime )
        if curTime = inTime then 
            lock ( AsyncExecutionEngine.TaskList ) ( fun _ -> AsyncExecutionEngine.TaskList.Add( cts, x, parti, job, ref Unchecked.defaultof<TPLTask>, ref TaskState.ReadyToExecute ) ) 

    /// Filter tasks
    static member FilterTask( func ) = 
        lock ( AsyncExecutionEngine.TaskList ) ( fun _ -> 
            AsyncExecutionEngine.TaskList |> Seq.filter( func ) |> Seq.toArray )
    /// Try to execute the first task in the queue. 
    static member TryExecuteTasks( numExecution ) = 
        if numExecution > 0 then 
            lock ( AsyncExecutionEngine.TaskList ) ( fun _ -> 
                let nExecuted = ref 0
                let idx = ref 0
                let existingJobs = Dictionary<_,_>()
                while !nExecuted < numExecution && !idx<AsyncExecutionEngine.TaskList.Count do
                    let cts, engine, parti, job, task, state = AsyncExecutionEngine.TaskList.[!idx]
                    if !state = TaskState.InExecution then 
                        if engine.CurDSet.NumParallelExecution > 0 then 
                            // No need to account for the existing jobs if there is no limit cap. 
                            if not ( existingJobs.ContainsKey( engine ) ) then 
                                existingJobs.Item( engine ) <- 1
                            else 
                                existingJobs.Item( engine ) <- existingJobs.Item( engine ) + 1
                    if !state = TaskState.ReadyToExecute then 
                        let bUnderCap = 
                            if engine.CurDSet.NumParallelExecution <= 0 then 
                                true
                            else
                                let nExistingJob = if existingJobs.ContainsKey( engine ) then existingJobs.Item( engine ) else 0
                                nExistingJob < engine.CurDSet.NumParallelExecution
                        if bUnderCap then 
                            nExecuted := !nExecuted + 1
                            task := Async.StartAsTask( job, taskCreationOptions=Tasks.TaskCreationOptions.LongRunning, cancellationToken=(cts) ) :> TPLTask
                            state := TaskState.InExecution
                            // Don't increment idx here so that this job will be accounted for in the if portion of !state = TaskState.InExecution
                        else
                            idx := !idx + 1
                    else
                        idx := !idx + 1
                !nExecuted > 0 
                )
        else
            false
    /// Try execute the task in the queue, until we reach the limit
    static member TryExecute() = 
        let activeTasks = ref ( AsyncExecutionEngine.FilterTask( fun (_, _, _, _, _, state) -> !state=TaskState.InExecution ) )
        let mutable bExecuteNew = true
        while bExecuteNew && (!activeTasks).Length < AsyncExecutionEngine.NumParallelExecution do
            bExecuteNew <- AsyncExecutionEngine.TryExecuteTasks( AsyncExecutionEngine.NumParallelExecution - (!activeTasks).Length )
            if bExecuteNew then 
                // Update Active tasks. 
                activeTasks := AsyncExecutionEngine.FilterTask( fun (_, _, _, _, _, state) -> !state=TaskState.InExecution )  
        !activeTasks
    /// Try Execute
    static member ExamineExecutedTask(timeOut:int) = 
        let mutable bAnyTerminate = true
        let mutable nActiveTasks = 0
        while bAnyTerminate do 
            let activeTasks = AsyncExecutionEngine.TryExecute()            
            if activeTasks.Length > 0 then 
                let tasks = activeTasks |> Array.map ( fun (_, _, _, _, ta, _ ) -> !ta ) 
                let index = TPLTask.WaitAny( tasks, timeOut ) 
                if index>=0 then 
                    let _, _, parti, _, _, state = activeTasks.[index] 
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Partition %d finished execution" parti ))
                    state := TaskState.Terminated
                    nActiveTasks <- activeTasks.Length - 1
                else 
                    nActiveTasks <- activeTasks.Length
                    bAnyTerminate <- false
            else
                nActiveTasks <- activeTasks.Length
                bAnyTerminate <- false
    /// Execute all
    member x.WaitForAll( timeOut ) = 
        let t1 = (PerfDateTime.UtcNow())
        let mutable bTerminate = false
        while not bTerminate do 
            let remainingTask = AsyncExecutionEngine.FilterTask( fun (_, eng, _, _, _, state) -> Object.ReferenceEquals( eng, x) && !state<>TaskState.Terminated ) 
            if remainingTask.Length > 0 then 
                AsyncExecutionEngine.ExamineExecutedTask( DeploymentSettings.TimeOutForWaitAny )
                let t2 = (PerfDateTime.UtcNow())
                if timeOut>=0 && int (t2.Subtract(t1).TotalMilliseconds)>=timeOut then 
                    // Timeout
                    bTerminate <- true
            else
                // All job executed. 
                bTerminate <- true

and [<AllowNullLiteral>]
    internal ExecutedTaskHolder() = 
        // The following variable are passed from Job
        member val SignatureName = "" with get, set
        member val SignatureVersion = 0L with get, set
        member val TypeOf = JobTaskKind.ApplicationMask with get, set
        member val JobDirectory = "" with get, set
        member val JobEnvVars : List<_> = null with get, set
        member val State = TaskState.ReadyToExecute with get, set
        member val Thread : Thread = null with get, set
        member val Process : System.Diagnostics.Process = null with get, set
        member val StartAttempted = ref 0 with get

        member val MonStdOutput = null with get, set
        member val MonStdError = null with get, set
        member val JobList = ConcurrentDictionary<_,Task>() with get, set     
        member val JobLoopbackQueue:NetworkCommandQueue = null with get, set
        member val ExecutionType = ExecutionKind.NotStarted with get, set
        member val ConnectedQueue = ConcurrentDictionary<_,_>() with get, set
        member val LaunchedServices = ConcurrentDictionary<string,bool>(StringComparer.Ordinal) with get, set
        member val EvLoopbackEstablished = new ManualResetEvent( false ) with get, set
        member val EvTaskLaunched = new ManualResetEvent( false ) with get, set
        member val TaskLaunchSuccess = false with get, set

        member val CurNodeInfo : NodeWithInJobInfo = null with get, set

        /// <summary>
        /// A new client queue is connected to the task. 
        /// </summary>
        member x.NewConnectedQueue( queue: NetworkCommandQueue ) = 
            let tuple = ref (int TaskConnectedQueueStatus.NewConnectionPending)
            let queueStatusRef = x.ConnectedQueue.GetOrAdd( queue.RemoteEndPointSignature, tuple )
            let oldStatus = !queueStatusRef
            if oldStatus >= (int TaskConnectedQueueStatus.ConnectionDeregistered) then 
                // If the previous queue has been disconnected, we will re-register. 
                Interlocked.CompareExchange( queueStatusRef, (int TaskConnectedQueueStatus.NewConnectionPending), oldStatus ) |> ignore  
            if !queueStatusRef = int TaskConnectedQueueStatus.NewConnectionPending then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "connected client from %s, to register with task" 
                                                               (LocalDNS.GetShowInfo( queue.RemoteEndPoint ) ) ))
        /// <summary>
        /// Register the client queue with task (after AppDomain/Exe is launched. 
        /// </summary>
        member x.RegisterConnectionsWithTasks() = 
            for pair in x.ConnectedQueue do 
                let queueSignature = pair.Key
                let queueStatusRef = pair.Value
                let priorQueueStatus = !queueStatusRef
                if priorQueueStatus = (int TaskConnectedQueueStatus.NewConnectionPending) &&
                    not (Utils.IsNull x.JobLoopbackQueue) && x.JobLoopbackQueue.CanSend then 
                    // Attempt to register. 
                    if Interlocked.CompareExchange( queueStatusRef, int TaskConnectedQueueStatus.ConnectionRegistered, priorQueueStatus ) = priorQueueStatus then 
                        use msRegister = new MemStream( 1024 )
                        msRegister.WriteInt64( queueSignature )
                        x.JobLoopbackQueue.ToSend( ControllerCommand( ControllerVerb.Open, ControllerNoun.Connection ), msRegister )
        /// <summary>
        /// Force terminating all tasks of an executing job. 
        /// Currently, this is executed when we detect a new version (of different signature) of task. 
        /// </summary>
        member x.ForceTerminate() =
            if not (Utils.IsNull x.JobLoopbackQueue) && x.JobLoopbackQueue.CanSend then 
                x.JobLoopbackQueue.ToSend( ControllerCommand( ControllerVerb.Delete, ControllerNoun.Program ), null )
        /// <summary>
        /// Deregister client queue. 
        /// Return: 
        ///     true: if the removal causes the last queue to be disconnected from the client. 
        ///     false: there is other queue connected, or the deregister doesnot trigger removal operation. 
        /// </summary>
        member internal x.CloseConnectedQueue( queue: NetworkCommandQueue ) = 
            let queueSignature = queue.RemoteEndPointSignature
            let refValue = ref Unchecked.defaultof<_>
            let bRemoved = x.ConnectedQueue.TryRemove( queueSignature, refValue )
            if bRemoved then
                let queueStatusRef = !refValue
                if Interlocked.CompareExchange( queueStatusRef, int TaskConnectedQueueStatus.ConnectionDeregistered, 
                                                    int TaskConnectedQueueStatus.ConnectionRegistered ) = int TaskConnectedQueueStatus.ConnectionRegistered then 
                    if not (Utils.IsNull x.JobLoopbackQueue) && x.JobLoopbackQueue.CanSend then 
                        use msRegister = new MemStream( 1024 )
                        msRegister.WriteInt64( queueSignature )
                        x.JobLoopbackQueue.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.Connection ), msRegister )
                        x.ConnectedQueue.IsEmpty 
                    else
                        false
                else
                    false        
            else
                false   
        member val internal AppDomainInfo = null with get, set
        member x.StartChildProcess (startInfo:ProcessStartInfo) =
            if DeploymentSettings.RunningOnMono then
                // On Mono, start the child as a process
                startInfo.Arguments <- " " + startInfo.FileName + " " + startInfo.Arguments
                startInfo.FileName <- "mono"
                Process.Start( startInfo )
            else
                // On windows, start the child process with a job object
                InterProcess.StartChild(startInfo)
        member x.StartProgram( ta: Task ) =
            if Interlocked.CompareExchange( x.StartAttempted, 0, 1 ) = 0 then   
                // Have the lock to start          
                let executeTypeOfRaw = x.TypeOf &&& JobTaskKind.ExecActionMask
                let executeTypeOf = 
                    if DeploymentSettings.AllowAppDomain then 
                        executeTypeOfRaw
                    else
                        // Always execute in EXE mode if need
                        executeTypeOfRaw &&& (~~~(JobTaskKind.AppDomainMask)) ||| JobTaskKind.ApplicationMask
                let executeTicks = (PerfADateTime.UtcNowTicks())
                let setExecutionType() =
                    match executeTypeOf with 
                    | JobTaskKind.ApplicationMask 
                    | JobTaskKind.ReleaseApplicationMask->
                        let nodeInfo = JobListeningPortManagement.Current.Use( x.SignatureName ) 
                        if Utils.IsNotNull nodeInfo then 
                            let proc = Process.GetCurrentProcess()
                            let clientInfo = sprintf "-clientId %i -clientModuleName %s -clientStartTimeTicks %i" proc.Id proc.MainModule.ModuleName proc.StartTime.Ticks
                            let mutable cmd_line = sprintf "-job %s -ver %d -ticks %d -loopback %d -jobport %d -mem %d -logdir %s -verbose %d %s" x.SignatureName x.SignatureVersion executeTicks DeploymentSettings.ClientPort nodeInfo.ListeningPort DeploymentSettings.MaxMemoryLimitInMB DeploymentSettings.LogFolder (int Prajna.Tools.Logger.DefaultLogLevel) clientInfo
                            if DeploymentSettings.StatusUseAllDrivesForData then 
                                cmd_line <- "-usealldrives " + cmd_line
                            if not (DeploymentSettings.ClientIP.Equals("")) then
                                cmd_line <- sprintf "%s -loopbackip %s" cmd_line DeploymentSettings.ClientIP
                                cmd_line <- sprintf "%s -jobip %s" cmd_line DeploymentSettings.ClientIP
                            let (requireAuth, guid, rsaParam, rsaPwd) = Cluster.Connects.GetAuthParam()
                            if (requireAuth) then
                                cmd_line <- sprintf "%s -auth %b -myguid %s -rsakeyauth %s -rsakeyexch %s -rsapwd %s" cmd_line requireAuth (guid.ToString("N")) (Convert.ToBase64String(fst rsaParam)) (Convert.ToBase64String(snd rsaParam)) rsaPwd
                            let curPath = Process.GetCurrentProcess().MainModule.FileName
                            let parentDir = Directory.GetParent( Path.GetDirectoryName( curPath ) )
                            let masterExecutable, masterConfig = 
                                match executeTypeOf with 
                                | JobTaskKind.ApplicationMask ->
                                    let curDir = Path.GetDirectoryName(curPath)
                                    let exe = Path.Combine(curDir, DeploymentSettings.ClientExtensionExcutable)
                                    if File.Exists exe then 
                                        exe, Path.Combine(curDir, DeploymentSettings.ClientExtensionConfig)
                                    else
                                        // If not located with the main module, let's search the current dir instead 
                                        let workDir = Environment.CurrentDirectory
                                        Path.Combine(workDir, DeploymentSettings.ClientExtensionExcutable), Path.Combine(workDir, DeploymentSettings.ClientExtensionConfig)
                                | _ -> 
                                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start job %s:%s in release mode ........... " x.SignatureName (x.SignatureVersion.ToString("X")) ) )
                                    Path.Combine( parentDir.FullName, "Releasex64", DeploymentSettings.ClientExtensionExcutable ), Path.Combine( parentDir.FullName, "Releasex64", DeploymentSettings.ClientExtensionConfig )
                            let newExecutable = 
                                if StringTools.IsNullOrEmpty x.JobDirectory then
                                    masterExecutable
                                else
                                    ta.LinkAllAssemblies( x.JobDirectory ) 
                                    let useExeutableName = Path.GetFileNameWithoutExtension( masterExecutable ) + "_" + ta.LaunchIDName + Path.GetExtension( masterExecutable )
                                    let useExeutable = Path.Combine( x.JobDirectory, useExeutableName )                                    
                                    let _, msg = CopyFile useExeutable masterExecutable
                                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Copy %s to %s : %s" useExeutable masterExecutable msg) )
                                    if File.Exists masterConfig then
                                        // Also make sure the config file is co-located with the executable
                                        let useConfig = useExeutable + ".config"
                                        let res, msg = CopyFile useConfig masterConfig
                                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Copy %s to %s : %s" useConfig masterConfig msg) )
                                    useExeutable
                            // Update asm bindings if needed
                            ta.JobAsmBinding |> ConfigurationUtils.ReplaceAssemblyBindingsForExeIfNeeded newExecutable
                            let startInfo = System.Diagnostics.ProcessStartInfo( newExecutable, cmd_line )
                            if not (Utils.IsNull x.JobDirectory) && x.JobDirectory.Length > 0 then
                                startInfo.WorkingDirectory <- x.JobDirectory
                                startInfo.CreateNoWindow <- false
                                startInfo.UseShellExecute <- false
                                startInfo.RedirectStandardError <- true
                                startInfo.RedirectStandardOutput <- true
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Current working directory <-- %s, execute %s ...  " x.JobDirectory newExecutable ))
                            else
                                startInfo.CreateNoWindow <- false
                                startInfo.UseShellExecute <- false
                                startInfo.RedirectStandardError <- true
                                startInfo.RedirectStandardOutput <- true
                            for vari=0 to x.JobEnvVars.Count-1 do
                                let var, value = x.JobEnvVars.[vari]
                                startInfo.EnvironmentVariables.[var] <- value
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Environment variable %s <-- %s " var value ))
                            if (x.JobEnvVars.Count <> 0) then
                                startInfo.UseShellExecute <- false
                            let proc = x.StartChildProcess(startInfo)
                            if (Utils.IsNull proc) then
                                Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Unable to start process"))
                            x.MonStdError <- new StreamMonitor( ) 
                            let errLog = Path.Combine( DeploymentSettings.LogFolder, x.SignatureName + "_stderr_" + VersionToString( DateTime(executeTicks)) + ".log" )
                            x.MonStdError.AddMonitorFile( errLog  )
                            proc.ErrorDataReceived.Add( x.MonStdError.DataReceived )
                            x.MonStdOutput <- new StreamMonitor(  ) 
                            let stdLog = Path.Combine( DeploymentSettings.LogFolder, x.SignatureName + "_stdout_" + VersionToString( DateTime(executeTicks)) + ".log" )
                            x.MonStdOutput.AddMonitorFile( stdLog )
                            proc.OutputDataReceived.Add( x.MonStdOutput.DataReceived ) 
                            proc.BeginOutputReadLine()
                            proc.BeginErrorReadLine()
                            x.Process <- proc
                            x.State <- TaskState.InExecution
                            x.TaskLaunchSuccess <- true
                            Process proc
                        else
                            Logger.Log( LogLevel.Info, ( sprintf "Exe: failed to retrieve the reserved port for job %s:%s" x.SignatureName (x.SignatureVersion.ToString("X")) ) )
                            x.TaskLaunchSuccess <- false
                            NotStarted  
                    | JobTaskKind.AppDomainMask ->                
                        if (x.JobEnvVars.Count <> 0) then
                            failwith "Environment variables not allowed in appdomain - only allowed if ApplicationMask is set"
                        let nodeInfo = JobListeningPortManagement.Current.Use( x.SignatureName ) 
                        if Utils.IsNotNull nodeInfo then 
                            if DeploymentSettings.RunningOnMono then
                                // Mono Notes: when launch the container in appdomain, we observed different behavior between Mono (on Linux) and Windows.
                                // When the container is launched as appdomain, the Prajna.dll and its dependencies are already loaded. 
                                // However, we observe BinaryFormatter.Deseralize sometimes attempt to load DLL that has already been loaded. The behavior is 
                                // like LoadFile, which cause the exact same DLL being loaded again and used for deseralize.
                                // 1. On windows, if we link assemblies to job directory, it causes the Prajna.dll being loaded from there directly As a 
                                //    result, the object returned by BinaryFormatter.Deseralize refers to the newly loaded Prajna.dll, when the code tries it to 
                                //    cast to a type that from the default loaded Prajna.dll, thus the casting fails. If we don't link, the normaly DLL search won't
                                //    find it, our handler registered to AssemblyResolve event is invoked. It uses LoadFrom, which ignores duplicates.
                                // 2. For Mono on Linux. It does not seem to suffer the above casting issue for duplicated DLLs. But when it tries to look for 
                                //    FSharp.Core 4.3.0.0, it somehow decides to load 4.3.1.0 from GAC. Thus we indeed get two different DLLs, which causes deserliazation fails for
                                //    casting a functor that extends FSharpFunc (in one DLL) to FSharFunc (in the other DLL) fail. So in Linux, we need to link assemblies, so Mono
                                //    does not look for FSharp.Core from GAC but from job directory.
                                ta.LinkAllAssemblies( x.JobDirectory ) 
                            let param = 
                                ContainerAppDomainInfo( Name = x.SignatureName, Version = x.SignatureVersion, Ticks = executeTicks, JobPort = nodeInfo.ListeningPort,
                                    JobDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), 
                                    JobEnvVars = x.JobEnvVars  )
                            x.AppDomainInfo <- param
                            x.Thread <- ThreadTracking.StartThreadForFunction ( fun _ -> sprintf "Launch remote container as AppDomain, port %d" nodeInfo.ListeningPort) ( fun _ -> ContainerAppDomainInfo.StartProgram param )
                            x.State <- TaskState.InExecution
                            x.TaskLaunchSuccess <- true
                            Thread x.Thread               
                        else
                            Logger.Log( LogLevel.Info, ( sprintf "AppDomain: failed to retrieve the reserved port for job %s:%s" x.SignatureName (x.SignatureVersion.ToString("X")) ) )
                            x.TaskLaunchSuccess <- false
                            NotStarted               
                    | _ ->
                        let errMsg = sprintf "Logic error, light task should not be started using a task holder object"
                        Logger.Log( LogLevel.Error, errMsg )
                        x.TaskLaunchSuccess <- false
                        failwith errMsg
                x.ExecutionType <- setExecutionType()
                x.EvTaskLaunched.Set() |> ignore
            else
                x.EvTaskLaunched.WaitOne() |> ignore

        interface IDisposable with
             member x.Dispose() = 
                x.EvLoopbackEstablished.Dispose()
                x.EvTaskLaunched.Dispose()
                if Utils.IsNotNull x.JobLoopbackQueue then
                    (x.JobLoopbackQueue :> IDisposable).Dispose()
                GC.SuppressFinalize(x)

/// Prajna Task 
/// A Prajna task is an action to be executed at Prajna Client, 
/// it usually involes one or more DSet peer parameters, and one or more functions to be executed upon. 
/// class Task is usually hosted at the Prajna Client. 
and [<AllowNullLiteral; Serializable>]
    internal Task() = 
    inherit Job() 
    let mutable bAllLoaded = false
    let mutable bTerminationCalled = false
    member val DSets : DSetPeer[] = null with get, set
    member val IncomingQueues = List<_>() with get, set
    member val IncomingQueuesToPeerNumber = ConcurrentDictionary<_,_>() with get, set
    member val IncomingQueuesClusterMembership = List<_>() with get, set
    member val IncomingQueuesAvailability = List<_>() with get, set
    member val PrimaryHostQueueIndex = -1 with get, set
    /// QueueAtClient resides at AppDomain/Exe, it is the local loopback interface to talk to the PrajnaClient
    member val QueueToClient : NetworkCommandQueue = null with get, set
    member val TaskHolder: ExecutedTaskHolder = null with get, set
    member val ClientQueueSignature = 0L with get, set
    /// DSet and DStream that will be referenced to form the execution graph
    member val DSetReferences = ConcurrentDictionary<_,DSet>(BytesCompare()) with get
    member val DStreamReferences = ConcurrentDictionary<_,DStream>(BytesCompare()) with get
    member x.ResolveDSetByName( name, ticks ) = 
        let dsetRef = ref null 
        for pair in x.DSetReferences do 
            let dset = pair.Value
            if String.Compare( name, dset.Name, StringComparison.Ordinal)=0 && ticks = dset.Version.Ticks then 
                dsetRef := dset
        !dsetRef

    /// QueueAtClient resides at PrajnaClient, it is the local loopback interface to talk to the job at the AppDomain/Exe
    member x.QueueAtClient with get() = 
                                        if Utils.IsNull x.TaskHolder then 
                                            let msg = sprintf "!!! Exception !!! Attempt to contact a job loopback before establish the task holder object"
                                            Logger.Log( LogLevel.Error, msg )
                                            failwith msg
                                            null
                                        else
                                            if Utils.IsNull x.TaskHolder.JobLoopbackQueue then 
                                                x.TaskHolder.EvLoopbackEstablished.WaitOne() |> ignore
                                            if Utils.IsNull x.TaskHolder.JobLoopbackQueue then
                                                let msg = sprintf "!!! Exception !!! Attempt to contact a job loopback but the job loopback fails to establish"
                                                Logger.Log( LogLevel.Error, msg )
                                                failwith msg
                                            else
                                                x.TaskHolder.JobLoopbackQueue
    /// Monitor QueueAtClient resides at PrajnaClient, give null if need to wait
    member x.QueueAtClientMonitor with get() = 
                                                if Utils.IsNull x.TaskHolder then 
                                                    null
                                                else
                                                    x.TaskHolder.JobLoopbackQueue    
    member val Partitions = List<int>() with get, set
    member val State = TaskState.NotReady with get, set
    member val Port = 0L with get, set
    member val Thread : Thread = null with get, set
    member val ConfirmStart = false with get, set

    member x.ClusterMembership( queue ) = 
        let membershipList = List<_>()
        if Utils.IsNotNull x.Clusters then 
            for cluster in x.Clusters do 
                if Utils.IsNotNull cluster then 
                    // If the cluster can not be decoded, it can take value null. 
                    let peerIdx = cluster.SearchForEndPoint( queue )
                    if peerIdx>=0 then 
                        membershipList.Add( (cluster, peerIdx) )
        membershipList
    /// Add incoming queue for the job. 
    member x.GetIncomingQueueNumber( queue:NetworkCommandQueuePeer ) = 
        let refValue = ref Unchecked.defaultof<_>
        if not (x.IncomingQueuesToPeerNumber.TryGetValue( queue.RemoteEndPointSignature, refValue )) then 
            let peeri = x.IncomingQueues.Count
            x.IncomingQueues.Add( queue )
            x.IncomingQueuesToPeerNumber.Item( queue.RemoteEndPointSignature ) <- peeri
            let membershipList = x.ClusterMembership( queue )
            x.IncomingQueuesClusterMembership.Add( membershipList )
            x.IncomingQueuesAvailability.Add( BlobAvailability( x.NumBlobs ) ) 
            if membershipList.Count<=0 && x.PrimaryHostQueueIndex<0 then 
                x.PrimaryHostQueueIndex <- peeri
            peeri
        else
            !refValue
    /// Add a new cluster to the job
    /// The function will recheck the membership relation of each peer (represented by queue), and redefine PrimaryHostQueueIndex
    /// that defines the PrimaryHostQueue to communicate with host. 
    member x.AddCluster( cluster, blob:Blob ) = 
        blob.Object <- cluster
        x.Clusters.[ blob.Index ] <- cluster
        // Peer membership examination. 
        for peeri=0 to x.IncomingQueues.Count-1 do
            let queue = x.IncomingQueues.[peeri]
            let membershipList = x.ClusterMembership( queue )
            x.IncomingQueuesClusterMembership.[peeri] <- membershipList
        // Check for PimaryHostQueueIndex
        if x.PrimaryHostQueueIndex>=0 && x.IncomingQueuesClusterMembership.[x.PrimaryHostQueueIndex].Count>0 then 
            // The previous host is no longer a primary host after adding the new cluster
            x.PrimaryHostQueueIndex <- -1    
//            for peeri=0 to x.IncomingQueues.Count-1 do
//                let membershipList = x.IncomingQueuesClusterMembership.[peeri]
//                if membershipList.Count<=0 && x.PrimaryHostQueueIndex<0 then 
//                    x.PrimaryHostQueueIndex <- peeri    
            x.UpdatePrimaryHostQueue()
    /// Return one host queue 
    member x.PrimaryHostQueue with get() = if x.PrimaryHostQueueIndex>=0 then x.IncomingQueues.[x.PrimaryHostQueueIndex] else null
    /// Check for validity of the Primary Host Queue, and make update. 
    member x.UpdatePrimaryHostQueue() = 
        if x.PrimaryHostQueueIndex >= 0 then 
            let activePrimaryHostQueue = x.PrimaryHostQueue
            if Utils.IsNotNull activePrimaryHostQueue && activePrimaryHostQueue.Shutdown then 
                x.PrimaryHostQueueIndex <- -1     
        if x.PrimaryHostQueueIndex < 0 then 
            // Find a new Primary Host queue 
            for peeri=0 to (Math.Min(x.IncomingQueues.Count,x.IncomingQueuesClusterMembership.Count))-1 do
                let queue = x.IncomingQueues.[peeri]
                if Utils.IsNotNull queue && not queue.Shutdown then 
                    let membershipList = x.IncomingQueuesClusterMembership.[peeri]
                    if membershipList.Count<=0 && x.PrimaryHostQueueIndex<0 then 
                        x.PrimaryHostQueueIndex <- peeri    
        ()
    /// Check blob availability 
//    member x.CheckBlobAvailability() = 
//        for blobi=0 to x.NumBlobs - 1 do
//            match blob.TypeOf with 
//            | BlobKind.ClusterMetaData ->
//                let useCluster = ClusterFactory.FindCluster( blob.Name, blob.Version )
//                if Utils.IsNotNull useCluster then 
//                    x.    
//
//            | SrcDSetMetaData = 2
//            | DstDSetMetaData = 3
//            | PassthroughDSetMetaData = 4
//            | AssemblyManagedDLL = 16
//            | AssemblyUnmanaged = 17
//        ()
    member x.StartLightTask() =
        if x.AllAvailable then 
            let executeTypeOf = x.TypeOf &&& JobTaskKind.ExecActionMask
            let executeTicks = (PerfDateTime.UtcNowTicks())
            match executeTypeOf with 
            | JobTaskKind.ApplicationMask 
            | JobTaskKind.ReleaseApplicationMask
            | JobTaskKind.AppDomainMask ->                
                let msg = sprintf "Job %s:%s with execution type %A should not be started as a light task" x.Name x.VersionString x.TypeOf  
                Logger.Log( LogLevel.Error, msg  )
                failwith msg
            | _ ->
                // Thread, without app domain, no loop back. 
                if (x.JobEnvVars.Count <> 0) then
                    failwith "Environment variables not allowed in thread/appdomain - only allowed if ApplicationMask is set"
                x.State <- TaskState.InExecution
//                let threadStart = Threading.ParameterizedThreadStart( Task.Start )
//                let thread = Threading.Thread( threadStart )
//                thread.IsBackground <- true
//                x.Thread <- thread
//                thread.Start( x )
                x.Thread <- ThreadTracking.StartThreadForFunction( fun _ -> "Light Task thread @ daemon") ( fun _ -> Task.Start x)
                true
        else
            Logger.Log( LogLevel.Info, ( sprintf "Some metadata is not available, and execution can't start: %A" x.AvailThis.AvailVector )   )
            false
    /// State of the job
    member x.EvaluateThreadState() = 
            if x.State=TaskState.InExecution || x.State=TaskState.PendingTermination then 
                if Utils.IsNotNull x.Thread then 
                    if x.Thread.ThreadState &&& 
                        ( ThreadState.Stopped )
                        // ( ThreadState.AbortRequested ||| ThreadState.Aborted ||| ThreadState.Stopped ||| ThreadState.StopRequested ) 
                            <> enum<Threading.ThreadState>(0) then 
                        // Release port only after thread has stopped
// Release port by delink
//                      JobListeningPortManagement.Current.Release( x.SignatureName, x.SignatureVersion ) 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "AppDomain thread of task %s has exited ! ..." 
                                                                                   x.SignatureName ))
                        x.State <- TaskState.Terminated
                        true
                    else
                        false
                else
                    false
            else
                false
    /// Error
    member x.ErrorAsSeparateApp(msg) = 
        Logger.Log( LogLevel.Error, msg )
        if Utils.IsNotNull x.QueueToClient then 
            use msSend = new MemStream( 1024 )
            msSend.WriteString( msg )
            let queue = x.QueueToClient
            queue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msSend ) 
            queue.Close()
    /// Resolve a single dependent DSet Object
    member x.ResolveDependentDSet( dep:DependentDObject ) = 
        if Utils.IsNull dep.Target then
            if Utils.IsNull dep.Hash then 
                dep.Target <- null 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Caution, a <null> reference for resolved DSet ... " ))
                null
            else
                let bExist, dset = x.DSetReferences.TryGetValue( dep.Hash ) // DSetPeerFactory.ResolveAnyDSet( dep.ParentName, dep.Version.Ticks )
                if bExist then 
                    dep.Target <- dset
                    dep.Target :?> DSet
                else
                    dep.Target <- null
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Caution, Fail to resolve DSet %A, hash %s" dep.ParamType (BytesToHex(dep.Hash)) ))
                    null
        else
            dep.Target :?> DSet
    /// Resolve a single dependent DSet Object
    member x.ResolveDependentDStream( dep:DependentDObject ) = 
        if Utils.IsNull dep.Target then
            if not (Utils.IsNull dep.Hash) then 
                let bExist, dstream = x.DStreamReferences.TryGetValue( dep.Hash ) // DSetPeerFactory.ResolveAnyDSet( dep.ParentName, dep.Version.Ticks )
                if bExist then 
                    dep.Target <- dstream
                    dep.Target :?> DStream
                else
                    // May be OK (dangling edges). 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Caution, fail to resolve DStream %A, hash %s ... " dep.ParamType (BytesToHex(dep.Hash)) ))
                    null
            else
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Caution, a <null> DStream reference ... "))
                null
        else
            dep.Target :?> DStream
    /// Action for registering callback. 
    member x.RegisterOne (bRegister:bool) (cur:DistributedObject ) =
        match cur with 
        | :? DSet as dset -> 
            // Callback is not registered for dset
            ()
        | :? DStream as stream -> 
            let mutable bRegisterStream = true
            // Traverse upstream
            // Register every stream
            if bRegisterStream then               
                if ( bRegister ) then 
                    stream.Cluster.RegisterCallback( x.JobID, stream.Name, stream.Version.Ticks, 
                        [| ControllerCommand(ControllerVerb.Unknown, ControllerNoun.DStream) |],
                        { new NetworkCommandCallback with 
                            member this.Callback( cmd, peeri, ms, jobID, name, verNumber, cl ) = 
                                stream.ProcessCallback( cmd, peeri, ms, jobID )
                        } )
                else
                    stream.Cluster.UnRegisterCallback( x.JobID, 
                        [| ControllerCommand(ControllerVerb.Unknown, ControllerNoun.DStream) |] )
        | _ -> 
            let msg = sprintf "RegisterOne failed, don't know how to do with type %A object %A" (cur.GetType()) cur
            Logger.Log( LogLevel.Error, msg  )
            failwith msg 
    /// Action for traverse, resolve object for both up and down dependency
    member x.ResolveOne direction (cur:DistributedObject ) = 
        match cur with 
        | :? DSet as dset -> 
            match dset.Dependency with 
            | StandAlone 
            | Source -> 
                // Nothing to resolve
                ()
            | MixFrom parent 
            | Passthrough parent -> 
                x.ResolveDependentDSet( parent) |> ignore
                if Utils.IsNotNull parent.TargetDSet then 
                    dset.SetUpstreamCanCloseEvent( parent.TargetDSet.CanCloseDownstreamEvent )
            | WildMixFrom ( parent, streamParent) -> 
                x.ResolveDependentDSet( parent) |> ignore
                x.ResolveDependentDStream( streamParent) |> ignore
                // For WildMixFrom, flush of partitions will wait until all peers have sent in their collections. 
                if Utils.IsNotNull streamParent.TargetStream then 
                    streamParent.TargetStream.SetToWaitForAllPeerCloseRcvd()
                    if Utils.IsNotNull parent.TargetDSet then 
                        dset.SetUpstreamCanCloseEvents( [| parent.TargetDSet.CanCloseDownstreamEvent; streamParent.TargetStream.CanCloseDownstreamEvent |] )
            | CorrelatedMixFrom parents 
            | UnionFrom parents ->
                let events = List<_>(parents.Count )
                for parent in parents do 
                    x.ResolveDependentDSet( parent) |> ignore
                    if Utils.IsNotNull parent.TargetDSet then 
                        events.Add( parent.TargetDSet.CanCloseDownstreamEvent )
                if events.Count > 0 then 
                    dset.SetUpstreamCanCloseEvents( events )
            | Bypass ( parent, brothers ) -> 
                // Resolve 
                x.ResolveDependentDSet( parent) |> ignore                 
                for bro in brothers do 
                    x.ResolveDependentDSet( bro ) |> ignore
                if Utils.IsNotNull parent.TargetDSet then 
                    dset.SetUpstreamCanCloseEvent( parent.TargetDSet.CanCloseDownstreamEvent )
            | DecodeFrom streamParent -> 
                let parentS = x.ResolveDependentDStream( streamParent)
                if Utils.IsNotNull parentS then 
                    dset.SetUpstreamCanCloseEvent( parentS.CanCloseDownstreamEvent )
                ()
            | HashJoinFrom  (pa0, pa1)
            | CrossJoinFrom (pa0, pa1) -> 
                x.ResolveDependentDSet( pa0) |> ignore
                x.ResolveDependentDSet( pa1) |> ignore
                if Utils.IsNotNull pa0.TargetDSet
                    && Utils.IsNotNull pa1.TargetDSet then 
                    dset.SetUpstreamCanCloseEvents( [| pa0.TargetDSet.CanCloseDownstreamEvent; pa1.TargetDSet.CanCloseDownstreamEvent |] )
            match dset.DependencyDownstream with 
            | Discard -> 
                // Nothing to further resolve
                ()
            | CorrelatedMixTo child
            | UnionTo child
            | MixTo child
            | Passforward child 
            | HashJoinTo child 
            | CrossJoinTo child -> 
                let childDSet = x.ResolveDependentDSet( child)
                ()
            | WildMixTo ( child, stream ) -> 
                let childDSet = x.ResolveDependentDSet( child)
                let childStream = x.ResolveDependentDStream( stream) 
                ()
            | DistributeForward children -> 
                for child in children do
                    x.ResolveDependentDSet( child) |> ignore
                ()
            | SaveTo stream
            | EncodeTo stream -> 
                let childStream = x.ResolveDependentDStream( stream) 
                ()
        | :? DStream as stream -> 
            // Traverse upstream
            match stream.Dependency with 
            | SaveFrom parentObj
            | EncodeFrom parentObj -> 
                let parentDSet =  x.ResolveDependentDSet( parentObj )
                if Utils.IsNotNull parentDSet then 
                    stream.SetUpstreamCanCloseEvent( parentDSet.CanCloseDownstreamEvent ) 
                ()
            | PassFrom parentStream ->
                let parentS = x.ResolveDependentDStream( parentStream )
                if Utils.IsNotNull parentS then 
                    stream.SetUpstreamCanCloseEvent( parentS.CanCloseDownstreamEvent ) 
                ()
            | MulticastFromNetwork parentStream
            | ReceiveFromNetwork parentStream -> 
                let parentS = x.ResolveDependentDStream( parentStream )
                // Receive from network don't depend upon upstream to close
                ()
            | SourceStream ->
                ()
            match stream.DependencyDownstream with 
            | PassTo childStream -> 
                let childS = x.ResolveDependentDStream( childStream )
//                match direction with 
//                | TraverseDownstream -> 
//                    childS.SetUpstreamCanCloseEvent( stream.CanCloseDownstreamEvent )
//                | _ ->
//                    ()
                ()
            | SendToNetwork childStream 
            | MulticastToNetwork childStream ->
                let childS = x.ResolveDependentDStream( childStream )
                // SendToNetwork do not depend on CanCloseDownstream for upstream events. 
                ()
            | DecodeTo childObj -> 
                let childDSet =  x.ResolveDependentDSet( childObj )
//                match direction with 
//                | TraverseDownstream -> 
//                    childDSet.SetUpstreamCanCloseEvent( stream.CanCloseDownstreamEvent )
//                | _ ->
//                    ()

                ()
            | SinkStream -> 
                ()
            /// Get all dependent DSets
        | _ -> 
            let msg = if Utils.IsNotNull cur then
                          sprintf "ResolveOne failed, don't know how to resolve type %A object %A" (cur.GetType()) cur
                      else
                          "ResolveOne failed, the object is null"
            Logger.Log( LogLevel.Error, msg  )
            failwith msg 
                
    member x.ResolveDSetParent( dset: DSet ) = 
//        x.ResolveObject( dset, TraverseUpstream, true )
        x.TraverseAllObjectsWDirection TraverseUpstream (List<_>(DeploymentSettings.NumObjectsPerJob)) dset x.ResolveOne
/// Register & Unregister Callback
    member x.RegisterInJobCallback( dset: DSet, bRegister:bool ) = 
        x.TraverseAllObjects TraverseUpstream (List<_>(DeploymentSettings.NumObjectsPerJob)) dset (x.RegisterOne bRegister)
        ()
    member x.SyncPreCloseAllStreams jbInfo (cur:DistributedObject ) = 
        cur.SyncPreCloseAllStreams(jbInfo)
    member val private WaitForCloseAllStreamsTasks = null with get, set
    member val private WaitForCloseAllStreamsHandleCollection = null with get, set
    member x.PostCloseAllStreams jbInfo (cur:DistributedObject ) = 
        cur.PostCloseAllStreams(jbInfo) 
        cur.ResetAll( jbInfo ) 
    member x.ResetAll jbInfo (cur:DistributedObject )= 
        cur.ResetAll( jbInfo ) 
    member x.PreBeginAsync jbInfo direction (cur:DistributedObject )= 
        cur.PreBegin( jbInfo, direction ) 
        match direction, cur with 
        | TraverseUpstream, ( :? DSet as dset ) -> 
            match dset.DependencyDownstream with 
            | WildMixTo ( childDSet, _ ) -> 
//                x.DSetInJobReadAll( jbInfo, dset )
                ()
            | _ -> 
                ()
        | _ ->
            // No need for processing at PreBegin for most of the tasks. 
            ()
    member x.PreBeginSync jbInfo direction (cur:DistributedObject )= 
        cur.PreBegin( jbInfo, direction ) 
        match direction, cur with 
        | TraverseDownstream, ( :? DSet as dset ) -> 
            match dset.Dependency with 
            | WildMixFrom ( childDSet, _ ) -> 
                // Start thread 
                let threadPoolName = sprintf "Threadpool for DSet %s:%s (WildMixFrom)" dset.Name dset.VersionString
                dset.ThreadPool <- new ThreadPoolWithWaitHandles<int>( threadPoolName, NumParallelExecution = dset.NumParallelExecution  )
                dset.InitializeCache( true ) 
                Logger.LogF( LogLevel.MediumVerbose, (fun _ -> sprintf "Eqneueing %d partitions for exectution" dset.NumPartitions))
                using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
                    if Utils.IsNull jobAction then 
                        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "PreBeginSync, Job %A has already been cancelled ... " jbInfo.JobID )
                    else
                        for parti=0 to dset.NumPartitions - 1 do 
                            dset.InitializeCachePartition( parti )
                            //dset.ThreadPool.EnqueueRepeatableFunction ( dset.NewThreadToExecuteDownstream jbInfo parti ) (jobAction.CTS) parti ( fun pi -> sprintf "ExecuteDownStream Job WildMixFrom DSet %s:%s partition %d" dset.Name dset.VersionString pi )
                            Component<_>.AddWorkItem ( dset.NewThreadToExecuteDownstream jbInfo parti ) dset.ThreadPool (jobAction.CTS) parti ( fun pi -> sprintf "ExecuteDownStream Job WildMixFrom DSet %s:%s partition %d" dset.Name dset.VersionString pi ) |> ignore
                        Component<_>.ExecTP dset.ThreadPool
                        dset.ThreadPool.TryExecute()
                )
            | _ -> 
                ()
        | _ ->
            // No need for processing at PreBegin for most of the tasks. 
            ()
    member val JobInfoCollections = ConcurrentDictionary<string,JobInformation>() with get
    member val AsyncExecutionEngine = ConcurrentDictionary<string,AsyncExecutionEngine>() with get
    member val SyncExecutionEngine  = ConcurrentDictionary<string,ThreadPoolWithWaitHandles<int>>() with get
        
    member x.BeginAllSync jbInfo ( dset: DSet ) = 
        x.TraverseAllObjects TraverseUpstream (List<_>(DeploymentSettings.NumObjectsPerJob)) dset (x.ResetAll jbInfo)
        x.TraverseAllObjectsWDirection TraverseUpstream (List<_>(DeploymentSettings.NumObjectsPerJob)) dset (x.PreBeginSync jbInfo)

    member x.CloseAllSync jbInfo ( dset: DSet ) = 
        let t1 = (PerfDateTime.UtcNow())       
        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "start PreClose %A %s:%s ........" dset.ParamType dset.Name dset.VersionString ) )
        if Utils.IsNull x.WaitForCloseAllStreamsHandleCollection then 
            let collectionName = sprintf "WaitHandle for job %s:%s dset %s:%s" x.Name x.VersionString dset.Name dset.VersionString 
            x.WaitForCloseAllStreamsHandleCollection <- new WaitHandleCollection(collectionName, DeploymentSettings.NumObjectsPerJob) 
        x.TraverseAllObjects TraverseUpstream (List<_>(DeploymentSettings.NumObjectsPerJob)) dset (x.SyncPreCloseAllStreams jbInfo)
        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "1st stage, PreClose %A %s:%s ........" dset.ParamType dset.Name dset.VersionString ) )
        // Use handle
        x.TraverseAllObjects TraverseUpstream (List<_>(DeploymentSettings.NumObjectsPerJob)) dset ( x.WaitForCloseAllStreamsViaHandle jbInfo t1 )
        let t2 = (PerfDateTime.UtcNow())
        let elapseMS = t2.Subtract(t1).TotalMilliseconds
        let timeoutInMS = Math.Max( DeploymentSettings.TimeOutJobFlushMilliseconds - int elapseMS, 1 )
        let bAllDone = x.WaitForCloseAllStreamsHandleCollection.WaitAll timeoutInMS DeploymentSettings.OneWaitForAllJobsDone DeploymentSettings.MonitorForLiveJobs DeploymentSettings.TraceLevelMonitorForLiveJobs
        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "2nd stage, WaitForClose %A %s:%s ........ all done is %A" dset.ParamType dset.Name dset.VersionString bAllDone ) )
        x.TraverseAllObjects TraverseUpstream (List<_>(DeploymentSettings.NumObjectsPerJob)) dset ( x.PostCloseAllStreams jbInfo )  
        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "3rd stage, PostClose %A %s:%s ........" dset.ParamType dset.Name dset.VersionString ) )
        x.JobInitializer <- null
        x.JobInfoCollections.Clear() |> ignore
        x.SyncExecutionEngine.Clear() |> ignore
        x.UnloadAll()
        x.WaitForCloseAllStreamsHandleCollection.CloseAll() 
//        ( x.WaitForCloseAllStreamsHandleCollection :> IDisposable ).Dispose()
        x.WaitForCloseAllStreamsHandleCollection <- null


    member x.IsDstDSet( dobj:DistributedObject ) = 
        match dobj with 
        | :? DSet as dset -> 
            match dset.DependencyDownstream with 
            | SaveTo _ -> 
                true
            | _ -> 
                false
        | _ -> 
            false
    // member val private bWarningOnReportClose = false with get, set
    member x.TaskReportClosePartition( jbInfo:JobInformation, dobj, meta:BlobMetadata, pos ) = 
        let dsetReport = x.FindDObject TraverseUpstream dobj x.IsDstDSet
        use msInfo = new MemStream( 128 )
        if Utils.IsNotNull dsetReport then 
            if meta.Parti>=0 then 
                msInfo.WriteGuid( jbInfo.JobID )     
                msInfo.WriteString( dsetReport.Name ) 
                msInfo.WriteInt64( dsetReport.Version.Ticks ) 
                msInfo.WriteVInt32( 1 ) 
                msInfo.WriteVInt32( meta.Parti ) 
                msInfo.WriteVInt32( int meta.SerialNumber ) 
                msInfo.WriteInt64( pos )
                jbInfo.ToSendHost( ControllerCommand(ControllerVerb.ReportPartition, ControllerNoun.DSet), msInfo )
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "ReportPartition, DSet for %s:%s %A length %d" dsetReport.Name dsetReport.VersionString (meta.ToString()) pos ))
            else
                msInfo.WriteGuid( jbInfo.JobID )
                msInfo.WriteString( dsetReport.Name ) 
                msInfo.WriteInt64( dsetReport.Version.Ticks ) 
                jbInfo.ToSendHost( ControllerCommand(ControllerVerb.ReportClose, ControllerNoun.DSet), msInfo )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ReportClose, DSet for %s:%s" dsetReport.Name dsetReport.VersionString ))
            // No flow control for reporting
            ()
        else
                let msg = sprintf "Attempt to Report, DSet by %A %s:%s, on %A at position %d, however, no upstream SaveTo DSet object can be found..." 
                                dobj.ParamType dobj.Name dobj.VersionString (meta.ToString()) pos
                // We expect the system to pick up the exception and send it to App
                raise( System.Exception( msg ) )
                // Logger.Log( LogLevel.Warning, msg )
                // msInfo.WriteString( msg ) 
                // jbInfo.ToSendHost( ControllerCommand(ControllerVerb.Warning, ControllerNoun.Message), msInfo )


    member x.WaitForCloseAllStreamsViaHandle jbInfo (t1:DateTime ) (cur:DistributedObject )= 
        cur.WaitForCloseAllStreamsViaHandle( x.WaitForCloseAllStreamsHandleCollection, jbInfo, t1 )

    member x.ConstructJobInfo( jobID: Guid, dset:DSet, queue:NetworkCommandQueue, endPoint:Net.IPEndPoint, bIsMainProject ) = 
        let jbInfo = JobInformation( jobID, bIsMainProject, dset.Name, dset.Version.Ticks, 
                                        ClustersInfo = x.ClustersInfo, 
                                        ReportClosePartition = x.TaskReportClosePartition, 
                                        HostQueue = queue, 
                                        ForwardEndPoint = endPoint, 
                                        JobReady = x.JobReady )
        jbInfo
    member val JobReady = new ManualResetEvent(false) with get
    member val JobInitializer = null with get, set
    member val JobStartTime = DateTime.MinValue with get, set
    member x.SetJobInitializer( dset: DSet ) = 
        if Utils.IsNull x.JobInitializer then 
            x.JobInitializer <- dset
            x.JobStartTime <- (PerfDateTime.UtcNow())
    member x.IsJobInitializer( dset: DSet ) = 
        Object.ReferenceEquals( x.JobInitializer, dset )
    member x.GetJobInfo( dset:DSet, queue, endPoint, bIsMainProject ) = 
        let dsetFullName = dset.Name + dset.VersionString
        let jbInfo = x.JobInfoCollections.GetOrAdd( dsetFullName, fun _ -> x.ConstructJobInfo( x.JobID, dset, queue, endPoint, bIsMainProject) )
        // Allow TryGetDerivedJobInformationFunc to use cached object
        jbInfo.TryGetDerivedJobInformationFunc <- ( fun dobj -> let fullname = dobj.Name + dobj.VersionString 
                                                                x.JobInfoCollections.GetOrAdd( fullname, fun _ -> jbInfo.TryGetDerivedJobInformationImpl( dobj ) )
                                                                )
        jbInfo
        
    static member ClosePartition (jbInfo:JobInformation) (dset:DSet) (meta:BlobMetadata) = 
        using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, fun _ -> sprintf "[ClosePartition, Job %A cancelled] DSet %s" jbInfo.JobID dset.Name )   
            else
                if not jobAction.IsCancelled then 
                    Logger.LogF( jbInfo.JobID, LogLevel.MediumVerbose, ( fun _ -> sprintf "Reaching end of part %d with %d mistakes" meta.Partition meta.NumElems ))
                if not jbInfo.HostShutDown then 
                        using( new MemStream( 1024 ) ) ( fun msWire ->
                            msWire.WriteGuid( jbInfo.JobID )
                            msWire.WriteString( dset.Name ) 
                            msWire.WriteInt64( dset.Version.Ticks )
                            msWire.WriteVInt32( meta.Partition )
                            // # of error in this partition. 
                            msWire.WriteVInt32( meta.NumElems )
                            jbInfo.ToSendHost( ControllerCommand( ControllerVerb.Close, ControllerNoun.Partition ), msWire )
                        )
        )

    /// OnJobFinish govern freeing of the resource of the Task object. 
    /// It has been registered when the Task is first established, and will garantee to execute when job is done or cancelled
    member x.OnJobFinish() = 
        for i=0 to x.NumBlobs-1 do
            if (Utils.IsNotNull x.Blobs.[i]) then
                if (Utils.IsNotNull x.Blobs.[i].Hash) then
                    BlobFactory.remove x.Blobs.[i].Hash
                if (Utils.IsNotNull x.Blobs.[i].Stream) then
                    (x.Blobs.[i].Stream :> IDisposable).Dispose()
        if (Utils.IsNotNull x.MetadataStream) then
            (x.MetadataStream :> IDisposable).Dispose()
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "SA Recv Stack size %d %d" Cluster.Connects.BufStackRecv.StackSize Cluster.Connects.BufStackRecv.GetStack.Size)
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "In blob factory: %d" BlobFactory.Current.Collection.Count)
        MemoryStreamB.DumpStreamsInUse()

/// Read, Job (DSet) 
    member x.DSetReadAsSeparateApp( queueHost:NetworkCommandQueue, endPoint:Net.IPEndPoint, dset: DSet, usePartitions ) = 
        let readFunc (jbInfo:JobInformation) ( meta, ms:StreamBase<byte> ) = 
            using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "[DSetReadAsSeparateApp, Job cancelled?] Fails to secure job action when writing DSet %s, %s" 
                                                                                        dset.Name 
                                                                                        (MetaFunction.MetaString(meta)) ))
                else 
                    if Utils.IsNotNull ms then 
                        Logger.LogF( jbInfo.JobID, LogLevel.WildVerbose, ( fun _ -> sprintf "DSetReadAsSeparateApp, to writeout %s" (MetaFunction.MetaString(meta)) ))
                        using( new MemStream( int ms.Length + 1024 ) ) ( fun msWire -> 
                            msWire.WriteGuid( jbInfo.JobID )
                            msWire.WriteString( dset.Name )
                            msWire.WriteInt64( dset.Version.Ticks )
                            msWire.WriteVInt32( meta.Partition )
                            msWire.WriteInt64( meta.Serial )
                            msWire.WriteVInt32( meta.NumElems )
                            //msWire.Write( ms.GetBuffer(), int ms.Position, int ms.Length - int ms.Position )
                            msWire.AppendNoCopy( ms, ms.Position, ms.Length-ms.Position)
                            (ms :> IDisposable).Dispose()
                            let cmd = ControllerCommand( ControllerVerb.Write, ControllerNoun.DSet )
                            let bSendout = ref false
                            while not jbInfo.HostShutDown && not !bSendout do
                                if jbInfo.bAvailableToSend( dset.SendingQueueLimit ) then 
                                    jbInfo.ToSendHost( cmd, msWire ) 
                                    bSendout := true
                                else
                                    // Flow control kick in
                                    ThreadPoolWaitHandles.safeWaitOne( jobAction.WaitHandle, 5 ) |> ignore                               
                        )                                             
                        if jbInfo.HostShutDown then 
                            // Attempt to cancel jobs 
                            jobAction.ThrowExceptionAtContainer( "___ DSetReadAsSeparateApp, Host has been closed ____")
                    else
                        // Do nothing, we will wait for all jobs to complete in Async.RunSynchronously
                        Task.ClosePartition jbInfo dset meta
            )

        let syncJobs jbInfo parti ()=  
            dset.SyncEncode jbInfo parti ( readFunc jbInfo) 

        x.SyncJobExecutionAsSeparateApp ( queueHost, endPoint, dset, usePartitions) "Read" syncJobs ( fun _ -> () ) ( fun _ -> x.OnJobFinish() )
            
/// ReadToNetwork, Job (DSet) 
    member x.DSetReadToNetworkAsSeparateAppSync( queueHost:NetworkCommandQueue, endPoint:Net.IPEndPoint, dset: DSet, usePartitions ) = 
        let dsetToSendDown = ref null
        let dStreamToSendSyncClose = ref null
        let bSendNullImmediately = ref false
        let beginJob (jbInfo:JobInformation) =
            using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "[DSetReadToNetworkAsSeparateAppSync, Job %A cancelled?] Fails to secure job action when try to begin job on DSet %s." 
                                                                                            jbInfo.JobID dset.Name 
                                                                                            ))
                else
                    match dset.DependencyDownstream with 
                    | MixTo oneChild -> 
                        dsetToSendDown := oneChild.TargetDSet
                        match (!dsetToSendDown).DependencyDownstream with 
                        | WildMixTo (child, childS) -> 
                            dStreamToSendSyncClose := childS.TargetStream
                        | _ -> 
                            ()
                    | WildMixTo (child, childS) -> 
                        dsetToSendDown := dset
                        dStreamToSendSyncClose := childS.TargetStream
                        bSendNullImmediately := true
                    | _ -> 
                        ()
                    if Utils.IsNull !dsetToSendDown || Utils.IsNull !dStreamToSendSyncClose then 
                        let msg = sprintf "Job %s:%s, DSet %s:%s DSetReadToNetworkAsSeparateApp expects WildMixTo/MixTo downstream dependency, but get %A"
                                            x.Name x.VersionString dset.Name dset.VersionString dset.DependencyDownstream 
                        jobAction.ErrorAtContainer( msg )
                    else
                        jbInfo.FoldState.Clear()
            )
        let readToNetworkFunci (jbInfo:JobInformation) parti ()= 
            let wrappedFunc (jbInfo:JobInformation) parti (param:BlobMetadata*Object) =
                let meta, elemObject = param
                let bNullObject = Utils.IsNull elemObject
                if not bNullObject || (!bSendNullImmediately) then 
                    Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "ReadToNetwork Job %s:%s, DSet %s:%s %s" 
                                                                                           x.Name x.VersionString dset.Name dset.VersionString (meta.ToString()) ))
                    (!dsetToSendDown).SyncExecuteDownstream jbInfo (meta.Partition) meta elemObject
                if bNullObject then 
                    jbInfo.FoldState.Item( meta.Partition ) <- meta.Serial
            let t1 = PerfADateTime.UtcNow()
            Logger.LogF( x.JobID, LogLevel.MediumVerbose, (fun _ -> sprintf "Start readToNetworkFunci partition %d" parti))
            let ret = dset.SyncIterateProtected jbInfo parti (wrappedFunc jbInfo parti )
            Logger.LogF( x.JobID, LogLevel.WildVerbose, (fun _ -> 
                       let t2 = PerfADateTime.UtcNow()
                       sprintf "%s End readToNetworkFunci partition %d - start %s elapse %f" (VersionToString(t2)) parti (VersionToString(t1)) (PerfADateTime.UtcNow().Subtract(t1).TotalSeconds) ))
            ret
                
        let finalJob (jbInfo:JobInformation) =
            using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "[DSetReadToNetworkAsSeparateAppSync, Job %A has already been cancelled when try to finalize on DSet %s." 
                                                                                            jbInfo.JobID dset.Name 
                                                                                    ))
                else
                    if Utils.IsNull !dStreamToSendSyncClose then 
                        let msg = sprintf "Job %s:%s, DSet %s:%s DSetReadToNetworkAsSeparateApp expects WildMixTo/MixTo downstream dependency, but get %A"
                                            x.Name x.VersionString dset.Name dset.VersionString dset.DependencyDownstream 
                        jobAction.ErrorAtContainer( msg )
                    else
                        (!dStreamToSendSyncClose).SyncSendCloseDStreamToAll(jbInfo)
                        Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "All ReadToNetwork tasks completed for job %s:%s DSet %s:%s, send SyncClose, DStream to all peers !" x.Name x.VersionString dset.Name dset.VersionString))
            )
            x.OnJobFinish()

        x.SyncJobExecutionAsSeparateApp ( queueHost, endPoint, dset, usePartitions) "ReadToNetwork" readToNetworkFunci
            beginJob finalJob      

/// Fold, Job (DSet) 
    member x.DSetFoldAsSeparateApp( queueHost:NetworkCommandQueue, endPoint:Net.IPEndPoint, dset: DSet, usePartitions, foldFunc: FoldFunction, aggregateFunc: AggregateFunction, serializeFunc: GVSerialize, stateFunc: unit->Object, msRep : StreamBase<byte> ) = 
        let syncFoldFunci (jbInfo:JobInformation) parti param = 
            let meta, elemObject = param
            jbInfo.FoldState.Item( parti ) <- foldFunc.FoldFunc (jbInfo.FoldState.GetOrAdd(parti, fun partitioni -> stateFunc() )) param
            if Utils.IsNull elemObject then 
                Task.ClosePartition jbInfo dset meta
        let beginJob (jbInfo:JobInformation)  =
            jbInfo.FoldState.Clear() |> ignore
                
        let finalJob (jbInfo:JobInformation)  =
            let usePartitionsArray = jbInfo.FoldState |> Seq.map ( fun pair -> pair.Key ) |> Seq.toArray
            let foldStates = jbInfo.FoldState |> Seq.map ( fun pair -> pair.Value )
            let finalState = foldStates |> Seq.fold aggregateFunc.AggregateFunc null           
            // GV to send back to the host. 
            use msWire = new MemStream( 1024 )
            msWire.WriteGuid( jbInfo.JobID )
            msWire.WriteString( dset.Name ) 
            msWire.WriteInt64( dset.Version.Ticks ) 
            msWire.WriteVInt32( usePartitionsArray.Length )
            for i = 0 to usePartitionsArray.Length - 1 do
                msWire.WriteVInt32( usePartitionsArray.[i] )
            let msSend = serializeFunc.SerializeFunc (msWire :> StreamBase<byte>) finalState
            Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "All aggregate fold completed for job %s:%s DSet %s:%s, send WriteGV,DSet to client!" x.Name x.VersionString dset.Name dset.VersionString))
            jbInfo.ToSendHost( ControllerCommand( ControllerVerb.WriteGV, ControllerNoun.DSet ), msSend )
            x.OnJobFinish()

        x.SyncJobExecutionAsSeparateApp ( queueHost, endPoint, dset, usePartitions) "Fold" (fun jbInfo parti () -> dset.SyncIterateProtected jbInfo parti (syncFoldFunci jbInfo parti ) ) beginJob finalJob
        if (Utils.IsNotNull msRep) then
            (msRep :> IDisposable).Dispose()

    member val JobFinished = List<WaitHandle>()

    member x.SyncJobExecutionAsSeparateApp ( queueHost:NetworkCommandQueue, endPoint:Net.IPEndPoint, dset: DSet, usePartitions ) 
                taskName syncAction beginJob endJob= 
        let constructThreadPool() = 
            new ThreadPoolWithWaitHandles<int>( sprintf "DSet %s:%s" dset.Name dset.VersionString, 
                NumParallelExecution = if dset.NumParallelExecution<=0 then DeploymentSettings.NumParallelJobs( Environment.ProcessorCount ) else dset.NumParallelExecution )
        let bIsMainProject = 
            if taskName <> "ReadToNetwork" then 
                x.SetJobInitializer( dset )
                true
            else
                false
        let jbInfo = x.GetJobInfo( dset, queueHost, endPoint, bIsMainProject )
        let priorTasks = ref null
        let bExistPriorTasks = ref true
        let jobFinish = new ManualResetEvent(false)
        x.JobFinished.Add(jobFinish :> WaitHandle)
        using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "[SyncJobExecutionAsSeparateApp, Job %A cancelled?] Fails to secure job action at beginning on DSet, %s." 
                                                                        jbInfo.JobID dset.Name 
                                                                        ))
            else
                try
                    let dsetFullName = dset.Name + dset.VersionString
                    while Utils.IsNull (!priorTasks) do
                        let bFind = x.SyncExecutionEngine.TryGetValue( dsetFullName, priorTasks ) 
                        if not bFind then 
                            bExistPriorTasks := not (x.SyncExecutionEngine.TryAdd( dsetFullName, constructThreadPool() ) )
                    if x.IsJobInitializer(dset) && not (!bExistPriorTasks) then 
                        // Only called once for the entire job
                        (!priorTasks).Reset()
                        x.JobReady.Reset() |> ignore
                        x.ResolveDSetParent( dset ) 
                        x.RegisterInJobCallback( dset, true )
                        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Contruct Job Execution Graph for task %s of DSet %s ............." taskName dset.Name ))
                        Logger.Do( LogLevel.MildVerbose, ( fun _ -> x.ShowAllDObjectsInfo() ))
                        x.BeginAllSync jbInfo dset
                    if not (!bExistPriorTasks) then 
                        if x.IsJobInitializer(dset) then 
                            beginJob jbInfo
                        else
                            x.JobReady.WaitOne() |> ignore
                            beginJob jbInfo
                    if x.IsJobInitializer(dset) && not (!bExistPriorTasks) then 
                        x.JobReady.Set() |> ignore
                    x.JobReady.WaitOne() |> ignore
                    if Utils.IsNotNull queueHost && not (jobAction.IsCancelled) then 
                        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "To execute task %s for DSet %s:%s with partitions %A!" taskName dset.Name dset.VersionString usePartitions ))
                        if x.IsJobInitializer(dset) && not (!bExistPriorTasks) then 
                            dset.ResetForRead( queueHost )   
                        let bAllCancelled = ref false
                        let tasks = !priorTasks
                        let wrappedSyncAction jbInfo parti () = 
                            try
                                syncAction jbInfo parti ()
                            with
                            | ex -> 
                                // Try to catch failure for the execution of single partition 
                                jbInfo.PartitionFailure( ex, "@_____ SyncJobExecutionAsSeparateApp _____", parti )
                                null, true

                        for parti in usePartitions do
                            // tasks.AddTask( x.CancellationToken.Token, parti, syncAction jbInfo parti )
                            //tasks.EnqueueRepeatableFunction (wrappedSyncAction jbInfo parti) jobAction.CTS parti ( fun pi -> sprintf "Job %A, %s Task %s:%s, DSet %s:%s, part %d" x.JobID taskName x.Name x.VersionString dset.Name dset.VersionString pi )
                            Component<_>.AddWorkItem (wrappedSyncAction jbInfo parti) tasks jobAction.CTS parti ( fun pi -> sprintf "Job %A, %s Task %s:%s, DSet %s:%s, part %d" x.JobID taskName x.Name x.VersionString dset.Name dset.VersionString pi ) |> ignore
                        try 
                        // JinL: 05/10/2014, need to find a way to wait for all intermediate task.  
                            if not (!bExistPriorTasks) then 
                                Component<_>.ExecTP tasks
                                let bDone = tasks.WaitForAll( -1 )
                                endJob jbInfo
                                // Release the resource of the execution engine
                                let tp = ref Unchecked.defaultof<ThreadPoolWithWaitHandles<int>>
                                let ret = x.SyncExecutionEngine.TryRemove( dsetFullName, tp )
                                if (ret) then
                                    (!tp).CloseAllThreadPool()
                                Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "All individual jobs completed for task %s for DSet %s:%s, completion status %A!" taskName dset.Name dset.VersionString bDone))
                        with 
                        | ex ->
                            jobAction.EncounterExceptionAtContainer( ex, "___ SyncJobExecutionAsSeparateApp (loop on WaitForAll) ___" )
                        // JinL: note some of the write task may not finish at this moment. 
                        if x.IsJobInitializer(dset) && not (!bExistPriorTasks) then 
                            x.CloseAllSync jbInfo dset
                            Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "%s Task %s:%s, DSet %s:%s is completed in %f ms........." taskName x.Name x.VersionString dset.Name dset.VersionString ((PerfDateTime.UtcNow()).Subtract( x.JobStartTime ).TotalMilliseconds) ))
                            Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> "=======================================================================================================================================" ))
                            x.JobReady.Reset() |> ignore
                        use msWire = new MemStream( 1024 )
                        msWire.WriteGuid( x.JobID )
                        msWire.WriteString( dset.Name )
                        msWire.WriteInt64( dset.Version.Ticks )
                        jbInfo.ToSendHost( ControllerCommand( ControllerVerb.Close, ControllerNoun.DSet ), msWire )
                        Logger.LogF( x.JobID, LogLevel.MediumVerbose, ( fun _ -> sprintf "Close, DSet sent for %s:%s" dset.Name dset.VersionString ))
                with 
                | ex -> 
                    jobAction.EncounterExceptionAtContainer( ex, "___ SyncJobExecutionAsSeparateApp (job loop) ___" )
                try
                    if x.IsJobInitializer(dset) && not (!bExistPriorTasks) then 
                        x.RegisterInJobCallback( dset, false )
                    Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "SyncJobExecutionAsSeparateApp, all done for executing task %s for DSet %s:%s" taskName dset.Name dset.VersionString ))
                with 
                | ex -> 
                    jobAction.EncounterExceptionAtContainer( ex, "___ SyncJobExecutionAsSeparateApp (UnRegisterCallback) ___" )
        )
        jobFinish.Set() |> ignore
        x.JobFinished.Remove( jobFinish :> WaitHandle ) |> ignore 

    /// Unusual error in a container
    /// The error cannot be localized to a certain job, but can be assoicated with a network queue 
    static member ErrorInSeparateApp (queue:NetworkCommandQueue, msg ) = 
        Logger.Log( LogLevel.Error, msg )
        using (new MemStream(1024)) ( fun msError ->
            msError.WriteString( msg )
            queue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
        )

    /// Throw exception to application
    static member ExceptionInTask (queue:NetworkCommandQueue, x:Task, msg ) = 
        Logger.Log( LogLevel.Info, ( sprintf "Exception in job %A, name %s:%s with exception %s" (x.JobID) (x.Name) (x.VersionString) msg ) )
        use ms = new MemStream()
        ms.WriteGuid( x.JobID )
        ms.WriteString( x.Name )
        ms.WriteInt64( x.Version.Ticks )
        ms.WriteString( msg )
        queue.ToSend( ControllerCommand(ControllerVerb.Exception, ControllerNoun.Job ), ms ) 

    static member val LogLevelNullJobAction = LogLevel.MildVerbose with get, set
    /// Throw general exception, not bound to a particular application
    /// Note that general exception may or may not be able to propagate back to application as it may miss routing information
    static member ExceptionInGeneral (queue:NetworkCommandQueue, msg ) = 
        Logger.Log( LogLevel.Info, ( sprintf "Exception: %s" msg ) )
        use ms = new MemStream()
        ms.WriteString( msg )
        queue.ToSend( ControllerCommand(ControllerVerb.Exception, ControllerNoun.Message ), ms ) 

    static member ParseQueueCommandAtContainer (task : Task ref)
                                    (queue : NetworkCommandQueue)
                                    (allConnections : ConcurrentDictionary<_,_>) 
                                    (allTasks :  ConcurrentDictionary<_,_>) 
                                    (showConnections : ConcurrentDictionary<int64,_>->unit)
                                    (bConnectedBefore : bool ref)
                                    (bTerminateJob : bool ref)
                                    (command : ControllerCommand)
                                    (ms : StreamBase<byte>) =
        if (Utils.IsNotNull !task) then
            ms.Info <- ms.Info + ":ParseQueueCommand:Task:" + (!task).Name
        match (command.Verb, command.Noun ) with
        | ( ControllerVerb.Open, ControllerNoun.Connection ) ->
            let queueSignature = ms.ReadInt64()
            allConnections.GetOrAdd( queueSignature, 0 ) |> ignore
            bConnectedBefore := true
            showConnections allConnections
            ()
        | ( ControllerVerb.Close, ControllerNoun.Connection ) ->
            let queueSignature = ms.ReadInt64()
            let bRemove, _ = allConnections.TryRemove( queueSignature) 
            showConnections allConnections
            if bRemove then 
                if allConnections.IsEmpty && ServiceCollection.Current.IsEmpty then 
                    bTerminateJob := true
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Close, Connection, no active connection remains and all service is terminated, job terminates" ))
            ()
        | ( ControllerVerb.Set, ControllerNoun.Job ) ->
            try 
                let jobID, name, verNumber = Job.PeekJob( ms )
                let jobLifeCyleObj = JobLifeCycleCollectionContainer.BeginJob( jobID, name, verNumber, queue.RemoteEndPointSignature )
                if Utils.IsNull jobLifeCyleObj then 
                    Task.ErrorInSeparateApp( queue, sprintf "Failed to create JobLifeCycle Object for Job %A, most probably because another job of the same job ID is running" jobID ) 
                else
                    using ( SingleJobActionContainer.TryFind( jobID )) ( fun jobAction -> 
                        if Utils.IsNull jobAction then 
                            Task.ErrorInSeparateApp( queue, sprintf "Set, Job: unable to secure a job action object for Job: %A even we have just allocated a jobLifeCyleObj" jobID )
                        else
                            try 
                                // Should be the 1st command to receive when link is established. 
                                let x = new Task()
                                task := x
                                let bRet = x.UnpackToBlob( ms )
                                if bRet then 
                                    x.QueueToClient <- queue 
                                    let y = allTasks.GetOrAdd( jobID, x )
                                    if Object.ReferenceEquals( x, y ) then 
                                        // Task entry inserted. Insert a removal entry 
                                        jobAction.LifeCycleObject.OnDisposeFS( x.OnJobFinish )
                                        jobAction.LifeCycleObject.OnDisposeFS( fun _ -> allTasks.TryRemove(jobID) |> ignore )
                                        jobAction.LifeCycleObject.OnDisposeFS( fun _ -> BlobFactory.unregisterAndRemove(jobID) |> ignore )
                                        x.ClientAvailability( queue.RemoteEndPointSignature, Some x.ReceiveBlobNoFeedback, None ) 
                                        Logger.LogF( jobID, 
                                                     LogLevel.MildVerbose, ( fun _ ->  let t1 = (PerfADateTime.UtcNowTicks())
                                                                                       sprintf "Set, Job %s:%s with %d blobs in %.2fms" 
                                                                                               x.Name x.VersionString
                                                                                               x.NumBlobs 
                                                                                               ( float (t1-x.JobStartTicks) / float TimeSpan.TicksPerMillisecond )
                                                                                               ))
                                    else
                                        let msg = sprintf "Set, Job finds an entry of a duplicate Job in the task list with the same Job ID %A, even it has succeeded in securing a job lifecyle object. Two jobs of the same job ID are started at about the same time?" 
                                                            x.JobID
                                        jobAction.ThrowExceptionAtContainer( msg )
                            with
                            | ex -> 
                                jobAction.EncounterExceptionAtContainer( ex, "____ Set, Job _____ " )
                    )
            with 
            | ex -> 
                Task.ErrorInSeparateApp( queue, sprintf "Failed to peek jobID information from Set, Job with exception %A" ex) 
        | ( ControllerVerb.Set, ControllerNoun.Blob ) ->
            try 
                let buf, pos, count = ms.GetBufferPosLength()
                let fastHash = buf.ComputeChecksum(int64 pos, int64 count)
                let bSuccess,cryptoHash = BlobFactory.receiveWriteBlob( fastHash, lazy buf.ComputeSHA256(int64 pos, int64 count), ms, queue.RemoteEndPointSignature )
                if bSuccess then 
                    Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "Rcvd Set, Blob from endpoint %s of %dB hash to %s (%d, %dB), successfully parsed"
                                                                           (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                           buf.Length
                                                                           (BytesToHex(if cryptoHash.IsSome then cryptoHash.Value else fastHash))
                                                                           pos count  ))
                else
                    Logger.LogF( LogLevel.Info, fun _ -> sprintf "[may be OK, job cancelled?] Rcvd Set, Blob from endpoint %s of %dB hash to %s (%d, %dB), but failed to find corresponding job"
                                                                           (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                           buf.Length
                                                                           (BytesToHex(fastHash))
                                                                           pos count )
                    
            with 
            | ex -> 
                Task.ErrorInSeparateApp( queue, sprintf "Failed to parse Set, Blob with exception %A" ex)                                                
        | ( ControllerVerb.Write, ControllerNoun.Blob ) ->
            let jobID = ms.ReadGuid()
            using ( SingleJobActionContainer.TryFind( jobID )) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Logger.LogF( LogLevel.Info, fun _ -> sprintf "[may be OK, job cancelled?] Job %A, Rcvd Write, Blob from endpoint %s of %dB payload, but failed to find corresponding job"
                                                                           jobID
                                                                           (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                           ms.Length )
                else
                    try 
                        let name = ms.ReadString()
                        let verNumber = ms.ReadInt64()
                        let bExist, x = allTasks.TryGetValue( jobID )
                        if bExist then
                            if String.Compare(name,x.Name,StringComparison.Ordinal)<>0 || x.Version.Ticks<>verNumber then 
                                let msg = sprintf "Error@AppDomain: Job %A, received Write, Blob with wrong job name & version number: %s:%d (instead of %s:%d)" 
                                                        jobID name verNumber x.Name x.Version.Ticks
                                jobAction.ThrowExceptionAtContainer( msg )
                            else                                
                                let blobi = ms.ReadVInt32()
                                if blobi<0 || blobi >= x.NumBlobs then 
                                    let msg = sprintf "Error@AppDomain: Job %A, %s:%s Write, Blob with idx %d that is outside of valid range (0-%d)" jobID x.Name x.VersionString blobi  x.NumBlobs
                                    jobAction.ThrowExceptionAtContainer( msg )
                                else
                                    let blob = x.Blobs.[blobi]
                                    let pos = ms.Position
                                    let buf = ms.GetBuffer()
                                    Logger.LogF( jobID, LogLevel.MediumVerbose, ( fun _ -> sprintf "Rcvd Write, Blob from job %s:%s, blob %d, pos %d (buf %dB)"
                                                                                            x.Name x.VersionString
                                                                                            blobi pos (ms.Length-ms.Position) ))
                                    blob.Stream <- ms.Replicate()
                                    /// Passthrough DSet is to be decoded at LoadAll() function. 
                                    let bSuccessful = x.ClientReceiveBlob( blobi, false )
                                    if not bSuccessful then 
                                        let msg = sprintf "Error@AppDomain: Job %A, Write, Blob, failed to decode the blob %d, type %A" jobID blobi blob.TypeOf
                                        jobAction.ThrowExceptionAtContainer( msg )
                        else 
                            let msg = sprintf "Error@Container: Job %A, Can't find job %s:%s in Write, Blob, eventhough the JobLifecyle object is still there." 
                                                jobID name (VersionToString(DateTime(verNumber)))
                            jobAction.ThrowExceptionAtContainer( msg )
                    with 
                    | ex -> 
                        jobAction.EncounterExceptionAtContainer( ex, "____ Write, Job _____ ")
            )
        | ( ControllerVerb.Start, ControllerNoun.Job ) ->
            // Load all Cluster, DSet, Assemblies. 
            let jobID = ms.ReadGuid()
            using ( SingleJobActionContainer.TryFind( jobID )) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Logger.LogF( jobID, LogLevel.Info, fun _ -> sprintf "[may be OK, job cancelled?] Rcvd Start, Job from endpoint %s of %dB payload, but failed to find corresponding job"
                                                                           (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                           ms.Length )
                else
                    let name = ms.ReadString()
                    let verNumber = ms.ReadInt64()
                    let bExist, x = allTasks.TryGetValue( jobID )
                    jobAction.LifeCycleObject.OnCancellationFS( fun _ -> allTasks.TryRemove( jobID ) |> ignore )
                    if bExist then 
        //                x.ClientAvailability(queue.RemoteEndPointSignature, None)
        //                if not x.AllAvailable then 
        //                    let msg = ( sprintf "Error@AppDomain: some blob is not available: %A" x.AvailThis.AvailVector )
        //                    failwith msg
        //                else
                            Logger.LogF( jobID, 
                                         LogLevel.MildVerbose, ( fun _ ->  let t1 = (PerfADateTime.UtcNowTicks())
                                                                           sprintf "Start, Job received %s:%s with %d blobs in %.2fms" 
                                                                                   x.Name x.VersionString
                                                                                   x.NumBlobs 
                                                                                   ( float (t1-x.JobStartTicks) / float TimeSpan.TicksPerMillisecond )
                                                                                   ))
                            let bSuccess = x.LoadAll()
                            using( new MemStream( 1024 ) ) ( fun msSend -> 
                                msSend.WriteGuid( jobID )
                                msSend.WriteString( x.Name )
                                msSend.WriteInt64( x.Version.Ticks ) 
                                msSend.WriteBoolean( bSuccess )
                                queue.ToSend( ControllerCommand( ControllerVerb.ConfirmStart, ControllerNoun.Job ), msSend ) 
                            )
                            if not bSuccess then 
                                let msg = ( sprintf "Error@AppDomain: Job %A, some blob of the job cannot be loaded: %A" jobID x.AvailThis.AvailVector )
                                jobAction.ThrowExceptionAtContainer( msg )
                            else
                                Logger.LogF( jobID, 
                                             LogLevel.MildVerbose, ( fun _ ->  let t1 = (PerfADateTime.UtcNowTicks())
                                                                               sprintf "All blobs of job %s:%s loaded in %.2fms" 
                                                                                           x.Name x.VersionString 
                                                                                           ( float (t1-x.JobStartTicks) / float TimeSpan.TicksPerMillisecond )
                                                                                           ))
                                ()
                    else
                        let msg = sprintf "Error@AppDomain: Start, Job (ID:%A) %s:%s can't find the relevant job in allTasks " jobID name (VersionToString(DateTime(verNumber)))
                        jobAction.ThrowExceptionAtContainer( msg )
            )
//        | ( ControllerVerb.Cancel, ControllerNoun.Job ) -> 
//            let jobID = ms.ReadGuid()
//            use jobAction = SingleJobActionContainer.TryFind( jobID )
//            if Utils.IsNull jobAction then 
//                    Logger.LogF( jobID, 
//                                 LogLevel.WildVerbose, fun _ -> sprintf "[may be OK, job cancelled?] Rcvd Cancel, Job from endpoint %s of %dB payload, but failed to find corresponding job"
//                                                                           (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
//                                                                           ms.Length )
//            else
//                jobAction.CancelJob()

        | ( ControllerVerb.Echo2, ControllerNoun.Job ) ->
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Echo2, Job from client"))
        | ( ControllerVerb.UpdateParam, ControllerNoun.Job ) 
        | ( ControllerVerb.Read, ControllerNoun.Job ) 
        | ( ControllerVerb.ReadToNetwork, ControllerNoun.Job ) 
        | ( ControllerVerb.Fold, ControllerNoun.Job ) 
        | ( ControllerVerb.Start, ControllerNoun.Service ) 
        | ( ControllerVerb.Stop, ControllerNoun.Service ) ->
            /// A Job, with end result being reading content from a DSet
            let jobID = ms.ReadGuid()
            using ( SingleJobActionContainer.TryFind( jobID )) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Logger.LogF( jobID, 
                                 LogLevel.Info, fun _ -> sprintf "[may be OK, job cancelled?] Rcvd Network Command %A from endpoint %s of %dB payload, but failed to find corresponding job"
                                                                           command
                                                                           (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                           ms.Length )
                else
                    let name = ms.ReadString()
                    let verNumber = ms.ReadInt64()
                    let bExist, x = allTasks.TryGetValue( jobID )
                    Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Receive command %A for %s" command name))
                    if bExist then 
                        try
                            let endPoint = ms.ReadIPEndPoint()
                            let paramType = enum<_>(ms.ReadVInt32())
                            match paramType with
                            | FunctionParamType.DSet ->
                                let JOBID = ms.ReadGuid()
                                let dsetName, dsetVersion = 
                                    match command.Verb with 
                                    | ControllerVerb.UpdateParam -> 
                                        DSet.Peek( ms ) 
                                    | _ -> 
                                        ms.ReadString(), ms.ReadInt64()
                                match (command.Verb, command.Noun ) with
                                | ( ControllerVerb.Start, ControllerNoun.Service ) ->
                                    let serviceName = ms.ReadString() 
                                    let service = ms.Deserialize() :?> WorkerRoleEntryPoint
                                    let param = ms.DeserializeObjectWithTypeName() 
                                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "attempt to start service %s ..." serviceName ))
                                    ServiceCollection.Current.BeginStartService( serviceName, service, param, 
                                                                    fun bInitializeSuccess -> use msInfo = new MemStream(1024)
                                                                                              msInfo.WriteGuid( jobID )
                                                                                              msInfo.WriteString( dsetName )
                                                                                              msInfo.WriteInt64( dsetVersion ) 
                                                                                              msInfo.WriteBoolean( bInitializeSuccess )
                                                                                              queue.ToForward( endPoint, ControllerCommand( ControllerVerb.ConfirmStart, ControllerNoun.Service), msInfo )
                                                                                              Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "send ConfirmStart, Service with %A to %s ..." bInitializeSuccess (LocalDNS.GetShowInfo(endPoint)) ))
                                                                                              ()
                                                                                       )
                                    ()
                                | ( ControllerVerb.Stop, ControllerNoun.Service ) ->
                                    let serviceName = ms.ReadString() 
                                    Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "attempt to stop service %s ..." serviceName ))
                                    let bSuccessToStop = ServiceCollection.Current.StopService( serviceName )
                                    use msInfo =  new MemStream( 1024 )
                                    msInfo.WriteGuid( jobID )
                                    msInfo.WriteString( dsetName )
                                    msInfo.WriteInt64( dsetVersion ) 
                                    msInfo.WriteBoolean( bSuccessToStop )
                                    queue.ToForward( endPoint, ControllerCommand( ControllerVerb.ConfirmStop, ControllerNoun.Service), msInfo )
                                | ( ControllerVerb.UpdateParam, ControllerNoun.Job ) ->
                                    let useDSet = x.ResolveDSetByName( dsetName, dsetVersion ) 
                                    if Utils.IsNull useDSet then
                                        let msg = sprintf "Fail in %A, %A. Can't find DSet %s:%s" (command.Verb) (command.Noun) dsetName (VersionToString(DateTime(dsetVersion)))
                                        jobAction.ThrowExceptionAtContainer( msg )
                                    else
                                        let dsetOption, errMsg, msSend = DSetPeer.Unpack( ms, true, queue, jobID )
                                        (msSend :> IDisposable).Dispose()
                                        match errMsg with 
                                        | ClientBlockingOn.Cluster ->
                                            // Cluster Information can't be parsed, Wait for cluster information. 
                                            let msg = sprintf "Update, DSet for %s:%s failed as the associated cluster cannot be found" useDSet.Name useDSet.VersionString
                                            jobAction.ThrowExceptionAtContainer( msg )
                                        | ClientBlockingOn.None ->
                                            match dsetOption with 
                                            | Some writeDSet -> 
                                                let isDStream (dobj:DistributedObject) = 
                                                    match dobj with 
                                                    | :? DStream -> 
                                                        true
                                                    | _ -> 
                                                        false
                                                // Find storage dstream 
                                                let dstream = x.FindDObject TraverseDownstream useDSet isDStream
                                                if Utils.IsNotNull dstream then 
                                                    writeDSet.StorageType <- dstream.StorageType
                                                    Logger.LogF( jobID, LogLevel.WildVerbose, ( fun _ -> sprintf "Update %A %s:%s, assign storage type from %A %s:%s as %A" 
                                                                                                               writeDSet.ParamType writeDSet.Name writeDSet.VersionString
                                                                                                               dstream.ParamType dstream.Name dstream.VersionString
                                                                                                               dstream.StorageType ))
                                                else
                                                    writeDSet.StorageType <- StorageKind.None
                                                    Logger.LogF( jobID, LogLevel.WildVerbose, ( fun _ -> sprintf "Update %A %s:%s, can't find destination dstream, use storage type of %A" 
                                                                                                               writeDSet.ParamType writeDSet.Name writeDSet.VersionString
                                                                                                               writeDSet.StorageType ))


                                                if writeDSet.ReadyStoreStreamArray() then 
                                                    writeDSet.WriteDSetMetadata()
                                                    writeDSet.CloseStorageProvider()
                                                Logger.LogF( jobID, LogLevel.MildVerbose, (fun _ -> sprintf "Calling dset metadata update for %A receive %A" writeDSet.Name command))
                                                useDSet.CopyMetaData( writeDSet, DSetMetadataCopyFlag.Update )
                                            | None -> 
                                                let msg = sprintf "!!! logic error !!! Update, DSet for %s:%s failed to parse the coded DSet information" useDSet.Name useDSet.VersionString
                                                jobAction.ThrowExceptionAtContainer( msg )
                                        | _ ->
                                            // Error, fail to set DSet
                                            let msg = sprintf "Update, DSet for %s:%s failed to parse the coded DSet information" useDSet.Name useDSet.VersionString
                                            jobAction.ThrowExceptionAtContainer( msg )
                                | _ -> 
                                    let npart = ms.ReadVInt32()
                                    let usePartitions = Array.zeroCreate<int> npart
                                    for i = 0 to npart - 1 do 
                                        usePartitions.[i] <- ms.ReadVInt32()
                                    let useDSet = x.ResolveDSetByName( dsetName, dsetVersion ) 
                                    if Utils.IsNull useDSet then
                                        let msg = sprintf "Fail in %A, %A. Can't find DSet %s:%s" (command.Verb) (command.Noun) dsetName (VersionToString(DateTime(dsetVersion)))
                                        jobAction.ThrowExceptionAtContainer( msg )
                                    else
                                        let ta = ref null
                                        match (command.Verb, command.Noun ) with
                                        | ( ControllerVerb.Read, ControllerNoun.Job ) ->
                                            ta := ThreadTracking.StartThreadForFunction ( fun _ -> sprintf "Job Thread, DSet Read %s" dsetName ) ( fun _ -> x.DSetReadAsSeparateApp( queue, endPoint, useDSet, usePartitions ) )
                                        | ( ControllerVerb.ReadToNetwork, ControllerNoun.Job ) -> 
                                            ta := ThreadTracking.StartThreadForFunction ( fun _ -> sprintf "Job Thread, DSet Read To Network %s" dsetName) ( fun _ -> x.DSetReadToNetworkAsSeparateAppSync( queue, endPoint, useDSet, usePartitions ) )
                                        | ( ControllerVerb.Fold, ControllerNoun.Job ) ->
                                            let bCommonStatePerNode = ms.ReadBoolean()
                                            let foldFunc = ms.Deserialize() :?> FoldFunction // Don't use DeserializeTo, as it may be further derived from FoldFunction
                                            let aggregateFunc = ms.Deserialize() :?> AggregateFunction
                                            let serializeFunc = ms.Deserialize() :?> GVSerialize
                                            let startpos = ms.Position
                                            let stateTypeName = ms.ReadString() 
                                            let refCount = ref -1  
                                            let state = 
                                                // Some function may not have a parameter, and that is OK. 
                                                if ms.Position < ms.Length then 
                                                    ms.CustomizableDeserializeToTypeName( stateTypeName )
                                                else
                                                    null
                                            let endpos = ms.Position
                                            let msRep, pos, length = 
                                                if bCommonStatePerNode || Utils.IsNull state then 
                                                    null, 0L, 0L
                                                else
                                                    ms.Replicate(), startpos, endpos-startpos
                                            let replicateMsStreamFunc()  = 
                                                if bCommonStatePerNode || Utils.IsNull state then 
                                                    null
                                                else 
                                                    // Start pos will not be the end of stream, garanteed by state not null 
                                                    let ms = new MemoryStreamB()
                                                    ms.Info <- "Replicated stream"
                                                    ms.AppendNoCopy(msRep, 0L, pos+length) // this is a write operation, position moves forward
                                                    ms.Seek(pos, SeekOrigin.Begin) |> ignore
                                                    ms
                                            let stateFunc() = 
                                                if bCommonStatePerNode || Utils.IsNull state then 
                                                    state
                                                else
                                                    let cnt = Interlocked.Increment( refCount )
                                                    if cnt = 0 then 
                                                        state 
                                                    else
                                                        use msRead = replicateMsStreamFunc()
                                                        // msRead will not be null, as the null condition is checked above
                                                        let stateTypeName = msRead.ReadString() 
                                                        let s = msRead.CustomizableDeserializeToTypeName( stateTypeName )
                                                        s
                                        
                                            ta := ThreadTracking.StartThreadForFunction ( fun _ -> sprintf "Job Thread, DSet Fold %s" dsetName) ( fun _ -> x.DSetFoldAsSeparateApp( queue, endPoint, useDSet, usePartitions, foldFunc, aggregateFunc, serializeFunc, stateFunc, msRep ) )
                                        | _ ->
                                            ()

                            | _ ->
                                Logger.LogF( jobID, LogLevel.Info, (fun _ -> sprintf "Unexpected type %A" paramType))
                                ()
                        with
                        | ex ->
                            jobAction.EncounterExceptionAtContainer( ex, sprintf "___ ParseQueueCommandAtContainer: Hybrid parse loop, command %A ___" command)
                    else
                        match (command.Verb, command.Noun ) with
                        | ( ControllerVerb.Stop, ControllerNoun.Service ) ->
                                    // Stop service doesn't need attached job holder.
                                    let endPoint = ms.ReadIPEndPoint()
                                    let paramV = ms.ReadVInt32()
                                    let dsetName = ms.ReadString()
                                    let dsetVersion = ms.ReadInt64()
                                    let serviceName = ms.ReadString() 
                                    Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "attempt to stop service %s ..." serviceName ))
                                    let bSuccessToStop = ServiceCollection.Current.StopService( serviceName )
                                    use msInfo = new MemStream( 1024 )
                                    msInfo.WriteString( dsetName )
                                    msInfo.WriteInt64( dsetVersion ) 
                                    msInfo.WriteBoolean( bSuccessToStop )
                                    queue.ToForward( endPoint, ControllerCommand( ControllerVerb.ConfirmStop, ControllerNoun.Service), msInfo )
                        | _ -> 
                            let msg = sprintf "Error@AppDomain: Can't find job %s:%s in %A, %A " name (VersionToString(DateTime(verNumber))) (command.Verb) (command.Noun) 
                            jobAction.ThrowExceptionAtContainer( msg )
            )
        | ( ControllerVerb.Cancel, ControllerNoun.Job )
        | ( ControllerVerb.Close, ControllerNoun.Job ) ->
            let jobID = ms.ReadGuid()
            using ( SingleJobActionContainer.TryFind( jobID )) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Logger.LogF( jobID, LogLevel.Info, fun _ -> sprintf "[may be OK, job cancelled?] Job %A, Rcvd Network Command %A from endpoint %s of %dB payload, but failed to find corresponding job"
                                                                           jobID
                                                                           command
                                                                           (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                           ms.Length )
                else
                    try 
                        jobAction.CancelJob() 
                        /// Cancel Job should automatically remove all jobs in tasks. 
                        let remainingJobs = allTasks.Count
                        // The trace below still trigger a Null reference exception, can't figure out why (remainingJobs is a value)..
                        // Logger.LogF( LogLevel.MildVerbose,  fun _ -> sprintf "Close, Job from loopback interface, remaining jobs ... %d " remainingJobs  )
                        // Logger.LogF( LogLevel.MildVerbose,  fun _ -> sprintf "Close, Job from loopback interface received ... "  )
                        if remainingJobs = 0 then 
                            if allConnections.IsEmpty && ServiceCollection.Current.IsEmpty then 
                                bTerminateJob := true
                    with 
                    | ex -> 
                        jobAction.EncounterExceptionAtCallback( ex, "___ ParseQueueCommandAtContainer: Close, Job ___" )    
            )
        | ( ControllerVerb.Delete, ControllerNoun.Program ) ->
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Delete, Program received, all jobs & service will be terminated ..." ))
            for jobLifeCycle in JobLifeCycleCollectionContainer.GetAllJobs() do
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "try to cancel job %A ..." jobLifeCycle.JobID ))
                jobLifeCycle.CancelJob()  
                                
            for serviceName in ServiceCollection.Current.AllServiceNames() do 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "try to stop service %s ..." serviceName ))
                ServiceCollection.Current.StopService( serviceName ) |> ignore 
            bTerminateJob := true
        | ( ControllerVerb.Acknowledge, _ ) ->
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Ack@AppDomain: %A, %A" command.Verb command.Noun ))
            ()
        | ( ControllerVerb.ConfirmStart, ControllerNoun.Program ) -> 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Link, Program acked with : %A, %A" command.Verb command.Noun ))
            ()
        | _ ->
            // Unparsed message is fine, it may be picked up by other message parsing pipeline. 
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Unparsed Message@AppDomain: %A, %A" command.Verb command.Noun ))
            ()


    static member val DefaultJobListener = null with get, set

    /// Check whether the host client is alive 
    static member CheckClientLiveness (clientProcessId, clientModuleName, clientStartTimeTicks : int64) =
        let proc = 
            try 
                Process.GetProcessById(clientProcessId) |> Some
            with
                _ -> None
        match proc with 
        | None -> Logger.LogF( LogLevel.Info,  fun _ -> sprintf "Host client (PID = %i) is no longer alive" clientProcessId )
                  false
        | Some p ->
            // has the same PID, same main module name, same StartTime, and has not yet exited, so the client process is still alive.
            // Note: Mono has a bug on Process.StartTime https://bugzilla.xamarin.com/show_bug.cgi?id=26363, hopefully the fix will reach release soon
            //       Before the fix, the check below is not fully reliable (since PID can be reclycled)
            let alive = p.MainModule.ModuleName = clientModuleName && p.StartTime.Ticks = clientStartTimeTicks && not p.HasExited
            if not alive then
                 Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Host client (PID = %i) is no longer alive: Main Module Name  = %s (expected %s), Start Time Ticks = %i (expected %i), HasExited = %b" 
                                                                 clientProcessId p.MainModule.ModuleName clientModuleName p.StartTime.Ticks clientStartTimeTicks p.HasExited))
            alive

    static member WaitForCompletition (jobs : WaitHandle[], clientProcessId : int option, clientModuleName : string option, clientStartTimeTicks : int64 option) =
        if jobs.Length > 0 then 
            let asAppDomain = Option.isNone clientProcessId || Process.GetCurrentProcess().Id = clientProcessId.Value

            // Wait for jobs to complete or the host client has terminated.
            if asAppDomain || not DeploymentSettings.RunningOnMono then
                // If the container started as an appdomain within the process of the client, there's no need to check the
                // liveness of the client. When host client terminates, the container terminates with it.
                // If the container started as a seperate process, on Windows, the container was started as a Windows Job 
                // with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE set. When the host client (parent) closes, the containers will be closed automatically by OS
                WaitHandle.WaitAll(jobs, DeploymentSettings.ContainerMaxWaitForJobToFinish) |> ignore
            else
                // If the container started as a sperate process and is running on Mono, check whether the client is alive at a fixed timer interval
//                let mutable clientAlive = true 
//                while (not (WaitHandle.WaitAll(jobs, DeploymentSettings.ContainerCheckClientLivenessInterval))) && clientAlive do
//                     clientAlive <- Task.CheckClientLiveness(clientProcessId.Value, clientModuleName.Value, clientStartTimeTicks.Value)
                WaitHandle.WaitAll(jobs, DeploymentSettings.ContainerMaxWaitForJobToFinish) |> ignore

    /// Entry point for task as a separate appdomain or exe 
    /// sigName: Signature Name of the Executable Module
    /// sigVersion: Version information of the executable module
    /// port: loopback port
    /// lport: listening port
    /// clientProcessId: the host client's process Id
    /// clientModuleName: the host client's module name
    /// clientStartTimeTicks: the ticks of the host client's start time
    static member StartTaskAsSeperateApp( sigName:string, sigVersion:int64, ip : string, port, jobip, jobport, authParams, clientProcessId, clientModuleName, clientStartTimeTicks) = 
        let (bRequireAuth, guid, rsaParam, pwd) = authParams
        // Start a client. 
        // Only in App Domain that we can load assembly, and deserialize function object. 
        DeploymentSettings.LoadCustomAssebly <- true
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Task %s:%s started" sigName (sigVersion.ToString("X")) ))
        let queue = 
            if (ip.Equals("", StringComparison.Ordinal)) then
                Cluster.Connects.AddLoopbackConnect(port, bRequireAuth, guid, rsaParam, pwd)
            else
                let qAdd = Cluster.Connects.AddConnect(ip, port)
                qAdd.AddLoopbackProps(bRequireAuth, guid, rsaParam, pwd)
                qAdd
        /// Allow exporting to daemon 
        ContractServerQueues.Default.AddQueue( queue )
        use listener = 
            if jobport > 0 then 
                JobListener.InitializeListenningPort( jobip, jobport )
            else 
                null
        if Utils.IsNotNull listener then 
            listener.OnAccepted( Action<_>(listener.OnIncomingQueue), fun _ -> "Accepted queue"  )
        
        if (Utils.IsNull listener) && jobport > 0 then 
            use msSend = new MemStream( 1024 )
            msSend.WriteString( sigName )
            msSend.WriteInt64( sigVersion ) 
            queue.ToSend( ControllerCommand( ControllerVerb.Stop, ControllerNoun.Program ), msSend ) 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "task %s:%s, fail to secure job port %d" sigName (sigVersion.ToString("X")) jobport ))
            // !!! Warning !!!The trace will work under Appdomain log when some sleep is done before unload the appdomain. 
            queue.Close()
            // Cancel all pending jobs. 
            CleanUp.Current.CleanUpAll()
        else
            Task.DefaultJobListener <- listener
            let allTasks = ConcurrentDictionary<Guid,_>()
            let allConnections = ConcurrentDictionary<_,_>()
            let bConnectedBefore = ref false
            let showConnections (connections: ConcurrentDictionary<int64,_>) = 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Active connection: %d, %s" connections.Count 
                                                               ( connections.Keys |> Seq.map( fun addr -> LocalDNS.Int64ToIPEndPoint( addr ) ) 
                                                                                    |> Seq.map( fun ep -> LocalDNS.GetShowInfo( ep ) ) 
                                                                                    |> String.concat "," ) ))
            let bTerminateJob = ref false
            let task : Task ref = ref null
            let procParseQueueTask = (
                fun (cmd : NetworkCommand) -> 
                    Task.ParseQueueCommandAtContainer task queue allConnections allTasks showConnections bConnectedBefore bTerminateJob cmd.cmd (cmd.ms)
                    ContractStoreAtProgram.Current.ParseContractCommand queue.RemoteEndPointSignature cmd.cmd (cmd.ms)
                    null
            )
            queue.GetOrAddRecvProc("ParseQueue", procParseQueueTask) |> ignore
            ContractStoreAtProgram.RegisterNetworkParser( queue )
            queue.Initialize()
            // Wait for connection to PrajnaClient to establish
            let maxWait = (PerfDateTime.UtcNow()).AddSeconds( 5. )
            while not queue.CanSend && (PerfDateTime.UtcNow()) < maxWait do
                Threading.Thread.Sleep( 1 )
            if not queue.CanSend then 
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Fail to loopback connect to local PrajnaClient, Job terminates" ))
            else
                try 
                    // The decoding logic isn't as well protected as the main link, this is because the module communicates with PrajnaClient, 
                    // We expect most of the errors to be weeded out by the PrajnaClient.
                    // Moreover, if there is error, the job simply dies, which is considered OK. 
                    use msSend = new MemStream( 1024 )
                    msSend.WriteString( sigName )
                    msSend.WriteInt64( sigVersion )
                    queue.ToSend( ControllerCommand( ControllerVerb.Link, ControllerNoun.Program ), msSend ) 
                    // This becomes the main loop for the job 
                    let mutable bIOActivity = false
                    let mutable lastActive = (PerfDateTime.UtcNow())
                    while not !bTerminateJob do
                        if not queue.Shutdown then 
                            if !bConnectedBefore then 
                                if allConnections.IsEmpty && ServiceCollection.Current.IsEmpty then 
                                    bTerminateJob := true
                        else
                            if not !bTerminateJob then 
                                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "!!! Abnormal Close !!! Main loopback thread is shutdown as daemon is terminated (loopback queue shutdown)"))
                            if allConnections.IsEmpty && ServiceCollection.Current.IsEmpty then 
                                bTerminateJob := true
                            else
                                bTerminateJob := true

                        if not !bTerminateJob then 
                            Thread.Sleep(10)
                            ThreadTracking.CheckActiveThreads()
                with 
                | e ->
                    Logger.Log( LogLevel.Error, (sprintf "Uncatched exception, Job failed with exception %A" e ))

            if Utils.IsNotNull listener then 
                listener.TerminateListenningTask()
                Task.DefaultJobListener <- null

            // wait for tasks to finish
            if Utils.IsNotNull task then
                let jobs = (!task).JobFinished.ToArray()
                Task.WaitForCompletition(jobs, clientProcessId, clientModuleName, clientStartTimeTicks)

            // Stop the Program and sever connection. 
            use msSend = new MemStream( 1024 )
            msSend.WriteString( sigName )
            msSend.WriteInt64( sigVersion ) 
            queue.ToSend( ControllerCommand( ControllerVerb.Stop, ControllerNoun.Program ), msSend ) 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "task %s:%s, stop program send to attached host" sigName (sigVersion.ToString("X")) ))
            // !!! Warning !!!The trace will work under Appdomain log when some sleep is done before unload the appdomain. 
            queue.Close()
            // Cancel all pending jobs. 
            CleanUp.Current.CleanUpAll()
            // The follows are done by Cluster.Connects.Close()
    //        ThreadPoolWait.TerminateAll()
    //        ThreadTracking.CloseAllActiveThreads DeploymentSettings.ThreadJoinTimeOut
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "task %s:%s ended" sigName (sigVersion.ToString("X")) ))
                  
    static member CreateReadOneDSetTask( dsetPeer, queue, parts:seq<int> ) = 
        let task = new Task( DSets = [| dsetPeer |], 
                                Partitions = List<int>(parts), 
                                TypeOf = JobTaskKind.StartReadOne, 
                                State = TaskState.ReadyToExecute, 
                                Name = dsetPeer.Name, 
                                Version = dsetPeer.Version
                                )
        task.GetIncomingQueueNumber( queue ) |> ignore
        task
    static member Start( o ) =
//        let x = o :?> Task
        o.ExecuteOneLightJob()
    /// Error: error in parsing 
    static member Error ( msg ) = 
        Logger.Log( LogLevel.Error, msg )
        use msgError = new MemStream( 1024 )
        msgError.WriteString( msg )
        ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msgError )
    /// Error: error in parsing 
    static member FError ( fmsg ) = 
        Logger.Log( LogLevel.Error, (fmsg()))
        use msgError = new MemStream( 1024 )
        msgError.WriteString( fmsg() )
        ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msgError )
    member x.Error ( msg ) = 
        Logger.Log( LogLevel.Error, msg )
        let hostQueue = x.PrimaryHostQueue
        if Utils.IsNotNull hostQueue && hostQueue.CanSend then 
            use msgError = new MemStream( 1024 )
            msgError.WriteString( msg )
            hostQueue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msgError )

    member x.ExecuteOneLightJob() = 
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Executing task %A" x.TypeOf ))
        try 
            let typeOf = x.TypeOf &&& JobTaskKind.JobActionMask
            match typeOf with
            | JobTaskKind.ReadOne ->
                let curDSet = x.DSets.[0]
                let hostQueue = x.PrimaryHostQueue
                curDSet.ResetForRead( hostQueue :> NetworkCommandQueue )
                let bAllCancelled = ref false
                let tasks = List<_>()
                use cts = new CancellationTokenSource()
                let jbInfo = JobInformation( x.JobID, true, curDSet.Name, curDSet.Version.Ticks )
                for parti in x.Partitions do
                    tasks.Add( curDSet.AsyncReadChunk jbInfo parti
                                ( fun (meta, ms:MemStream ) ->
                                    if Utils.IsNotNull ms then 
                                        Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "Sending %s over the wire for read" (MetaFunction.MetaString(meta)) ))
                                        use msWire = new MemStream( int ms.Length + 1024 )
                                        msWire.WriteString( curDSet.Name )
                                        msWire.WriteInt64( curDSet.Version.Ticks )
                                        msWire.WriteVInt32( meta.Partition)
                                        msWire.WriteInt64( meta.Serial )
                                        msWire.WriteVInt32( meta.NumElems )
                                        //msWire.Write( ms.GetBuffer(), int ms.Position, int ms.Length - int ms.Position )
                                        msWire.Append(ms, ms.Position, ms.Length-ms.Position)
                                        let cmd = ControllerCommand( ControllerVerb.Write, ControllerNoun.DSet )
                                        let bSendout = ref false
                                        while Utils.IsNotNull hostQueue && not hostQueue.Shutdown && not !bSendout do
                                            //if hostQueue.CanSend && hostQueue.SendQueueLength<5 && hostQueue.UnProcessedCmdInBytes < int64 curDSet.SendingQueueLimit then 
                                            if hostQueue.CanSend then
                                                hostQueue.ToSend( cmd, msWire )
                                                bSendout := true
                                            else
                                                // Flow control kick in
                                                Threading.Thread.Sleep(5)                                                                           
                                        if Utils.IsNull hostQueue || hostQueue.Shutdown then 
                                            // Attempt to cancel jobs 
                                            if not !bAllCancelled then 
                                                lock ( bAllCancelled ) ( fun _ ->
                                                    if not !bAllCancelled then 
                                                        bAllCancelled := true
                                                        cts.Cancel() 
                                                )
                                    else
                                        // Do nothing, we will wait for all jobs to complete in Async.RunSynchronously
                                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Reaching end of part %d with %d mistakes" meta.Partition meta.NumElems ))
                                        if Utils.IsNotNull hostQueue && not hostQueue.Shutdown then 
                                            use msWire = new MemStream( 1024 )
                                            msWire.WriteString( curDSet.Name ) 
                                            msWire.WriteInt64( curDSet.Version.Ticks )
                                            msWire.WriteVInt32( meta.Partition )
                                            // # of error in this partition. 
                                            msWire.WriteVInt32( meta.NumElems )
                                            hostQueue.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.Partition ), msWire )
                                )
                            )
                        
                let res = tasks |> Async.Parallel
                try 
                    Async.RunSynchronously( res, cancellationToken = cts.Token ) |> ignore
                with 
                | e ->
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Executing JobTaskKind.ReadOne during Async.RunSynchronously encounter exception %A (this may due to cancellation of task)" e ))
                curDSet.CloseAllStreams( true )
                use msWire = new MemStream( 1024 )
                msWire.WriteString( curDSet.Name )
                msWire.WriteInt64( curDSet.Version.Ticks )
                if Utils.IsNotNull hostQueue && not hostQueue.Shutdown then 
                    hostQueue.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.DSet ), msWire )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Close, DSet sent for %s:%s" curDSet.Name curDSet.VersionString ))
                /// Read DSet, and send the result to the Queue.[0]
                // let curPartitions = 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Complete executing task %A" x.TypeOf ))
                ()        
            | _ ->
                x.Error (sprintf "Unknown Task Type %A" x.TypeOf )
        with
        | e ->
            x.Error ( sprintf "Executing task %A encounter unexpected exception %A" x.TypeOf e )
        // Read partition i of a certain DSet
    // We put the function here to allow connection related jobs to be scheduled for execution. 
    static member ReadDSet( curDSet:DSetPeer, queue:NetworkCommandQueuePeer, partitions ) = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read, DSet %s:%s from %s partitions %A" 
                                                               curDSet.Name curDSet.VersionString
                                                               (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) partitions ))
        let bFindArray = curDSet.PartitionExist( partitions )
        let bAllFind = bFindArray |> Array.fold( fun res v -> res && v ) true

        let msInfo = new MemStream( 1024 )
        let cmd, usePartitions = 
            if bAllFind then 
                msInfo.WriteString( curDSet.Name ) 
                msInfo.WriteInt64( curDSet.Version.Ticks )            
                ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.DSet ), partitions
            else
                msInfo.WriteString( curDSet.Name ) 
                msInfo.WriteInt64( curDSet.Version.Ticks ) 
                msInfo.WriteByte( byte (DSetErrorType.NonExistPartition ) )
                let notFindPartitions = 
                    bFindArray |> Seq.mapi ( fun idx bFind -> idx, bFind ) |> Seq.filter( fun (idx, bFind) -> not bFind ) |> Seq.map( fun (idx, bFind) -> partitions.[idx] ) |> Seq.toArray
                let FindPartitions = 
                    bFindArray |> Seq.mapi ( fun idx bFind -> idx, bFind ) |> Seq.filter( fun (idx, bFind) -> bFind ) |> Seq.map( fun (idx, bFind) -> partitions.[idx] ) |> Seq.toArray
                msInfo.WriteVInt32( notFindPartitions.Length ) 
                for i = 0 to notFindPartitions.Length-1 do
                    msInfo.WriteVInt32( notFindPartitions.[i] ) 
                ControllerCommand( ControllerVerb.Info, ControllerNoun.DSet ), FindPartitions

        if Utils.IsNotNull usePartitions && usePartitions.Length>0 then 
            let newDSet = DSetPeer( curDSet.Cluster )
            newDSet.CopyMetaData( curDSet, DSetMetadataCopyFlag.Copy ) 
            // Use a new DSet, allow multiple read. 
            let task = Task.CreateReadOneDSetTask( newDSet, queue, usePartitions )
            ( cmd, msInfo, task )  
        else
            ( cmd, msInfo, null ) 

    /// Initialize the avaliablity vector
    member x.ClientAvailability(epSignature:int64, registerFuncOpt, availFuncOpt ) = 
        // Initialize current peer availability vector 
        if Utils.IsNull x.AvailThis then 
            x.AvailThis <- BlobAvailability( x.NumBlobs )
        if not x.AvailThis.AllAvailable then 
            // Examine availability 
            let mutable bAllAvailable = true
            for blobi = 0 to x.NumBlobs - 1 do 
                let avStatus = Microsoft.FSharp.Core.LanguagePrimitives.EnumOfValue<_,_>(x.AvailThis.AvailVector.[blobi])
                match  avStatus with 
                | BlobStatus.AllAvailable ->
                    // Decoded 
                    ()
                | BlobStatus.Error ->
                    // Error in blob decoding or retrieval. 
                    bAllAvailable <- false
                    ()
                | BlobStatus.NoInformation ->
                    /// beginning. 
                    let blob = x.Blobs.[blobi]
                    let stream = 
                        match blob.TypeOf with 
                        | BlobKind.ClusterWithInJobInfo -> 
                            // Only Blob not using hash
                            null 
                        | _ -> 
                            match registerFuncOpt with 
                            | Some registerFunc -> 
                                if (Utils.IsNull blob.Stream) then
                                    BlobFactory.register(x.JobID, blob.Hash, registerFunc blobi blob, epSignature)
                                blob.Stream
                            | None -> 
                                null
                    let bExist = not (Utils.IsNull stream)
                    match blob.TypeOf with 
                    | BlobKind.ClusterMetaData ->
                        let cluster = ClusterFactory.FindCluster( blob.Name, blob.Version )
                        if Utils.IsNotNull cluster then 
                            // Cluster exist
//                            blob.Object <- cluster
//                            x.Clusters.[ blob.Index ] <- cluster
                            x.AddCluster( cluster, blob )
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        elif bExist then 
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        else
                            // Cluster do not exist
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable
                            bAllAvailable <- false
                    | BlobKind.DstDSetMetaData
                    | BlobKind.PassthroughDSetMetaData ->
                        if blob.IsAllocated then 
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        else
                            if bExist then 
                                let buf, pos, count = stream.GetBufferPosLength()
                                blob.Stream <- stream.GetNew()
                                blob.Stream.Info <- "BlobKind.PassthroughDSetMetaData"
                                blob.Stream.AppendNoCopy(buf, 0L, buf.Length)
                                blob.Stream.Seek( int64 pos, SeekOrigin.Begin ) |> ignore
                                match availFuncOpt with 
                                | Some availFunc -> 
                                    availFunc blobi blob ( blob.Stream, epSignature )
                                | None ->
                                    ()    
                                x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            else
                                x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable                            
                                bAllAvailable <- false
                    | BlobKind.SrcDSetMetaData ->
                        // Source should be available from peer
                        let dset = DSetPeer.GetDSet( x.JobID, blob.Name, blob.Version, 0L )
                        if Utils.IsNotNull dset then 
                            blob.Object <- dset
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            x.SrcDSet.[blob.Index] <- dset :> DSet
                        elif bExist then 
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        else
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable                            
                            bAllAvailable <- false
                    | BlobKind.AssemblyManagedDLL 
                    | BlobKind.AssemblyUnmanagedDir ->
                        let assemTypeOf = enum<_>( int( blob.TypeOf - BlobKind.AssemblyManagedDLL ) + int AssemblyKind.ManagedDLL )
                        let assem = AssemblyEx.GetAssembly( blob.Name, blob.Hash, assemTypeOf, false )
                        if Utils.IsNotNull assem then 
                            blob.Object <- assem
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            x.Assemblies.[blob.Index] <- assem
                        elif bExist then 
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        else
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable
                    | BlobKind.DStream ->
                        if blob.IsAllocated then 
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        else
                            if bExist then
                                let buf, pos, count = stream.GetBufferPosLength()
                                blob.Stream <- stream.Replicate(int64 pos, int64 count)
                                blob.Stream.Info <- "BlobKind.DStream"
                                blob.Stream.Seek( int64 pos, SeekOrigin.Begin ) |> ignore
                                x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            else
                                x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable                            
                                bAllAvailable <- false
                    | BlobKind.ClusterWithInJobInfo -> 
                        let clJobInfo = ClusterJobInfoFactory.ResolveClusterJobInfo( x.SignatureName, x.SignatureVersion, blob.Name, blob.Version ) 
                        if Utils.IsNotNull clJobInfo then 
                            blob.Object <- clJobInfo
                            x.ClustersInfo.[ blob.Index ] <- clJobInfo
                    | BlobKind.JobDependencyFile ->
                        let jobDep = JobDependency.GetDependency(blob.Name, blob.Hash, false)
                        if Utils.IsNotNull jobDep then
                            blob.Object <- jobDep
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            x.JobDependencies.[blob.Index] <- jobDep
                        elif bExist then 
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        else
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable
                    | _ ->
                        ()
                | _ ->
                    ()
                // For bAllSrcAvailable=false, CalculateDependantDSet will be called at TrySyncMetaData
            // JinL: 05/12/2014, with this, it is possible to clearup some of the logic up to set availability vector. 
            for blobi = 0 to x.NumBlobs - 1 do 
                let blob = x.Blobs.[blobi]
                if Utils.IsNotNull blob.Object || blob.IsAllocated then 
                    x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable 
                else
                    x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable
                    bAllAvailable <- false
            x.AvailThis.AllAvailable <-  bAllAvailable 
    member x.PeekBlobDSet blobi (blob:Blob) (ms:StreamBase<byte>, epSignature) = 
        let pos = ms.Position
        let name, verNumber = DSet.Peek( ms ) 
        blob.Name <- name
        blob.Version <- verNumber
        ms.Seek( pos, SeekOrigin.Begin ) |> ignore
    member x.ReceiveBlob blobi (blob:Blob) (ms:StreamBase<byte>, epSignature) = 
        if not (blob.IsAllocated) then 
            let buf, pos, count = ms.GetBufferPosLength()
            blob.Stream <- ms.GetNew()
            blob.Stream.Info <- "ReceiveBlob"
            blob.Stream.AppendNoCopy(buf, 0L, buf.Length)
            blob.Stream.Seek( int64 pos, SeekOrigin.Begin ) |> ignore
            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
            x.AvailThis.CheckAllAvailable()
            let bSuccess = x.ClientReceiveBlob( blobi, false ) 
            if not bSuccess then 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Fail to decode Blob %d from peer %s" blobi (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)) ) ))
            let queue = Cluster.Connects.LookforConnectBySignature( epSignature )
            if Utils.IsNotNull queue && queue.CanSend then 
                use msFeedback = new MemStream( 1024 )
                msFeedback.WriteGuid( x.JobID ) 
                msFeedback.WriteString( x.Name ) 
                msFeedback.WriteInt64( x.Version.Ticks )
                msFeedback.WriteVInt32( blobi ) 
                msFeedback.WriteBoolean( bSuccess ) 
                Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Acknowledge the receipt of Blob %A %d from peer %s, this %A" 
                                                                           blob.TypeOf blobi (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)) ) 
                                                                           x.AvailThis
                                                                           ))
                queue.ToSend( ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.Blob ), msFeedback )
        else
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Receive Blob %A %d from peer %s, but blob has already been allocated" blob.TypeOf blobi (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)) ) ))

    member x.ReceiveBlobNoFeedback blobi blob (ms, epSignature) = 
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Rcvd Write, Blob %d from peer %s" blobi (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)) ) ))
        if not (blob.IsAllocated) then 
            let buf, pos, count = ms.GetBufferPosLength()
            blob.Stream <- buf.Replicate(int64 pos, int64 count)
            blob.Stream.Info <- "ReceiveBlobNoFeedback"
            blob.Stream.Seek( int64 pos, SeekOrigin.Begin ) |> ignore
            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
            x.AvailThis.CheckAllAvailable()
            let bSuccess = x.ClientReceiveBlob( blobi, false ) 
            if not bSuccess then 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Fail to decode Blob %d from peer %s" blobi (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(epSignature)) ) ))

    /// Load all Assemblies
    member x.LoadAllAssemblies() = 
        if x.IsContainer then 
            let mutable bSuccess = true
            for blobi=0 to x.NumBlobs-1 do 
                let blob = x.Blobs.[blobi]
                match blob.TypeOf with 
                | BlobKind.AssemblyManagedDLL 
                | BlobKind.AssemblyUnmanagedDir ->
                    // Load assemblies
                    let assemTypeOf = enum<_>( int( blob.TypeOf - BlobKind.AssemblyManagedDLL ) + int AssemblyKind.ManagedDLL )
                    let assem = AssemblyEx.GetAssembly( blob.Name, blob.Hash, assemTypeOf, false )
                    if Utils.IsNotNull assem then 
                        blob.Object <- assem
                        x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        x.Assemblies.[blob.Index] <- assem
                        assem.Load()
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Examine assembly at %s, loaded as %s" assem.Location assem.FullName ) )
                    else
                        bSuccess <- false
                        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Failed to load assembly %s:%s" blob.Name (blob.Version.ToString("X")) ))
                | _ -> 
                    ()
            bSuccess
        else
            true
    /// Load all job depencies            
    member x.LoadAllJobDependencies() = 
        if x.IsContainer then 
            let mutable bSuccess = true
            for blobi=0 to x.NumBlobs-1 do 
                let blob = x.Blobs.[blobi]
                match blob.TypeOf with 
                | BlobKind.JobDependencyFile ->
                        // Load dependencies
                        let jobDep = JobDependency.GetDependency(blob.Name, blob.Hash, false)
                        if (Utils.IsNotNull jobDep) then
                            try
                                blob.Object <- jobDep
                                x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                                x.JobDependencies.[blob.Index] <- jobDep
                                jobDep.LoadJobDirectory(x.RemoteMappingDirectory)
                            with 
                            | e -> 
                                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "(May actualbe be OK) Exception when try to handle dependency file %s:%s, %A" blob.Name (blob.Version.ToString("X")) e )                                )
                        else
                            bSuccess <- false
                            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Failed to find dependency file %s:%s" blob.Name (blob.Version.ToString("X"))))
                | _ -> 
                    ()
            bSuccess
        else
            true
    /// Link all assemblies
    member x.LinkAllAssemblies( assemblyDirectory:string ) = 
            DirectoryInfoCreateIfNotExists (assemblyDirectory) |> ignore
            for blobi=0 to x.NumBlobs-1 do 
                let blob = x.Blobs.[blobi]
                match blob.TypeOf with 
                | BlobKind.AssemblyManagedDLL 
                | BlobKind.AssemblyUnmanagedDir ->
                    // Load assemblies
                    let linkedFile = AssemblyEx.ConstructLocation( blob.Name, blob.Hash ) 
                    let assemblyFile = Path.Combine( assemblyDirectory, blob.Name + ".dll" ) 
                    let _, msg = LinkFile assemblyFile linkedFile
                    Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Link %s -> %s: %s" assemblyFile linkedFile msg) )
                | _ -> 
                    ()
            
              
        
    /// Load all Cluster, DSet, GV, Assemblies
    /// For DSet, DStream, ClusterJobInfo, they are only decoded in AppDomain/Exe, and is not available in the main loop.  
    /// This should only be called in AppDomain. 
    member x.LoadAll() = 
        if not bAllLoaded then 
            let dsetLists = Dictionary<_,_>()
            let mutable bSuccess = true
            bSuccess <- bSuccess && x.LoadAllAssemblies()
//            if not bSuccess then 
//                bSuccess <- x.LoadAllAssemblies()
//                if not bSuccess then 
//                    failwith "Fail to load assemblies"    

            bSuccess <- bSuccess && x.LoadAllJobDependencies()
//            if not bSuccess then 
//                bSuccess <- x.LoadAllJobDependencies()
//                if not bSuccess then 
//                    failwith "Fail to load dependency file"    

            // Load Cluster, DSet, GV, etc..            
            for blobi=0 to x.NumBlobs-1 do 
                let blob = x.Blobs.[blobi]
                match blob.TypeOf with
                | BlobKind.ClusterMetaData ->
                    if Utils.IsNull blob.Object then 
                        // Decode cluster
                        let cluster = ClusterFactory.FindCluster( blob.Name, blob.Version )
                        if Utils.IsNotNull cluster then 
                            x.AddCluster( cluster, blob )
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        else
                            let bSuccessDecodeCluster = x.DecodeFromBlob( blobi, -1 ) 
                            if not bSuccessDecodeCluster then 
                                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Failed to load cluster %s:%s" blob.Name (StringTools.VersionToString(DateTime(blob.Version))) )    )
                            bSuccess <- bSuccess && bSuccessDecodeCluster
                | BlobKind.SrcDSetMetaData 
                | BlobKind.DstDSetMetaData
                | BlobKind.PassthroughDSetMetaData -> 
                    if Utils.IsNull blob.Object then 
//                        let dset = DSetPeerFactory.ResolveAnyDSet( blob.Name, blob.Version )
//                        let fullname = blob.Name + VersionToString( DateTime( blob.Version) )
//                        let dsetOpt = DSetPeerFactory.Retrieve( fullname ) 
//                        let dset = 
//                            match dsetOpt with 
//                            | Some ( curDSet ) ->
//                                curDSet
//                            | None ->
//                                    null
//                        if Utils.IsNotNull dset then 
//                            blob.Object <- dset
//                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
//                        else
                            let bDecodeSuccess = x.DecodeFromBlob( blobi, -1 )                           
                            if not bDecodeSuccess then 
                                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Failed to load DSet %s:%s" blob.Name (StringTools.VersionToString(DateTime(blob.Version))) ) )
                            else
//                                let fullname = blob.Name + VersionToString( DateTime( blob.Version) )
//                                DSetFactory.Store( fullname, blob.Object :?> DSet )
                                // JinL, 03/03/2015, deal with In memory object later. 
                                ()
                            bSuccess <- bSuccess && bDecodeSuccess

                    if Utils.IsNotNull blob.Object then 
                        let dset0 = blob.Object :?> DSet
                        match blob.TypeOf with
                        | BlobKind.SrcDSetMetaData -> 
                            // Source DSet will only bind against name + version. 
                            dset0.HashNameVersion() 
                            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Source DSet %s:%s, Hash = %s" dset0.Name dset0.VersionString (BytesToHex(dset0.Hash)) ))
                        | _ -> 
                            ()
                        let dset1 = 
                            if dset0.IsCached then 
                                /// We always use DSet Hash (only upstream dependency for cachable object. 
                                let dset2 = DSetFactory.CacheUseOld( dset0.Hash, dset0 )
                                if not (Object.ReferenceEquals( dset2, dset0 )) then 
                                    // Using a cached DSet
                                    dset2.Dependency <- dset0.Dependency
                                    dset2.DependencyDownstream <- dset0.DependencyDownstream
                                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Reset dependency for DSet %s:%s" dset2.Name dset2.VersionString ))
                                dset2
                            else
                                dset0
                        let dset = x.DSetReferences.GetOrAdd( dset1.Hash, dset1 ) 
                        blob.Object <- dset
                        dsetLists.Item( dset.Name ) <- dset
                        
                        match blob.TypeOf with
                        | BlobKind.SrcDSetMetaData ->
                            x.SrcDSet.[blob.Index] <- dset
                        | BlobKind.DstDSetMetaData ->
                            x.DstDSet.[blob.Index] <- dset
                        | BlobKind.PassthroughDSetMetaData ->
                            x.PassthroughDSet.[blob.Index] <- dset
                        | _ ->
                            failwith ( sprintf "Task.LoadAll, logic error, doesn't have a DSet type %A" blob.TypeOf )
                | BlobKind.DStream ->
                    if Utils.IsNull blob.Object then 
                            let bDecodeSuccess = x.DecodeFromBlob( blobi, -1 )
                            if not bDecodeSuccess then 
                                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Failed to load DStream %s:%s" blob.Name (StringTools.VersionToString(DateTime(blob.Version))) ) )
                            else
                                let dstream0 = x.DStreams.[blob.Index] 
                                dstream0.Blob <- blob // Set Hash
                                let dstream1 = DStreamFactory.CacheDStream( x.JobID, dstream0 )  
                                let dstream = x.DStreamReferences.GetOrAdd( dstream1.Hash, dstream1 ) 

                                if not (Object.ReferenceEquals( dstream, dstream0 )) then 
                                    blob.Object <- dstream
                                    x.DStreams.[blob.Index] <- dstream
                            bSuccess <- bSuccess && bDecodeSuccess
                | BlobKind.ClusterWithInJobInfo ->
                    if Utils.IsNull blob.Object then 
                        let clJobInfo = ClusterJobInfoFactory.ResolveClusterJobInfo( x.SignatureName, x.SignatureVersion, blob.Name, blob.Version ) 
                        if Utils.IsNotNull clJobInfo then 
                            blob.Object <- clJobInfo
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            x.ClustersInfo.[ blob.Index ] <- clJobInfo
                        else
                            let bDecodeSuccess = x.DecodeFromBlob( blobi, -1 ) 
                            if not bDecodeSuccess then 
                                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Failed to parse ClusterWithInJobInfo %s:%s" blob.Name (StringTools.VersionToString(DateTime(blob.Version))) ) )
                            bSuccess <- bSuccess && bDecodeSuccess
                | _ ->
                    ()
            if bSuccess then 
                // Parameter evaluation
                for param in x.ParameterList do 
                    if Utils.IsNull param.Object then 
                        param.Object <- x.Blobs.[ param.BlobIndex ].Object :?> DistributedObject
//                        match param.ParamType with 
//                        | FunctionParamType.DSet ->
//                            let fullname = param.Name + VersionToString( param.Version )
//                            let mutable dsetPeerOpt = DSetPeerFactory.Retrieve( fullname ) 
//                            match dsetPeerOpt with
//                            | Some (dsetPeer ) ->
//                                param.Object <- dsetPeer
//                            | None ->    
//                                let dsetOpt = DSetFactory.Retrieve( fullname ) 
//                                match dsetOpt with 
//                                | Some (dset ) ->
//                                    param.Object <- dset
//                                | None ->
//                                    if dsetLists.ContainsKey( param.Name ) then 
//                                        param.Object <- dsetLists.Item( param.Name )
//                        | _ ->
//                            failwith ( sprintf "Task.LoadAll, unknown parameter type for parameter %s:%s %x" param.Name (VersionToString(param.Version)) (int param.ParamType) )                    
                    if Utils.IsNull param.Object then 
                        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Failed to resolve Job %s Parameter" x.Name ))
                        // Failed to decode parameter
                        bSuccess <- false    
            bAllLoaded <- bSuccess
        bAllLoaded
    /// Unload all Cluster, DSet, GV, this is particularly important for static reference in DStreamFactory
    member x.UnloadAll() = 
        DStreamFactory.RemoveDStream( x.JobID )
        x.Blobs <- null
        x.DStreams.Clear()
        x.SrcDSet.Clear()
        x.DstDSet.Clear()
        x.PassthroughDSet.Clear()
        x.DStreamReferences.Clear()
        x.DSetReferences.Clear() 

    /// Sync metadata, in P2P fashion. 
    member x.TrySyncMetadataClient() = 
        ()
    /// Client: perform necessary processing of arrival of Blobi, e.g., 
    ///    for assembly, write the blob data to assembly file
    member x.ClientReceiveBlob( blobi, bDecodeAll ) = 
        let blob = x.Blobs.[blobi]
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Receive blob %s %d, type: %A, %s" (if bDecodeAll then "Decoding" else "NonDecoding") blobi blob.TypeOf blob.Name ))
        match blob.TypeOf with 
        | BlobKind.AssemblyManagedDLL 
        | BlobKind.AssemblyUnmanagedDir 
        | BlobKind.JobDependencyFile
        | BlobKind.ClusterMetaData ->
            let bDecode = x.DecodeFromBlob( blobi, -1 )
            bDecode
        | BlobKind.SrcDSetMetaData
        | BlobKind.DstDSetMetaData
        | BlobKind.PassthroughDSetMetaData
        | BlobKind.ClusterWithInJobInfo 
        | _ ->
            if bDecodeAll then 
                let bDecodeSuccessful = x.DecodeFromBlob( blobi, -1 )
                if bDecodeSuccessful then 
                    x.UpdateBlobInfo(blobi)
                    // Successful decoding means (Utils.IsNotNull blob.Object)
//                    let dset = blob.Object :?> DSet
//                    blob.Name <- dset.Name
//                    blob.Version <- dset.Version.Ticks
                bDecodeSuccessful
            else            // Don't process DSet, they will be passed to the job to process
                // Update the Name and version number information. 
//                let stream = blob.StreamForRead()
//                let name, verNumber = DSet.Peek( stream ) 
//                blob.Name <- name
//                blob.Version <- verNumber
                x.UpdateBlobInfo(blobi)
                true
    /// Process incoming command for the task queue. 
    /// true: Command parsed. 
    /// false: Command Not parsed
    member x.ParseTaskCommandAtDaemon( jobAction: SingleJobActionDaemon, queue:NetworkCommandQueuePeer, cmd:ControllerCommand, ms, taskQueue:TaskQueue ) = 
        match (cmd.Verb, cmd.Noun) with 
        | ControllerVerb.Unknown, _ -> 
            true
        | ControllerVerb.Availability, ControllerNoun.Blob ->
            /// Availability, Blob should be the first command sent by host/peers, as it will initialize proper structure, such as clusters, SrcDSet, etc..
            let peeri = x.GetIncomingQueueNumber( queue )
            // Decode availability information. 
            let availInfo = x.IncomingQueuesAvailability.[peeri]
            availInfo.Unpack( ms )
            let membershipList = x.IncomingQueuesClusterMembership.[peeri]
            if membershipList.Count<=0 then 
                // For host, each peer replies on Peer availability 
                use msInfo = new MemStream(1024)
                msInfo.WriteGuid( x.JobID )
                msInfo.WriteString( x.Name ) 
                msInfo.WriteInt64( x.Version.Ticks )
                x.AvailThis.Pack( msInfo ) 
                Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Availability, Blob for job %s:%s host %A this %A" 
                                                                                   x.Name x.VersionString
                                                                                   availInfo x.AvailThis ))
                queue.ToSend( ControllerCommand( ControllerVerb.Availability, ControllerNoun.Blob ), msInfo )
                // Send source DSet, Information. 
                x.TrySendSrcMetadataToHost(queue, availInfo)
            else
                // for peer
                x.TrySyncMetadataClient()
            true 
        /// Write Blob is being processed here. 
        | ControllerVerb.Write, ControllerNoun.Blob ->
            let blobi = ms.ReadVInt32()
            let pos = ms.Position
            //let buf = ms.GetBuffer()
//            let hash = HashByteArrayWithLength( buf, int pos, lenblob )
            Logger.LogF( jobAction.JobID, LogLevel.MediumVerbose, ( fun _ -> sprintf "Write, Blob for job %s:%s blob %d " 
                                                                                       (x.Blobs.[blobi].Name) x.VersionString
                                                                                       blobi ))
            let bCorrectFormatted = blobi>=0 && blobi < x.NumBlobs
            if bCorrectFormatted then 
                let blob = x.Blobs.[blobi]
                x.ReceiveBlob blobi blob (ms, queue.RemoteEndPointSignature )
                true
            else
                let errorMsg = sprintf "Receive a blob for Job %s:%s idx %d, but is out of the range of (0-%d) " 
                                        x.Name x.VersionString blobi x.NumBlobs
                jobAction.ThrowExceptionAtContainer( errorMsg )
                true
        | ControllerVerb.Acknowledge, ControllerNoun.Blob ->
            let blobi = ms.ReadVInt32()
            if blobi<0 || blobi >= x.NumBlobs then 
                Logger.LogF( LogLevel.Warning, fun _ ->  sprintf "Job %A, Error: Acknowledge, Blob with serial %d that is outside of range of valid serial number (0-%d)" jobAction.JobID blobi x.NumBlobs )
                true
            else
                let peeri = x.GetIncomingQueueNumber( queue )
                // Decode availability information. 
                let availInfo = x.IncomingQueuesAvailability.[peeri]
                availInfo.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                availInfo.CheckAllAvailable()
                true 
        | ControllerVerb.Start, ControllerNoun.Job ->
            if not x.ConfirmStart || x.State<>TaskState.InExecution then 
                Logger.LogF( jobAction.JobID, 
                             LogLevel.MildVerbose, ( fun _ ->  let t1 = (PerfADateTime.UtcNowTicks())
                                                               sprintf "Start, Job: launch  a new job %s:%s with mode %A, in %.2fms" 
                                                                       x.Name x.VersionString x.LaunchMode 
                                                                       ( float (t1-x.JobStartTicks) / float TimeSpan.TicksPerMillisecond )
                                                                       ))
                let bSuccess = 
                    match x.LaunchMode with 
                    | TaskLaunchMode.DonotLaunch -> 
                        taskQueue.ConnectTaskToTaskHolder( x, queue )
                    | _ -> 
                        let taskHolder = taskQueue.GetRelatedTaskHolder( x )
                        if Utils.IsNull taskHolder then 
                            // Execute the task if no related task holder is found
                            if x.IsContainer then 
                                taskQueue.ExecuteTask( x, queue )
                            else
                                false
                        else
                            // Otherwise, just connect the task to task holder, no extra execution needed. 
                            taskQueue.ConnectTaskByTaskHolder( taskHolder, x, queue )

                use msFeedback = new MemStream( 1024 )
                msFeedback.WriteGuid( jobAction.JobID )
                msFeedback.WriteString( x.Name )
                msFeedback.WriteInt64( x.Version.Ticks )
                msFeedback.WriteBoolean( bSuccess )
                queue.ToSend( ControllerCommand( ControllerVerb.Echo, ControllerNoun.Job ), msFeedback )
                true
            else
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Start, Job, try reuse an old new job %s:%s" x.Name x.VersionString))
                use msFeedback = new MemStream( 1024 )
                msFeedback.WriteGuid( jobAction.JobID )
                msFeedback.WriteString( x.Name )
                msFeedback.WriteInt64( x.Version.Ticks )
                msFeedback.WriteBoolean( true )
                queue.ToSend( ControllerCommand( ControllerVerb.ConfirmStart, ControllerNoun.Job ), msFeedback )
                true
        | ControllerVerb.ConfirmStart, ControllerNoun.Job ->
            // ConfirmStart, Job should only be called from a local callback interface. 
            // It will link the the job back to PrajnaClient
            let bSuccess = ms.ReadBoolean()
            // Job started at the AppDomain/Exe
            use msFeedback = new MemStream( 1024 )
            msFeedback.WriteGuid( x.JobID )
            msFeedback.WriteString( x.Name )
            msFeedback.WriteInt64( x.Version.Ticks )
            msFeedback.WriteBoolean( bSuccess )
            if Utils.IsNotNull x.PrimaryHostQueue && x.PrimaryHostQueue.CanSend then 
                x.PrimaryHostQueue.ToSend( ControllerCommand( ControllerVerb.ConfirmStart, ControllerNoun.Job ), msFeedback )
            // Confirm start of a job
            x.ConfirmStart <- true
            true
        | ControllerVerb.Close, ControllerNoun.Job ->
            x.TerminateTask() 
            taskQueue.RemoveTask( x ) 
            taskQueue.RemoveSeparateTask( x )
            taskQueue.DelinkQueueAndTask( x ) 
            jobAction.CancelJob()
            true
        | _ ->
            false    
    /// Try send missing srouce metadata (srcDSet) to host
    /// The information is light, so the sending command is executed synchronously. 
    member x.TrySendSrcMetadataToHost(queue, hostAvail) = 
        if not hostAvail.AllAvailable then 
            let thisAvailVector = x.AvailThis.AvailVector
            let hostAvailVector = hostAvail.AvailVector
            for blobi = 0 to x.NumBlobs - 1 do
                if thisAvailVector.[blobi]=byte BlobStatus.AllAvailable && 
                    hostAvailVector.[blobi]<>byte BlobStatus.AllAvailable then 
                    let blob = x.Blobs.[blobi] 
                    match blob.TypeOf with
                    | BlobKind.SrcDSetMetaData ->
                        // Src DSet doesn't have any dependency
                        x.SendBlobToHost( queue, blobi )
                        ()
                    | _ ->
                        // This logic is only executed for lightweight source metadata information. 
                        ()                            
            
        ()
    /// Do we need to kill the task?
    member x.ShouldTerminate() = 
        x.UpdatePrimaryHostQueue()            
        if x.TypeOf &&& JobTaskKind.PersistentMask <> JobTaskKind.None then 
            /// Persistent job is never killed
            false
        else
            x.State = TaskState.InExecution && Utils.IsNull x.PrimaryHostQueue
    /// Stop the job 
    member x.TerminateTask() =
        if not bTerminationCalled then 
            bTerminationCalled <- true
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "To terminate task %A, %s:%s" x.TypeOf x.Name x.VersionString ))
            let execTypeOf = x.TypeOf &&& JobTaskKind.ExecActionMask
            match execTypeOf with 
            | JobTaskKind.ApplicationMask 
            | JobTaskKind.ReleaseApplicationMask 
            | JobTaskKind.AppDomainMask ->
                let queue = x.QueueAtClientMonitor
                if ( Utils.IsNotNull queue && queue.CanSend )  then 
                    use msSend = new MemStream( 1024 )
                    msSend.WriteGuid( x.JobID )
                    msSend.WriteString( x.Name )
                    msSend.WriteInt64( x.Version.Ticks )
                    queue.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.Job ), msSend )
                    // Aggresive in preventing job from block others to execute. 
//                    JobListeningPortManagement.Current.Release( x.SignatureName, x.SignatureVersion ) 
                    x.State <- TaskState.PendingTermination
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Release Job Resources by sending Close, Job for job %s:%s" x.Name x.VersionString ))
                else
                    if x.State=TaskState.InExecution then 
                        if Utils.IsNotNull x.Thread then 
                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Close, Job for job %A, %s:%s, but we don't find the loopback queue, so no action is done" x.TypeOf x.Name x.VersionString ))
                            // Radical measure, should not be used. 
                            // JinL: comment out abort, as this is the job of the task holder. 
                            // x.Thread.Abort()
            | _ ->
                if x.State=TaskState.InExecution then 
                    if Utils.IsNotNull x.Thread then 
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "!!! Terminate Thread task %A, %s:%s with abort !!!" x.TypeOf x.Name x.VersionString ))
                        x.Thread.Abort()  
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Unknown execution type of task type %A, %s:%s, assume terminated... " x.TypeOf x.Name x.VersionString )                                                             )
                x.State <- TaskState.Terminated
        ()
    member val JobStarted = ref 0 with get
    member val EvJobStarted = new ManualResetEvent( false ) with get
    /// Send Start Job Command to the linked program. 
    member x.ToStartJob() = 
        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Send (Set, Job), metadata, (Start, Job) to AppDomain/Exe of task %s:%s" x.Name x.VersionString) )
        /// Send job metadata to the loopback interface. 
        use jobMetadataStream = 
            if Utils.IsNotNull x.MetadataStream then 
                x.MetadataStream.Replicate()
            else 
                let st = new MemStream( 4096 ) 
                x.Pack( st )
                st :> StreamBase<byte>
        let queue = x.QueueAtClient
        if Utils.IsNotNull queue && queue.CanSend then 
            if Interlocked.CompareExchange( x.JobStarted, 1, 0 ) = 0 then 
                // Make sure Set, Job & Start Job is called only once. 
                queue.ToSend( ControllerCommand( ControllerVerb.Set, ControllerNoun.Job ), jobMetadataStream )
                /// Send blob to the loopback interface. 
                for blobi=0 to x.NumBlobs-1 do
                    let blob = x.Blobs.[blobi]
                    match blob.TypeOf with 
                    | BlobKind.DstDSetMetaData
                    | BlobKind.PassthroughDSetMetaData 
                    | BlobKind.DStream 
                    | BlobKind.ClusterWithInJobInfo ->
                        x.SendBlobToJob( queue, blobi ) 
                    | _ ->
                        // SrcDSet, Cluster, Assembly should already be at the client, and doesn't need to be sent
                        ()
                use ms = new MemStream( 1024 )
                ms.WriteGuid( x.JobID )
                ms.WriteString( x.Name )
                ms.WriteInt64( x.Version.Ticks )
                queue.ToSend( ControllerCommand( ControllerVerb.Start, ControllerNoun.Job ), ms )
                x.EvJobStarted.Set() |> ignore
            else
                x.EvJobStarted.WaitOne() |> ignore
        else
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "!!! The queue to the associated job of task %s:%s is not operational" x.Name x.VersionString) )

    interface IDisposable with
        member x.Dispose() = 
            x.EvJobStarted.Dispose()
            base.DisposeResource()
            GC.SuppressFinalize(x)

and internal ContainerAppDomainLauncher() = 
    inherit System.MarshalByRefObject() 
    member x.Start(name, ver, bUseAllDrive, ticks, memory_size, ip, port, jobip, jobport, logdir, verbose_level:int, jobdir:string, jobenvvars:List<string*string>,
                   authParams : bool*Guid*(byte[]*byte[])*string) = 
        // Need to setup monitoring too  
        DeploymentSettings.ClientPort <- port     
        DeploymentSettings.LogFolder <- logdir
        RemoteExecutionEnvironment.ContainerName <- "AppDomain:" + name 
        let logfname = Path.Combine( logdir, name + "_appdomain_" + VersionToString( DateTime(ticks) ) + ".log" )
        let args = [| @"-log"; logfname; "-verbose"; verbose_level.ToString() |]
        if bUseAllDrive then 
            DeploymentSettings.UseAllDrivesForData()
        
        let parse = ArgumentParser(args)
        DeploymentSettings.MaxMemoryLimitInMB <- memory_size
        // Need to first write a line to log, otherwise, MakeFileAccessible will fails. 
//        Logger.Log( LogLevel.Info,  sprintf "Logging in New AppDomain...................... %s, %d MB " DeploymentSettings.PlatformFlag (DeploymentSettings.MaximumWorkingSet>>>20)  )
        Logger.Log( LogLevel.Info, ( sprintf "Logging in New AppDomain, verbose = %A ...................... %s, %d MB " (Logger.DefaultLogLevel) DeploymentSettings.PlatformFlag (DeploymentSettings.MaxMemoryLimitInMB) ))
        if not (Utils.IsNull jobdir) && jobdir.Length > 0 then 
            Directory.SetCurrentDirectory( jobdir ) 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Current working directory <-- %s " (Directory.GetCurrentDirectory()) ))
        if not (Utils.IsNull jobenvvars) then 
            for tuple in jobenvvars do 
                let var, value = tuple
                Environment.SetEnvironmentVariable( var, value )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Environment variable %s <-- %s " var value ))
        // MakeFileAccessible( logfname )
//        let task = Task( SignatureName=name, SignatureVersion=ver )
        Task.StartTaskAsSeperateApp( name, ver, ip, port, jobip, jobport, authParams, None, None, None)
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "ContainerAppDomainLauncher.Start returns"))

and [<AllowNullLiteral>]
    internal ContainerAppDomainInfo() =
    member val Name = "" with get, set
    member val Version = 0L with get, set  
    member val Ticks = DateTime.MinValue.Ticks with get, set          
    member val JobIP = "" with get, set
    member val JobPort = -1 with get, set          
    member val JobDir = "" with get, set  
    member val JobEnvVars = null with get, set
    member val Connects : NetworkConnections = null with get, set
//    member val AppDomain = null with get, set         
    /// Start task with Name and version, 
    /// this is usually called from another appdomain or exe
    static member StartProgram( o: Object ) = 
        try
            let x = o :?> ContainerAppDomainInfo
            // Trigger Tracefile creation, otherwise, MakeFileAccessible( logfname ) will fail. 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Start AppDomain for task %s:%s...................... " x.Name (x.Version.ToString("X")) ))
            let ad2 =
                if (x.JobDir = "") then
                    // Make sure that we have unique name, even if the same job is launched again and again
                    AppDomain.CreateDomain( x.Name + "_" + x.Version.ToString("X") + VersionToString(DateTime(x.Ticks)) )
                else
                    let ads = AppDomainSetup()
                    ads.ApplicationBase <- x.JobDir
                    // Make sure that we have unique name, even if the same job is launched again and again
                    AppDomain.CreateDomain( x.Name + "_" + x.Version.ToString("X") + VersionToString(DateTime(x.Ticks)), new Security.Policy.Evidence(), ads)
            let fullTypename = Operators.typeof< ContainerAppDomainLauncher >.FullName
            let exeAssembly = Assembly.GetExecutingAssembly()
            try
                if Utils.IsNotNull exeAssembly then 
                    let proxy = ad2.CreateInstanceFromAndUnwrap( exeAssembly.Location, fullTypename ) 
                    let mbrt = proxy :?> ContainerAppDomainLauncher
                    mbrt.Start( x.Name, x.Version, DeploymentSettings.StatusUseAllDrivesForData, 
                        x.Ticks, DeploymentSettings.MaxMemoryLimitInMB, DeploymentSettings.ClientIP, DeploymentSettings.ClientPort, x.JobIP, x.JobPort, DeploymentSettings.LogFolder, int (Prajna.Tools.Logger.DefaultLogLevel), x.JobDir, x.JobEnvVars, Cluster.Connects.GetAuthParam() ) 
                    Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "ContainerAppDomainLauncher.Start has returned, current domain is '%s'" AppDomain.CurrentDomain.FriendlyName)
                else
                    failwith "Can't find ContainerAppDomainLauncher in Assemblies"
            finally
                Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "ContainerAppDomainInfo.StartProgram reached finally, current domain is '%s', sleep %d ms" AppDomain.CurrentDomain.FriendlyName DeploymentSettings.SleepMSBeforeUnloadAppDomain)
                try 
                    if DeploymentSettings.SleepMSBeforeUnloadAppDomain > 0 then 
                        Threading.Thread.Sleep( DeploymentSettings.SleepMSBeforeUnloadAppDomain )
                    Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "ContainerAppDomainInfo.StartProgram starts to unload AppDomain %s" ad2.FriendlyName)
                    // Currently, during unit test clean up phase, the container hosted by appdomain takes very long time to unload for unknow reasons until this thread
                    // is aborted when the daemon appdomain is unloaded. As a temporary workaround before the root cause was figured out, the code below limits the 
                    // time given to unload the appdomain. 
                    // Notes:
                    //  1. When we reach this point, the execution for the container has already completed. Unload the now un-used appdomain is the only thing left.
                    //  2. If the unload itself hit any exception, the Wait should rethrow it here and it would be caught by the "with" below.
                    if Async.StartAsTask(async { AppDomain.Unload( ad2 ) }).Wait(DeploymentSettings.AppDomainUnloadWaitTime) then
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Successfully Unload AppDomain for task %s:%s...................... " x.Name (x.Version.ToString("X")) )  )
                    else 
                        Logger.LogF( LogLevel.MildVerbose, 
                            ( fun _ -> sprintf "Time budget (%f ms) for unload AppDomain for task %s:%s is used up, continue without waiting " 
                                         DeploymentSettings.AppDomainUnloadWaitTime.TotalMilliseconds x.Name (x.Version.ToString("X")) )  )
                with 
                | e -> 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "failed to unload AppDomain@StartProgram...... %A" e))
        with 
        | e ->
            Logger.Log( LogLevel.Error, ( sprintf "ContainerAppDomainLauncher.StartProgram exception: %A" e ))
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "ContainerAppDomainInfo.StartProgram returns"))

and internal TaskQueue() = 
    // ToDo: 12/23/2014, to change tasks, lookupTable and executionTable to concurrent data structure
//    let tasks = ConcurrentBag<Task>()
    // JinL, 9/21/2015: index Task by Guid
    let lookupTable = ConcurrentDictionary<Guid, Task>()
    //       2/24/2014, executionTable now holds job that are exeucting and being terminated, it is indexed by Job Signature,  
    let executionTable = ConcurrentDictionary<_ , ConcurrentDictionary<_,_>>(StringComparer.Ordinal)
    //      1/6/2016, map remote queue to tasks. 
    let queueToTasks = ConcurrentDictionary<_ , ConcurrentDictionary<_,_>>()
    member x.IsEmptyExecutionTable()=
        executionTable.IsEmpty    
    /// Whether there are any remote container attached to daemon. 
    member val EvEmptyExecutionTable = new ManualResetEvent(true) with get
    member val JobManagement = JobListeningPortManagement.Initialize( DeploymentSettings.JobIP, DeploymentSettings.JobPortMin, DeploymentSettings.JobPortMax ) with get
    /// TODO: JinL
    /// We intentionally set a low job limit for debugging purpose, this limit will be raised considering the capacity of the machines. 
    /// Current # of Light Jobs. 
    member val CurLightJobs = 0 with get, set 
    /// Current # of AppDomain Jobs. 
    member val CurAppDomainJobs = 0 with get, set 
    /// Current # of Exe Jobs. 
    member val CurExeJobs = 0 with get, set 
    member x.Count with get() = lookupTable.Count
    /// List of Tasks
    member x.Tasks with get() = lookupTable.Values
    /// Get a list of current tasks 
    member x.GetTasks() = 
        x.Tasks :> seq<_>
    /// is any of the DSet embedded in Task (therefore not visible globally? )
    member x.FindDSet( name, verNumber ) = 
        let dsetTask = ref null
        let tasks = x.GetTasks() 
        for task in tasks do 
            if Utils.IsNotNull task then 
                let paramLists = task.Blobs 
                if Utils.IsNotNull paramLists then 
                    for blob in paramLists do 
                        match blob.TypeOf with 
                        | BlobKind.SrcDSetMetaData
                        | BlobKind.DstDSetMetaData
                        | BlobKind.PassthroughDSetMetaData ->
                            if blob.Name = name && blob.Version = verNumber then 
                                dsetTask := task
                        | _ -> 
                            ()
                else
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "task %A %s:%s has null blob" task.TypeOf task.Name task.VersionString ))
        if Utils.IsNotNull !dsetTask then 
            if (!dsetTask).State <> TaskState.InExecution || Utils.IsNull ((!dsetTask).QueueAtClient) || not (!dsetTask).QueueAtClient.CanSend then 
                dsetTask := null
        (!dsetTask)
    /// Monitoring all tasks 
    member x.MonitorTasks() = 
        Logger.Do( LogLevel.MildVerbose, ( fun _ -> 
           let tasks = x.GetTasks() 
           let count = ref 0 
           for task in tasks do 
               if Utils.IsNotNull task then 
                   Logger.LogF( task.JobID, 
                                LogLevel.MildVerbose, ( fun _ ->  count := !count + 1
                                                                  let queue = task.QueueAtClientMonitor
                                                                  sprintf "Active Task %d: %s, state %A queue is %s" 
                                                                      (!count)
                                                                      task.Name 
                                                                      task.State
                                                                      ( if Utils.IsNotNull queue && queue.CanSend then "ready" else "not ready" )
                                                                  ))
           ))

    /// Find a certain task by name & vernumber
    /// If verNumber = 0L, find the latest task in the system. 
    member x.FindTask( jobID ) = 
        let valueRef = ref Unchecked.defaultof<_>
        if lookupTable.TryGetValue( jobID, valueRef ) then 
            Some( !valueRef ) 
        else
            None
    member x.AddTask( ta:Task, queue: NetworkCommandQueue ) = 
        let updateFunc k (priorTask:Task) = 
            if not (Object.ReferenceEquals( priorTask, ta )) then 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "when Add job %s:%s, we find another job with exact same signature. That job (of task %s:%s) of execution mode %A is being removed. " 
                                                               ta.Name ta.VersionString 
                                                               priorTask.SignatureName (priorTask.SignatureVersion.ToString("X")) priorTask.LaunchMode ))
                x.RemoveTaskWithQueueSignature( priorTask )
            ta
        // If previous version exist, update it to a new version
        let taskAdd = lookupTable.AddOrUpdate( ta.JobID, ta, updateFunc )
        if not(Utils.IsNull queue) then 
            x.LinkQueueAndTask( ta, queue )
        if Object.ReferenceEquals( taskAdd, ta ) then 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Add task %s:%A to task queue" ta.Name ta.TypeOf ))
            ta
        else
            // Never executed. 
            taskAdd

    member x.RemoveTaskByJobID( jobID ) = 
        lookupTable.TryRemove( jobID ) |> ignore

    member x.RemoveTask( ta:Task ) = 
        if (Utils.IsNotNull ta) then
            ta.OnJobFinish()
        x.RemoveTaskByJobID( ta.JobID )
            
    member x.LinkQueueAndTask( ta:Task, queue: NetworkCommandQueue ) = 
        let remoteSignature = queue.RemoteEndPointSignature
        ta.ClientQueueSignature <- remoteSignature
        let dict = queueToTasks.GetOrAdd( remoteSignature, fun _ -> ConcurrentDictionary<_,_>(StringDateTimeTupleComparer(StringComparer.Ordinal)) ) 
        dict.GetOrAdd( (ta.Name, ta.Version), ta ) |> ignore
    member x.DelinkQueueAndTask( ta:Task ) = 
        let remoteSignature = ta.ClientQueueSignature
        let dict = queueToTasks.GetOrAdd( remoteSignature, fun _ -> ConcurrentDictionary<_,_>(StringDateTimeTupleComparer(StringComparer.Ordinal)) ) 
        dict.TryRemove( (ta.Name, ta.Version) ) |> ignore
    /// Process incoming command for the task queue. 
    /// Return:
    ///     True: if command has been parsed. 
    ///     False: if command has not been parsed. 
    member x.ParseCommandAtDaemon( queue:NetworkCommandQueuePeer, cmd:ControllerCommand, ms:StreamBase<byte> ) = 
        let mutable fullname = null
        let mutable cb = null
        let mutable callbackItem = null
        // The command that will be processed by this callback. 
        match (cmd.Verb,cmd.Noun) with 
        // Set, Job (name is not parsed)
        | ControllerVerb.Set, ControllerNoun.Job ->
            let jobID, name, verNumber = Job.PeekJob( ms )
            JobLifeCycleCollectionDaemon.BeginJob( jobID, name, verNumber, queue.RemoteEndPointSignature ) |> ignore 
            using ( SingleJobActionDaemon.TryFind(jobID)) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Task.ErrorInSeparateApp( queue, sprintf "Failed to create JobLifeCycle Object or find Job Action object for Job %A, most probably because another job of the same job ID is running" jobID ) 
                    true
                else
                    try 
                        jobAction.LifeCycleObject.OnDisposeFS( fun _ -> x.RemoveTaskByJobID(jobID))
                        let task = new Task()
                        let bRet = task.UnpackToBlob( ms )
                        task.ClientAvailability(queue.RemoteEndPointSignature, Some task.ReceiveBlob, Some task.PeekBlobDSet )  
                        Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Set, Job job %s:%s, launchMode %A" task.Name task.VersionString task.LaunchMode ))
                        if bRet then 
                            if task.IsContainer then 
                                let mutable bInExecution = false
                                let mutable bLaunchFailed = false
                                let mutable taskHolder : ExecutedTaskHolder = x.GetRelatedTaskHolder( task ) 
                                let otherTaskHolder : seq<KeyValuePair<int64,ExecutedTaskHolder>> = x.GetTaskHolderOfDifferentVersion( task )
                                match task.LaunchMode with 
                                | TaskLaunchMode.LaunchAndFailWhenDifferentVersionExist -> 
                                    if Seq.length otherTaskHolder > 0 then 
                                        bLaunchFailed <- true
                                | TaskLaunchMode.LaunchAndTerminateDifferentVersion -> 
                                    for pair in otherTaskHolder do 
                                        let diffSignature = pair.Key
                                        let otherTask = pair.Value
                                        Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "terminate Task %s:%s that is a different version than the current task of version %s"
                                                                                                        task.SignatureName (diffSignature.ToString("X")) (task.SignatureVersion.ToString("X")) ))
                                        otherTask.ForceTerminate()
                                | TaskLaunchMode.DonotLaunch -> 
                                    ()
                                | _ -> 
                                    ()
                            
                                if not ( Utils.IsNull taskHolder ) && 
                                    not ( Utils.IsNull taskHolder.JobLoopbackQueue) && 
                                    taskHolder.JobLoopbackQueue.CanSend then     
                                        // Any job that are in execution? 
                                        bInExecution <- true
                                if not bInExecution then 
                                    Logger.LogF( jobID, LogLevel.WildVerbose, ( fun _ -> sprintf "Set, Job for %s:%s, find related job still executing" task.Name task.VersionString))
                //                    x.RemoveAllRelatedTask( task )
                                // Reserve job related resource 
                                let nodeInfo = 
                                    match task.LaunchMode with 
                                    | TaskLaunchMode.DonotLaunch -> 
                                        let foundTask = x.AddTask( task, queue ) // Useable for future reference.
                                        if not (Object.ReferenceEquals( foundTask, task )) then 
                                            Logger.LogF( jobID, LogLevel.Warning, ( fun _ -> sprintf "Set, Job job %s:%s, when get node info, we have found another job with launchMode %A" task.Name task.VersionString foundTask.LaunchMode ))
                                        try
                                            x.JobManagement.Use( foundTask.SignatureName )  
                                        with 
                                        | e -> 
                                            null
                                    | _ -> 
                                        x.JobManagement.Reserve( task.SignatureName, task.JobPort )  
                                if not (Utils.IsNull nodeInfo) then 
                                    if DeploymentSettings.bDaemonRelayForJobAllowed || nodeInfo.ListeningPort > 0 then 
                                    // Is the task already in the task queue?
                                        let foundTask = x.AddTask( task, queue )
                                        if not (Object.ReferenceEquals( foundTask, task )) then 
                                            Logger.LogF( jobID, LogLevel.Warning, ( fun _ -> sprintf "Set, Job %s:%s, but we have found another job with launchMode %A" task.Name task.VersionString foundTask.LaunchMode ))
                                        use msSend = new MemStream( 1024 )
                                        msSend.WriteGuid( jobID )
                                        msSend.WriteString( foundTask.Name ) 
                                        msSend.WriteInt64( foundTask.Version.Ticks )
                                        nodeInfo.Pack( msSend )
                                        queue.ToSend( ControllerCommand( ControllerVerb.InfoNode, ControllerNoun.Job ), msSend )   
                                        true
                                    else
                                        use msSend = new MemStream( 1024 )
                                        msSend.WriteGuid( jobID )
                                        msSend.WriteString( task.Name ) 
                                        msSend.WriteInt64( task.Version.Ticks )
                                        if task.LaunchMode <> TaskLaunchMode.DonotLaunch then 
                                            Logger.LogF( jobID, LogLevel.Warning, ( fun _ -> sprintf "Task %s failed to secure a valid port (return port <=0 ) .............." task.SignatureName ))
                                        queue.ToSend( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.Job ), msSend )                              
                                        true
                                else
                                    use msSend = new MemStream( 1024 )
                                    msSend.WriteGuid( jobID )
                                    msSend.WriteString( task.Name ) 
                                    msSend.WriteInt64( task.Version.Ticks )
                                    if task.LaunchMode <> TaskLaunchMode.DonotLaunch then 
                                        Logger.LogF( jobID, LogLevel.Warning, ( fun _ -> sprintf "Task %s failed in port reservation .............." task.SignatureName ))
                                    queue.ToSend( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.Job ), msSend )   
                                    true
                            else
                                let mutable taskHolder = x.GetRelatedTaskHolder( task ) 
                                if Utils.IsNull taskHolder && task.LaunchMode = TaskLaunchMode.DonotLaunch then 
                                    let otherTaskHolder = x.GetTaskHolderOfDifferentVersion( task )
                                    if Utils.IsNotNull otherTaskHolder && Seq.length otherTaskHolder > 0 then 
                                        taskHolder <- otherTaskHolder |> Seq.map ( fun pair -> pair.Value ) |> Seq.exactlyOne
                                        task.TaskHolder <- taskHolder

                                if Utils.IsNull taskHolder || Utils.IsNull taskHolder.CurNodeInfo then 
                                    /// Running job, can't find the associated container. 
                                    use msSend = new MemStream( 1024 )
                                    msSend.WriteGuid( jobID )
                                    msSend.WriteString( task.Name ) 
                                    msSend.WriteInt64( task.Version.Ticks )
                                    queue.ToSend( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.Job ), msSend )  
                                    true
                                else
                                    let nodeInfo = taskHolder.CurNodeInfo
                                    task.TypeOf <- taskHolder.TypeOf
                                    let foundTask = x.AddTask( task, queue )
                                    foundTask.TaskHolder <- taskHolder
                                    use msSend = new MemStream( 1024 )
                                    msSend.WriteGuid( jobID )
                                    msSend.WriteString( task.Name ) 
                                    msSend.WriteInt64( task.Version.Ticks )
                                    nodeInfo.Pack( msSend )
                                    queue.ToSend( ControllerCommand( ControllerVerb.InfoNode, ControllerNoun.Job ), msSend )
                                    true
                        else
                            let msg = sprintf "Failed to parse Set, Job command with payload of %dB" (ms.Length)
                            jobAction.ThrowExceptionAtContainer( msg )
                            true
                    with
                    | ex -> 
                        jobAction.EncounterExceptionAtContainer( ex, "___ ParseCommandAtDaemon, (Set, Job) ___ ")
                        true
            )
        | ControllerVerb.Set, ControllerNoun.Blob -> 
            // Blob not attached to job
            let buf, pos, count = ms.GetBufferPosLength()
            let fastHash = buf.ComputeChecksum(int64 pos, int64 count)
            let bSuccess, cryptoHash = BlobFactory.receiveWriteBlob( fastHash, lazy buf.ComputeSHA256(int64 pos, int64 count), ms, queue.RemoteEndPointSignature )
            if bSuccess then 
                Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "Rcvd Set, Blob from endpoint %s of %dB hash to %s (%d, %dB), successfully parsed"
                                                                       (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                       buf.Length
                                                                       (BytesToHex(if cryptoHash.IsSome then cryptoHash.Value else fastHash))
                                                                       pos count  ))
                true
            else
                let errMsg = sprintf "Rcvd Set, Blob from endpoint %s of %dB hash to %s (and %s) (%d, %dB), failed to find corresponding job"
                                                                        (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                        buf.Length
                                                                        (BytesToHex(fastHash))
                                                                        (BytesToHex(cryptoHash.Value))
                                                                        pos count
                // Set, Blob error can't be localiazed to a job                                                                
                Task.ErrorInSeparateApp( queue, errMsg )
                true
        | ControllerVerb.Start, ControllerNoun.Job ->
            // Figure
            let jobID = ms.ReadGuid() 
            using ( SingleJobActionDaemon.TryFind(jobID)) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Task.ErrorInSeparateApp( queue, sprintf "(Start, Job) Failed to find Job Action object for Job %A, error has happened before? " jobID ) 
                    true
                else
                    try
                        let name = ms.ReadString()
                        let verNumber = ms.ReadInt64()
                        let taskOpt = x.FindTask( jobID )
                        match taskOpt with 
                        | Some( task ) ->
                            if task.Version.Ticks = verNumber && String.Compare( task.Name, name, StringComparison.Ordinal)=0 then 
                                task.ParseTaskCommandAtDaemon( jobAction, queue, cmd, ms, x )
                            else
                                let msg = sprintf "Job %A, Miss matched task name & version number %s:%s vs %s:%s in queue" jobID name (verNumber.ToString("X")) task.Name task.VersionString
                                jobAction.ThrowExceptionAtContainer( msg )
                                true 
                        // Job command 
                        | None -> 
                            use msSend = new MemStream( 1024 )
                            msSend.WriteGuid( jobID )
                            msSend.WriteString( name ) 
                            msSend.WriteInt64( verNumber )
                            Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Job %s:%s failed to start, as we can't find Set, Job entry  .............." name (VersionToString(DateTime(verNumber))) ))
                            queue.ToSend( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.Job ), msSend )   
                            true
                    with 
                    | ex -> 
                        jobAction.EncounterExceptionAtContainer( ex, "___ ParseCommandAtDaemon( Start, Job) ___")
                        true
            )
        // Forward exception message to a proper application
        | ControllerVerb.Exception, ControllerNoun.Job -> 
            // Figure
            let pos = ms.Position
            let jobID = ms.ReadGuid() 
            using ( SingleJobActionDaemon.TryFind(jobID)) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Task.ErrorInSeparateApp( queue, sprintf "(Exception, Job) Failed to find Job Action object for Job %A, unable to forward the exception message." jobID ) 
                    true
                else
                    ms.Seek( pos, SeekOrigin.Begin ) |> ignore 
                    jobAction.ToSend( cmd, ms )
                    true
            )
        // Other than set, all other command are processed by each individual task
        | _, ControllerNoun.Job 
        | _, ControllerNoun.Blob ->
            let jobID  = ms.ReadGuid()
            using ( SingleJobActionDaemon.TryFind(jobID)) ( fun jobAction -> 
                if Utils.IsNull jobAction then 
                    Task.ErrorInSeparateApp( queue, sprintf "(%A) Failed to find Job Action object for Job %A, error has happened before? " cmd jobID ) 
                    true
                else
                    try
                        let name = ms.ReadString()
                        let verNumber = ms.ReadInt64()
                        let taskOpt = x.FindTask( jobID )
                        match taskOpt with 
                        | Some( task ) ->
                            if task.Version.Ticks = verNumber && String.Compare( task.Name, name, StringComparison.Ordinal)=0 then 
                                task.ParseTaskCommandAtDaemon( jobAction, queue, cmd, ms, x )
                            else
                                let msg = sprintf "Job %A, Miss matched task name & version number %s:%s vs %s:%s in queue" jobID name (verNumber.ToString("X")) task.Name task.VersionString
                                jobAction.ThrowExceptionAtContainer( msg )
                                true 
                        // Job command 
                        | None -> 
                            use msSend = new MemStream( 1024 )
                            msSend.WriteGuid( jobID )
                            msSend.WriteString( name ) 
                            msSend.WriteInt64( verNumber )
                            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Job %A failed to perform action %A, as we can't find Set, Job entry  .............." jobID cmd ))
                            queue.ToSend( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.Job ), msSend )  
                            true 
                    with
                    | ex -> 
                        jobAction.EncounterExceptionAtContainer( ex, sprintf "___ ParseCommandAtDaemon( %A ) ___" cmd )
                        true
            )
// Don't use error. 
//                let msg = sprintf "Failed to find Task %s:%s in queue" name (VersionToString(DateTime(verNumber)) )
//                Some( Task.Error( msg ) )
        | _ ->
            false
        
    /// Add a task with feedback Queue
    member x.AddSeparateTask( ta:Task, queue: NetworkCommandQueue ) = 
        let taExecTypeOf = ta.TypeOf &&& JobTaskKind.ExecActionMask
        let mutable bLaunchNew = false
        match taExecTypeOf with 
        | JobTaskKind.ApplicationMask
        | JobTaskKind.ReleaseApplicationMask
        | JobTaskKind.AppDomainMask -> 
            let execJobSets = executionTable.GetOrAdd( ta.SignatureName, fun _ -> ConcurrentDictionary<_,ExecutedTaskHolder>() )
            x.EvEmptyExecutionTable.Reset() |> ignore 
            let refInitialHolder = ref Unchecked.defaultof<_>
            let addFunc key = 
                refInitialHolder := new ExecutedTaskHolder( SignatureName = ta.SignatureName, SignatureVersion = ta.SignatureVersion, TypeOf = ta.TypeOf, JobDirectory = ta.JobDirectory, JobEnvVars = ta.JobEnvVars)
                let nodeInfo = x.JobManagement.Use( ta.SignatureName )
                (!refInitialHolder).CurNodeInfo <- nodeInfo
                !refInitialHolder
            let taskHolder = execJobSets.GetOrAdd( ta.SignatureVersion, addFunc )
            if not (Utils.IsNull taskHolder) && 
               not (Utils.IsNull taskHolder.CurNodeInfo) then 
                ta.TaskHolder <- taskHolder
                if Object.ReferenceEquals( taskHolder, !refInitialHolder ) then 
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "AddSeparateTask, to launch a new task %s:%s" ta.Name ta.VersionString ))
                    // this thread gets to launch the task 
                    taskHolder.StartProgram( ta )
                else
                    taskHolder.EvTaskLaunched.WaitOne() |> ignore

                if not taskHolder.TaskLaunchSuccess then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Mark task %s:%s as terminated due to failure of TaskHolder Launch" ta.Name ta.VersionString ))
                    ta.State <- TaskState.Terminated
                else
                    let mutable bCanExecute = true
                    match taskHolder.ExecutionType with 
                    | Process _
                    | Thread _ -> 
                        ()
                    | NotStarted ->
                        // Logic error, NotStarted should not be added 
                        let msg = sprintf "TaskQueue.AddSeparateTask, link job to a failed start job " 
                        Logger.Log( LogLevel.Error, msg )
                        bCanExecute <- false
                    | Terminated ->
                        // Logic error, NotStarted should not be added 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "to start a task %s:%s, but the job holder has just been terminated." 
                                                                       (ta.SignatureName) (ta.SignatureVersion.ToString("X")) ))
                        bCanExecute <- false
                    if bCanExecute then 
                        // Add Queue in Task Holder. 
                        taskHolder.NewConnectedQueue( queue )
                        let jobList = taskHolder.JobList
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> 
                           sprintf "For job %s:%s, we have found job with same signature, the job will become a group of %s:%s" 
                               ta.Name ta.VersionString ta.SignatureName (ta.SignatureVersion.ToString("X")) ))

                        jobList.Item( (ta.Name, ta.Version) ) <- ta
                        let connQueue = taskHolder.JobLoopbackQueue
                        if not (Utils.IsNull connQueue) then 
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> 
                               sprintf "For job %s:%s, previous job doesn't have a feedback queue, we will need to restart the job for group of %s:%s" 
                                   ta.Name ta.VersionString ta.SignatureName (ta.SignatureVersion.ToString("X")) ))
                            taskHolder.RegisterConnectionsWithTasks()
                            ta.ToStartJob()
                        ta.State <- TaskState.InExecution
                    else
                        // Message is shown above. 
                        ta.State <- TaskState.Terminated
            else
                Logger.LogF( LogLevel.Info, ( fun _ -> 
                   sprintf "AddSeparateTask:failed to launch job %s:%s. Can't find the reserved port for the job group of %s:%s" 
                       ta.Name ta.VersionString ta.SignatureName (ta.SignatureVersion.ToString("X")) ))
                ta.State <- TaskState.Terminated
        | _ ->
            ta.StartLightTask() |> ignore
            bLaunchNew <- true
            ta.State <- TaskState.InExecution
    /// Add a task with feedback Queue
    member x.ConnectTaskToTaskHolder( ta:Task, queue: NetworkCommandQueue ) = 
        let refValue = ref Unchecked.defaultof<_>
        if not (executionTable.TryGetValue( ta.SignatureName, refValue )) then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ConnectTaskToTaskHolder failed as the job %s:%s of taskholder %s cannot be found " 
                                                           (ta.Name) (ta.VersionString)
                                                           (ta.SignatureName) )               )
            ta.State <- TaskState.Terminated
            false
        else
            let execJobSets = !refValue
            let refValue1 = ref Unchecked.defaultof<_>
            let mutable bFindTaskHolder = (execJobSets.TryGetValue( ta.SignatureVersion, refValue1))
            if not bFindTaskHolder then 
                match ta.LaunchMode with 
                | TaskLaunchMode.DonotLaunch -> 
                    let otherTaskHolder = x.GetTaskHolderOfDifferentVersion( ta ) 
                    try
                        if Seq.length otherTaskHolder > 0 then 
                            refValue1 := otherTaskHolder |> Seq.map ( fun pair -> pair.Value ) |> Seq.exactlyOne
                            bFindTaskHolder <- true
                    with 
                    | e -> 
                        ()
                | _ -> 
                    ()
            if not bFindTaskHolder then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ConnectTaskToTaskHolder failed as the job %s:%s of taskholder %s:%s cannot be found " 
                                                               (ta.Name) (ta.VersionString)
                                                               (ta.SignatureName) (ta.SignatureVersion.ToString("X")) )                    )
                if ta.LaunchMode<> TaskLaunchMode.DonotLaunch then 
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "For task that needs to be launched, should not connect to an non existing task holder" )                    )
                ta.State <- TaskState.Terminated
                false
            else
                let taskHolder = !refValue1
                x.ConnectTaskByTaskHolder( taskHolder, ta, queue )

    member x.ConnectTaskByTaskHolder( taskHolder, ta:Task, queue: NetworkCommandQueue ) = 
        ta.TaskHolder <- taskHolder
        taskHolder.EvTaskLaunched.WaitOne() |> ignore
        if not taskHolder.TaskLaunchSuccess then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ConnectTaskToTaskHolder, job %s:%s of taskholder %s:%s cannot connect as the task holder fail to launch " 
                                                           (ta.Name) (ta.VersionString)
                                                           (ta.SignatureName) (ta.SignatureVersion.ToString("X")) )                    )
            ta.State <- TaskState.Terminated
        else
            let mutable bCanExecute = true
            match taskHolder.ExecutionType with 
            | Process _ 
            | Thread _ ->
                ()
            | NotStarted ->
                // Logic error, NotStarted should not be added 
                let msg = sprintf "TaskQueue.AddSeparateTask, link job to a failed start job " 
                Logger.Log( LogLevel.Error, msg )
                bCanExecute <- false
            | Terminated ->
                // Logic error, NotStarted should not be added 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "to start a task %s:%s, but the job holder has just been terminated." 
                                                               (ta.SignatureName) (ta.SignatureVersion.ToString("X")) ))
                bCanExecute <- false
            if bCanExecute then 
                // Add Queue in Task Holder. 
                taskHolder.NewConnectedQueue( queue )
                let jobList = taskHolder.JobList
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> 
                   sprintf "For job %s:%s, we have found job with same signature, the job will become a group of %s:%s" 
                       ta.Name ta.VersionString ta.SignatureName (ta.SignatureVersion.ToString("X")) ))

                jobList.Item( (ta.Name, ta.Version) ) <- ta
                let connQueue = taskHolder.JobLoopbackQueue
                if not (Utils.IsNull connQueue) then 
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> 
                       sprintf "For job %s:%s, previous job doesn't have a feedback queue, we will need to restart the job for group of %s:%s" 
                           ta.Name ta.VersionString ta.SignatureName (ta.SignatureVersion.ToString("X")) ))
                    taskHolder.RegisterConnectionsWithTasks()
                    ta.ToStartJob()
                ta.State <- TaskState.InExecution
            else
                // Message above. 
                ta.State <- TaskState.Terminated
        ta.State = TaskState.InExecution



            
            

    member x.MonitorExecutionTableLong() = 
        seq {
            for namePair in executionTable do 
                for verPair in namePair.Value do 
                    let jobList = verPair.Value.JobList
                    for elem in jobList do 
                        let ta = elem.Value
                        yield sprintf "Execution table, signature %s:%s, job %s:%s" namePair.Key (verPair.Key.ToString("X")) ta.Name ta.VersionString 
                    // Avoid using .Keys
                    for elem in verPair.Value.LaunchedServices do 
                        let serviceName = elem.Key
                        yield sprintf "Execution table, signature %s:%s, service %s" namePair.Key (verPair.Key.ToString("X")) serviceName
        } |> String.concat Environment.NewLine
    member x.MonitorExecutionTable() = 
        let countRef = ref 0 
        let allTaskInfo = 
            if executionTable.IsEmpty then 
                x.EvEmptyExecutionTable.Set() |> ignore 
            else 
                x.EvEmptyExecutionTable.Reset() |> ignore 
            seq {
                for namePair in executionTable do 
                    for verPair in namePair.Value do 
                        countRef := !countRef + 1
                        let taskHolder = verPair.Value
                        let jobList = taskHolder.JobList
                        // Avoid using .Keys
                        let servicesInfo = taskHolder.LaunchedServices |> Seq.map ( fun elem -> elem.Key ) |> String.concat ","
                        yield sprintf "Task %s:%s(%d, %s)" namePair.Key (verPair.Key.ToString("X")) jobList.Count servicesInfo
            } |> String.concat ","
        sprintf "Utc %s, %d active tasks -> %s" (UtcNowToString()) (!countRef) allTaskInfo
    /// is any of the DSet embedded in Task by name only
    member x.FindTaskHolderByService( nameService ) = 
        let taskHolderRef = ref null
        for pair0 in executionTable do 
            for pair1 in pair0.Value do 
                let taskHolder = pair1.Value
                let bExist, _ =  taskHolder.LaunchedServices.TryGetValue( nameService )
                if bExist then 
                    taskHolderRef := taskHolder   
        (!taskHolderRef)
    /// Called by PrajnaClient, to link tasks
    member x.LinkSeparateProgram( incomingQueue:NetworkCommandQueue, sigName, sigVersion ) = 
        let mutable bExist = true
        let refValue = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( sigName, refValue ) then 
            let execJobSets = !refValue
            let refValue1 = ref Unchecked.defaultof<_>
            if execJobSets.TryGetValue( sigVersion, refValue1 ) then 
                let jobHolder = !refValue1
                jobHolder.JobLoopbackQueue <- incomingQueue
                jobHolder.EvLoopbackEstablished.Set() |> ignore
                jobHolder.RegisterConnectionsWithTasks()
                let jobList = jobHolder.JobList
                for elem in jobList do 
                    let ta = elem.Value
                    if ta.AvailThis.AllAvailable then 
                        ta.ToStartJob( )
            else
                bExist <- false
        else
            bExist <- false
        if bExist then 
            ( ControllerCommand( ControllerVerb.ConfirmStart, ControllerNoun.Program ), null )
        else
            let msg1 = sprintf "receive Link, Job but Job with signature %s:%s does not exist........." sigName (sigVersion.ToString("X"))
            let msg = msg1 + Environment.NewLine + x.MonitorExecutionTableLong()
            Logger.Log( LogLevel.Error, msg )
            let msError = new MemStream( )
            msError.WriteString( (UtcNowToString()) + ": " + msg ) 
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
    member x.DelinkSeparateProgram( incomingQueue:NetworkCommandQueue, sigName, sigVersion ) = 
        let mutable bExist = false
        let mutable errorMsg = ""
            
        let refValue = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( sigName, refValue ) then 
            let execJobSets = !refValue
            let refJobHolder = ref Unchecked.defaultof<_>
            if execJobSets.TryGetValue( sigVersion, refJobHolder) then 
                let jobHolder = !refJobHolder
                if not (Utils.IsNull jobHolder.MonStdOutput) then 
                    jobHolder.MonStdOutput.Close()
                if not (Utils.IsNull jobHolder.MonStdError) then 
                    jobHolder.MonStdError.Close()

                bExist <- Object.ReferenceEquals( jobHolder.JobLoopbackQueue, incomingQueue ) || Utils.IsNull jobHolder.JobLoopbackQueue
                if not bExist then 
                    errorMsg <- sprintf "!!! Error !!! Try delink a task %s:%s, but the loopback queue doesn't match" sigName (sigVersion.ToString("X"))
                else
                    let bRemoved, jobHolder = execJobSets.TryRemove( sigVersion )
                    if bRemoved then 
                        // Block entry 
                        jobHolder.ExecutionType <- Terminated
                        let jobList = jobHolder.JobList
                        for elem in jobList do 
                            let ta = elem.Value
                            x.RemoveTaskWithQueueSignature( ta )
                        jobList.Clear()
                        if execJobSets.IsEmpty then 
                            executionTable.TryRemove( sigName ) |> ignore 
                            if executionTable.IsEmpty then 
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "All remote container discontinued, last one is %s:%s" sigName (sigVersion.ToString("X")) ))
                                x.EvEmptyExecutionTable.Set() |> ignore 
                        // Release port
                        JobListeningPortManagement.Current.Release( sigName ) 
            else
                errorMsg <- sprintf "!!! Error !!! Try delink a task %s:%s, but the specific version of the task is not in execution table " sigName (sigVersion.ToString("X"))
        else
            errorMsg <- sprintf "!!! Error !!! Try delink a task %s:%s, but the task name doesn't exist not in execution table " sigName (sigVersion.ToString("X"))
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Delink task %s:%s" sigName (sigVersion.ToString("X")) ))
        Logger.LogF( LogLevel.MildVerbose, x.MonitorExecutionTable)
        if bExist then 
            ( ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Unknown ), null )              
        else
            let msError = new MemStream(1024)
            msError.WriteString( errorMsg )
            Logger.Log( LogLevel.Info, errorMsg )
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError ) 
    /// Find task tolder 
    member x.FindTaskHolderByloopbackQueue( queue:NetworkCommandQueue ) = 
        let mutable returnTuple = (null, 0L, null)
        for pair in executionTable do 
            for pair1 in pair.Value do 
                let jobHolder = pair1.Value
                if Object.ReferenceEquals( jobHolder.JobLoopbackQueue, queue ) then 
                    returnTuple <- ( pair.Key, pair1.Key, jobHolder )
        returnTuple
    /// <summary>
    /// Shutdown associated connection queue
    /// </summary>
    member x.CloseConnectedQueueForAllTasks( queue: NetworkCommandQueue ) = 
        let sigName, sigVer, jobHolder = x.FindTaskHolderByloopbackQueue( queue ) 
        if Utils.IsNull jobHolder then         
            let remoteQueueSignature = queue.RemoteEndPointSignature
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "client node %s is disconnected, structure clean up" 
                                                           ( LocalDNS.GetShowInfo(queue.RemoteEndPoint) ) ))
            let bTaskList, tasksAssociatedWithQueue = queueToTasks.TryGetValue( remoteQueueSignature )
            for pair in executionTable do 
                let signatureName = pair.Key
                let execJobSets = pair.Value
                for pair1 in execJobSets do
                    let signatureVersion = pair1.Key
                    let taskHolder = pair1.Value
                    taskHolder.CloseConnectedQueue( queue ) |> ignore
                    let jobList = taskHolder.JobList
                    if bTaskList then 
                        // Remove those tasks that are specifically attached to the queue
                        for pair in tasksAssociatedWithQueue do 
                            let bRemove, ta = jobList.TryRemove( pair.Key ) 
                            if bRemove then 
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> let name, ver = pair.Key 
                                                                              sprintf "Remove job %s:%s from taskHolder of %s" 
                                                                                    name (VersionToString(ver)) ta.SignatureName ))

            if bTaskList then 
                for pair in tasksAssociatedWithQueue do 
                    // No need to remove queueSignature, as it has been removed. 
                    x.RemoveTask( pair.Value )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> let name, ver = pair.Key 
                                                                  sprintf "Attemp to Remove job %s:%s from lookup queue" 
                                                                           name (VersionToString(ver)) ))
        else
            use errMS = new MemStream( 1024 )
            errMS.WriteString( sigName )
            errMS.WriteInt64( sigVer )
            errMS.WriteString( sprintf "Program %s crashes" sigName ) 
            x.ReportCrash( sigName, sigVer, errMS )
            x.DelinkSeparateProgram( queue, sigName, sigVer ) |> ignore
    /// Report Crash message. 
    member x.ReportCrash( sigName, sigVer, errMS ) =
        let repBack = ConcurrentDictionary<_,_>()
        let refValue = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( sigName, refValue) then 
            // Terminate all other jobs. 
            let execJobSets = !refValue
            for oneJobSet in execJobSets do 
                let taskHolder = oneJobSet.Value
                for pair in taskHolder.ConnectedQueue do 
                    let signature = pair.Key
                    let queue = Cluster.Connects.LookforConnectBySignature( signature )
                    if not (Utils.IsNull queue) && queue.CanSend then 
                        queue.ToSend( ControllerCommand(ControllerVerb.Error, ControllerNoun.Program), errMS )

    /// Terminate All tasks in the app domain/exe
    member x.TerminateSeparateTask( jobSetNames ) = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "terminate all jobs in task %s" jobSetNames ))
        let refValue = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( jobSetNames, refValue) then 
            // Terminate all other jobs. 
            let execJobSets = !refValue
            for oneJobSet in execJobSets do 
                let taskHolder = oneJobSet.Value
                let ta_lists = taskHolder.JobList 
                for elem in ta_lists do 
                    let ta = elem.Value
                    ta.TerminateTask()
                    ta.State <- TaskState.Terminated   
                                             
    /// Find a task 
    member x.FindSeparateTask( ta:Task ) = 
        let valueRef = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( ta.SignatureName, valueRef ) then 
            let execJobSets = !valueRef
            let inJobInfo = ref Unchecked.defaultof<_>
            if execJobSets.TryGetValue( ta.SignatureVersion, inJobInfo) then 
                let taskHolder = !inJobInfo
                let connQueue = taskHolder.JobLoopbackQueue
                connQueue
            else
                null
        else
            null
    /// Find a task 
    member x.RemoveSeparateTask( ta:Task ) = 
        let valueRef = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( ta.SignatureName, valueRef ) then 
            let execJobSets = !valueRef
            let inJobRef = ref Unchecked.defaultof<_>
            if execJobSets.TryGetValue( ta.SignatureVersion, inJobRef ) then 
                let taskHolder = !inJobRef  
                let jobList = taskHolder.JobList 
                let bRemoved = jobList.TryRemove( (ta.Name, ta.Version), ref Unchecked.defaultof<_> ) 
                if bRemoved && jobList.IsEmpty then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Task %s:%s has no associated jobs" ta.SignatureName (ta.SignatureVersion.ToString("X")) ))
    /// Get All tasks with the same signature
    member x.GetAllRelatedTasksDeprecated( ta:Task ) = 
        let valueRef = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( ta.SignatureName, valueRef ) then 
            let execJobSets = !valueRef
            let inJobRef = ref Unchecked.defaultof<_>
            if execJobSets.TryGetValue( ta.SignatureVersion, inJobRef) then 
                let taskHolder = !inJobRef  
                let jobList = taskHolder.JobList
                jobList.Values :> seq<_>
            else
                Seq.empty
        else
            Seq.empty
    /// Get related task holder
    member x.GetRelatedTaskHolder( ta: Task ) = 
        let valueRef = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( ta.SignatureName, valueRef ) then 
            let execJobSets = !valueRef
            let taskHolderRef = ref Unchecked.defaultof<_>
            if execJobSets.TryGetValue( ta.SignatureVersion, taskHolderRef ) then 
                !taskHolderRef
            else
                null
        else 
            null
    /// Get related task holder
    member x.GetTaskHolderOfDifferentVersion( ta: Task ) = 
        let valueRef = ref Unchecked.defaultof<_>
        if executionTable.TryGetValue( ta.SignatureName, valueRef ) then 
            let execJobSets = !valueRef
            execJobSets |> Seq.filter( fun pair -> pair.Key<>ta.SignatureVersion )
        else 
            Seq.empty

                                    
    /// Remote Finished Jobs, and reset # of active jobs. 
    member x.RemoveFinishedJobs( ) = 
        x.CurLightJobs <- 0
        x.CurAppDomainJobs <- 0 
        x.CurExeJobs <- 0 
        // Remove finished job, 
        let taskArr = x.GetTasks()
        for ta in taskArr do
            if ta.ShouldTerminate() then 
                ta.TerminateTask() 
                
            if ta.State=TaskState.InExecution || ta.State=TaskState.PendingTermination then 
                ta.EvaluateThreadState() |> ignore
            match ta.State with 
            | TaskState.InExecution ->
                let taExecTypeOf = ta.TypeOf &&& JobTaskKind.ExecActionMask
                match taExecTypeOf with 
                | JobTaskKind.ApplicationMask 
                | JobTaskKind.ReleaseApplicationMask 
                | JobTaskKind.AppDomainMask ->
                    ()
                | JobTaskKind.LightCPULoad ->
                    x.CurLightJobs <- x.CurLightJobs + 1   
                | _ ->
                    failwith ( sprintf "RemoveFinishedJobs: Unknown JobTaskKind for task %s, %x" ta.Name (int ta.TypeOf) )
            | TaskState.Terminated ->
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Task %s:%s is detected as terminated ................" ta.Name ta.VersionString ))
                x.RemoveTaskWithQueueSignature( ta )
                // x.RemoveAllRelatedTask( ta )
                // Only remove object if the task is the latest reference.                     
                ()
            | _ -> 
                ()
        for pair in executionTable do 
            for pair1 in pair.Value do 
                let taskHolder = pair1.Value
                match taskHolder.ExecutionType with 
                | Thread _ -> 
                    x.CurAppDomainJobs <- x.CurAppDomainJobs + 1
                | Process _ -> 
                    x.CurExeJobs <- x.CurExeJobs + 1
                | _ -> 
                    ()
//                failwith ( sprintf "Wrong logic: Job state transit from InExecution to %A" ta.State )
    /// Execute a task
    /// Return: true, job executed
    ///         false, job failed (no slot)
    member x.ExecuteTask( ta:Task, queue: NetworkCommandQueue ) = 
        match ta.State with 
        | TaskState.InExecution -> 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Task %s:%s already in execution" ta.Name ta.VersionString ))
            /// Already executed. 
            true
        | TaskState.Terminated ->
            failwith ( sprintf "ExecuteTask: Exucute job %s that has already been terminated" ta.Name )
        | _ -> 
            let totalJobs = x.CurLightJobs + x.CurAppDomainJobs + x.CurExeJobs
            let mutable bSuccess = totalJobs < DeploymentSettings.TotalJobLimit 
            if bSuccess then 
                let taExecTypeOf = ta.TypeOf &&& JobTaskKind.ExecActionMask
                let limit = ref 0
                match taExecTypeOf with 
                | JobTaskKind.ApplicationMask 
                | JobTaskKind.ReleaseApplicationMask ->
                    if x.CurExeJobs < DeploymentSettings.ExeJobLimit then 
                        x.CurExeJobs <- x.CurExeJobs + 1
                    else
                        limit := DeploymentSettings.ExeJobLimit
                        bSuccess <- false
                | JobTaskKind.AppDomainMask ->
                    if x.CurAppDomainJobs < DeploymentSettings.AppDomainJobLimit then 
                        x.CurAppDomainJobs <- x.CurAppDomainJobs + 1
                    else
                        limit := DeploymentSettings.AppDomainJobLimit
                        bSuccess <- false
                | JobTaskKind.LightCPULoad ->
                    if x.CurLightJobs < DeploymentSettings.LightJobLimit then 
                        x.CurLightJobs <- x.CurLightJobs + 1
                    else
                        limit := DeploymentSettings.LightJobLimit
                        bSuccess <- false
                | _ ->
                    failwith ( sprintf "ExecuteTask: Unknown task type %s: %x" ta.Name (int ta.TypeOf) )
                if bSuccess then 
// As we don't use lock, it is possible for the job to be executed before. 
//                    ta.StartTask()
                    x.AddSeparateTask( ta, queue )
                    ta.State = TaskState.InExecution
                else
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Failed to start job %s of type %A as the limit of Exe/AppDomain/LightCPU task of %d has reached" ta.Name ta.TypeOf (!limit) ))
                    bSuccess
            else
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Failed to start job %s of type %A as the TotalJobLimit of %d has reached" ta.Name ta.TypeOf DeploymentSettings.TotalJobLimit ))
                bSuccess
    /// Find one task to execute. 
    member x.ExecuteLightJobs() = 
        x.RemoveFinishedJobs()
        let taskArr = x.GetTasks()
        for ta in taskArr do
            let taExecTypeOf = ta.TypeOf &&& JobTaskKind.ExecActionMask
            if taExecTypeOf=JobTaskKind.LightCPULoad && ta.State=TaskState.ReadyToExecute then 
                // LightCPU Load doesn't need a job holder. 
                x.ExecuteTask( ta, null ) |> ignore

    member x.RemoveTaskWithQueueSignature( ta:Task ) = 
        x.RemoveTask( ta ) 
        for pair in queueToTasks do 
            let bRemove, _ = pair.Value.TryRemove( (ta.Name, ta.Version ) )
            if bRemove then 
                if pair.Value.IsEmpty then 
                    queueToTasks.TryRemove( pair.Key ) |> ignore  
        for pair in executionTable do 
            let execJobSets = pair.Value
            for pair1 in execJobSets do 
                let jobHolder = pair1.Value
                let bRemove, _ = jobHolder.JobList.TryRemove( (ta.Name, ta.Version ) )
                ()
            
    interface IDisposable with
        member x.Dispose() = 
            x.EvEmptyExecutionTable.Dispose()            
            GC.SuppressFinalize(x)

type internal ContainerLauncher() = 
    static member Main orgargv = 
        let argv = Array.copy orgargv
        let firstParse = ArgumentParser(argv, false)

        let logdir = firstParse.ParseString( "-logdir", (DeploymentSettings.LogFolder) )
        let name = firstParse.ParseString( "-job", "" )
        let ticks = firstParse.ParseInt64( "-ticks", (DateTime.MinValue.Ticks) )
        let logFileName = Path.Combine( logdir, name + "_exe_" + VersionToString( DateTime(ticks) ) + ".log"  )
        
        let argv2 = Array.concat (seq { yield (Array.copy argv); yield [|"-log"; logFileName|] })
        let parse = ArgumentParser(argv2)

        let jobip = parse.ParseString( "-jobip", "" )        
        Prajna.Core.Cluster.Connects.IpAddr <- jobip
        let jobport = parse.ParseInt( "-jobport", -1 )
        let ver = parse.ParseInt64( "-ver", 0L )
        let ip = parse.ParseString( "-loopbackip", "" )
        DeploymentSettings.ClientIP <- ip
        let port = parse.ParseInt( "-loopback", DeploymentSettings.ClientPort )
        DeploymentSettings.ClientPort <- port
        let requireAuth = parse.ParseBoolean( "-auth", false )
        let guidStr = parse.ParseString( "-myguid", "" )
        let rsaKeyStrAuth = parse.ParseString( "-rsakeyauth", "" )
        let rsaKeyStrExch = parse.ParseString( "-rsakeyexch", "" )
        let rsaKeyPwd = parse.ParseString( "-rsapwd", "" )
        let mutable guid = Guid()
        let mutable rsaKey : byte[]*byte[] = (null, null)
        let clientId = parse.ParseInt( "-clientPid", -1 )
        let clientModuleName = parse.ParseString( "-clientModuleName", "" )
        let clientStartTimeTicks = parse.ParseInt64( "-clientStartTimeTicks", 0L )

        if (requireAuth) then
            guid <- new Guid(guidStr)
            rsaKey <- (Convert.FromBase64String(rsaKeyStrAuth), Convert.FromBase64String(rsaKeyStrExch))

    //    DeploymentSettings.ClientPort <- port
        let memory_size = parse.ParseInt64( "-mem", (DeploymentSettings.MaxMemoryLimitInMB) )
        DeploymentSettings.MaxMemoryLimitInMB <- memory_size
        RemoteExecutionEnvironment.ContainerName <- "Exe:" + name 
    // Need to first write a line to log, otherwise, MakeFileAccessible will fails. 
    // Logger.Log( LogLevel.Info,  sprintf "Logging in New AppDomain...................... %s, %d MB " DeploymentSettings.PlatformFlag (DeploymentSettings.MaximumWorkingSet>>>20)  )
        Logger.Log( LogLevel.Info, ( sprintf "%s executing in new Executable Environment ...................... %s, %d MB " 
                                                   (Process.GetCurrentProcess().MainModule.FileName) DeploymentSettings.PlatformFlag (DeploymentSettings.MaxMemoryLimitInMB) ))
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Current working directory <-- %s " (Directory.GetCurrentDirectory()) ))

        let argsToLog = Array.copy orgargv
        for i in 0..argsToLog.Length-1 do
            if String.Compare(argsToLog.[i], "-rsapwd", StringComparison.InvariantCultureIgnoreCase) = 0 then
                argsToLog.[i + 1] <- "****"

        Logger.Log( LogLevel.Info, ( sprintf "Verbose level = %A, parameters %A " (Logger.DefaultLogLevel) argsToLog ))
    //    MakeFileAccessible( logfname )
    //        let task = Task( SignatureName=name, SignatureVersion=ver )
        Task.StartTaskAsSeperateApp( name, ver, ip, port, jobip, jobport, (requireAuth, guid, rsaKey, rsaKeyPwd), clientId |> Some, clientModuleName |> Some, clientStartTimeTicks |> Some)
        0 // return an integer exit code
        
/// Access state of RemoteContainer 
type RemoteContainer() = 
    /// Get the Listener used by the current container 
    static member DefaultJobListener with get() = Task.DefaultJobListener
