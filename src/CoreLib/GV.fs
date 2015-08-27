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
        GV.fs
  
    Description: 
        Global Variable for Prajna. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Oct. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Linq
open System.Net
open System.Text.RegularExpressions
open Microsoft.FSharp.Collections
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.BytesTools
open Prajna.Tools.Network

type internal FunctionParamType = 
    | None = 0
    | DSet = 1
    | GV = 2
    | DStream = 3 

/// Enumeration class of Traverse Upstream or downstream.
type internal TraverseDirection = 
    // Traverse execution graph upstream
    | TraverseUpstream
    // Traverse execution graph downstream
    | TraverseDownstream



[<AllowNullLiteral>]
/// DParam, setting parameters for DSet mapping
type 
    /// Base of PrajnaDistributed Object
    DParam internal ( cl: Cluster, name:string, ver: DateTime) = 
    let mutable clusterInternal = cl
    let mutable nameInternal = name
    let mutable versionInternal = ver
    let mutable numReplicationsInternal = 1
    let mutable loadBalancerInternal = LoadBlanceAlg.DefaultLoadBlanceAlg() 
    let mutable storageTypeInternal = StorageKind.HDD 
    let mutable sendingQueueLimitInternal = DeploymentSettings.MaxSendingQueueLimit
    let mutable maxDownStreamAsyncTasksInternal = DeploymentSettings.MaxDownStreamAsyncTasks
    let mutable maxCollectionTaskTiemoutInternal = DeploymentSettings.CollectionTaskTimeout
    let mutable partitionerInternal=Partitioner()
    let mutable cacheTypeInternal = CacheKind.None
    let mutable serializationLimit = DeploymentSettings.DefaultSerializationLimit
    member val internal bClusterSet = false with get, set
    /// Get and Set Cluster 
    member x.Cluster with get() = clusterInternal
                      and set(cl) = x.bClusterSet <- true
                                    clusterInternal <- cl        
    /// Get Current Cluster associated with the object, if the current cluster is not set, use the default cluster 
    member x.GetCluster() = 
        let cl = x.Cluster
        if Utils.IsNull cl then 
            match Cluster.Current with
            | Some clDefault -> 
                clusterInternal <- clDefault
                clDefault
            | None -> 
                null
        else
            cl
    member val internal bNameSet = false with get, set
    /// Get and Set name 
    member x.Name with get() = nameInternal 
                   and set(name) = x.bNameSet <- true
                                   nameInternal <- name
    member val internal bVersionSet = false with get, set
    /// Get and Set version 
    member x.Version with get() = versionInternal
                      and set(v) = x.bVersionSet <- true
                                   versionInternal <- v 
    /// Represent version in a string for display 
    member x.VersionString with get() = StringTools.VersionToString(x.Version)
    member val internal bSendingQueueLimitSet = false with get, set
    /// Sender flow control, DSet/DStream limits the total sending queue to SendingQueueLimit
    /// If it is communicating with N peer, each peer, the sending queue limit is SendingQueueLimit/N
    member x.SendingQueueLimit with get() = sendingQueueLimitInternal
                                and set(l) = x.bSendingQueueLimitSet <- true
                                             sendingQueueLimitInternal <- l
    member val internal bMaxDownStreamAsyncTasksSet = false with get, set
    /// Maximum number of tasks that can be pending in a downstream direction per partition
    member internal x.MaxDownStreamAsyncTasks with get() = maxDownStreamAsyncTasksInternal
                                              and set(l) = x.bMaxDownStreamAsyncTasksSet <- true
                                                           maxDownStreamAsyncTasksInternal <- l
    member val internal bMaxCollectionTaskTimeoutSet = false with get, set
    /// Maximum amount of time a single task can run without considered as failed. 
    member internal x.MaxCollectionTaskTimeout with get() = maxCollectionTaskTiemoutInternal
                                                  and set(t) = x.bMaxCollectionTaskTimeoutSet <- true
                                                               maxCollectionTaskTiemoutInternal <- t
    /// Partitioner wraps in two entity, 
    /// TypeOfPartitioner & NumPartitions
    member val internal bParitionerSet = false with get, set
    member internal  x.Partitioner with get() = partitionerInternal
                                    and set( p ) = x.bParitionerSet <- true
                                                   partitionerInternal <- p
    /// Number of partitions  
    abstract NumPartitions : int with get,set 
    default x.NumPartitions with get() = x.Partitioner.NumPartitions
                            and set(n:int) = x.bParitionerSet <- true
                                             partitionerInternal <- Partitioner( partitionerInternal, n )
    /// Is the partition of DSet formed by a key function that maps a data item to an unique partition
    member x.IsPartitionByKey with get() = x.Partitioner.bPartitionByKey
                               and set(b:bool ) = x.bParitionerSet <- true
                                                  partitionerInternal <- Partitioner( partitionerInternal, b )
    member val internal bStorageTypeSet = false with get, set
    /// Storage Type, which include StorageMedia and IndexMethod
    member x.StorageType with get() = storageTypeInternal
                          and set(t) = x.bStorageTypeSet <- true
                                       storageTypeInternal <- t
    /// Do we need to cache the specific DStream/DSet
    member internal x.IsCached with get() = storageTypeInternal=StorageKind.RAM
    /// Retrieve the Storage Media Type alone
    member internal x.StorageMedia with get()= x.StorageType &&& StorageKind.StorageMediumMask
    /// Retrieve the Index Type alone
    // member val bCacheTypeSet = false with get, set
    member internal x.CacheType with get() = cacheTypeInternal 
                                    and set(t) = // x.bCacheTypeSet <- true
                                                 cacheTypeInternal <- t

    member val internal bLoadBalancerSet = false with get, set
    member internal x.LoadBalancer with get() = loadBalancerInternal
                                    and set(l) = x.bLoadBalancerSet <- true
                                                 loadBalancerInternal <- l
    /// Get or Set Load Balancer
    /// Note that the change will affect Partitioner
    member x.TypeOfLoadBalancer 
        with get() = x.LoadBalancer.TypeOf
        and  set( typeOf ) = 
            x.LoadBalancer <- LoadBlanceAlg.CreateLoadBalanceAlg( typeOf )
    member val internal bNumReplicationsSet = false with get, set
    /// Required number of replications for durability
    member x.NumReplications with get() = numReplicationsInternal 
                             and set(r) = x.bNumReplicationsSet <- true
                                          numReplicationsInternal <- r
    /// In BinSort/MapReduce, indicate whether need to regroup collection before sending a collection of data across network 
    member val PreGroupByReserialization = DeploymentSettings.DefaultGroupBySerialization with get, set
    /// In BinSort/MapReduce, indicate the collection size after the a collection of data is received from network  
    member val PostGroupByReserialization = 0 with get, set
    /// Password that will be hashed and used for triple DES encryption and decryption of data. 
    member val Password = "" with get, set
    /// ConfirmDelivery = true:
    /// Turn on logic that will deliver elem to an alternate peer if the current peer is not live. 
    /// The logic is disabled at the moment. 
    member val internal ConfirmDelivery = false with get, set
    /// Flow control, limits the total bytes send out to PeerRcvdSpeedLimit
    /// If it is communicating with N peer, each peer, the sending queue limit is PeerRcvdSpeedLimit/N
    member val PeerRcvdSpeedLimit = 40000000000L with get, set
    member val internal bSerializationLimitSet = false with get, set
    member val internal ChangeSerializationLimit = ( fun (s:int) -> ()) with get, set
    /// Number of record in a collection during data analytical jobs. 
    /// This parameter will not change number of record in an existing collection of a DSet. To change the 
    /// number of record of an existing collection, please use RowsReorg(). 
    member x.SerializationLimit with get() = serializationLimit 
                                and set(s) = serializationLimit <- s
                                             x.bSerializationLimitSet <- true
                                             x.ChangeSerializationLimit(s)
    /// Encode Storage flag 
    member internal x.EncodeStorageFlag( ) = 
        ( if Utils.IsNotNull x.Password && x.Password.Length>0 then 0x01 else 0x00 ) |||
        ( if x.ConfirmDelivery then 0x02 else 0x00 )
    /// A null mapping matrix should only be used for ClusterReplicationType.ClusterReplicate mode, in which a partition can be attached to any peer
    member val internal Mapping : int[][] = null with get, set
    /// Construct a empty DParam object
    new () = 
        DParam( null, "", DateTime.MinValue ) 
    /// Copy metadata
    member internal x.ReplicateBaseMetadata( dobj: DParam ) = 
        if not x.bClusterSet then 
            clusterInternal <- dobj.Cluster
        if not x.bLoadBalancerSet then 
            loadBalancerInternal <- dobj.LoadBalancer 
        if not x.bParitionerSet then 
            partitionerInternal <- dobj.Partitioner
        if not x.bVersionSet then 
            versionInternal <- dobj.Version
        if not x.bNumReplicationsSet then 
            numReplicationsInternal <- dobj.NumReplications
        if not x.bStorageTypeSet then 
            storageTypeInternal <- dobj.StorageType
        if not x.bMaxCollectionTaskTimeoutSet then 
            maxCollectionTaskTiemoutInternal <- dobj.MaxCollectionTaskTimeout
// Cache type does not usually propagate, it is tied to the current object
//        if not x.bCacheTypeSet then 
//            x.CacheType <- dobj.CacheType
        if not x.bSendingQueueLimitSet then 
            x.SendingQueueLimit <- dobj.SendingQueueLimit
    /// Construct a DParam object based on a prior template 
    new (dobj:DParam ) as x = 
        DParam( dobj.Cluster, dobj.Name, dobj.Version ) 
        then 
            x.ReplicateBaseMetadata( dobj: DParam )

/// Cache information. 
type internal ClusterJobInfoFactory() = 
    inherit CacheFactory<ClusterJobInfo>()
    static member FullName( sigName, sigVersion:int64, clJobInfoName, clJobInfoVersionTicks ) = 
        sigName + sigVersion.ToString("X") + ":" + clJobInfoName + Version64ToString( clJobInfoVersionTicks )
    static member ResolveClusterJobInfoFullname( fullname ) = 
        match ClusterJobInfoFactory.Retrieve( fullname ) with
        | Some clJobInfo ->
            clJobInfo
        | None ->
            null
    // Retrieve a DSet
    static member ResolveClusterJobInfo( tuple ) = 
        let fullname = ClusterJobInfoFactory.FullName( tuple ) 
        ClusterJobInfoFactory.ResolveClusterJobInfoFullname( fullname ) 
    // Cache a DSet, if there is already a DSet existing in the factory with the same name and version information, then use it. 
    static member CacheClusterJobInfo( sigName, sigVersion, clJobInfo: ClusterJobInfo ) = 
        let fullname = ClusterJobInfoFactory.FullName( sigName, sigVersion, clJobInfo.Name, clJobInfo.Version.Ticks ) 
        let oldClusterJobInfo = ClusterJobInfoFactory.ResolveClusterJobInfoFullname( fullname )
        if Utils.IsNotNull oldClusterJobInfo then 
            oldClusterJobInfo
        else
            ClusterJobInfoFactory.Store( fullname, clJobInfo )
            clJobInfo
and /// Information of Within Job cluster information. 
    [<AllowNullLiteral>]
    internal ClusterJobInfo(name, ver:DateTime, numNodes ) = 
    static member val internal JobListenningPortCollection = ConcurrentDictionary<int, bool>() with get
    member val Name = name with get, set
    member val Version:DateTime = ver with get, set
    member val LinkedCluster: Cluster = null with get, set
    member x.VersionString with get() = StringTools.VersionToString( x.Version ) 
    member val bValidMetadata = false with get, set
    member val internal NodesInfo = if numNodes=0 then null else Array.zeroCreate<NodeWithInJobInfo> numNodes with get, set    
    member x.Pack( ms:StreamBase<byte> ) =
        ms.WriteString( x.Name ) 
        ms.WriteInt64( x.Version.Ticks )
        ms.WriteVInt32( x.NodesInfo.Length ) 
        for i = 0 to x.NodesInfo.Length - 1 do 
            let nodeInfo = x.NodesInfo.[i] 
            if Utils.IsNull nodeInfo then 
                ms.WriteVInt32( int NodeWithInJobType.NonExist )
            else
                nodeInfo.Pack( ms ) 
    static member Unpack( ms:StreamBase<byte> ) =
        let x = ClusterJobInfo( null, DateTime.MinValue, 0 ) 
        x.Name <- ms.ReadString() 
        x.Version <- DateTime( ms.ReadInt64() ) 
        let numNodes = ms.ReadVInt32() 
        x.NodesInfo <- Array.zeroCreate<NodeWithInJobInfo> numNodes
        for i = 0 to x.NodesInfo.Length - 1 do 
            x.NodesInfo.[i] <- NodeWithInJobInfo.Unpack( ms ) 
        x
    member x.Validate( cl: Cluster ) = 
        x.Name=cl.Name && x.Version=cl.Version && x.NodesInfo.Length=cl.NumNodes
    override x.ToString() = 
        seq { 
            yield sprintf "Within Job Cluster Information for %s:%s, %d Nodes" x.Name x.VersionString x.NodesInfo.Length
            for i = 0 to x.NodesInfo.Length - 1 do 
                if Utils.IsNull x.NodesInfo.[i] then 
                    yield sprintf "Node %d not online" i
                else
                    yield sprintf "Node %d: %s" i (x.NodesInfo.[i].ToString())
        } |> String.concat Environment.NewLine
    member val CurPeerIndex = Int32.MinValue with get, set
    /// Get the Index of the Current Peer
    member x.GetCurPeerIndex() = 
        if x.CurPeerIndex = Int32.MinValue then 
            let nodes = x.LinkedCluster.Nodes
            let curMachineName = Config().MachineName
            let mutable idx = 0
            let mutable bFound = false
            while idx < nodes.Length && not bFound do 
                // Match both machine name and listening port
                if (System.Text.RegularExpressions.Regex.Match(nodes.[idx].MachineName, "^"+curMachineName+"""(\.|$)""", Text.RegularExpressions.RegexOptions.IgnoreCase).Success)  
                    && not (Utils.IsNull x.NodesInfo.[idx]) 
                    && (ClusterJobInfo.JobListenningPortCollection.ContainsKey(x.NodesInfo.[idx].ListeningPort)) then 
                    bFound <- true
                if (not bFound) then 
                    idx <- idx + 1
            if idx >= nodes.Length then
                idx <- 0
                while idx < nodes.Length && not bFound do
                    let machineName = Dns.GetHostEntry(nodes.[idx].MachineName)
                    // Match both machine name and listening port
                    if System.Text.RegularExpressions.Regex.Match(machineName.HostName, "^"+curMachineName+"""(\.|$)""", RegexOptions.IgnoreCase).Success || String.Compare(machineName.HostName, "localhost", StringComparison.InvariantCultureIgnoreCase) = 0 then
                        if not (Utils.IsNull x.NodesInfo.[idx]) 
                            && (ClusterJobInfo.JobListenningPortCollection.ContainsKey(x.NodesInfo.[idx].ListeningPort)) then 
                            bFound <- true
                    if (not bFound) then
                        idx <- idx + 1

            if idx >= nodes.Length then 
                x.CurPeerIndex <- -1
            else
                x.CurPeerIndex <- idx
        x.CurPeerIndex
    member x.QueueForWriteBetweenContainer( peeri ) =
        let queue = x.LinkedCluster.Queues.[peeri]
        if Utils.IsNull queue then
            try 
                let ndInfo = x.NodesInfo.[peeri] 
                if Utils.IsNull ndInfo then 
                    null
                else
                    let queue = 
                        match ndInfo.NodeType with 
                        | NodeWithInJobType.TCPOnly -> 
                            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Attempt to connect to %s:%d as peer %d" x.LinkedCluster.Nodes.[peeri].MachineName ndInfo.ListeningPort peeri ))
                            //Cluster.Connects.AddConnect( x.LinkedCluster.Nodes.[peeri].MachineName, ndInfo.ListeningPort )
                            //for azure cluster
                            Cluster.Connects.AddConnect(  IPAddress(x.LinkedCluster.Nodes.[peeri].InternalIPAddress.[0]), ndInfo.ListeningPort )
                        | _ -> 
                            Logger.Fail( sprintf "ClusterJobInfo.QueueForWriteBetweenContainer, in cluster %s:%s, unknown node type %A for peer %d to connect to" 
                                            x.LinkedCluster.Name x.LinkedCluster.VersionString ndInfo.NodeType peeri )
                    queue.AddRecvProc (Cluster.ParseHostCommand queue peeri) |> ignore
                    queue.OnConnect.Add( fun _ -> let ms = new MemStream(1024)
                                                  ms.WriteString( x.LinkedCluster.Name )
                                                  ms.WriteInt64( x.LinkedCluster.Version.Ticks )
                                                  ms.WriteVInt32( x.CurPeerIndex )
                                                  queue.ToSend( ControllerCommand( ControllerVerb.ContainerInfo, ControllerNoun.Job ), ms )
                                                   )
                    x.LinkedCluster.Queues.[peeri] <- queue
                    queue
            with 
            | e -> 
                Logger.Fail( sprintf "ClusterJobInfo.QueueForWriteBetweenContainer fails with exception %A" e )
        else
            queue
    member x.Queue( peeri ) = 
        x.LinkedCluster.Queues.[peeri]  



/// Internal data structure to pass data analytical job related information 
/// Class exposed because of being used in an Abstract function
[<AllowNullLiteral>]
type JobInformation internal () = 
    /// ClusterInfo object that holds in Job node information of a cluster. 
    /// To differentiate job 
    member val internal JobName = "" with get, set
    member val internal DSetName = "" with get, set
    member val internal ClustersInfo = List<ClusterJobInfo>() with get, set
    member val internal CancellationToken =  new CancellationTokenSource() with get
//    member val MaxSingleTaskTimeout = DeploymentSettings.SingleTaskTimeout with get, set
    static member internal NullReportClosePartition( jbInfo:JobInformation, dobj:DistributedObject, meta:BlobMetadata, pos:int64 ) = 
        ()
    member val internal ReportClosePartition = JobInformation.NullReportClosePartition with get, set
    member val internal HostQueue : NetworkCommandQueue = null with get, set
    member val internal ForwardEndPoint : Net.IPEndPoint = null with get, set  
    member val internal FoldState = ConcurrentDictionary<int, Object>() with get  
    member val internal JobReady : ManualResetEvent = null with get, set
    member internal x.ToSendHost( cmd, ms ) = 
        if Utils.IsNotNull x.HostQueue && not x.HostQueue.Shutdown then 
            // Feedback to host are not subject to flow control, we assume that the information in feedback is small
            if Utils.IsNotNull x.ForwardEndPoint then 
                x.HostQueue.ToForward( x.ForwardEndPoint, cmd, ms )
            else
                x.HostQueue.ToSend( cmd, ms ) 
    member internal x.ErrorMsg msg = 
        Logger.Log( LogLevel.Error, msg )
        let ms = new MemStream( msg.Length * 2 + 128 ) 
        ms.WriteString( msg ) 
        x.ToSendHost( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), ms )
    member internal x.HostShutDown with get() = Utils.IsNull x.HostQueue || x.HostQueue.Shutdown
    member internal x.bAvailableToSend( queueLimit:int ) = x.HostQueue.CanSend && x.HostQueue.SendQueueLength<5 && x.HostQueue.UnProcessedCmdInBytes < int64 queueLimit

/// Used for Async Traverse, 
/// A AsyncTaskQueue wraps a series of Async Tasks, which is executed one after another, garantted in order.  
/// Each task is of type 
and [<AllowNullLiteral>]
    internal AsyncTaskQueue(dobj: DistributedObject, maxAsyncTasks:int, cts:CancellationTokenSource) =  
    // This empty object is needed to facilitate compare & exchange
    static member val EmptyTask = Unchecked.defaultof<_> with get
    member val MaxAsyncTasks = maxAsyncTasks with get, set
    member val CTS = cts with get, set
    member val DObj = dobj with get, set // The distributed object that associated with the job
    member val CurTask : (BlobMetadata*Async<unit>) ref = ref AsyncTaskQueue.EmptyTask with get
    member val TaskQueue = ConcurrentQueue<_>() with get, set
    /// Last operation succeed, try execute another task 
    member x.TryExecute( ) = 
        let mutable bOpsSucceeded = false
        // The condition to stay in loop is:
        // 1. Cancellation is not called
        // 2. There is still task to execute
        // 3. No other task is in execution x.CurTask is null 
        // 4. Current execution does not succeed
        while (not (x.CTS.IsCancellationRequested)) && not x.TaskQueue.IsEmpty && Object.ReferenceEquals( !x.CurTask, AsyncTaskQueue.EmptyTask) && not bOpsSucceeded do 
            let bSuccess, firstTask = x.TaskQueue.TryPeek()
            if bSuccess then 
                // Can't compare and exchange with Unchecked.defaultof<_>
                let curTask = Interlocked.CompareExchange( x.CurTask, firstTask, AsyncTaskQueue.EmptyTask  )
                if (Object.ReferenceEquals( curTask, AsyncTaskQueue.EmptyTask )) then 
                    // x.CurTask is set, successfully execute the task. 
                    let ta = ref AsyncTaskQueue.EmptyTask
                    let meta, exetask = !(x.CurTask)
                    while not (x.TaskQueue.TryDequeue( ta )) && (not x.TaskQueue.IsEmpty) do 
                        Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Fail to dequeue %A(%s:%s) %s, we should have the lock, queue depth %d" x.DObj.ParamType x.DObj.Name x.DObj.VersionString (meta.ToString()) x.TaskQueue.Count ))
// Wrong, need to reset CurTask to execute
//                    if x.TaskQueue.IsEmpty then 
//                        Async.Start( !(x.CurTask), x.CTS.Token )
//                    else
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Execute job %A %s:%s %s" x.DObj.ParamType x.DObj.Name x.DObj.VersionString (meta.ToString()) ))
                    Async.StartWithContinuations( exetask, x.ExecuteNext, x.ExceptionContinuation, x.CancelContinuation, x.CTS.Token )
                    bOpsSucceeded <- true
                else
                    // Execution is unsuccessful, spin and wait
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> 
                       let meta, _ = curTask
                       sprintf "ConcurrentQueue contention in executing %A %s:%s %s" x.DObj.ParamType x.DObj.Name x.DObj.VersionString (meta.ToString()) ))
    /// Add a set of async task to queue
//    member x.AddRange( tasks: seq<Async<unit>> ) = 
//        let mutable bOpsSucceeded = false
//        while (not (x.CTS.IsCancellationRequested)) && not bOpsSucceeded do 
//            let oldList = !x.TaskQueue
//            let newList = List<_>( oldList )
//            newList.AddRange( tasks ) 
//            let retValue = Interlocked.CompareExchange( x.TaskQueue, newList, oldList) 
//            if Object.ReferenceEquals( retValue, oldList ) then 
//                bOpsSucceeded <- true   
    member x.AddRange( tasks: seq<BlobMetadata*Async<unit>> ) = 
        for ta in tasks do 
            let meta, _ = ta
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Enqueue job %A(%s) %s" x.DObj.ParamType x.DObj.Name (meta.ToString()) ))
            while x.TaskQueue.Count >= x.MaxAsyncTasks do 
                // If there are too many tasks, try to execute some. 
                x.TryExecute() 
                if x.TaskQueue.Count >= x.MaxAsyncTasks then 
                    Threading.Thread.Sleep(0)
            x.TaskQueue.Enqueue( ta )
        x.TryExecute()
    /// Exeception continuation, both case, remove task to unblock execution
    member x.ExceptionContinuation exn = 
        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "AsyncTaskQueue has exception %A" exn ) )
        x.CurTask := AsyncTaskQueue.EmptyTask
    /// Cancellation continueation
    member x.CancelContinuation res = 
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "AsyncTaskQueue operations is cancelled %A" res ) )
        x.CurTask := AsyncTaskQueue.EmptyTask
    /// Continue with execution 
    member x.ExecuteNext() = 
        x.CurTask := AsyncTaskQueue.EmptyTask
        x.TryExecute( )
//    member x.TryExecute( ) = 
//        let mutable bOpsSucceeded = false
//        // The condition to stay in loop is:
//        // 1. Cancellation is not called
//        // 2. There is still task to execute
//        // 3. No other task is in execution x.CurTask is null 
//        // 4. Current execution does not succeed
//        while (not (x.CTS.IsCancellationRequested)) && (!x.TaskQueue).Count>0 && Object.ReferenceEquals( !x.CurTask, Unchecked.defaultof<_>) && not bOpsSucceeded do 
//            let oldList = !x.TaskQueue
//            if oldList.Count>0 then 
//                let oldTask = Interlocked.CompareExchange( x.CurTask, oldList.[0], Unchecked.defaultof<_> )
//                if not (Object.ReferenceEquals( oldTask, Unchecked.defaultof<_> )) then 
//                    // x.CurTask is not null any more, some other jobs of x.ExecuteOneMore has already executed the task 
//                    bOpsSucceeded <- true
//                else   
//                    // We got the lock to execute the current task 
//                    let newList = oldList.GetRange( 1, oldList.Count - 1 )
//                    let retValue = Interlocked.CompareExchange( x.TaskQueue, newList, oldList) 
//                    if Object.ReferenceEquals( retValue, oldList ) then 
//                        bOpsSucceeded <- true
//                        if newList.Count>0 then 
//                            Async.StartWithContinuations( !(x.CurTask), x.ExecuteNext, x.ExceptionContinuation, x.CancelContinuation )
//                        else
//                            Async.Start( !(x.CurTask), x.CTS.Token )
//                    else
//                        // Retry, fail to acquire lock, release the lock on the current task to try to reexecute
//                        x.CurTask := Unchecked.defaultof<_> 
    /// Wait for all task to be executed. 
    [<Obsolete("Use New Version with AsyncWaitAll")>]
    member x.WaitAll() = 
        while (not (x.CTS.IsCancellationRequested)) && not x.TaskQueue.IsEmpty do 
            x.TryExecute() 
            if not x.TaskQueue.IsEmpty then 
                // The call TryExecute() will attempt to execute all task in sequence. 
                Thread.Sleep( 5 )
        while (not (x.CTS.IsCancellationRequested)) && not (Object.ReferenceEquals( !x.CurTask, AsyncTaskQueue.EmptyTask)) do 
            // Any current task still in execution? 
            SystemBug.Sleep( 5 )                
        // In case cancellation is called, clear task queue
        if x.CTS.IsCancellationRequested then 
            // Clear remaining task, notice ConcurrentQueue doesn't have a clear method
            x.TaskQueue <- ConcurrentQueue<_>()
            x.CurTask := AsyncTaskQueue.EmptyTask
    member x.AsyncWaitAll() = 
        async {
            while (not (x.CTS.IsCancellationRequested)) && not x.TaskQueue.IsEmpty do 
                x.TryExecute() 
                if not x.TaskQueue.IsEmpty then 
                    // The call TryExecute() will attempt to execute all task in sequence. 
//                    do! Async.Sleep(5)
                    SystemBug.Sleep( 5 )
            while (not (x.CTS.IsCancellationRequested)) && not (Object.ReferenceEquals( !x.CurTask, AsyncTaskQueue.EmptyTask)) do 
                // Any current task still in execution? 
                SystemBug.Sleep( 5 )                
            // In case cancellation is called, clear task queue
            if x.CTS.IsCancellationRequested then 
                // Clear remaining task, notice ConcurrentQueue doesn't have a clear method
                x.TaskQueue <- ConcurrentQueue<_>()
                x.CurTask := AsyncTaskQueue.EmptyTask
        }        
    /// Has all tasks been completed?
    /// This is equivalent to check that the task queue is empty & no operation is being executed currently. 
    member x.IsEmpty with get() = x.TaskQueue.IsEmpty && Object.ReferenceEquals( !x.CurTask, AsyncTaskQueue.EmptyTask)
    member x.IsFull with get() = (x.TaskQueue.Count >= x.MaxAsyncTasks)

and /// Dependency of PrajnaObject to construct link between DSet
    [<AllowNullLiteral>]
    [<System.Diagnostics.DebuggerDisplay("{DebuggerDisplay()}")>]
    internal DependentDObject ( target: DistributedObject, hash:byte[] ) =
    member val Target = target with get, set
    member val Hash = hash with get, set
    member val ParamType = if Utils.IsNotNull target then target.ParamType else FunctionParamType.None with get, set
    new ( target ) = 
        DependentDObject( target, if Utils.IsNull target.Blob then null else target.Blob.Hash )
    new ( hash ) = 
        DependentDObject( null, hash )
    new () = 
        DependentDObject( null, null )
    // Display object
    override x.ToString () =
        x.Display false
    member private x.DebuggerDisplay() =
        x.Display true
    member private x.Display showHash =
        if Utils.IsNull x.Target then 
            "<null>" 
        else if Utils.IsNull x.Target.Name then 
            "<Unresolved>" 
        else if showHash then
            (sprintf "%s (%s) (Hash: %s)" x.Target.Name (x.ParamType.ToString()) (BytesToHex(x.Hash)))
        else
            (sprintf "%s (%s)" x.Target.Name (x.ParamType.ToString()))

    static member Pack( depDObjectArray:DependentDObject[], ms:StreamBase<byte> ) =
        let len = depDObjectArray.Length
        ms.WriteVInt32( len )
        if len > 0 then 
            for i=0 to depDObjectArray.Length - 1 do
                let dep = depDObjectArray.[i]
                ms.WriteVInt32( int dep.ParamType )
                ms.WriteBytesWVLen( dep.Hash )
                if Utils.IsNull dep.Hash then 
                    // Maybe OK (execution graph which has dangling edges. 
                    Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Caution (maybe OK) Encode dependency %A %s:%s before it has been precoded" dep.ParamType dep.Target.Name dep.Target.VersionString ))
                    ()
    static member Unpack( ms: StreamBase<byte> ) = 
        let len = ms.ReadVInt32() 
        if len > 0 then 
            let depDObjectArray = Array.zeroCreate<_> len
            for i = 0 to len - 1 do 
                let paramTypeNum = ms.ReadVInt32()
                let paramType = enum<FunctionParamType>( paramTypeNum  )
                let hash = ms.ReadBytesWVLen() 
                depDObjectArray.[i] <- DependentDObject( hash, ParamType=paramType )
            depDObjectArray 
        else 
            Array.empty
and /// Enumeration class that determines how to derive partition mapping for DSet/DStream
    internal ParentMapping = 
    /// Error 
    | UndefinedMapping
    /// The class should support a function to generate the partition mapping 
    | GenerateMapping
    /// Each partition is available on any node of the cluster. 
    | FullMapping 
    /// Use partition mapping of a dependent obect 
    | UseParent of DependentDObject
    /// Aggregate the partition mapping of multiple parents to form the partition mapping of the current DSet
    | AggregateParents of IEnumerable<DependentDObject>
    /// Use a particular partition mapping matrix. 
    | UseMapping of int[][]
and [<AllowNullLiteral>]
    /// Base of DStream/DSet, all exposed functions in this class is internal function to be executed 
    /// during a data analytical jobs. They shoould not be used by programmer. 
    DistributedObject internal ( cl: Cluster, ty:FunctionParamType, name:string, ver: DateTime) as this = 
    inherit DParam( cl, name, ver) 
    internal new () = 
        DistributedObject( null, FunctionParamType.None, "", DateTime.MinValue )

    /// <summary> 
    /// Blob that represent the coded stream of the current object. It is used to speed up serialization (i.e., if the Hash exists, we assume 
    /// that the DistributedObject has been serialized and coded in the current form). If you have changed any internal operation of the DistributedObject, 
    /// e.g., mapping, closure, please explicitly set the Blob to null, which force a reserialization of the current object.  
    /// </summary>
    member val internal Blob : Blob = null with get, set
//    member x.Hash with get() = if Utils.IsNull x.Blob then null else x.Blob.Hash
    member val internal Hash : byte[] = null with get, set
    /// encode object links. 
    member val internal PrecodeDependentObjs: unit -> seq<DependentDObject> = fun _ -> Seq.empty with get, set
    /// Setup dependency hash
    member val internal SetupDependencyHash: unit -> unit = fun _ -> () with get,set
    member val internal ParamType = ty with get, set
    /// internal clock frequency
    member val internal ClockFrequency = System.Diagnostics.Stopwatch.Frequency with get
    // clock for communication 
    // let clock = System.Diagnostics.Stopwatch.StartNew()
    // clock frequency
    // let clockFrequency = System.Diagnostics.Stopwatch.Frequency
    /// a stop watch for timeout management. 
    member val internal Clock = System.Diagnostics.Stopwatch.StartNew() with get
    /// Should this Mapping be encoded?
    member val internal bEncodeMapping = true with get, set
    /// Get a full mapping
    member val internal FullMappingI = null with get, set
    /// Get a full mapping of node i
    /// If x.Cluster is not set, the call will result in an exception. 
    member internal x.GetFullMappingI() = 
        if Utils.IsNull x.FullMappingI then 
            x.FullMappingI <- Array.init<int> (x.Cluster.NumNodes) ( fun i -> i )
        x.FullMappingI 
    member val internal FullMapping = null with get, set
    /// Initialize partition
    member val internal InitializePartition: unit -> unit = this.InitializePartitionImpl with get, set
    // Initialize Partition, if necessary
    member private x.InitializePartitionImpl() =
        let nodes, resources = x.Cluster.GetNodeIDsWithResource( PartitionByKind.Uniform )
        let nodeID = nodes |> Array.map ( fun node -> node.MachineID )
        // First setup load balancer, with ID & resource
        x.LoadBalancer.Set( nodeID, resources )
        // Use default partitioner if none is specified. 
        MetaFunction.DefaultInitializePartitioner( x.Cluster, x.LoadBalancer, x.Partitioner )
    /// Get a full mapping for the class
    /// If x.Cluster or partition information is not set, the call will result in an exception.
    member internal x.GetFullMapping() =
        if Utils.IsNull x.FullMapping then 
            if x.NumPartitions<0 then 
                x.InitializePartition()    
            x.FullMapping <- Array.create (x.NumPartitions) (x.GetFullMappingI())
        x.FullMapping        

    /// Is the metadata of the current DObject valid
    member val internal bValidMetadata = false with get, set
    /// Function to setup partition. 
    member val internal SetupPartitionMapping: unit -> unit  = this.GeneratePartitionMapping with get, set
    
    // The raw value for NumPartitions. This call will not fail
    member internal x.NumPartitionsRaw with get () = base.NumPartitions

    /// Get num of partitions
    override x.NumPartitions with get() = let np = base.NumPartitions
                                          if np <= 0 then
                                              x.SetupPartitionMapping()
                                              let newNp = base.NumPartitions
                                              if newNp <= 0 then
                                                  failwith(sprintf "The partitions of DSet %s are not properly initialized in the current execution graph" x.Name)
                                              else newNp
                                           else np                    
                               and set(n:int) = base.NumPartitions <- n

    member internal x.OperationToGeneratePartitionMapping() =
            if x.NumReplications > x.Cluster.NumNodes then 
                let msg = sprintf "Replication %d is larger than number of nodes in the cluster %d, execution fails" x.NumReplications x.Cluster.NumNodes
                Logger.Log( LogLevel.Error, msg )
                failwith msg
            x.InitializePartition()
            // Finally, set up mapping of the load balancer. 
            // Note: always pass 0UL as contentKey for stable partition mapping across different DSets. Revisit this design later.
            x.Mapping <- x.LoadBalancer.GetPartitionMapping( 0UL, x.NumPartitions, x.NumReplications )
            // x.Mapping <- x.LoadBalancer.GetPartitionMapping( uint64 x.Version.Ticks, x.NumPartitions, x.NumReplications )
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "------- Generate Mapping Matrix for %A (%s:%s) -----------\n%s" x.ParamType x.Name x.VersionString (x.MappingToString()) ))
            
    member internal x.GeneratePartitionMapping() =
        /// Mapping information will be generated. 
        if not x.bValidMetadata then
            x.OperationToGeneratePartitionMapping()
            x.bValidMetadata <- true
    member internal x.MappingToString( ) = 
        seq {
            if Utils.IsNull x.Mapping then 
                yield "Null Mapping"
            else
                for i = 0 to x.Mapping.Length - 1 do
                    yield ( sprintf "%d : %A" i x.Mapping.[i] )
        } |> String.concat Environment.NewLine
    /// Retrieve the current mapping
    member internal x.GetMapping() = 
        let mutable errorMsg = null
        if Utils.IsNull x.Mapping then 
            let parentMapping = x.GetParentMapping() 
            match parentMapping with 
            | FullMapping -> 
                x.bEncodeMapping <- false
                x.Mapping <- x.GetFullMapping()
            | UseParent p -> 
                let parent = if Utils.IsNotNull p then p.Target else null
                if Utils.IsNotNull parent then 
                    if Object.ReferenceEquals(parent.Cluster, x.Cluster) then 
                    // If the parent is not null & it is at the same cluster, use parent's mapping
                    // There is no need to encode the mapping for this Object
                        x.bEncodeMapping <- false
                        x.Mapping <- parent.GetMapping()
                        // Propagate number of partition information. 
                        x.NumPartitions <- parent.NumPartitions
                    else
                        // If the parent is at a different cluster, we assume that the parent object is reliable, 
                        // therefore, it is always a full mapping at the current object. 
                        // ToDo: check further
                        x.bEncodeMapping <- false
                        x.Mapping <- x.GetFullMapping()
                else
                    errorMsg <- sprintf "PrajnaVariableBase.GetMapping fails, UseParent but with a null parent object, for object %s:%s" x.Name x.VersionString 
            | AggregateParents parents -> 
                let mutable mapping = List<_>()
                for pa in parents do 
                    let pobj = if Utils.IsNotNull pa then pa.Target else null
                    if Utils.IsNotNull pobj && Utils.IsNotNull (pobj.GetMapping()) then 
                        if Utils.IsNotNull mapping then 
                            mapping.AddRange( pobj.GetMapping() )
                    else
                        // One of the parent mapping is not resolved. 
                        mapping <- null
                x.bEncodeMapping <- false
                if Utils.IsNotNull mapping then 
                    x.Mapping <- mapping.ToArray()
                    x.NumPartitions <- x.Mapping.Length
                else
                    // Need to recalculate mapping 
                    x.Mapping <- null 
                    x.NumPartitions <- 0
            | UseMapping parentMapping ->
                x.bEncodeMapping <- false
                x.Mapping <- parentMapping 
                // Number of partition is defined by the parentMapping Matrix. 
                x.NumPartitions <- parentMapping.Length
            | GenerateMapping -> 
                x.SetupPartitionMapping()
                x.bEncodeMapping <- true
            | UndefinedMapping ->
                errorMsg <- sprintf "PrajnaVariableBase.GetMapping fails, GetParentMapping() return Undefined for %A %s:%s" x.ParamType x.Name x.VersionString 
            if Utils.IsNull x.Mapping then
                if Utils.IsNull errorMsg then 
                    errorMsg <- sprintf "PrajnaVariableBase.GetMapping fails, can't resolve partition mapping for %A %s:%s" x.ParamType x.Name x.VersionString 
                Logger.Log( LogLevel.Error, errorMsg )
                failwith errorMsg
        x.Mapping
    /// Obtain the mapping matrix for partition i. 
    member internal x.PartiMapping( parti ) = 
        x.GetMapping().[parti]
    /// Internal function to derive partition mapping 
    member val internal GetParentMapping : unit ->  ParentMapping = fun _ -> ParentMapping.UndefinedMapping with get, set
    /// Get current mapping
    member val internal JobMapping = null with get, set
    member internal x.GetCurrentMapping() = 
        if Utils.IsNull x.JobMapping then 
            let mapping = x.GetMapping()
            x.JobMapping <- mapping |> Array.map ( fun arr -> arr.[0] ) 
        x.JobMapping
    /// Copy metadata
    member internal x.UpdateBaseMetadata( dobj: DistributedObject ) = 
            x.Cluster <- dobj.Cluster
            x.TypeOfLoadBalancer <- dobj.TypeOfLoadBalancer
            x.Partitioner <- dobj.Partitioner
            x.NumPartitions <- dobj.NumPartitions
            x.Version <- dobj.Version
            x.NumReplications <- dobj.NumReplications
            x.StorageType <- dobj.StorageType
            x.SendingQueueLimit <- dobj.SendingQueueLimit
            x.Mapping <- dobj.Mapping
            x.NumReplications <- dobj.NumReplications

    member internal x.PackBase( ms: StreamBase<byte> ) = 
        /// Only take input of HasPassword flag, other flag is igonored. 
        let ticks = x.Cluster.Version.Ticks
        ms.WriteString( x.Cluster.Name )
        ms.WriteInt64( ticks )
        ms.WriteVInt32( int x.TypeOfLoadBalancer )
        ms.WriteVInt32( int x.Partitioner.TypeOf )
        ms.WriteVInt32( int x.StorageType )
        ms.WriteString( x.Name )
        ms.WriteInt64( x.Version.Ticks ) 
        ms.WriteInt32( x.SendingQueueLimit )
        ms.WriteVInt32( x.MaxDownStreamAsyncTasks ) 
        ms.WriteInt32( x.MaxCollectionTaskTimeout )
        let numPartitions = if Utils.IsNotNull x.Mapping then x.NumPartitions else -x.NumPartitions 
        ms.WriteVInt32( numPartitions )
        let numReplications = 
            if x.bEncodeMapping then 
                x.NumReplications
            else
                - x.NumReplications
        ms.WriteVInt32( numReplications ) 
        if numReplications > 0 && numPartitions>0 then 
            for p = 0 to x.NumPartitions-1 do
                ms.WriteVInt32( x.Mapping.[p].Length )
                for r = 0 to x.Mapping.[p].Length-1 do
                    ms.WriteVInt32( x.Mapping.[p].[r] )
    // Peek readStream to get name and version information, read pointer is set back to the origin.
    static member internal PeekBase( readStream: MemStream ) = 
        let orgpos = readStream.Position
        let dobj = DistributedObject()
        dobj.UnpackHead( readStream )
        readStream.Seek( orgpos, SeekOrigin.Begin ) |> ignore
        dobj.Name, dobj.Version.Ticks
    member internal x.UnpackHead( readStream:StreamBase<byte> ) = 
        let clname = readStream.ReadString()
        let clVerNumber = readStream.ReadInt64()
//        let verCluster = DateTime( ticks )
//        let clusterName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( clname, verCluster ) 
        let loadBalancerType = enum<LoadBalanceAlgorithm>( readStream.ReadVInt32() )
        let partType = readStream.ReadVInt32()
        let partitioner = Partitioner( TypeOf = enum<_>(partType) )
        let storageType = enum<StorageKind>( readStream.ReadVInt32() )
        let name = readStream.ReadString()
        let ver = DateTime( readStream.ReadInt64() )
        let sendingQueueLimit = readStream.ReadInt32()
        let maxAsncQueue = readStream.ReadVInt32()
        let maxCollectionTaskTimeout = readStream.ReadInt32( )
        let useCluster = ClusterFactory.FindCluster( clname, clVerNumber )
        x.Cluster <- useCluster
        x.TypeOfLoadBalancer <- loadBalancerType
        x.Partitioner <- partitioner
        x.StorageType <- storageType
        x.Name <- name
        x.Version <- ver
        x.SendingQueueLimit <- sendingQueueLimit
        x.MaxDownStreamAsyncTasks <- maxAsncQueue
        x.MaxCollectionTaskTimeout <- maxCollectionTaskTimeout
    member internal x.UnpackBase( readStream:StreamBase<byte> ) = 
        x.UnpackHead( readStream )
        let numPartitions = readStream.ReadVInt32()
        x.NumPartitions <- Math.Abs( numPartitions )
        let numReplications = readStream.ReadVInt32()
        if numReplications > 0 && numPartitions > 0 then 
            x.NumReplications <- numReplications
            // DSet unpacking does not call initialize. 
            // Set Mapping, may not be necessary. 
            let mapping = Array.create<int[]> numPartitions null 
            for p = 0 to numPartitions-1 do
                let numRep = readStream.ReadVInt32()
                mapping.[p] <- Array.zeroCreate<int> numRep 
                for r = 0 to numRep-1 do
                    mapping.[p].[r] <- readStream.ReadVInt32()
            x.Mapping <- mapping 
        else
            x.NumReplications <- Math.Abs( numReplications )
            if numReplications = 0 then 
                /// Flat mapping 
                if Utils.IsNotNull x.Cluster then 
                    x.NumReplications <- x.Cluster.NumNodes
            /// Undetermined mapping, the mapping should be resolved when x.GetMapping() is called. 
            x.Mapping <- null
            x.bEncodeMapping <- false
    member internal x.PeekBase( readStream:StreamBase<byte> ) = 
        let pos = readStream.Position
        x.UnpackBase( readStream ) 
        readStream.Seek( pos, SeekOrigin.Begin )  |> ignore  
    /// Iterate operation upstream
    /// Return: 
    ///     _, true: iterate operation completes
    ///     handle, false: iterate operation wait on a certain handle
    ///     null, false: iterate operation complete one step, to execute again. 
    member val internal SyncIterate: JobInformation -> int -> ( BlobMetadata*Object -> unit ) -> ManualResetEvent * bool = this.SyncIterateImpl with get, set
    member private x.SyncIterateImpl (jbInfo:JobInformation) (parti : int) (func : BlobMetadata*Object -> unit) : ManualResetEvent * bool = 
        null, true
    /// Iterate operation downstream, the synchronous interface.
    member val internal SyncDecodeToDownstream: JobInformation -> int -> BlobMetadata -> Object -> unit = this.SyncDecodeToDownstreamImpl with get, set
    /// Iterate operation downstream, the synchronous interface.   
    member private x.SyncDecodeToDownstreamImpl jbInfo parti meta o = 
        x.SyncExecuteDownstream jbInfo parti meta o

    member val internal ThreadPool : ThreadPoolWithWaitHandles<int> = null with get, set
    member val internal SyncExecuteDownstream: JobInformation -> int -> BlobMetadata -> Object -> unit = this.SyncExecuteDownstreamImpl with get, set
    member private x.SyncExecuteDownstreamImpl jbInfo parti meta o = 
        Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Default SyncExecuteDownstream is called upon for %A %s:%s, the blob %A will be discarded" x.ParamType x.Name x.VersionString meta))
    member val private lastMonitorDownstreamTaskQueue = ref (PerfDateTime.UtcNowTicks()) with get, set
    /// Is all downstream task completed?
    /// Name of the partition file 
    member internal x.PartitionFileName( parti:int ) = 
        parti.ToString("00000000")+".dat"
    /// static member of constructing the path of DSet
    static member internal ConstructDSetPath( name, ver ) = 
        let verString = VersionToString( DateTime(ver) )
        let tname = Path.Combine( name, verString )
        let patharr = tname.Split( @"\/".ToCharArray(), StringSplitOptions.RemoveEmptyEntries )
        patharr
    /// Construct the path of DSet. 
    member internal x.ConstructDSetPath() = 
//        let tname = Path.Combine( x.Name, x.VersionString )
//        let patharr = tname.Split( @"\/".ToCharArray(), StringSplitOptions.RemoveEmptyEntries )
//        patharr
        DistributedObject.ConstructDSetPath( x.Name, x.Version.Ticks )

    member val internal CloseAllStreams : bool -> unit = fun _ -> () with get, set
    member val internal TasksWaitAll = None with get, set
    member val internal WaitForUpstreamCanCloseEvents = List<ManualResetEvent>() with get
    member internal x.SetUpstreamCanCloseEvent( event ) = 
        x.WaitForUpstreamCanCloseEvents.Clear()
        x.WaitForUpstreamCanCloseEvents.Add( event ) 
    member internal  x.SetUpstreamCanCloseEvents( events ) = 
        x.WaitForUpstreamCanCloseEvents.Clear()
        x.WaitForUpstreamCanCloseEvents.AddRange( events ) 
    member val internal  CanCloseDownstreamEvent = new ManualResetEvent(false) with get
    /// Close all streams at the end of job
    member val internal SyncPreCloseAllStreams : JobInformation -> unit = this.BaseSyncPreCloseAllStreams with get, set
    member internal x.BaseSyncPreCloseAllStreams (jbInfo) = 
        if x.WaitForUpstreamCanCloseEvents.Count = 0 && Utils.IsNull x.ThreadPool then 
            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "BaseSyncPreCloseAllStreams %A %s Set CanCloseDownStreamEvent" x.ParamType x.Name))
            x.CanCloseDownstreamEvent.Set() |> ignore
        else
            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "BaseSyncPreCloseAllStreams %A %s ReSet CanCloseDownStreamEvent" x.ParamType x.Name))
            x.CanCloseDownstreamEvent.Reset() |> ignore
    member internal x.BaseWaitForUpstreamEvents (waithandles:WaitHandleCollection) contFunc = 
        if x.WaitForUpstreamCanCloseEvents.Count > 0 then 
            let nFired = ref 0
            let wrappedContFunc() = 
                if (!nFired)=0 then 
                    // Other wise, the contFunc has called, ensure it is called only once. 
                    let mutable bAllDone = true
                    for ev in x.WaitForUpstreamCanCloseEvents do 
                        if not (ev.WaitOne(0)) then 
                            bAllDone <- false
                    if bAllDone then 
                        if Interlocked.CompareExchange( nFired, 1, 0 )=0 then 
                            contFunc()
            for i = 0 to x.WaitForUpstreamCanCloseEvents.Count - 1  do 
                let ev = x.WaitForUpstreamCanCloseEvents.[i]
                waithandles.EnqueueWaitHandle ( fun _ -> sprintf "Upstream handle %d for %A %s:%s" i x.ParamType x.Name x.VersionString ) ev wrappedContFunc null
        else
            contFunc() 
    /// wait for all streams to confirm closing
    member val internal WaitForCloseAllStreamsViaHandle : WaitHandleCollection * JobInformation * DateTime -> unit = this.BaseWaitForCloseAllStreamsViaHandle with get, set

    // wait for all streams to confirm closing
    member internal x.BaseWaitForCloseAllStreamsViaHandle( waithandles, jbInfo, tstart ) = 
        if not (Utils.IsNull x.ThreadPool) then 
            let closeDownstream() =
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Done wait for handle done execution"))
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "WaitForCloseAllStreamsViaHandle %A %s:%s CanCloseDownstreamEvent set" x.ParamType x.Name x.VersionString ))
                x.CanCloseDownstreamEvent.Set() |> ignore

            let contWaitAllJobDone() = 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "WaitForCloseAllStreamsViaHandle %A %s:%s waiting for ThreadPool Jobs" x.ParamType x.Name x.VersionString ))
                let event = x.ThreadPool.WaitForAllNonBlocking()
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Starting wait for handle done execution"))
                ThreadPoolWait.WaitForHandle (fun _ -> sprintf "Wait For Thread Termination") event closeDownstream null

            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "WaitForCloseAllStreamsViaHandle %A %s:%s wait for upstream close events & threadpool jobs" x.ParamType x.Name x.VersionString ))
            x.BaseWaitForUpstreamEvents waithandles contWaitAllJobDone

        elif x.WaitForUpstreamCanCloseEvents.Count > 0 then 
            let contWaitAllUpstreamDone() = 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "WaitForCloseAllStreamsViaHandle %A %s:%s CanCloseDownstreamEvent set" x.ParamType x.Name x.VersionString ))
                x.CanCloseDownstreamEvent.Set() |> ignore 

            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "WaitForCloseAllStreamsViaHandle %A %s:%s wait for upstream close events" x.ParamType x.Name x.VersionString ))
            x.BaseWaitForUpstreamEvents waithandles contWaitAllUpstreamDone
        else
            x.CanCloseDownstreamEvent.Set() |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "WaitForCloseAllStreamsViaHandle %A %s:%s CanCloseDownstreamEvent set" x.ParamType x.Name x.VersionString ))

    member val internal ForkedThreads = null with get, set
    member internal x.LaunchForkedThreadsAction( parti:int, nameFunc: int -> string, actions: Action<unit>[] ) = 
        if ( x.ForkedThreads = null ) then
            lock ( x ) ( fun _ -> 
                if Utils.IsNull x.ForkedThreads then 
                    x.ForkedThreads <- ConcurrentDictionary<_,Thread[] ref>()
        )
        if not (x.ForkedThreads.ContainsKey( parti )) then 
            // Launch thread
            let arrThreads = ref null
            let arr = x.ForkedThreads.GetOrAdd( parti, fun _ -> arrThreads )
            if Object.ReferenceEquals( arr, arrThreads ) then 
                // Only one set of threads will be launched. 
                arr := actions |> Array.mapi( fun pi act -> ExecutionTasks.StartThreadForAction ( fun _ -> nameFunc pi) act )
    member internal x.LaunchForkedThreadsFunction numParallelExecutions cts (parti:int) (nameFunc: int -> string) (func:(unit->ManualResetEvent*bool)[]) = 
        if ( Utils.IsNull x.ThreadPool ) then
            lock ( x ) ( fun _ -> 
                if Utils.IsNull x.ThreadPool then 
                    x.ThreadPool <- new ThreadPoolWithWaitHandles<int>( (sprintf "Forked threads for execution on %A %s:%s" x.ParamType x.Name x.VersionString), NumParallelExecution = numParallelExecutions  ) 
        )
        for pi = 0 to func.Length - 1 do
            let funci = func.[pi]
                // Only one set of threads will be launched. 
            x.ThreadPool.EnqueueRepeatableFunction funci cts ( parti * func.Length + pi ) ( fun _ -> nameFunc(pi))
        x.ThreadPool.TryExecute()
    member internal x.FreeBaseResource() = 
        // Reset: Can Close
        x.CanCloseDownstreamEvent.Reset() |> ignore
        x.JobMapping <- null
        x.ForkedThreads <- null
        if not (Utils.IsNull x.ThreadPool) then
            x.ThreadPool.CloseAllThreadPool()
            x.ThreadPool <- null
    /// Final clean up job resources
    member val internal PostCloseAllStreams : JobInformation -> unit = this.PostCloseAllStreamsBaseImpl with get, set
    member internal x.PostCloseAllStreamsBaseImpl (jbInfo) = 
        x.FreeBaseResource()
        if Utils.IsNotNull x.ThreadPool then 
            x.ThreadPool.CheckForAll()
    /// Clear all resources used in job
    member val internal ResetAll: unit -> unit = this.ResetAllImpl with get, set
    /// Clear all resources used in job
    member private x.ResetAllImpl() = 
        x.FreeBaseResource()
    member val internal ExecutionDirection = TraverseDirection.TraverseUpstream with get, set
    member internal x.BasePreBegin( jbInfo, direction ) = 
        x.ExecutionDirection <- direction
    /// Begining of a job
    member val internal PreBegin: JobInformation * TraverseDirection -> unit = this.PreBeginImpl with get, set
    /// Begining of a job
    member private x.PreBeginImpl( jbInfo, direction ) = 
        x.BasePreBegin( jbInfo, direction ) 
    
    member val internal CurClusterInfo:ClusterJobInfo = null with get, set
    member val internal DefaultJobInfo: JobInformation = null with get, set
    member val internal CurPeerIndex = Int32.MinValue with get, set
    member val internal NumActiveConnections = 0 with get, set
    member val internal bConnected = null with get, set
    member val internal bNetworkInitialized = false with get, set 
    member val internal bSentPeer = null with get, set
    /// Two dimension array
    /// bRcvdPeer.[peeri].[parti] = true, has receive some command from peeri on partition i. 
    member val internal bRcvdPeer = null with get, set
    member val internal bRcvdPeerCloseCalled = null with get, set
    member val internal bSentPeerCloseConfirmed = null with get, set
    member val internal AllPeerClosedEvent = new ManualResetEvent(true) with get   
    member val private PeerMonitorTimer = DateTime.MaxValue with get, set    
    member val private bAllRcvdPeerClosed = false with get, set
    member val private bAllSentPeerConfirmed = false with get, set
    member internal x.BaseNetworkReady (jbInfo:JobInformation ) =
        let bExecuteInitialization = ref false
        if not x.bNetworkInitialized then
            lock (x ) ( fun _ -> 
                if not x.bNetworkInitialized then
                    bExecuteInitialization := true
                    x.AllPeerClosedEvent.Reset() |> ignore
                    x.PeerMonitorTimer <- (PerfDateTime.UtcNow())
                    x.bAllRcvdPeerClosed <- false
                    x.bAllSentPeerConfirmed <- false
                    x.bSentPeer <- Array.create x.Cluster.NumNodes false
                    x.bRcvdPeer <- Array.init x.Cluster.NumNodes ( fun i -> Array.create x.NumPartitions false )
                    x.bRcvdPeerCloseCalled <- Array.create x.Cluster.NumNodes false
                    x.bSentPeerCloseConfirmed <- Array.create x.Cluster.NumNodes false
                    
                    x.DefaultJobInfo <- jbInfo
                    for clusteri = 0 to jbInfo.ClustersInfo.Count - 1 do 
                        if Object.ReferenceEquals( jbInfo.ClustersInfo.[clusteri].LinkedCluster, x.Cluster ) then 
                            x.CurClusterInfo <- jbInfo.ClustersInfo.[clusteri]
                    if Utils.IsNull x.CurClusterInfo then 
                        Logger.Fail( sprintf "NetworkReady, %A %s:%s can't find cluster information for current cluster %s:%s" 
                                        x.ParamType x.Name x.VersionString x.Cluster.Name x.Cluster.VersionString )
                    // Proactive operation to establish connection to all peers 
                    x.NumActiveConnections <- 0 
                    x.bConnected <- Array.create x.Cluster.NumNodes false
                    x.CurPeerIndex <- x.CurClusterInfo.GetCurPeerIndex()                        
                    for peeri = 0 to x.Cluster.NumNodes - 1 do 
                        if peeri <> x.CurPeerIndex then 
                            let queue = x.CurClusterInfo.QueueForWriteBetweenContainer(peeri) 
                            if Utils.IsNotNull queue && not queue.Shutdown then 
                                x.NumActiveConnections <- x.NumActiveConnections + 1        
                                x.bConnected.[peeri] <- true
                                queue.Initialize()
                    x.Cluster.QueuesInitialized := 1
                    // This is the one thread that will win the race, the other thread will spin to wait for initialization      
                    x.bNetworkInitialized <- true
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Setup network for %A %s:%s" x.ParamType x.Name x.VersionString ) )
                    // Make sure don't increment beyond 1
                )
        !bExecuteInitialization
    member internal x.AllRcvdPeerClosed() = 
        if not x.bNetworkInitialized then 
            true
        else
            let mutable bAllPeerClosed = true
            for peeri = 0 to x.Cluster.NumNodes - 1 do 
                if bAllPeerClosed && not x.bRcvdPeerCloseCalled.[peeri] && peeri<>x.CurPeerIndex then 
                        let peerQueue = x.CurClusterInfo.QueueForWriteBetweenContainer(peeri)
                        if Utils.IsNotNull peerQueue && (not peerQueue.Shutdown) then 
                            // bAllPeerClosed <- x.bRcvdPeer.[peeri] |> Array.fold ( fun bAllClose v -> bAllClose && v ) true
                            bAllPeerClosed <- false
                    // become an issue if packet from a socket is late arriving. 
                    // x.bRcvdPeerCloseCalled.[peeri] <- bAllPeerClosed 
            bAllPeerClosed
    member internal x.AllSentPeerConfirmed() = 
        if not x.bNetworkInitialized then 
            true
        else
            let mutable bAllPeerConfirmed = true
            for peeri = 0 to x.Cluster.NumNodes - 1 do 
                if peeri<>x.CurPeerIndex then 
                    let peerQueue = x.CurClusterInfo.QueueForWriteBetweenContainer(peeri)
                    if Utils.IsNotNull peerQueue && (not peerQueue.Shutdown) && not x.bSentPeerCloseConfirmed.[peeri] then 
                        bAllPeerConfirmed <- false
            bAllPeerConfirmed
    member internal x.CheckAllPeerClosed() = 
        if not (x.AllPeerClosedEvent.WaitOne(0)) then  
            // No test if the AllPeerClosedEvent is already set
            if not x.bAllRcvdPeerClosed then 
                x.bAllRcvdPeerClosed <- x.AllRcvdPeerClosed() 
                if x.bAllRcvdPeerClosed then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%A %s:%s CheckAllPeerClosed: AllRcvdPeerClosed." 
                                                                   x.ParamType x.Name x.VersionString ))
            if not x.bAllSentPeerConfirmed then 
                x.bAllSentPeerConfirmed <- x.AllSentPeerConfirmed()
                if x.bAllSentPeerConfirmed then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%A %s:%s CheckAllPeerClosed: AllSentPeerConfirmed." 
                                                                   x.ParamType x.Name x.VersionString ))
            if x.bAllRcvdPeerClosed && x.bAllSentPeerConfirmed then 
                x.AllPeerClosedEvent.Set() |> ignore
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%A %s:%s CheckAllPeerClosed: all network peer closed & received" 
                                                               x.ParamType x.Name x.VersionString ))
            else
                let t2 = (PerfDateTime.UtcNow())
                if t2.Subtract( x.PeerMonitorTimer ).TotalSeconds > DeploymentSettings.PeerMonitorInterval then 
                    x.PeerMonitorTimer <- t2
                    x.MonitorPeerStatus()

    member internal x.MonitorPeerStatus() = 
        if x.bNetworkInitialized then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%A network status %s:%s, CurPeerIndex: %d %s Receving Close,DStream: %A %s Sending Confimred Close, DStream: %A" 
                                                               x.ParamType x.Name x.VersionString x.CurPeerIndex Environment.NewLine 
                                                               x.bRcvdPeerCloseCalled Environment.NewLine x.bSentPeerCloseConfirmed ))

    /// Setup network before remote job
    member val internal NetworkReady: JobInformation -> unit = this.NetworkReadyImpl with get, set
    /// Setup network before remote job
    member private x.NetworkReadyImpl( jbInfo ) = 
        x.BaseNetworkReady( jbInfo ) |> ignore

// A global variable in Prajna
[<Serializable; AllowNullLiteral>]
type internal GV( cl, name, ver) = 
    inherit DistributedObject( cl, FunctionParamType.GV, name, ver )
    new () = 
        GV( null, DeploymentSettings.GetRandomName(), (PerfDateTime.UtcNow()) )


// Prajna Aggregate functions
[<AllowNullLiteral>]
type internal AggregateFunction( func: Object -> Object -> Object ) = 
    member val AggregateFunc = func with get

/// Prajna Aggregate functions
/// func: State1 State2 -> Aggregated State
[<AllowNullLiteral>]
type internal AggregateFunction<'K>( func: 'K -> 'K -> 'K ) = 
    inherit AggregateFunction( 
        let wrapperFunc func (O1:Object) (O2:Object) =
            if Utils.IsNotNull O1 && Utils.IsNotNull O2 then 
                let state1 = O1 :?> 'K
                let state2 = O2 :?> 'K
                func state1 state2 :> Object
            elif Utils.IsNull O1 then 
                O2
            else
                O1
        wrapperFunc func )
    member val FoldStateFunc = func with get

// Prajna Serialize function
// Used at Prajna Client
[<AllowNullLiteral>]
type internal GVSerialize( func: MemStream -> Object  -> MemStream ) = 
    member val SerializeFunc = func with get

// Prajna Serialization functions
// Use at Prajna Host
[<AllowNullLiteral>]
type internal GVSerialize<'K>( ) = 
    inherit GVSerialize( 
        let wrapperFunc (ms:MemStream) (O1:Object) =
            let state1 = O1 :?> 'K
            Strm.SerializeFrom( ms, state1 )
            ms
        wrapperFunc )

// Prajna Aggregate functions
[<AllowNullLiteral>]
type internal FoldFunction( func: Object -> (BlobMetadata*Object) -> Object ) = 
    member val FoldFunc = func with get

// Prajna Fold functions
[<AllowNullLiteral>]
type internal FoldFunction<'U, 'State >( func ) = 
    inherit FoldFunction( 
        let wrapperFunc (stateobj:Object) (meta, elemObject:Object ) = 
            if Utils.IsNotNull elemObject then 
               let state = stateobj :?> 'State
               let elemArray = elemObject :?> ('U)[]                              
               ( elemArray |> Array.fold func state ) :> Object
            else
                stateobj
        wrapperFunc 
        )


