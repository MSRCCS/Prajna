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
        DSet.fs
  
    Description: 
        The non-generic distributed dataset (DSet). 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Aug. 2013
    
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
open Microsoft.FSharp.Collections
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.FileTools
open Prajna.Tools.BytesTools

type internal ClientBlockingOn = 
    | None = 0x00
    | Cluster = 0x01
    | DSet = 0x02
    | ClusterDontHaveCurrentMachine = 0x80
    | Undefined = 0xffff

/// DSetFlag Controlling behavior of the DSet at host
type internal DSetFlag = 
    | None = 0x00
    | CountWriteDSet = 0x01
    | ReadAllReplica = 0x02

type internal DSetMetadataStorageFlag =
    | None = 0x00
    | HasPassword = 0x01
    | HasFunction = 0x02
    | HasDependency = 0x04
    | HasMappingElems = 0x08
    | HasNumParallelExecution = 0x10
    | PartitionByKey = 0x20
    | All = 0xff
    | StoreMetadata = 0x8000

type internal DSetMetadataCopyFlag =
    | Copy = 0x00           // Construct the DSet by copying the core parameter, no relationship is setup between the DSet being constructed. 
    | Passthrough = 0x01    // One-to-One pass through (like seq.filter, seq.map, in which the target DSet performs certain function but doesn't retain in memory )
    | Propagate = 0x02      // Copy information that needs to be propagate via DSet
    | Update = 0x03
    | AttachUpstream = 0x04 // One-to-One pass through, the only difference is that we don't set the DownStream dependency of the other DSet. 
    
type internal DSetErrorType = 
    | None = 0
    | NonExistPartition = 1

type internal DSetChainFlag = 
    | None = 0x00   
    | Source = 0x01         // A source DSet persists in Prajna, and maybe read
    | Passthrough = 0x02    // Passthrough DSets don't persist, just transition
    | Destination = 0x04    // Content of destination DSet will be persisted. 
    | DStream = 0x08        // Add stream in traversal. 

type internal DSetCacheState = 
    | None = 0
    | InProcess = 1
    | All = 2

type internal DSetUpdateFunctionState = 
    | None = 0                      // No need to update DSet date (mainly the embedded function)
    | UpdateAtSerialization = 1     // Update DSet at time of serialization 
    | UpdateAtFailure = 2           // Update DSet if certain node fails. 


[<Serializable; AllowNullLiteral>]
/// Dependency between DSet
type internal DependentDSet ( target: DSet, hash) =
    inherit DependentDObject( target, hash, ParamType = FunctionParamType.DSet )
    member x.TargetDSet with get() = x.Target :?> DSet
                                 and set( p: DSet ) = x.Target <- p
    new ( target:DSet ) = 
        DependentDSet( target, if Utils.IsNull target.Blob then null else target.Blob.Hash )
    new ( hash ) = 
        DependentDSet( null, hash )
    new () = 
        DependentDSet( null, null )
    new ( depDObject:DependentDObject ) = 
        DependentDSet( null, depDObject.Hash )   
and /// For backward dependency, trace back on what other DStream/DObject that this DSet depends upon
    internal DSetDependencyType = 
    /// The current DSet doesn't depend on other DSet, it is a source or sink
    | StandAlone
    /// Source will generate data by calling the map function. It is used to implement DSet.init. 
    | Source
    /// Passthrough is a common type of dependency, with only a single parentDSet with same partition mapping structure
    /// Partition Mapping, Content Key of the derived DSet is inheritted from the patent DSet
    | Passthrough of DependentDSet
    /// CorrelatedMix is inverse of DistributeForward, in which the downstream DSet will mix data from multiple upstream sources
    /// Each upstream DSet should have the same number of partitions. 
    /// CorrelatedMix should only be used in pull dataflow. 
    | CorrelatedMixFrom of System.Collections.Generic.List<DependentDSet>
    /// MergeFrom mix multiple upstream DSet, the downstream DSet has number of partitions equal to the sum of all partitions of upstream DSet. 
    /// Merge should only be used in pull dataflow. 
    | MergeFrom of System.Collections.Generic.List<DependentDSet>
    /// MixInNode: the upper parents send information through repartitioning function, doesn't across networkk
    | MixFrom of DependentDSet
    /// Wild Mix: the upper parents sends information through repartitioning function, may cross network 
    /// If the information is within the same node, the information is pushed down to the 1st DSet straightforward, 
    /// If the information is sent across the network, the information is sent to the 2nd DSet, which will merge with the 1st DSet in some later time.
    | WildMixFrom of DependentDSet * DependentDStream
    /// Bypass: one parentDSet with multiple side streams, the first DSet on the list is the parentDSet
    /// while the rest are considered siblings. 
    | Bypass of DependentDSet * System.Collections.Generic.List<DependentDSet>
    /// Decode from another stream
    | DecodeFrom of DependentDStream
    /// Hash join two DSets
    | HashJoinFrom of DependentDSet * DependentDSet
    /// Cross join two DSets 
    | CrossJoinFrom of DependentDSet * DependentDSet
and internal DSetForwardDependency = 
    /// no action, if content is pushed downstream, it will be discarded here
    | Discard
    /// Send DSet downward
    | Passforward of DependentDSet
    /// CorrelatedMix is inverse of DistributeForward, in which the downstream DSet will mix data from multiple upstream sources
    /// Each upstream DSet should have the same number of partitions. 
    /// CorrelatedMix should only be used in pull dataflow. 
    | CorrelatedMixTo of DependentDSet
    /// MergeTo mix multiple upstream DSet, the downstream DSet has number of partitions equal to the sum of all partitions of upstream DSet. 
    /// Merge should only be used in pull dataflow. 
    | MergeTo of DependentDSet
    /// Mix within node, the upper parents send information through repartitioning function, doesn't across networkk
    | MixTo of DependentDSet
    /// Wild Mix: the upper parents sends information through repartitioning function, may cross network 
    /// If the information is within the same node, the information is pushed down to the dependentDSet, 
    /// If the information is sent across the network, the information is sent to the DependentDStream. 
    | WildMixTo of DependentDSet * DependentDStream
    /// Distribute DSet to multiple DSet
    | DistributeForward of System.Collections.Generic.List<DependentDSet>
    /// Encode to another stream
    | EncodeTo of DependentDStream
    /// Save to another stream, the DSet is a destination DSet
    | SaveTo of DependentDStream
    /// Hash join two DSets
    | HashJoinTo of DependentDSet 
    /// Cross join two DSets 
    | CrossJoinTo of DependentDSet 
and internal DSetFactory() = 
    inherit HashCacheFactory<DSet>()
    // Retrieve a DSet
    static member ResolveDSet( hash ) = 
        DSetFactory.Resolve( hash ) 
    // Cache a DSet, if there is already a DSet existing in the factory with the same name and version information, then use it. 
    static member CacheDSet( hash, newDSet ) = 
        DSetFactory.CacheUseOld( hash, newDSet )

and [<Serializable; AllowNullLiteral>] 
    [<System.Diagnostics.DebuggerDisplay("{DebuggerDisplay()}")>]
    /// DSet is a distributed data set. It is one of the central entity in Prajna. 
    /// Please use the generic version of this class. 
    DSet internal ( cl, assignedName, ver ) as thisDSet = 
    inherit DistributedObject( cl, FunctionParamType.DSet, assignedName, ver ) 

    do
        thisDSet.PrecodeDependentObjs <- thisDSet.PrecodeDependentObjsImpl
        thisDSet.SetupDependencyHash <- thisDSet.SetupDependencyHashImpl
        thisDSet.InitializePartition <- thisDSet.InitializePartitionImpl
        thisDSet.SetupPartitionMapping <- thisDSet.SetupPartitionMappingImpl
        thisDSet.GetParentMapping <- thisDSet.GetParentMappingImpl
        thisDSet.SyncIterate <- thisDSet.SyncIterateImpl
        thisDSet.SyncDecodeToDownstream <- thisDSet.SyncDecodeToDownstreamImpl
        thisDSet.SyncExecuteDownstream <- thisDSet.SyncExecuteDownstreamImpl
        thisDSet.SyncPreCloseAllStreams <- thisDSet.SyncPreCloseAllStreamsImpl
        thisDSet.ResetAll <- thisDSet.ResetAllImpl
        thisDSet.PreBegin <- thisDSet.PreBeginImpl

    let mutable contentKey = StringTools.GetHashCodeQuickUInt64( assignedName )
    let mutable bFirstCommand = Array.create 0 false
    // Track if Report,DSet or ReportClose,DSet were received from peers
    let mutable reportReceived = lazy( Array.create thisDSet.Cluster.NumNodes false )
    /// A dictionary that holds key, values that is used to confirm the successful store of DSet. 
    /// If a certain peer becomes unavailable, we may choose to redelivery the elem
    /// Key: byte[], hash (Fletcher64 currently) of the elem stream to be stored. 
    /// Value: < Time: in CLock.ElapsedTicks, 
    ///          parti: partition value (used for redelivery
    ///          byte[]: the byte sent by the network queue 
    ///          int: count of delivery )
    let deliveryQueue = ConcurrentDictionary<byte[], (int64*int*StreamBase<byte>*(int ref))>()
    /// Partition Progress monitoring. 
    let mutable partitionCheckmark = Array.zeroCreate<int64> 0
    /// Partition Progress monitoring. 
    let mutable partitionProgress = Array.zeroCreate<int64> 0
    /// Outstanding command in partitioning. 
    let mutable partitionPending = Array.zeroCreate<ConcurrentQueue<int>> 0
    /// Used to establish a serial # within each partition
    let mutable partitionSerial = Array.zeroCreate<int64> 0
    /// Used to establish a serial # within each partition
    let mutable partitionSerialConfirmed = Array.zeroCreate<ConcurrentQueue<int64*int*int>> 0
    /// Used to establish a serial # within each partition
    let mutable rcvdSerialInitialValue : int64[] = null 
    // Timeout, this is per partition. 
    let mutable enterTimeout = Array.create 0 0L
    let mutable nActiveConnection = 0 
    let mutable bInitialized = false
    // callback command
    let mutable peerReport = null
    do
        thisDSet.WaitForCloseAllStreamsViaHandle <- thisDSet.WaitForCloseAllStreamsViaHandleImpl
        thisDSet.ChangeSerializationLimit <- thisDSet.ChangeSerializationLimitImpl
    member private x.ReportReceived( peeri ) = 
        reportReceived.Value.[peeri] <- true
    override x.ToString () =
        x.Display false
    member private x.DebuggerDisplay() =
        x.Display true
    member private x.Display showHash =
        if showHash then
            sprintf "%s (DSet) (Hash: %s)" x.Name (BytesToHex(x.Hash))
        else
            sprintf "%s (DSet)" x.Name

    member val internal CallbackCommand = [| ControllerCommand( ControllerVerb.Unknown, ControllerNoun.DSet ) ;
                                    ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Partition ) ;
                                    ControllerCommand( ControllerVerb.Get, ControllerNoun.ClusterInfo ); |] with get

    /// Whether it is the first command of this peer
    member internal x.FirstCommand with get() = bFirstCommand 
                                    and set( c ) = bFirstCommand <- c
    /// Number of active outgoing connection from this node in the DSet
    member internal x.NumActiveConnection with get() = nActiveConnection
                                          and set( v ) = nActiveConnection <- v
    /// Initial Receiving Serial Value of a peer before the first command of this DSet (usually SET DSet) is issued. 
    /// Checking this value can indicate whether any command has been received from the server. 
    member internal x.RcvdSerialInitialValue with get() = rcvdSerialInitialValue
    /// Maximum time to wait for execution of a certain command (in second)
    member val internal MaxWait = 60. with get, set
    member val internal Flag = ( DSetFlag.CountWriteDSet ) with get, set
    /// Specify what resource that the DSet is using (RAM, SSD, HDD or Uniform ) when construction the partitioning function. 
    member val internal PartionBy = PartitionByKind.Uniform with get, set
    /// Required number of replication for durability
    // move to base class
    //    member val NumReplications = 1 with get, set
    /// Upstream dependency
    member val internal Dependency = DSetDependencyType.StandAlone with get, set
    /// Downstream dependency
    member val internal DependencyDownstream = DSetForwardDependency.Discard with get, set
    /// Downstream dependency
    member val internal ChildDSet : DependentDSet = null with get, set
    member val internal InternalFunction : Function = null with get, set
    member internal x.Function with get() = x.InternalFunction
                                and set( f ) = x.InternalFunction <- f
                                               if Utils.IsNotNull f && x.bSerializationLimitSet then 
                                                    x.InternalFunction.FunctionObj.SerializationLimit <- x.SerializationLimit
                            
    member internal x.FunctionObj with get() = if Utils.IsNull x.InternalFunction || Utils.IsNull (x.InternalFunction.FunctionObj) then                         
//                                            let msg = sprintf "Error in DSet.FunctionObj, DSet %s:%s doesn't have function installed" x.Name x.VersionString 
//                                            Logger.Log(LogLevel.Error, msg)
//                                            failwith msg
                                                        null
                                                  else
                                                        x.InternalFunction.FunctionObj
    member val internal UpdateFuncState = DSetUpdateFunctionState.None with get, set
    member val internal UpdateFuncObj = fun (cl:Cluster) (peeri:int) -> null:Function with get, set
//   /// Sender flow control, DSet limits the total sending queue to SendingQueueLimit
//   /// If it is communicating with N peer, each peer, the sending queue limit is SendingQueueLimit/N
// JinL: 05/13/2014, move to base class
//    member val SendingQueueLimit = 1024 * 1024 * 100 with get, set


    /// Maximum number of parallel threads that will execute the data analytic jobs in a remote container. 
    /// If 0, the remote container will determine the number of parallel threads used according to its computation and memory resource
    /// available. 
    member val NumParallelExecution = 0 with get, set
    member val internal PartitionExecutionMode = DeploymentSettings.PartitionExecutionMode with get, set
    /// DSet's meta data may have a version. The version is associated with DSet######.meta and records system's status of a current DSet
    /// ( e.g., whether passed integrity check, whether has index, change of mapping, etc.. )
    member val internal MetaDataVersion = 0 with get, set
    /// A mapping matrix indicates number of Key Values in each partition
    member val internal MappingNumElems : int[][] = null with get, set
    /// A mapping matrix indicates Length of stream in each partition
    member val internal MappingStreamLength: int64[][] = null with get, set
    /// <summary>
    /// Get the number of key-values or blobs in DSet. .Length can be applied to either 1) source DSet (metadata is read via .LoadSource()), 
    /// 2) intermediate DSet which is derived from source DSet, 
    /// or 3) destination DSet after save operation has succeeded. 
    /// </summary>
    member x.Length with get() = 
                            if Utils.IsNull x.MappingNumElems then 
                                Int64.MinValue
                            else
                                let mutable totalNumElems = 0L
                                for i = 0 to x.MappingNumElems.Length - 1 do 
                                    totalNumElems <- totalNumElems + int64 (Array.max (x.MappingNumElems.[i]))
                                totalNumElems
    
    /// <summary> 
    /// The number of values in the DSet. This function can only be used for Source/Destination DSet that is persisted, it will return Int64.MinValue for other DSet. </summary>
    /// <return> number of values </return>
    static member length ( x:DSet) =
        x.Length
    

    /// <summary>
    /// Get the size of all key-values or blobs in DSet
    /// </summary>
    member x.SizeInBytes with get() = 
                            if Utils.IsNull x.MappingStreamLength then 
                                Int64.MinValue
                            else
                                let mutable totalLength = 0L
                                for i = 0 to x.MappingStreamLength.Length - 1 do 
                                    totalLength <- totalLength + (Array.max (x.MappingStreamLength.[i]))
                                totalLength

    /// <summary> 
    /// The storage footprint of the DSet. This function can only be used for Source/Destination DSet that is persisted, it will return Int64.MinValue for other DSet. </summary>
    /// <return> storage footprint in bytes. </return>
    static member sizeInBytes ( x:DSet) =
        x.SizeInBytes

    member private x.GetParentMappingImpl() = 
        match x.Dependency with  
        | CorrelatedMixFrom parents ->
            UseParent parents.[0]
        | MergeFrom parents -> 
            let parentObjects = parents |> Seq.map ( fun o -> o:> DependentDObject )
            AggregateParents (parentObjects)            
        | MixFrom oneParent
        | Passthrough oneParent
        | Bypass ( oneParent, _ ) ->
             UseParent oneParent
        | WildMixFrom (parent, parentS) ->
            // Mapping for WildMix used the mapping of the streams 
            UseParent parentS
        | CrossJoinFrom (parent0, parent1)    
        | HashJoinFrom (parent0, parent1) -> 
            UseParent parent0
        | Source -> 
            GenerateMapping
        | StandAlone 
        | DecodeFrom _ ->
            // For these DSet, the Mapping matrix should be populated. The GetParentMapping() function should never be called. 
            UndefinedMapping            
        
    member val internal ProgressMonitor = 10000000L with get, set
    /// For Metadata read, # of nodes that need to respond before we consider the metadata to be valid 
    /// If the # is minus, it is considered a percentage number of the cluster. 
    member val internal MinNodeResponded = -50 with get, set
    /// For Metadata read, # of valid response that need before we consider the metadata to be valid 
    /// If the # is minus, it is considered a percentage number of the cluster. 
    member val internal MinValidResponded = -50 with get, set

    internal new ( cl ) = 
        DSet( cl, DeploymentSettings.GetRandomName(), (PerfADateTime.UtcNow()) )
    internal new () = 
        DSet( Cluster.GetCurrent(), DeploymentSettings.GetRandomName(), (PerfADateTime.UtcNow()) )
    // inherit from a previous DSet, with flag indicating 
    // method of inherent. 
    internal new ( dset:DSet, flag ) as x = 
        DSet( dset.Cluster, DeploymentSettings.GetRandomName(), (PerfADateTime.UtcNow()) )
        then 
            x.CopyMetaData( dset, flag )
    member val internal MappingInfoUpdatedEvent = new ManualResetEvent(false) with get, set
    /// Bind Hash only to the name & version of the DSet (used for persisted DSet only). 
    member internal x.HashNameVersion() = 
        let byt = System.Text.UTF8Encoding().GetBytes( x.Name + x.Version.ToString() )
        x.Hash <- HashByteArray( byt )
    // Copy DSet meta data from another DSet
    member internal x.CopyMetaData( dset, flag ) = 
        match flag with 
        | DSetMetadataCopyFlag.Copy ->
            // This should copy everying in DSet.pack
            x.Hash <- dset.Hash
            x.Cluster <- dset.Cluster
            x.TypeOfLoadBalancer <- dset.TypeOfLoadBalancer
            x.Partitioner <- dset.Partitioner
            x.StorageType <- dset.StorageType
            x.Name <- dset.Name
            x.MetaDataVersion <- dset.MetaDataVersion
            x.ConfirmDelivery <- dset.ConfirmDelivery
            x.Version <- dset.Version
            x.ContentKey <- dset.ContentKey
            x.NumPartitions <- dset.NumPartitionsRaw
            x.NumReplications <- dset.NumReplications
            x.Mapping <- dset.Mapping
            x.MappingNumElems <- dset.MappingNumElems
            x.MappingStreamLength <- dset.MappingStreamLength
            x.NumParallelExecution <- dset.NumParallelExecution
            x.PartitionExecutionMode <- dset.PartitionExecutionMode
            x.Password <- dset.Password
            x.Function <- dset.Function
            x.Dependency <- dset.Dependency
            x.DependencyDownstream <- dset.DependencyDownstream
            x.bValidMetadata <- dset.bValidMetadata
        | DSetMetadataCopyFlag.Passthrough -> 
            x.Cluster <- dset.Cluster
            x.TypeOfLoadBalancer <- dset.TypeOfLoadBalancer
            x.Partitioner <- dset.Partitioner
            x.StorageType <- StorageKind.Passthrough
            x.ConfirmDelivery <- dset.ConfirmDelivery
            x.ContentKey <- dset.ContentKey
            x.Version <- dset.Version
            x.NumPartitions <- dset.NumPartitionsRaw
            x.NumReplications <- dset.NumReplications
            x.NumParallelExecution <- dset.NumParallelExecution
            x.PartitionExecutionMode <- dset.PartitionExecutionMode
            // Passthrough use null as mapping
            x.Mapping <- null
            x.Dependency <- Passthrough (DependentDSet(dset))
            dset.DependencyDownstream <- Passforward (DependentDSet(x))
            let mutable bValidParent = dset.bValidMetadata 
            match dset.Dependency with 
            | DSetDependencyType.Source -> 
                bValidParent <- true
            | _ -> 
                ()
            x.bValidMetadata <- bValidParent 
            
        | DSetMetadataCopyFlag.AttachUpstream -> 
            x.Cluster <- dset.Cluster
            x.TypeOfLoadBalancer <- dset.TypeOfLoadBalancer
            x.Partitioner <- dset.Partitioner
            x.StorageType <- StorageKind.Passthrough
            x.ConfirmDelivery <- dset.ConfirmDelivery
            x.ContentKey <- dset.ContentKey
            x.Version <- dset.Version
            x.NumPartitions <- dset.NumPartitionsRaw
            x.NumReplications <- dset.NumReplications
            x.NumParallelExecution <- dset.NumParallelExecution
            x.PartitionExecutionMode <- dset.PartitionExecutionMode
            // Passthrough use null as mapping
            x.Mapping <- null
            x.Dependency <- Passthrough (DependentDSet(dset))
            let mutable bValidParent = dset.bValidMetadata 
            match dset.Dependency with 
            | DSetDependencyType.Source -> 
                bValidParent <- true
            | _ -> 
                ()
            x.bValidMetadata <- bValidParent 
            // First time, attach downstream 
//            if dset.DependencyDownstream = Discard then 
//                dset.DependencyDownstream <- Passforward (DependentDSet(x))            
        | DSetMetadataCopyFlag.Propagate -> 
            ()
        | DSetMetadataCopyFlag.Update -> 
            x.UpdateBaseMetadata( dset ) 
            x.MappingNumElems <- dset.MappingNumElems
            x.MappingStreamLength <- dset.MappingStreamLength
            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Mapping updated set for %s" x.Name))
            x.MappingInfoUpdatedEvent.Set() |> ignore 
        | _ ->
            let msg = sprintf "Unknown DSet construction flag %A" flag 
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    /// Set a content key for DSet that governs partition mapping, 
    /// For two DSets that have the same content key, a single key will be mapped uniquely to a partition
    member x.ContentKey with get() = contentKey 
                        and  set(k) = contentKey <- k
    /// Get or set the name of DSet
    member x.Name with get() = (x:>DistributedObject).Name
                  and  set(n) = (x:>DistributedObject).Name <- n
                                // Set the content key for this DSet
                                match x.Dependency with 
                                | Passthrough oneParent ->
                                    // For a single parent, the content Key is inherited 
                                    let y = oneParent.TargetDSet
                                    x.ContentKey  <- (y.ContentKey)
                                | CorrelatedMixFrom parents 
                                | MergeFrom parents -> 
                                    ()
                                | Bypass ( parent, brothers) -> 
                                    let y = parent.TargetDSet
                                    x.ContentKey  <- (y.ContentKey)
                                | _ ->
                                    // No dependency
                                    if Utils.IsNotNull n then 
                                        x.ContentKey <- StringTools.GetHashCodeQuickUInt64( n )
                                    else
                                        x.ContentKey <- 0UL
                                // Set the content key of dependents
                                match x.Dependency with 
                                | Bypass ( parent, brothers) -> 
                                    for childDSet in brothers do 
                                        childDSet.TargetDSet.ContentKey <- parent.TargetDSet.ContentKey
                                | _ ->
                                    ()
    /// Number of record in a collection during data analytical jobs 
    member private x.ChangeSerializationLimitImpl(s:int) = 
                                             if Utils.IsNotNull x.Function && Utils.IsNotNull x.Function.FunctionObj then 
                                                 x.Function.FunctionObj.SerializationLimit <- s   
    /// Timeout Multiple, timeout throttling & error flagging is triggered if SerializationLimit * TimeoutMultiple can't be sent out 
    member val internal TimeoutMultiple = 10 with get, set
    /// Timeout limit (in second, if still can't write any parition )
    member val internal TimeoutLimit = 30 with get, set
    /// Sleep (in ms) if Timeout is triggered
    member val internal TimeoutSleep = 50 with get, set
    member internal x.InitializePartitioner with get() = x.Function.FunctionObj.InitializePartitioner
                                             and set(f) = x.Function.FunctionObj.InitializePartitioner <- f


    /// Translate required numeber of nodes, such as MinNodeResponded, MinValidResponded to a number based
    /// on cluster size. if v>0, v is # of nodes, if v<0, v is interpretted as percentage #
    member internal x.RequiredNodes( v ) = 
        if v > 0 then v else 
            Math.Max( Math.Min( x.Cluster.NumNodes, ( (-v) * ( x.Cluster.NumNodes ) + 99) / 100), 1 )
        // For debug purpose, to test what if we require all nodes to be active. 
//        x.Cluster.NumNodes
    member private x.InitializePartitionImpl() =
        // No dependency
        let nodes, resources = x.Cluster.GetNodeIDsWithResource( x.PartionBy )
        let nodeID = nodes |> Array.map ( fun node -> node.MachineID )
        // First setup load balancer, with ID & resource
        x.LoadBalancer.Set( nodeID, resources )
        // Next, set up partioner, in which the NumPartitions becomes set
        if Utils.IsNotNull x.Function && Utils.IsNotNull x.Function.FunctionObj then 
            x.Function.FunctionObj.InitializePartitioner( x.Cluster, x.LoadBalancer, x.Partitioner )
        else
            // Use default partitioner if none is specified. 
            MetaFunction.DefaultInitializePartitioner( x.Cluster, x.LoadBalancer, x.Partitioner )
    // Two functions : Initialize Partition
    // && LoadBalancer GetPartitionMapping
    member internal x.InitializeMapping() = 
                x.InitializePartition()
                // Finally, set up mapping of the load balancer. 
                // Note: always pass 0UL as contentKey for stable partition mapping across different DSets. Revisit this design later.
                x.Mapping <- x.LoadBalancer.GetPartitionMapping( 0UL, x.NumPartitions, x.NumReplications )
                // x.Mapping <- x.LoadBalancer.GetPartitionMapping( x.ContentKey, x.NumPartitions, x.NumReplications )
        
    /// Setup partition mapping for use in save
    /// derivatives & version information
    member private x.SetupPartitionMappingImpl() =
        match x.Dependency with
        | StandAlone
        | Source 
        | WildMixFrom _ -> 
            /// Mapping information will be generated. 
            if not x.bValidMetadata || Utils.IsNull x.Mapping then 
                if x.NumReplications > x.Cluster.NumNodes then 
                    let msg = sprintf "Replication %d is larger than number of nodes in the cluster %d, execution fails" x.NumReplications x.Cluster.NumNodes
//                    Logger.Log(LogLevel.Error, msg)
//                    failwith msg
                    Logger.Log( LogLevel.Warning, msg )
                    // Reset # of replicaton to the size of the cluster. 
                    x.NumReplications <- x.Cluster.NumNodes
                x.InitializeMapping()                   
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "------- Generate Mapping Matrix for DSet %s:%s -----------\n%s" x.Name x.VersionString (x.MappingToString()) ))
                x.bValidMetadata <- true
        | MixFrom oneParent
        | Passthrough ( oneParent ) 
        | Bypass ( oneParent, _ ) 
        | HashJoinFrom (oneParent, _ )
        | CrossJoinFrom (oneParent, _ ) -> 
            // Use Getmapping Call to obtain mapping
            x.GetMapping() |> ignore
            // Num partition should be set in the process. 
            // x.NumPartitions <- oneParent.ParentDSet.NumPartitions
            // mapping of the DSet will be parents mapping, we use null to indicate this
            // 02/24/2014: let derivative DSet takes parent's version information. 
            if not x.bVersionSet then 
                x.Version <- oneParent.TargetDSet.Version
            if Utils.IsNotNull x.Mapping then 
                x.bValidMetadata <- true
        | CorrelatedMixFrom parents ->
            let oneParent = parents.[0]
            x.GetMapping() |> ignore
            // 02/24/2014: let derivative DSet takes parent's version information. 
            if not x.bVersionSet then 
                x.Version <- oneParent.TargetDSet.Version
            if Utils.IsNotNull x.Mapping then 
                x.bValidMetadata <- true
        | MergeFrom parents -> 
            x.GetMapping() |> ignore
            if not x.bVersionSet then 
                // Version of the merge is the maximum of all merged DSets. 
                x.Version <- DateTime.MinValue            
                for pa in parents do 
                    if x.Version < pa.TargetDSet.Version then 
                        x.Version <- pa.TargetDSet.Version
            if Utils.IsNotNull x.Mapping then 
                x.bValidMetadata <- true
        | _ -> 
            if Utils.IsNotNull x.Mapping then 
                x.bValidMetadata <- true           
            else
                let msg = sprintf "DSet.SetupPartitionMapping, for DSet %s:%s of type %A, the mapping matrix should be provided" x.Name x.VersionString x.Dependency    
                Logger.Log( LogLevel.Error, msg )
                failwith msg
//        | _ ->
//            let msg = sprintf "Don't know how to handle more than dependency type %A" x.Dependency
//            Logger.Log(LogLevel.Error, msg)
//            failwith msg


    /// Any write operation will trigger ClearTimeout()
    member internal x.ClearTimeout( parti ) = 
        enterTimeout.[parti] <- x.Clock.ElapsedTicks
    /// Is a certain partition timeout, this is defined as no write operation to queue 
    /// for TimeoutLimit second. 
    member internal x.IsTimeout( parti ) = 
         ( x.Clock.ElapsedTicks - enterTimeout.[parti] > x.ClockFrequency * int64 x.TimeoutLimit )

    /// Initialize connect to all peers that needed for DSet operation. 
    /// This is an internal function that should be called once per each DSet. 
    /// Main function is to SetupPartitionMapping. If the partition mapping is already available (e.g., read in), 
    /// don't call this function. 
    member internal x.Initialize() = 
        if not bInitialized then 
            bInitialized <- true    
            x.SetupPartitionMapping() 

    member internal x.IncrementMetaDataVersion() = 
        x.MetaDataVersion <- x.MetaDataVersion + 1
    member internal x.PackWithPeerInfo (peeri:int) (ms:MemStream) flagPack = 
        match x.UpdateFuncState with 
        | DSetUpdateFunctionState.None -> 
            ()
        | _ -> 
            let newFunc = x.UpdateFuncObj (x.Cluster) peeri
            if not (Utils.IsNull newFunc) then 
                x.Function <- newFunc
        x.Pack( ms, flagPack )
    member val private FlagOnNetwork = DSetMetadataStorageFlag.None with get, set
    member val private HasDownStreamDependency = false with get, set
    /// Serialization of dset to Memory Stream, couldn't figure out the best way for customized serialization of compact in F#
    /// so I wrote my own function of pack & unpack. 
    member internal x.Pack( ms: StreamBase<byte>, ?flagPack, ?shouldCodeHasDownStreamDependencyFlagArg ) = 
        let mutable flag = defaultArg flagPack DSetMetadataStorageFlag.None
        let shouldCodeHasDownStreamDependencyFlag = defaultArg shouldCodeHasDownStreamDependencyFlagArg true
        x.HasDownStreamDependency <- false
        /// Only take input of HasPassword flag, other flag is igonored. 
        let bRemoveFunction = ( flag &&& DSetMetadataStorageFlag.StoreMetadata )<> DSetMetadataStorageFlag.None
        flag <- flag &&& ( DSetMetadataStorageFlag.HasPassword )
        let ticks = x.Cluster.Version.Ticks
        ms.WriteString( x.Cluster.Name )
        ms.WriteInt64( ticks )
        ms.WriteVInt32( int x.TypeOfLoadBalancer )
        ms.WriteVInt32( int x.Partitioner.TypeOf )
        ms.WriteVInt32( int x.StorageType )
        ms.WriteString( x.Name )
        ms.WriteInt64( x.Version.Ticks ) 
        ms.WriteVInt32( x.MetaDataVersion )
        ms.WriteBoolean( x.ConfirmDelivery )
        ms.WriteUInt64( x.ContentKey )
        if x.bEncodeMapping && Utils.IsNull x.Mapping then 
            x.GetMapping() |> ignore 
        if x.NumPartitions < 0 then 
            let msg = sprintf "Failed, DSet %s:%s NumPartitions is %d" x.Name x.VersionString x.NumPartitions
            failwith msg
        let numPartitions = if Utils.IsNotNull x.Mapping then x.NumPartitions else -x.NumPartitions 
        ms.WriteVInt32( numPartitions )
        let numReplications = 
            if x.bEncodeMapping then 
                x.NumReplications
            else
                - x.NumReplications
        ms.WriteVInt32( numReplications ) 
        if numReplications > 0 && numPartitions>0 then 
            let mapping = x.GetMapping()
            for p = 0 to x.NumPartitions-1 do
                ms.WriteVInt32( mapping.[p].Length )
                for r = 0 to mapping.[p].Length-1 do
                    ms.WriteVInt32( mapping.[p].[r] )
        if Utils.IsNull x.Password || x.Password.Length=0 then 
            flag <- flag &&& (~~~ DSetMetadataStorageFlag.HasPassword )
        if Utils.IsNotNull x.MappingNumElems then 
            flag <- flag ||| DSetMetadataStorageFlag.HasMappingElems
        else
            flag <- flag &&& (~~~ DSetMetadataStorageFlag.HasMappingElems )
        if x.Partitioner.bPartitionByKey then 
            flag <- flag ||| DSetMetadataStorageFlag.PartitionByKey
        else
            flag <- flag &&& (~~~ DSetMetadataStorageFlag.PartitionByKey )
        if not bRemoveFunction then 
            // For DSet metadata stored on disk, remove all metadata. 
            if Utils.IsNotNull x.Function then 
                flag <- flag ||| DSetMetadataStorageFlag.HasFunction
            else
                flag <- flag &&& (~~~ DSetMetadataStorageFlag.HasFunction )
            match x.Dependency with 
            | StandAlone ->
                flag <- flag &&& (~~~ DSetMetadataStorageFlag.HasDependency )
            | _ -> 
                flag <- flag ||| DSetMetadataStorageFlag.HasDependency
            if x.NumParallelExecution > 0 then 
                flag <- flag ||| DSetMetadataStorageFlag.HasNumParallelExecution
            else 
                flag <- flag &&& (~~~ DSetMetadataStorageFlag.HasNumParallelExecution )
        ms.WriteInt32( int flag )
        x.FlagOnNetwork <- flag
        if ( flag&&&DSetMetadataStorageFlag.HasMappingElems)<>DSetMetadataStorageFlag.None then 
            for p = 0 to x.NumPartitions - 1 do
                ms.WriteVInt32( x.MappingNumElems.[p].Length )
                if x.MappingNumElems.[p].Length > 0 then 
                    for j = 0 to x.MappingNumElems.[p].Length - 1 do 
                        ms.WriteVInt32( x.MappingNumElems.[p].[j] )
            for p = 0 to x.NumPartitions - 1 do
                ms.WriteVInt32( x.MappingStreamLength.[p].Length )
                if x.MappingStreamLength.[p].Length > 0 then 
                    for j = 0 to x.MappingStreamLength.[p].Length - 1 do 
                        ms.WriteInt64( x.MappingStreamLength.[p].[j] )

        if ( flag&&&DSetMetadataStorageFlag.HasNumParallelExecution)<>DSetMetadataStorageFlag.None then 
            ms.WriteVInt32( x.NumParallelExecution )
        // These may not need to be decoded other than job
        if (flag&&&DSetMetadataStorageFlag.HasPassword)<>DSetMetadataStorageFlag.None then 
            ms.WriteString( x.Password )
        if (flag&&&DSetMetadataStorageFlag.HasFunction)<>DSetMetadataStorageFlag.None then 
            ms.WriteInt32( x.SendingQueueLimit )
            ms.WriteVInt32( x.MaxDownStreamAsyncTasks ) 
            ms.WriteVInt32( int x.CacheType )
            ms.WriteInt32( x.MaxCollectionTaskTimeout ) 
            ms.WriteVInt32( int x.PartitionExecutionMode ) 
            x.Function.Pack( ms )
            let funObjType = x.FunctionObj.GetType()
            if funObjType.ContainsGenericParameters then 
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "DSet %s:%s FunctionObj contains generic type, base type %A, generic type arg %A" x.Name x.VersionString funObjType.BaseType funObjType.GenericTypeArguments ))
        if (flag&&&DSetMetadataStorageFlag.HasDependency)<>DSetMetadataStorageFlag.None then 
            x.EncodeDependency( ms ) 

        if not bRemoveFunction then
            match x.DependencyDownstream with 
            | Discard -> 
                x.HasDownStreamDependency <- false
            | _ -> 
                x.HasDownStreamDependency <- true
        if shouldCodeHasDownStreamDependencyFlag then
            ms.WriteBoolean(x.HasDownStreamDependency)
        // Note: Dependency object is encoded later, to avoid loop calculation of hash. 
    // In case where x.Pack should not encode the HasDownStreamDependency flag when it was called, this method can be used  later
    member internal x.PackHasDownStreamDependencyFlag (ms : StreamBase<byte>) =
        ms.WriteBoolean(x.HasDownStreamDependency)
    // Encode Dependency 
    member internal x.EncodeDependency( ms )= 
        let dependencyCode, depObjects = 
            match x.Dependency with 
            | Bypass (parent, _ ) -> 
                let code, _ = x.UpStreamDependentObjs()
                code, Seq.singleton ( parent :> DependentDObject )
            | _ -> 
                x.UpStreamDependentObjs()
        let depDSetArray = depObjects |> Seq.toArray
        ms.WriteVInt32( dependencyCode )
        DependentDObject.Pack( depDSetArray, ms ) 
    member internal x.UpStreamDependentObjs() =
            match x.Dependency with 
            | Passthrough parent ->
                1, Seq.singleton ( parent :> DependentDObject )
            | CorrelatedMixFrom parents -> 
                2, parents |> Seq.map ( fun x -> x :> DependentDObject )
            | WildMixFrom ( parent, parentStream ) -> 
                3, seq [ parent :> DependentDObject; parentStream :> DependentDObject ]
            | Bypass (parent, brothers ) ->
                4, seq { yield parent
                         yield! brothers } 
                   |> Seq.map ( fun x -> x :> DependentDObject )
            | Source ->
                5, Seq.empty
            | DecodeFrom pstream ->
                6, Seq.singleton ( pstream :> DependentDObject )
            | MixFrom parent -> 
                7, Seq.singleton ( parent :> DependentDObject )
            | MergeFrom parents -> 
                8, parents |> Seq.map ( fun x -> x :> DependentDObject )
            | StandAlone ->            
                0, Seq.empty
            | HashJoinFrom ( parent0,parent1 ) -> 
                9, seq [ parent0 :> DependentDObject; parent1 :> DependentDObject ]
            | CrossJoinFrom ( parent0,parent1 ) -> 
                10, seq [ parent0 :> DependentDObject; parent1 :> DependentDObject ]
    /// Only bypass has only parents as its dependency
    member private x.PrecodeDependentObjsImpl() =
            match x.Dependency with 
            | Bypass (parent, brothers ) ->
                Seq.singleton ( parent :> DependentDObject )
            | _ ->
                let _, dobjs = x.UpStreamDependentObjs() 
                dobjs
    member internal x.DecodeDependency( ms:StreamBase<byte> ) = 
        let dependencyCode = ms.ReadVInt32()
        let depDObjectArray = DependentDObject.Unpack( ms )
        try
            match dependencyCode with 
            | 1 -> 
                let depDSetArray = depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) 
                x.Dependency <- Passthrough depDSetArray.[0]
            | 2 -> 
                let depDSetArray = depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) 
                x.Dependency <- CorrelatedMixFrom (List<_>( depDSetArray ))
            | 3 -> 
                x.Dependency <- WildMixFrom ( DependentDSet( depDObjectArray.[0] ), DependentDStream( depDObjectArray.[1] ) )
            | 4 -> 
                // Bypass objects will be coded later. 
                let depDSetArray = depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) 
                x.Dependency <- Bypass ( depDSetArray.[0], List<_>() )
            | 5 -> 
                x.Dependency <- Source
            | 6 -> 
                let depDStream = List<_>(depDObjectArray |> Array.map( fun x -> DependentDStream(x) ) )
                x.Dependency <- DecodeFrom ( depDStream.[0] )
            | 7 -> 
                let depDSetArray = depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) 
                x.Dependency <- MixFrom depDSetArray.[0]
            | 8 -> 
                let depDSetArray = depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) 
                x.Dependency <- MergeFrom (List<_>( depDSetArray ))
            | 9 -> 
                let depDSetArray = depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) 
                x.Dependency <- HashJoinFrom ( depDSetArray.[0], depDSetArray.[1] )
            | 10 -> 
                let depDSetArray = depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) 
                x.Dependency <- CrossJoinFrom ( depDSetArray.[0], depDSetArray.[1] )
            | _ -> 
                let msg = sprintf "Fail in DSet.DecodeDependency, unsupported dependency code %d " dependencyCode
                Logger.Log( LogLevel.Error, msg )
                failwith msg
        with 
        | e -> 
            let msg = sprintf "Fail in DSet.DecodeDependency, exception %A " e
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    member internal x.EncodeDownStreamDependency( ms : StreamBase<byte> ) =
        /// Additional dependency coding for bypass
        match x.Dependency with 
        | Bypass ( _, brothers ) -> 
            let dobjs = brothers |> Seq.map ( fun o -> o:> DependentDObject ) |> Seq.toArray
            DependentDObject.Pack( dobjs, ms )
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Encode Bypass DSet %s:%s with %d brothers" x.Name x.VersionString dobjs.Length ))
        | _ -> 
            ()
        if x.HasDownStreamDependency then 
            let dependencyCode, depObjects = x.DownStreamDependentObjs()
            let depDSetArray = depObjects |> Seq.toArray
            ms.WriteVInt32( dependencyCode )
            DependentDObject.Pack( depDSetArray, ms )
    member internal x.DownStreamDependentObjs() = 
            match x.DependencyDownstream with 
            | Passforward child ->
                1, Seq.singleton ( child :> DependentDObject )
            | EncodeTo cstream ->
                2, Seq.singleton ( cstream :> DependentDObject )
            | DistributeForward children -> 
                3, children |> Seq.map ( fun x -> x :> DependentDObject )
            | SaveTo cstream ->
                4, Seq.singleton ( cstream :> DependentDObject )
            | MixTo child -> 
                5, Seq.singleton ( child :> DependentDObject )
            | WildMixTo ( child, cstream ) -> 
                6, seq [ child :> DependentDObject; cstream :> DependentDObject ]
            | CorrelatedMixTo ( child ) -> 
                7, Seq.singleton ( child :> DependentDObject )
            | MergeTo ( child ) -> 
                8, Seq.singleton ( child :> DependentDObject )
            | Discard ->
                0, Seq.empty
            | HashJoinTo child -> 
                9, Seq.singleton ( child :> DependentDObject )
            | CrossJoinTo child -> 
                10, Seq.singleton ( child :> DependentDObject )
    member internal x.DecodeDownStreamDependency( ms:StreamBase<byte> ) = 
        let hasDownStreamDependency = ms.ReadBoolean()
        match x.Dependency with 
        | Bypass (parent, _ ) -> 
            // Decode brothers here. 
            let depDObjectArray = DependentDObject.Unpack( ms )   
            let depDSetList = List<_>(depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) )
            x.Dependency <- Bypass ( parent, depDSetList )
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Decode Bypass DSet %s:%s with %d brothers" x.Name x.VersionString depDSetList.Count ))
        | _ -> 
            ()        
        if hasDownStreamDependency then 
            let dependencyCode = ms.ReadVInt32()
            let depDObjectArray = DependentDObject.Unpack( ms )
            match dependencyCode with 
            | 1 -> 
                x.DependencyDownstream <- Passforward (DependentDSet( depDObjectArray.[0] ))
            | 2 -> 
                x.DependencyDownstream <- EncodeTo (DependentDStream( depDObjectArray.[0] ))
            | 3 -> 
                let depDSetList = List<_>(depDObjectArray |> Array.map( fun x -> DependentDSet(x) ) )
                x.DependencyDownstream <- DistributeForward depDSetList
            | 4 -> 
                x.DependencyDownstream <- SaveTo (DependentDStream( depDObjectArray.[0] ))
            | 5 -> 
                x.DependencyDownstream <- MixTo (DependentDSet( depDObjectArray.[0] ))
            | 6 -> 
                x.DependencyDownstream <- WildMixTo (DependentDSet( depDObjectArray.[0] ), DependentDStream( depDObjectArray.[1] ))
            | 7 -> 
                x.DependencyDownstream <- CorrelatedMixTo (DependentDSet( depDObjectArray.[0] ))
            | 8 -> 
                x.DependencyDownstream <- MergeTo (DependentDSet( depDObjectArray.[0] ))
            | 9 -> 
                x.DependencyDownstream <- HashJoinTo (DependentDSet( depDObjectArray.[0] ))
            | 10 -> 
                x.DependencyDownstream <- CrossJoinTo (DependentDSet( depDObjectArray.[0] ))
            | _ -> 
                let msg = sprintf "Fail in DSet.DecodeDownStreamDependency, unsupported dependency code %d " dependencyCode
                Logger.Log( LogLevel.Error, msg )
                failwith msg
    static member val internal UpStreamInfo = [| "StandAlone"; 
                                         "Passthrough";
                                         "CorrelatedMixFrom"; 
                                         "WildMixFrom";
                                         "Bypass"; 
                                         "Source"; 
                                         "DecodeFrom"; 
                                         "MixFrom";
                                         "MergeFrom"; 
                                         "HashJoinFrom";
                                         "CrossJoinFrom" |] with get
    static member val internal DownStreamInfo = [| "Discard"; 
                                           "Passforward"; 
                                           "EncodeTo"; 
                                           "DistributeForward";
                                           "SaveTo";
                                           "MixTo";
                                           "WildMixTo";
                                           "CorrelatedMixTo";
                                           "MergeTo";
                                           "HashJoinTo";
                                           "CrossJoinTo" |] with get
    /// Setup Dependency hash
    member private x.SetupDependencyHashImpl() = 
        let dependencyCode, depObjects = x.UpStreamDependentObjs()
        for dobj in depObjects do 
            let parent = dobj.Target
            dobj.Hash <- parent.Hash
        let dependencyCode, depObjects = x.DownStreamDependentObjs()
        for dobj in depObjects do 
            let parent = dobj.Target
            dobj.Hash <- parent.Hash

    member internal x.ShowDependencyInfo( ) = 
        let code_upstream, objs_upstream = x.UpStreamDependentObjs()
        let code_downstream, objs_downstream = x.DownStreamDependentObjs()
        sprintf "DSet %s:%s %d partitions: %s %s : %s %s" 
            x.Name x.VersionString x.NumPartitions
            DSet.UpStreamInfo.[code_upstream] 
            ( objs_upstream |> Seq.map ( fun o -> o.ToString() ) |> String.concat "," )
            DSet.DownStreamInfo.[code_downstream] 
            ( objs_downstream |> Seq.map ( fun o -> o.ToString() ) |> String.concat "," )

    // Peek readStream to get DSet name and version information, read pointer is set back to the origin.             
    static member internal Peek( readStream: StreamBase<byte> ) = 
        let orgpos = readStream.Position
        let clname = readStream.ReadString()
        let clVerNumber = readStream.ReadInt64()
        let loadBalancerType = enum<LoadBalanceAlgorithm>( readStream.ReadVInt32() )
        let partType = readStream.ReadVInt32()
        let storageType = enum<StorageKind>( readStream.ReadVInt32() )
        let name = readStream.ReadString()
        let verNumber = readStream.ReadInt64()
        readStream.Seek( orgpos, SeekOrigin.Begin ) |> ignore
        name, verNumber
    /// Deserialization of DSet 
    static member internal Unpack( readStream: StreamBase<byte>, bUnpackFunc ) = 
        //let buf = readStream.GetBuffer()
        let startpos = readStream.Position
        let clname = readStream.ReadString()
        let clVerNumber = readStream.ReadInt64()
        //let verCluster = DateTime( ticks )
        //let clusterName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( clname, verCluster ) 
        let loadBalancerType = enum<LoadBalanceAlgorithm>( readStream.ReadVInt32() )
        let partType = readStream.ReadVInt32()
        let storageType = enum<StorageKind>( readStream.ReadVInt32() )
        let name = readStream.ReadString()
        let ver = DateTime( readStream.ReadInt64() )
//        let useCluster = 
//            match ClusterFactory.Retrieve( clusterName ) with 
//            | Some (cl ) -> cl
//            | None -> 
//                if File.Exists clusterName then 
//                    let loadCluster = Cluster( null, clusterName )
//                    ClusterFactory.Store( clusterName, loadCluster )
//                    loadCluster
//                else
//                    null
        let useCluster = ClusterFactory.FindCluster( clname, clVerNumber )
        let curDSet = DSet( useCluster )
        curDSet.TypeOfLoadBalancer <- loadBalancerType
        let partitioner = Partitioner( TypeOf = enum<_>(partType) )
        curDSet.Partitioner <- partitioner
        curDSet.StorageType <- storageType
        curDSet.Name <- name
        let metaDataVersion = readStream.ReadVInt32( )
        curDSet.MetaDataVersion <- metaDataVersion
        curDSet.ConfirmDelivery <- readStream.ReadBoolean(  )
        curDSet.Version <- ver
        let contentKey = readStream.ReadUInt64()
        curDSet.ContentKey <- contentKey
        let numPartitions = readStream.ReadVInt32()
        curDSet.NumPartitions <- Math.Abs( numPartitions )
        let numReplications = readStream.ReadVInt32()
        if numReplications > 0 && numPartitions > 0 then 
            curDSet.NumReplications <- numReplications
            // DSet unpacking does not call initialize. 
            // curDSet.Initialize()
            // Set Mapping, may not be necessary. 
            let mapping = Array.create<int[]> numPartitions null 
            for p = 0 to numPartitions-1 do
                let numRep = readStream.ReadVInt32()
                mapping.[p] <- Array.zeroCreate<int> numRep 
                for r = 0 to numRep-1 do
                    mapping.[p].[r] <- readStream.ReadVInt32()
            curDSet.Mapping <- mapping 
        else
            curDSet.NumReplications <- Math.Abs( -numReplications )
            if numReplications = 0 then 
                /// Flat mapping 
                if Utils.IsNotNull useCluster then 
                    curDSet.NumReplications <- useCluster.NumNodes
            /// Undetermined mapping, the mapping should be resolved when x.GetMapping() is called. 
            curDSet.Mapping <- null
            curDSet.bEncodeMapping <- false
        let flag = enum<DSetMetadataStorageFlag>(readStream.ReadInt32())
        curDSet.FlagOnNetwork <- flag
        // Decode # of Elems of the Mapping
        if ( flag&&&DSetMetadataStorageFlag.HasMappingElems)<>DSetMetadataStorageFlag.None then 
            curDSet.MappingNumElems <- Array.zeroCreate<_> curDSet.NumPartitions
            for p = 0 to curDSet.NumPartitions - 1 do
                let len = readStream.ReadVInt32() 
                if len > 0 then 
                    curDSet.MappingNumElems.[p] <- Array.zeroCreate<_> len 
                    for j = 0 to len - 1 do 
                        curDSet.MappingNumElems.[p].[j] <- readStream.ReadVInt32() 
            curDSet.MappingStreamLength <- Array.zeroCreate<_> curDSet.NumPartitions
            for p = 0 to curDSet.NumPartitions - 1 do
                let len = readStream.ReadVInt32() 
                if len > 0 then 
                    curDSet.MappingStreamLength.[p] <- Array.zeroCreate<_> len 
                    for j = 0 to len - 1 do 
                        curDSet.MappingStreamLength.[p].[j] <- readStream.ReadInt64()
        if (flag&&&DSetMetadataStorageFlag.HasPassword)<>DSetMetadataStorageFlag.None then 
            curDSet.Password <- readStream.ReadString()
        if (flag &&& DSetMetadataStorageFlag.PartitionByKey)<>DSetMetadataStorageFlag.None then 
            curDSet.Partitioner.bPartitionByKey <- true
            curDSet.bParitionerSet <- true
        // Decode flag on # of parallel executions allowed. 
        if ( flag&&&DSetMetadataStorageFlag.HasNumParallelExecution)<>DSetMetadataStorageFlag.None then 
            curDSet.NumParallelExecution <- readStream.ReadVInt32()
        if (flag&&&DSetMetadataStorageFlag.HasFunction)<>DSetMetadataStorageFlag.None then 
            curDSet.SendingQueueLimit <- readStream.ReadInt32( )
            curDSet.MaxDownStreamAsyncTasks <- readStream.ReadVInt32(  ) 
            curDSet.CacheType <- enum<_>( readStream.ReadVInt32( ) )
            curDSet.MaxCollectionTaskTimeout <- readStream.ReadInt32( )
            curDSet.PartitionExecutionMode <- enum<_> ( readStream.ReadVInt32( ) )
            // This monitoring is added as function unpacking is a likely place of errors. 
            if DeploymentSettings.LoadCustomAssebly && bUnpackFunc then 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "To Unpack the function of DSet %s:%s" curDSet.Name curDSet.VersionString ))
            curDSet.InternalFunction <- Function.Unpack( readStream, bUnpackFunc )
            let func = curDSet.FunctionObj
            if Utils.IsNotNull func then 
                // Set Serialization Limit
                curDSet.SerializationLimit <- func.SerializationLimit
        if (flag&&&DSetMetadataStorageFlag.HasDependency)<>DSetMetadataStorageFlag.None then 
            curDSet.DecodeDependency( readStream ) 
        /// Get hash
        let endpos = readStream.Position
        //curDSet.Hash <- HashByteArrayWithLength( buf, int startpos, int (endpos-startpos))
        curDSet.Hash <- readStream.ComputeChecksum(startpos, endpos-startpos)
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Decode DSet %s:%s, Hash = %s (%d-%d)" curDSet.Name curDSet.VersionString (BytesToHex(curDSet.Hash)) startpos endpos ))
        curDSet.DecodeDownStreamDependency( readStream )
        curDSet
    member val private bAllocateWriteLifeCycleObj = false with get, set
    member val private WriteLifeCycleObjRef : JobLifeCycle ref = ref null with get
    // This write ID is reserved only for the cleanup, to make sure that clean up operation can always be executed 
    member val internal WriteIDForCleanUp = Guid.Empty with get, set
    member internal x.BeginWriteJobOnDaemon( jobLifeCycleObj: JobLifeCycle ) = 
        let oldObj = Interlocked.CompareExchange( x.WriteLifeCycleObjRef, jobLifeCycleObj, null )
        if Utils.IsNull oldObj then 
            // Store Job ID for clean up operation. 
            x.WriteIDForCleanUp <- jobLifeCycleObj.JobID
            jobLifeCycleObj.OnDisposeFS( fun _ -> x.WriteIDForCleanUp <- Guid.Empty
                                                  x.WriteLifeCycleObjRef := null
                
                                        )
        elif Object.ReferenceEquals( oldObj, jobLifeCycleObj) then 
            ()
        else
            failwith ( sprintf "Try to initilialize a new write job %A on DSet %s:%s while an existing job %A is still in process. A DSet can't be simultaneously involved in two jobs that both write to it."
                                oldObj.JobID x.Name x.VersionString jobLifeCycleObj.JobID )
    member internal x.BeginWriteJob( jobLifeCycleObj: JobLifeCycle ) = 
        let oldObj = Interlocked.CompareExchange( x.WriteLifeCycleObjRef, jobLifeCycleObj, null )
        if Utils.IsNull oldObj then 
            // Store Job ID for clean up operation. 
            x.WriteIDForCleanUp <- jobLifeCycleObj.JobID
            jobLifeCycleObj.OnCancellationFS( x.SendCancelWriteToNetwork)
            jobLifeCycleObj.OnDisposeFS( x.OnCancelWriteJob )
        elif Object.ReferenceEquals( oldObj, jobLifeCycleObj) then 
            ()
        else
            failwith ( sprintf "Try to initilialize a new write job %A on DSet %s:%s while an existing job %A is still in process. A DSet can't be simultaneously involved in two jobs that both write to it."
                                oldObj.JobID x.Name x.VersionString jobLifeCycleObj.JobID )
    member internal x.BeginWriteJob() = 
        let jobLifeCycleObj = JobLifeCycleCollectionApp.BeginJob()
        x.BeginWriteJob( jobLifeCycleObj )
        x.bAllocateWriteLifeCycleObj <- true 
    member internal x.BeginWriteJob( cts : CancellationToken ) =
        x.BeginWriteJob() 
        let jobLifeCyleObj = Volatile.Read( x.WriteLifeCycleObjRef )
        if Utils.IsNotNull jobLifeCyleObj then 
            jobLifeCyleObj.RegisterCancellation( cts )
    member internal x.DisposeWriteJob() = 
        let oldObj = Volatile.Read( x.WriteLifeCycleObjRef )
        if Utils.IsNotNull oldObj && 
            Object.ReferenceEquals( Interlocked.CompareExchange( x.WriteLifeCycleObjRef, null, oldObj ), oldObj ) then 
                // We are responsible to dispose the JobLifeCycle Ojbect if it is allocated here 
                if x.bAllocateWriteLifeCycleObj then 
                    JobLifeCycleCollectionApp.UnregisterJob( oldObj )
                    ( oldObj :> IDisposable ).Dispose()
    /// Finalize write
    member internal x.OnCancelWriteJob() = 
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Unregister for DSet Write %A" x.Name))
        /// Unregister can always be called. 
        let currentWriteID = x.WriteIDForCleanUp
        x.Cluster.UnRegisterCallback( currentWriteID, x.CallbackCommand )
        x.DisposeWriteJob()
    /// Normal ending of a write job 
    member internal x.EndWriteJob() = 
        let oldObj = Volatile.Read( x.WriteLifeCycleObjRef ) 
        if Utils.IsNotNull oldObj then 
            // OnCancelWriteJob will be called by EndJob when all in process job actions are done
            oldObj.EndJob()

    /// Grab a single job action object, when secured, the cancellation of the underlying jobLifeCycle object will be delayed 
    /// until this action completes. 
    member internal x.TryExecuteSingleJobAction() = 
        let writeObj = Volatile.Read( x.WriteLifeCycleObjRef )
        SingleJobActionApp.TryEnterAndThrow( writeObj )
        
    /// BeginStore: called to begin storing operation of a DSet to cloud. 
    member internal x.BeginWriteToNetwork ( ) = 
        let mapping = x.GetMapping()
        let bUsed = Array.create x.Cluster.NumNodes false
        mapping |> Array.iter ( Array.iter ( fun n -> bUsed.[n] <- true ) )
        nActiveConnection <- 0
        for i = 0 to bUsed.Length-1 do
            if bUsed.[i] then 
                // This triggers a connect to the corresponding peer, even nothing has been written out yet. 
                x.Cluster.QueueForWrite( i ) |> ignore
                nActiveConnection <- nActiveConnection + 1
        x.Cluster.InitializeQueues()
        bFirstCommand <- Array.create bUsed.Length true
        partitionCheckmark <- Array.zeroCreate<int64> x.NumPartitions
        partitionProgress <- Array.zeroCreate<int64> x.NumPartitions
        partitionPending <- Array.create (x.NumPartitions) ( new ConcurrentQueue<int>())
        partitionSerial <- Array.zeroCreate<int64> x.NumPartitions
        partitionSerialConfirmed <- Array.init(x.NumPartitions) ( fun i -> new ConcurrentQueue<int64*int*int>())
        // Clear all timeout clocks.
        enterTimeout <- Array.create x.NumPartitions x.Clock.ElapsedTicks
        /// This logic will check if the current write job has been cancelled, and properly process cancellation function 
        using ( x.TryExecuteSingleJobAction() ) ( fun jobAction -> 
            if Utils.IsNotNull jobAction then 
                let currentWriteID = jobAction.JobID
                if currentWriteID = Guid.Empty then 
                    let msg = "!!! Write Job ID should never be null !!!"
                    jobAction.ThrowExceptionAtCallback( msg )
        // Register call back to parse incoming message directed to this DSet. 
                if nActiveConnection > 0 then 
                    x.Cluster.RegisterCallback( currentWriteID, x.Name, x.Version.Ticks, x.CallbackCommand, 
                        { new NetworkCommandCallback with 
                                    member this.Callback( cmd, peeri, ms, jobID, name, verNumber, cl ) = 
                                        x.WriteDSetCallback( cmd, peeri, ms, jobID )
                        } )
            )
        ()
    member val collectionWriteInitiated = ConcurrentDictionary<int, bool>() with get 
    member internal x.DoFirstWrite( peeri ) = 
        if bFirstCommand.[peeri] then 
            using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
                if Utils.IsNotNull jobObj then 
                    let currentWriteID = jobObj.JobID
                    let queuePeer = x.Cluster.QueueForWrite(peeri)
                    // Get the receiving serial of the queuePeer, we may later know 
                    // whether any command has ever been received from that peer. 
                    if Utils.IsNull rcvdSerialInitialValue then
                        lock(x) (fun _ ->
                            if (Utils.IsNull rcvdSerialInitialValue) then
                                rcvdSerialInitialValue <- Array.zeroCreate<int64> x.Cluster.NumNodes
                        )
                    rcvdSerialInitialValue.[peeri] <- queuePeer.RcvdCommandSerial
                    // send cluster info
                    let cmd = ControllerCommand( ControllerVerb.Set, ControllerNoun.ClusterInfo )
                    using ( new MemStream( 10240 )) ( fun msSend ->
                        x.Cluster.ClusterInfo.Pack( msSend )
                        queuePeer.ToSend( cmd, msSend, true )
                    )
                    // send dset to partners. 
                    using ( new MemStream( 1024 )) ( fun msSend -> 
                        msSend.WriteGuid( currentWriteID)
                        x.Pack( msSend, DSetMetadataStorageFlag.HasPassword )
                        let cmd = ControllerCommand( ControllerVerb.Set, ControllerNoun.DSet ) 
                        queuePeer.ToSend( cmd, msSend ) )
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Send first write command (Set, ClusterInfo) to peer %d of cluster %s"
                                                                           peeri 
                                                                           x.Cluster.Name ))

                    bFirstCommand.[peeri] <- false

                    // Do we need to limit speed?
                    if x.NumReplications>1 then 
                        let bToSent = ref false
                        using( new MemStream( 1024 ) ) ( fun msSpeed -> 
                            msSpeed.WriteGuid( currentWriteID )
                            msSpeed.WriteString( x.Name ) 
                            msSpeed.WriteInt64( x.Version.Ticks )
                            let node = x.Cluster.Nodes.[peeri]
                            msSpeed.WriteInt64( x.PeerRcvdSpeedLimit )
                            if x.PeerRcvdSpeedLimit < node.NetworkSpeed then 
                                queuePeer.SetRcvdSpeed(x.PeerRcvdSpeedLimit)
                                bToSent := true
                            if !bToSent then 
                                queuePeer.ToSend( ControllerCommand( ControllerVerb.LimitSpeed, ControllerNoun.DSet), msSpeed )  
                        )
           ) 
    /// Sending cancellation to all other peers. 
    /// Job should not be cancelled at that time
    member private x.SendCancelWriteToNetwork() = 
        using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
            if Utils.IsNotNull jobObj then 
                using ( new MemStream( 1024 )) ( fun msSend -> 
                    let currentWriteID = jobObj.JobID
                    msSend.WriteGuid( currentWriteID )
                    msSend.WriteString( x.Name ) 
                    msSend.WriteInt64( x.Version.Ticks )
                    for peeri=0 to x.Cluster.NumNodes-1 do 
                        let queuePeer = x.Cluster.QueueForWrite(peeri)
                        if Utils.IsNotNull queuePeer && not queuePeer.Shutdown then 
                            queuePeer.ToSend( ControllerCommand( ControllerVerb.Cancel, ControllerNoun.DSet), msSend )
                )
        )
    member internal x.bIsClusterReplicate() = 
        match x.Cluster.ReplicationType with 
        | ClusterReplicationType.ClusterReplicate -> 
            true
        | _ ->
            false                    
    /// CanWrite: can we write to partition i, and which peer should it be write to?
    /// Return: a list of peer idx that it should write to. 
    ///     [| -1 |] : all peers are blocked, please check later to write. 
    ///     [| Int32.MinValue |] : no peer can be found to write this partition, the caller may want to raise an exception. 
    ///     one peer for P2PReplicate or ClusterReplicate, a list of peers for DirectReplicate
    member internal x.CanWrite( parti ) =       
        // Find the peer to be written to
        let partimapping = x.PartiMapping( parti )
        let peerReplicateArray = 
            partimapping |> Array.map ( fun peeri -> let peerQueue = x.Cluster.QueueForWrite(peeri)
                                                     if Utils.IsNotNull peerQueue && (not peerQueue.Shutdown) then 
                                                        // Peer is available 
                                                        if peerQueue.CanSend then 
                                                            // The higher the number, the more favorable the peer is. 
                                                            peeri, (x.SendingQueueLimit / nActiveConnection) - int peerQueue.UnProcessedCmdInBytes
                                                        else
                                                            peeri, -1 
                                                     else
                                                        peeri, Int32.MinValue )
        match x.Cluster.ReplicationType with 
        | ClusterReplicationType.P2PReplicate
        | ClusterReplicationType.ClusterReplicate -> 
            // For P2P Replicate && ClusterReplicate, we only need to send out one replication to the peer with the highest resource number. 
            let peeri, peeriresource = peerReplicateArray |> Seq.maxBy( fun (peeri, resource) -> resource )
            if peeriresource < 0 then 
                Array.create 1 peeriresource
            else
                Array.create 1 peeri
        | ClusterReplicationType.DirectReplicate -> 
            // Need to replicate content to all node, 
            let validPeers = peerReplicateArray |> Seq.filter ( fun (peeri, resource) -> resource > Int32.MinValue ) |> Seq.toArray
            let peeri, peeriresource = validPeers |> Array.minBy( fun (peeri, resource) -> resource )
            if validPeers.Length < x.NumReplications then 
                let msg = sprintf "The number of valid peers for partition %d is %d, need to find alternative replication plan" parti validPeers.Length
                Logger.Log( LogLevel.Error, msg )
                failwith msg
            else
                if peeriresource < 0 then 
                    // Wait on that peer
                    Array.create 1 peeriresource
                else 
                    validPeers |> Array.map ( fun ( peeri, resource ) -> peeri )
        | _ -> 
            let msg = sprintf "DSet.CanWrite, invalid ClusterReplication Type %A" x.Cluster.ClusterInfo.ReplicationType
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    /// Serial Number Used for parition parti 
    member internal x.GetSerialForWrite( parti, numElems:int ) = 
        let serial = partitionSerial.[parti]
        partitionSerial.[parti] <- partitionSerial.[parti] + int64 numElems
        serial
    /// Common Write Routine for DSets. 
    member internal x.WriteCommon( parti, peeri, ms : StreamBase<byte>, verb ) = 
        // If it is the first time write to a peer, do some operation. 
        x.DoFirstWrite( peeri )
        let cmd = ControllerCommand( verb, ControllerNoun.DSet )
        partitionPending.[parti].Enqueue( int ms.Length ) 
        let peerQueue = x.Cluster.QueueForWrite( peeri )
        peerQueue.ToSend( cmd, ms )

        if x.ConfirmDelivery then 
//            // let sha512 = Security.Cryptography.SHA512CryptoServiceProvider()
//            let sha512 = Security.Cryptography.SHA512Managed()
//            let res = sha512.ComputeHash( buf, offset, length )
            let res = ms.ComputeSHA512(0L, ms.Length)
            // Overwrite old item, if there is one. s
            let msRep = ms.Replicate()
            deliveryQueue.Item( res ) <- ( x.Clock.ElapsedTicks, parti, msRep, ref 0 )
                
    /// Write certain data to the DSet
    member internal x.Write( parti, peeri, ms ) = 
        x.WriteCommon( parti, peeri, ms, ControllerVerb.Write )

    /// Write and replicate peer data to the DSet
    member internal x.WriteAndReplicate( parti, peeri, ms ) = 
        // If it is the first time write to a peer, do some operation. 
        x.WriteCommon( parti, peeri, ms, ControllerVerb.WriteAndReplicate )

    /// End partition parti peeri
    member internal x.EndParition parti peeri = 
        Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "Sending Close, Partition of partition %d to peer %d" parti peeri ))
        let cmd = ControllerCommand( ControllerVerb.Close, ControllerNoun.Partition )
        use msSend = new MemStream( 1024 )
        msSend.WriteGuid( x.WriteIDForCleanUp )
        msSend.WriteString( x.Name ) 
        msSend.WriteInt64( x.Version.Ticks )
        msSend.WriteVInt32(parti)
        // 0 errors. 
        msSend.WriteVInt32(0)
        let peerQueue = x.Cluster.QueueForWrite( peeri )
        peerQueue.ToSend( cmd, msSend )

    /// End Partition parti peeri
//    member x.EndParition = 
//        x.EndParitionCommon ControllerVerb.EndPartition
//    /// End Partition parti peeri
//    member x.EndParitionAndReplicate = 
//        x.EndParitionCommon ControllerVerb.EndPartitionAndReplicate

    /// Graceful shutdown:
    /// Wait for clients to shut down, i.e., to receive a confirmation of (Report, DSet) or (ReportClose, DSet)
    /// maxWait: maximum wait for time, in seconds. 
    member private x.GracefulWaitForReprot( maxWait ) = 
        let t1 = (PerfDateTime.UtcNow())
        using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
            if Utils.IsNotNull jobObj then 
                let mutable shouldContinueToWait = true
                while shouldContinueToWait do
                    if jobObj.IsCancelledAndThrow then 
                        shouldContinueToWait <- false
                    else
                        let mutable nLive = 0
                        for i=0 to x.Cluster.NumNodes-1 do 
                            let q = x.Cluster.Queue( i )
                            if Utils.IsNotNull q && not q.Shutdown && not reportReceived.Value.[i] then 
                                nLive <- nLive + 1 
                        if nLive <= 0 then 
                            shouldContinueToWait <- false
                            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Exit GracefulWaitForReprot for DSet %A: has received report from all peers (or peer has shutdown)" x.Name))
                        else
                            let t2 = (PerfDateTime.UtcNow())
                            // maximum wait time is 60s for the client to exit
                            if t2.Subtract(t1).TotalSeconds >= maxWait then
                                shouldContinueToWait <- false
                                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Exit GracefulWaitForReprot for DSet %A: reached max wait time" x.Name))
                        if shouldContinueToWait then 
                            ThreadPoolWaitHandles.safeWaitOne( jobObj.WaitHandle, 1 ) |> ignore 
        )
    /// Cancel by Exception
    member internal x.CancelWriteByException( ex ) = 
        using ( x.TryExecuteSingleJobAction() ) ( fun jobAction -> 
            if Utils.IsNotNull jobAction then 
                jobAction.CancelByException(ex )
        )
    /// EndStore: called to end storing operation of a DSet to cloud
    member internal x.EndWriteToNetwork( timeToWait ) =
        let bCloseDSetSent = Array.create (x.Cluster.NumNodes) false
        let bAllCloseDSetSent = ref false
        let t1 = (PerfDateTime.UtcNow())
        let bRetRef = ref false
        using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
            if Utils.IsNotNull jobObj then 
                let currentWriteID = jobObj.JobID               
                let cmd = ControllerCommand( ControllerVerb.Close, ControllerNoun.DSet ) 
                using ( new MemStream( 1024 ) ) ( fun msSend -> 
                    msSend.WriteGuid( currentWriteID )
                    msSend.WriteString( x.Name ) 
                    msSend.WriteInt64( x.Version.Ticks )

                    Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Entering DSet EndWrite for %A" x.Name))
                    while not !bAllCloseDSetSent && (PerfDateTime.UtcNow()).Subtract(t1).TotalSeconds<timeToWait do
                        bAllCloseDSetSent := true
                        if not jobObj.IsCancelledAndThrow then 
                            // Check on cancellation 
                            for i=0 to x.Cluster.NumNodes-1 do 
                                // Send a Close DSet command only for active peer. 
                                x.DoFirstWrite( i ) 
                                let q = x.Cluster.Queue( i )
                                if Utils.IsNotNull q && ( not q.Shutdown ) then 
                                        if not bCloseDSetSent.[i] then 
                                            if not bFirstCommand.[i] then 
                                                q.ToSend( cmd, msSend )
                                                bCloseDSetSent.[i] <- true
                                            else
                                                if q.CanSend then 
                                                    q.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.All ) , msSend )
                                                    bCloseDSetSent.[i] <- true
                                                else
                                                    bAllCloseDSetSent := false
                                    
                            if not !bAllCloseDSetSent then 
                                // Wait for input command. 
                                ThreadPoolWaitHandles.safeWaitOne( jobObj.WaitHandle, 10 ) |> ignore 
                    )
                let remainingWait = Math.Max( 0., timeToWait - (PerfDateTime.UtcNow()).Subtract(t1).TotalSeconds )
                x.GracefulWaitForReprot(remainingWait)
                bRetRef := 
                    if (x.Flag &&& DSetFlag.CountWriteDSet)<>DSetFlag.None then 
                        x.ReplicationAnalysis()
                    else
                        true
        )
        x.EndWriteJob()
        !bRetRef


    /// Decode Storage flag 
    /// Return: (bPassword, bConfirmDelivery )
    member internal x.InterpretStorageFlag( flag ) =
        ( flag &&& 0x01=0, flag &&& 0x02=0 ) 
    /// Save the Meta Data of DSet to a stream
    member internal x.SaveToMetaData( stream:Stream, flag ) = 
        use ms = new MemStream( 10240 ) 
        x.Pack( ms, flag )
        ms.Flush()
        //stream.Write( ms.GetBuffer(), 0, int ms.Length )
        ms.ReadToStream(stream, 0L, ms.Length)
        stream.Flush()
    /// Save the Meta Data of DSet to a file 
    member internal x.SaveToMetaData( filename, flag ) =
        let dirpath = Path.GetDirectoryName( filename )
        if Utils.IsNotNull dirpath then
            // Create directory if necessary
            DirectoryInfoCreateIfNotExists (dirpath) |> ignore

        use ms = new MemStream( 10240 ) 
        x.Pack( ms, flag )
        WriteBytesToFileConcurrentP filename (ms.GetBuffer()) 0 (int ms.Length)

    /// The root path information for the metadata, used for all DSet of different version.
    member internal x.RootPath( ) = 
        let usename = x.Name.Replace( '/', '\\' )
        Path.Combine( DeploymentSettings.LocalFolder, "DSetMetadata", usename )
    /// Root metadata path
    static member internal RootMetadataPath() = 
        Path.Combine( DeploymentSettings.LocalFolder, "DSetMetadata" )
    /// The path information for the metadata
    member internal x.PathInfo () = 
        Path.Combine( x.RootPath(), x.VersionString )
    /// DSet metadata name
    member internal x.MetadataFilename() = 
        Path.Combine( x.PathInfo(), sprintf "DSet%08d.meta" x.MetaDataVersion )
    /// Save Metadata
    member internal x.SaveMetadata() = 
        // Note that during save, function and dependency are not stored in metadata. 
        x.SaveToMetaData( x.MetadataFilename(), DSetMetadataStorageFlag.StoreMetadata )
    /// Try Load Metadata, search local metadata store to find the latest metadata file 
    member internal x.LoadMetadata( ms:MemStream, bUnpackFunction ) = 
        let curDSet = DSet.Unpack( ms, bUnpackFunction )
        if Utils.IsNull x.Name || x.Name.Length=0 || curDSet.Name.ToUpper() = x.Name.ToUpper() then 
            if Utils.IsNotNull curDSet.Cluster then 
                x.CopyMetaData( curDSet, DSetMetadataCopyFlag.Copy )
                x.bValidMetadata <- true
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Successfully load metadata for DSet %s:%s, metadata version %d" (x.Name) (x.VersionString) (x.MetaDataVersion) ))
                true
            else
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DSet %s:%s, can't find valid cluster file" (x.Name) (x.VersionString) ))
                false
        else
            let msg = sprintf "Load metadata for DSet %s. The loaded DSet has name %s" x.Name curDSet.Name 
            Logger.Log( LogLevel.Warning, msg )
            false
    /// Try Load Metadata, search local metadata store to find the latest metadata file 
    member internal x.TryLoadDSetMetadata( bUnpackFunction) = 
            let mutable bSuccess = true
            let path = x.RootPath() 
            if not (Directory.Exists( path )) then 
                bSuccess <- false
            if bSuccess then 
                let dirs = Directory.GetDirectories( path ) |> Array.sort
                bSuccess <- dirs.Length > 0
                if bSuccess then 
                    let mutable dirpos = dirs.Length - 1
                    let mutable bFind = false
                    let orgCluster = x.Cluster
                    while not bFind && dirpos >=0 do
                    // Retrieve latest version 
                        let dir = dirs.[ dirpos ]
                        let files = Directory.GetFiles( dir, "DSet*.meta" ) |> Array.sort
                        let mutable filepos = files.Length - 1
                        if filepos >= 0 then 
                            // Only read last version of metadata file per DSet version. 
                            let filename = files.[ filepos ]
                            let loadDSet = DSet( x, DSetMetadataCopyFlag.Copy ) 
                            let byt = ReadBytesFromFile filename
                            use ms = new MemStream( byt, 0, byt.Length, false, true )
                            let bLoadMetadata = loadDSet.LoadMetadata( ms, bUnpackFunction )
                            // Find the DSet in the same cluster, if cluster is specified
                            if bLoadMetadata && ( Utils.IsNull orgCluster || orgCluster=loadDSet.Cluster) then 
                                bFind <- true
                                let nParallel = x.NumParallelExecution
                                x.CopyMetaData( loadDSet, DSetMetadataCopyFlag.Copy ) 
                                x.NumParallelExecution <- nParallel
    //                        bFind <- x.LoadMetadata( filename, bUnpackFunction )
                        dirpos <- dirpos - 1
                    bSuccess <- bFind
            bSuccess



    member internal x.CompareForUpdate( y:DSet ) = 
        if System.Object.ReferenceEquals( x, y ) then 
            0
        else
            // Cluster includes Load Balancer and Partitioner
            let mutable retVal = 0
            if retVal = 0 then retVal <- x.Name.CompareTo( y.Name )
            if retVal = 0 then retVal <- x.Version.CompareTo( y.Version )
            if retVal = 0 then retVal <- ( x.Cluster :> IComparable<_>).CompareTo( y.Cluster )
            retVal
        
    /// IsSource = True, Trigger Load Metadata
    [<Obsolete("This property is deprecated and will be removed in future. Please use x.loadSource() or |> DSet.LoadSource")>]
    member internal x.IsSource with set( v ) = if v then x.TryLoadDSetMetadata( false ) |> ignore 
       
    /// Defining IComparable interface of DSet
    interface IComparable<DSet> with 
        member x.CompareTo y = 
            if System.Object.ReferenceEquals( x, y ) then 
                0
            else
                // Cluster includes Load Balancer and Partitioner
                let mutable retVal = 0
                if retVal = 0 then retVal <- x.Name.CompareTo( y.Name )
                if retVal = 0 then retVal <- x.Version.CompareTo( y.Version )
                if retVal = 0 then retVal <- x.MetaDataVersion.CompareTo( y.MetaDataVersion )
                if retVal = 0 then retVal <- ( x.Cluster :> IComparable<_>).CompareTo( y.Cluster )
                if retVal = 0 then retVal <- x.TypeOfLoadBalancer.CompareTo( y.TypeOfLoadBalancer )
                if retVal = 0 then retVal <- x.PartionBy.CompareTo( y.PartionBy )
                if retVal = 0 then retVal <- x.StorageType.CompareTo( y.StorageType )
                if retVal = 0 then retVal <- x.ContentKey.CompareTo( y.ContentKey )
                if retVal = 0 then retVal <- x.NumPartitions.CompareTo( y.NumPartitions )
                if retVal = 0 then retVal <- x.NumReplications.CompareTo( y.NumReplications )
// Mapping is not used in DSet comparison
//                if retVal = 0 then 
//                    for p = 0 to x.NumPartitions-1 do
//                        if retVal = 0 then 
//                            retVal <- x.Mapping.[p].Length.CompareTo( y.Mapping.[p].Length )
//                        if retVal = 0 then 
//                            for r = 0 to x.Mapping.[p].Length-1 do
//                                if retVal = 0 then 
//                                    retVal <- x.Mapping.[p].[r].CompareTo( y.Mapping.[p].[r] )
                retVal
    interface IComparable with 
        member x.CompareTo other =
            match other with 
            | :? DSet as y ->
                ( x :> IComparable<_>).CompareTo y
            | _ -> 
                InvalidOperationException() |> raise
    interface IEquatable<DSet> with 
        member x.Equals y = 
            ( x :> IComparable<_>).CompareTo( y ) = 0
    override x.Equals other = 
        ( x :> IComparable ).CompareTo( other ) = 0
    override x.GetHashCode() =
        let hasnValue0 = hash( x.Name, x.Version, x.MetaDataVersion )
        let hashValue1 = hash( hasnValue0, x.Cluster, x.TypeOfLoadBalancer, x.PartionBy )
        let hashValue2 = hash( hashValue1, x.StorageType )
        let hashValue3 = hash( hashValue2, x.ContentKey, x.NumPartitions, x.NumReplications )
//        x.Mapping |> Array.fold ( fun state arr -> 
//                                        arr |> Array.fold( fun state mappingValue -> hash ( state, mappingValue) ) state ) hashValue3
        hashValue3
    member val internal PeerReported = null with get, set
    member val internal refPrepareFinalReport = ref 0 with get
    /// Give a Report that for peeri.
    /// activePartition is an array, where the two element in the array is parti, numElems written in partition i. 
    member internal x.ReportDSet( peeri, activePartitions: (int*int*int64)[], bClientReport ) = 
        if Utils.IsNull peerReport  || Utils.IsNull x.PeerReported then
            lock(x) (fun _ -> 
                if (Utils.IsNull peerReport) then
                    peerReport <- Array.zeroCreate<_> x.Cluster.NumNodes
                if Utils.IsNull x.PeerReported then 
                    x.PeerReported <- Array.zeroCreate<_> x.Cluster.NumNodes
            )
        if Utils.IsNull peerReport.[peeri] then 
            peerReport.[peeri] <- List<_>( activePartitions )
        else
            peerReport.[peeri].AddRange( activePartitions )
        if bClientReport then 
            x.PeerReported.[peeri] <- x.PeerReported.[peeri] + 1
        // Has all peer report received?
        let mutable bAllReportReceived = bClientReport
        if bAllReportReceived then 
            // Ignore local report, they don't count.
            let mutable pi = 0
            while bAllReportReceived && pi < x.Cluster.NumNodes do 
                let queueExamine = x.Cluster.QueueForWrite( pi )
                if Utils.IsNotNull queueExamine && (not queueExamine.Shutdown) && queueExamine.CanSend then 
                    // Have we got all queues? 
    //                            if Utils.IsNull peerReport.[pi] then 
                    if x.PeerReported.[pi] <= 0 then 
                        // Still wait for report from queueExamine
                        bAllReportReceived <- false  
                pi <- pi + 1
        if bAllReportReceived then
            if Interlocked.CompareExchange( x.refPrepareFinalReport, 1, 0 ) = 0 then                     
                x.MappingNumElems <- Array.zeroCreate<_> x.NumPartitions 
                x.MappingStreamLength <- Array.zeroCreate<_> x.NumPartitions 
                for parti = 0 to x.NumPartitions - 1 do
                    if x.bEncodeMapping then 
                        x.MappingNumElems.[parti] <- Array.zeroCreate<_> ( x.Mapping.[parti].Length )
                        x.MappingStreamLength.[parti] <- Array.zeroCreate<_> ( x.Mapping.[parti].Length )
                    else
                        x.MappingNumElems.[parti] <- Array.zeroCreate<_> 1
                        x.MappingStreamLength.[parti] <- Array.zeroCreate<_> 1
                for pi = 0 to x.Cluster.NumNodes - 1 do 
                    if Utils.IsNotNull peerReport.[pi] then 
                        for tuple in peerReport.[pi] do 
                            let parti, numElems, streamLength = tuple

                            if x.bEncodeMapping then 
                                let partimapping = x.Mapping.[parti]
                                let mutable bMappingFound = false
                                for j = 0 to partimapping.Length - 1 do 
                                    if partimapping.[j] = pi then 
                                        x.MappingNumElems.[parti].[j] <- numElems
                                        x.MappingStreamLength.[parti].[j] <- streamLength
                                        bMappingFound <- true
                                if not bMappingFound then 
                                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Partition %d, mapping does have peer %d, but with report numElems %d" parti pi numElems ))
                            else
                                // Cluster Replicate
                                x.MappingNumElems.[parti].[0] <- numElems
                                x.MappingStreamLength.[parti].[0] <- streamLength
                x.ReportConsistencyAnalysis()
                x.MulticastMetadataAfterReport()

    member internal x.ReportConsistencyAnalysis() = 
        if Utils.IsNotNull x.MappingNumElems then 
            // Skip if no report is received. 
            // Consistency analysis. 
            let maxNumElems = Array.zeroCreate<_> x.NumPartitions 
            let maxStreamLength = Array.zeroCreate<_> x.NumPartitions 
            let nodesError = Array.zeroCreate<_> x.Cluster.NumNodes
            let StreamLengthError = List<_>()
            let zeroPartitions = List<_>()
            for parti = 0 to x.NumPartitions - 1 do
                if Utils.IsNotNull x.MappingNumElems.[parti] then 
                    maxNumElems.[parti] <- Array.max x.MappingNumElems.[parti]
                    maxStreamLength.[parti] <- Array.max x.MappingStreamLength.[parti]
                    for j = 0 to x.MappingNumElems.[parti].Length - 1 do 
                        if maxNumElems.[parti] > x.MappingNumElems.[parti].[j] then 
                            let pi = x.Mapping.[parti].[j]
                            if Utils.IsNull nodesError.[pi] then 
                                nodesError.[pi] <- List<_>()
                            nodesError.[pi].Add( parti, maxNumElems.[parti], x.MappingNumElems.[parti].[j] )
                        if maxStreamLength.[parti] > x.MappingStreamLength.[parti].[j] then 
                            StreamLengthError.Add( parti, maxStreamLength.[parti], x.MappingStreamLength.[parti].[j] )
//                                let pi = x.Mapping.[parti].[j]
//                                if Utils.IsNull nodesError.[pi] then 
//                                    nodesError.[pi] <- List<_>()
//                                nodesError.[pi].Add( parti, maxNumElems.[parti], x.MappingNumElems.[parti].[j] )  
                else
                    if x.bEncodeMapping  then 
                        x.MappingNumElems.[parti] <- Array.zeroCreate x.Mapping.[parti].Length
                        x.MappingStreamLength.[parti] <- Array.zeroCreate x.Mapping.[parti].Length
                    else
                        x.MappingNumElems.[parti] <- Array.zeroCreate 1
                        x.MappingStreamLength.[parti] <- Array.zeroCreate 1
                if x.MappingNumElems.[parti].[0] <=0 then 
                    zeroPartitions.Add( parti )    
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DSet %s:%s Mapping : %A" x.Name x.VersionString x.Mapping))
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DSet %s:%s NumElems in Partitions: %A\n Length %A " x.Name x.VersionString maxNumElems maxStreamLength))
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DSet %s:%s Total Elems : %d " x.Name x.VersionString (Array.fold ( fun sum v -> sum + int64 v ) 0L maxNumElems ) ))
            if zeroPartitions.Count > 0 then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DSet %s:%s Following partitions doesn't exist %A" x.Name x.VersionString (zeroPartitions.ToArray()) ))
            for pi = 0 to x.Cluster.NumNodes - 1 do 
                if Utils.IsNotNull nodesError.[pi] then 
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Note replication error report (parti, maxElems, writeElems): %A" ( nodesError.[pi] ) ))
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Total # of stream length inconsistency: %d" StreamLengthError.Count ))

    member internal x.WriteFirstMetadataToAllPeers() = 
        using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
            if Utils.IsNotNull jobObj then 
                let currentWriteID = jobObj.JobID   
                for pi = 0 to x.Cluster.NumNodes - 1 do 
                    using ( new MemStream( 1024 ) ) ( fun msSend -> 
                        msSend.WriteGuid( currentWriteID )
                        x.Pack( msSend, DSetMetadataStorageFlag.HasPassword ||| DSetMetadataStorageFlag.StoreMetadata )
                        let cmd = ControllerCommand( ControllerVerb.WriteMetadata, ControllerNoun.DSet ) 
                        let queuePeer = x.Cluster.QueueForWrite(pi)
                        // Get the receiving serial of the queuePeer, we may later know 
                        // whether any command has ever been received from that peer. 
                        if Utils.IsNotNull queuePeer && (not queuePeer.Shutdown) && queuePeer.CanSend then 
                            queuePeer.ToSend( cmd, msSend )
                            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Send Metadata of DSet %s:%s to peer %d" x.Name x.VersionString pi ))
                    ) 
        )
    member internal x.SendMetadataToAllPeers() = 
        using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
            if Utils.IsNotNull jobObj then 
                let currentWriteID = jobObj.JobID   
                for pi = 0 to x.Cluster.NumNodes - 1 do 
                    using ( new MemStream( 1024 ) ) ( fun msSend -> 
                        msSend.WriteGuid( currentWriteID )
                        x.Pack( msSend, DSetMetadataStorageFlag.HasPassword ||| DSetMetadataStorageFlag.StoreMetadata )
                        let cmd = ControllerCommand( ControllerVerb.Update, ControllerNoun.DSet ) 
                        let queuePeer = x.Cluster.QueueForWrite(pi)
                        // Get the receiving serial of the queuePeer, we may later know 
                        // whether any command has ever been received from that peer. 
                        if Utils.IsNotNull queuePeer && (not queuePeer.Shutdown) && queuePeer.CanSend then 
                            queuePeer.ToSend( cmd, msSend )
                            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Send Metadata of DSet %s:%s to peer %d" x.Name x.VersionString pi ))
                        else
                            Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Unable to send metadata of DSet %s:%s to peer %d" x.Name x.VersionString pi ))
                    )
        )        
    member private x.MulticastMetadataAfterReport() = 
        x.IncrementMetaDataVersion()
        if not (x.bIsClusterReplicate()) then 
            x.bEncodeMapping <- true
        x.SaveMetadata()
        Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Metadata saved for %s" x.Name))
        x.SendMetadataToAllPeers() 
        for pi = 0 to x.Cluster.NumNodes - 1 do 
            x.ReportReceived(pi)
                        
    /// Give a Report that for peeri.
    /// activePartition is an array, where the two element in the array is parti, numElems written in partition i. 
    member internal x.ReportPartition( peeri, activePartitions: (int*int*int64)[] ) = 
        if Utils.IsNull x.Mapping then 
            x.Mapping <- Array.copy (x.GetMapping()) // Build a new array for the current mapping. 
        if Utils.IsNull x.MappingNumElems then 
            x.MappingNumElems <- Array.zeroCreate<_> x.NumPartitions 
        if Utils.IsNull x.MappingStreamLength then
            x.MappingStreamLength <- Array.zeroCreate<_> x.NumPartitions 
        for partReport in activePartitions do 
            let parti, numElems, writeLength = partReport
            Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "Report, DSet %s:%s from peer %d, partition %d, numElems: %d, Length %d" x.Name x.VersionString peeri parti numElems writeLength ))
            let partimapping = x.Mapping.[parti]
            let mutable nMappingFound = partimapping.Length
            for j = 0 to partimapping.Length - 1 do 
                if partimapping.[j] = peeri then 
                    nMappingFound <- j
            if nMappingFound >= partimapping.Length then 
                let arr = ref x.Mapping.[parti]
                Array.Resize( arr, nMappingFound + 1 )
                x.Mapping.[parti] <- !arr
                x.Mapping.[parti].[nMappingFound] <- peeri
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Report, DSet %s:%s from peer %d, partition %d, numElems: %d, Length %d, we will need to add the peer as it is not in original mapping, the mapping matrixes become %A" 
                                                           x.Name x.VersionString peeri parti numElems writeLength x.Mapping.[parti] ))
            let partiLength = x.Mapping.[parti].Length
            if Utils.IsNull x.MappingNumElems.[parti] || ( x.bEncodeMapping && nMappingFound >= x.MappingNumElems.[parti].Length ) then 
                let arr = ref x.MappingNumElems.[parti]
                Array.Resize( arr, if x.bEncodeMapping then partiLength else 1 )
                x.MappingNumElems.[parti] <- !arr
            if x.bEncodeMapping then 
                x.MappingNumElems.[parti].[nMappingFound] <- numElems 
            else
                if x.MappingNumElems.[parti].[0] <> 0 &&  x.MappingNumElems.[parti].[0]<>numElems then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Report, DSet %s:%s from peer %d, partition %d, numElems: %d but original numElems is %d " 
                                                               x.Name x.VersionString peeri parti numElems x.MappingNumElems.[parti].[0] ))
                x.MappingNumElems.[parti].[0] <- Math.Max( x.MappingNumElems.[parti].[0], numElems ) 

            if Utils.IsNull x.MappingStreamLength.[parti] || ( x.bEncodeMapping && nMappingFound >= x.MappingStreamLength.[parti].Length ) then 
                let arr1 = ref x.MappingStreamLength.[parti]
                Array.Resize( arr1, if x.bEncodeMapping then partiLength else 1 )
                x.MappingStreamLength.[parti] <- (!arr1)
            if x.bEncodeMapping then 
                x.MappingStreamLength.[parti].[nMappingFound] <- writeLength 
            else
                if x.MappingStreamLength.[parti].[0] <> 0L &&  x.MappingStreamLength.[parti].[0]<>writeLength then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Report, DSet %s:%s from peer %d, partition %d, streamLength: %d but original legnth is %d " 
                                                               x.Name x.VersionString peeri parti writeLength x.MappingStreamLength.[parti].[0] ))
                x.MappingStreamLength.[parti].[0] <- Math.Max( x.MappingStreamLength.[parti].[0], writeLength ) 

    member internal x.ReportClose( peeri ) = 
        if Utils.IsNull x.PeerReported then
            lock(x) (fun _ -> 
                if (Utils.IsNull x.PeerReported) then
                    x.PeerReported <- Array.zeroCreate<_> x.Cluster.NumNodes
            )
        x.PeerReported.[peeri] <- x.PeerReported.[peeri] + 1
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DSet %s:%s ReportClose received from peer %d" x.Name x.VersionString peeri ) )
        // Has all peer report received?
        let mutable bAllReportReceived = true
        if bAllReportReceived then 
            // Ignore local report, they don't count.
            let mutable pi = 0
            while bAllReportReceived && pi < x.Cluster.NumNodes do 
                let queueExamine = x.Cluster.QueueForWrite( pi )
                if Utils.IsNotNull queueExamine && (not queueExamine.Shutdown) && queueExamine.CanSend then 
                    // Have we got all queues? 
//                            if Utils.IsNull peerReport.[pi] then 
                    if x.PeerReported.[pi] <= 0 then 
                        // Still wait for report from queueExamine
                        bAllReportReceived <- false  
                pi <- pi + 1
        if bAllReportReceived then 
            if Interlocked.CompareExchange( x.refPrepareFinalReport, 1, 0 ) = 0 then 
                x.ReportConsistencyAnalysis() 
                x.MulticastMetadataAfterReport()    
    member internal x.bWaitForUpdateDSet() = 
            match x.DependencyDownstream with 
            | SaveTo _ -> 
                true
            | _ -> 
                false
    member private x.WaitForCloseAllStreamsViaHandleImpl( waithandles, jbInfo, start ) =
        if x.bWaitForUpdateDSet() then 
            x.MappingInfoUpdatedEvent.Reset() |> ignore
            if Utils.IsNull x.MappingStreamLength then 
                waithandles.EnqueueWaitHandle ( fun _ -> sprintf "Waiting for MappingNumElems update from host for DSet %s:%s" x.Name x.VersionString ) x.MappingInfoUpdatedEvent (fun _ -> ()) null
            else
                x.MappingInfoUpdatedEvent.Set() |> ignore
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "MappingNumElems preupdate from host for DSet %s:%s" x.Name x.VersionString ))
        x.BaseWaitForCloseAllStreamsViaHandle( waithandles, jbInfo, start )

    member internal x.WriteDSetCallback( cmd:ControllerCommand, peeri, msRcvd:StreamBase<byte>, jobID:Guid ) = 
        using ( x.TryExecuteSingleJobAction() ) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                try
                    match ( cmd.Verb, cmd.Noun ) with 
                    | ( ControllerVerb.Exception, ControllerNoun.DSet ) -> 
                        let ex = msRcvd.ReadException()
                        let showMsg = sprintf "[WriteDSetCallback, Job already cancelled] Job %A, DSet %s, receive exception from peer %d message: %A" jobID x.Name peeri ex
                        Logger.Log( LogLevel.Info, showMsg )
                    | _ -> 
                        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "[(may be OK)WriteDSetCallback, Job already cancelled] Job %A, DSet %s, receive cmd %A from peer %d of %dB"
                                                                                jobID (x.Name) cmd peeri msRcvd.Length ) 
                with
                | ex -> 
                    Logger.Log( LogLevel.Info, ( sprintf "WriteDSetCallback, Job %A, failed to parse exception message cmd %A, peer %d, with exception %A" jobID cmd peeri ex )    )
            else 
                try
                    let q = x.Cluster.Queue( peeri )
                    match ( cmd.Verb, cmd.Noun ) with 
                    | ( ControllerVerb.Exception, ControllerNoun.DSet ) -> 
                        let ex = msRcvd.ReadException()
                        let showMsg = sprintf "WriteDSetCallback, Job %A, exception at peer %d, message: %A" jobID peeri ex
                        jobAction.ReceiveExceptionAtCallback( ex, showMsg )
                    | ( ControllerVerb.Acknowledge, ControllerNoun.DSet ) ->
                        ()
                    | ( ControllerVerb.Close, ControllerNoun.DSet ) ->
                        Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Close DSet %s:%s received from peer %d" x.Name x.VersionString peeri ))
                        x.Cluster.CloseQueueAndRelease( peeri )
                    | ( ControllerVerb.Report, ControllerNoun.DSet ) ->
                        let nPartitions = msRcvd.ReadVInt32()
                        let activePartitions = Array.zeroCreate<_> nPartitions
                        for i = 0 to nPartitions - 1 do 
                            let parti = msRcvd.ReadVInt32()
                            let numElems = msRcvd.ReadVInt32()
                            let streamLength = msRcvd.ReadInt64()
                            activePartitions.[i] <- parti, numElems, streamLength
                        Logger.LogF( LogLevel.MediumVerbose, (fun _ -> sprintf "Report DSet %s:%s received from peer %d with partitions information %A"  x.Name x.VersionString peeri activePartitions ))
                        // Save peer reports
                        x.ReportDSet( peeri, activePartitions, true )
                    | ( ControllerVerb.ReportPartition, ControllerNoun.DSet ) ->
                        let nPartitions = msRcvd.ReadVInt32()
                        let activePartitions = Array.zeroCreate<_> nPartitions
                        for i = 0 to nPartitions - 1 do 
                            let parti = msRcvd.ReadVInt32()
                            let numElems = msRcvd.ReadVInt32()
                            let streamLength = msRcvd.ReadInt64()
                            activePartitions.[i] <- parti, numElems, streamLength
                        Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "ReportPartition DSet %s:%s received from peer %d with partitions information %A"  x.Name x.VersionString peeri activePartitions ))
                        // Save peer reports
                        x.ReportPartition( peeri, activePartitions )
                    | ( ControllerVerb.ReportClose, ControllerNoun.DSet ) ->
                        // Final command after all streams have received. 
                        x.ReportClose( peeri )                
                    | ( ControllerVerb.ReplicateClose, ControllerNoun.DSet ) ->
                        let peerj = msRcvd.ReadVInt32()
                        Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "ReplicateClose DSet confirmed from peer %d relayed by peer %d" peerj peeri ))
                    | ( ControllerVerb.Get, ControllerNoun.ClusterInfo ) ->
                        using( new MemStream( 10240 ) ) ( fun msSend -> 
                            x.Cluster.ClusterInfo.Pack( msSend )
                            let cmd = ControllerCommand( ControllerVerb.Set, ControllerNoun.ClusterInfo ) 
                            // Expediate delivery of Cluster Information to the receiver
                            q.ToSend( cmd, msSend, true ) 
                        )
                    | ( ControllerVerb.Duplicate, ControllerNoun.DSet ) 
                    | ( ControllerVerb.Echo2, ControllerNoun.DSet ) ->
                        let parti = msRcvd.ReadVInt32()
                        let serial = msRcvd.ReadInt64()
                        let numElems = msRcvd.ReadVInt32()
                        let peerIdx = msRcvd.ReadVInt32()
                        if peeri = peerIdx then 
                            Logger.Log( LogLevel.MediumVerbose, ( sprintf "%A dup confirm partition %d serial %d:%d by peer %d" cmd parti serial numElems peerIdx ))
                        else
                            Logger.Log( LogLevel.MediumVerbose, ( sprintf "%A dup confirm partition %d serial %d:%d by peer %d relayed by peer %d" cmd parti serial numElems peerIdx peeri ))
                    | ( ControllerVerb.EchoReturn, ControllerNoun.DSet ) 
                    | ( ControllerVerb.Echo2Return, ControllerNoun.DSet ) ->
                        let parti = msRcvd.ReadVInt32()
                        if parti > partitionPending.Length then 
                            // Client has received Current Cluster Info
                            let msg =sprintf "Confirmed partition %d not exist" parti
                            Logger.Log( LogLevel.Error, ( msg ))
                            failwith msg 
                        else
                            if ( x.Flag &&& DSetFlag.CountWriteDSet )<>DSetFlag.None then 
                                let serial = msRcvd.ReadInt64()
                                let numElems = msRcvd.ReadVInt32()
                                let peerIdx = msRcvd.ReadVInt32()
                                if ( cmd.Verb<>ControllerVerb.Echo2 ) then 
                                    partitionSerialConfirmed.[parti].Enqueue( serial, numElems, peerIdx )
                                if peeri = peerIdx then 
                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "%A Confirmed partition %d serial %d:%d by peer %d" cmd parti serial numElems peerIdx ))
                                else
                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "%A Confirmed partition %d serial %d:%d by peer %d relayed by peer %d" cmd parti serial numElems peerIdx peeri ))
                            if ( cmd.Verb = ControllerVerb.EchoReturn ) then 
                                let retVal = ref 0
                                let mutable cont = true
                                while (cont && not partitionPending.[parti].IsEmpty) do
                                    if (partitionPending.[parti].TryDequeue(retVal)) then
                                        cont <- false
                                        let confirmed = !retVal
                                        partitionProgress.[parti] <- partitionProgress.[parti] + int64 confirmed
                                        if ( partitionProgress.[parti] - partitionCheckmark.[parti] > x.ProgressMonitor ) then 
                                            partitionCheckmark.[parti] <- partitionCheckmark.[parti] + x.ProgressMonitor   
                                            Logger.Log( LogLevel.MediumVerbose, ( sprintf "Partition %d: confirmed writing of %d MB" parti (partitionProgress.[parti]>>>20) ))
                            let resHash = msRcvd.ReadBytesToEnd() 
                            let refValue = ref Unchecked.defaultof<_>
                            if resHash.Length>0 && deliveryQueue.TryGetValue( resHash, refValue ) then 
                                let startTicks, parti, bufSend, count = !refValue
                                // All replication is accounted for
                                if (!count)+1 >= x.NumReplications then
                                    if (deliveryQueue.TryRemove( resHash, refValue )) then
                                        let _, _, bufSendMs, _ = !refValue
                                        (bufSendMs :> IDisposable).Dispose()
                                else
                                    Interlocked.Increment( count ) |> ignore
                    | _ ->
                        Logger.Log( LogLevel.Info, ( sprintf "DSet.Callback: Unexpected command from peer %d, command %A" peeri cmd ))
                with
                | e ->
                    Logger.Log( LogLevel.Info, ( sprintf "Error in processing DSet.Callback, cmd %A, peer %d, with exception %A" cmd peeri e )    )
        )
        true
    /// AggregateSegmentByPeer will processing series of:
    ///     peeri, parti, serial, numElems
    /// into 
    ///     Array of peers (numPeers items ), each element with
    ///         Array of parti, with serial:numElems[] that are continuous. 
    static member internal AggregateSegmentByPeer( inp: seq< int*int*int64*int> ) =
        // Note: partition that to peer that is not live has already been removed. 
        let segmentList = Dictionary<int,_>()
        for itemInput in inp do 
            let peeri, parti, serial, numElems = itemInput
            if not (segmentList.ContainsKey( peeri ) ) then 
                segmentList.Item( peeri ) <- List<_>()   
            let lst = segmentList.Item(peeri)
            lst.Add( parti, serial, numElems )

        // Connecting partitions. 
        let outputDictionary = Dictionary<int,_>()
        for pair in segmentList do
            let peeri, peerList = pair.Key, pair.Value
            if peerList.Count>0 then 
                peerList.Sort( { new IComparer<_> with 
                                    member self.Compare(x, y) = 
                                        let xparti, xseriali, xnumElems = x
                                        let yparti, yseriali, ynumElems = y
                                        let mutable resVal = xparti-yparti 
                                        if resVal=0 then 
                                            resVal <- int ( xseriali - yseriali )
                                        if resVal=0 then 
                                            resVal <- xnumElems - ynumElems
                                        resVal 
                                        } ) 
                let agg = peerList |> Seq.groupBy( fun ( parti, serial, numElems) -> parti ) |> Seq.toArray
                let res = 
                    seq {
                        for aggitem in agg do 
                            let parti, missLst = aggitem
                            let missArr = Seq.toArray missLst
                            if missArr.Length>0 then 
                                let lst = 
                                    seq {
                                        let _, serial0, _ = missArr.[0]
                                        let start_serial = ref serial0
                                        let continuousElems = ref 0
                                        for missItem in missArr do
                                            let _, serial, numElems = missItem
                                            if serial - !start_serial = int64 !continuousElems then 
                                                continuousElems := !continuousElems + numElems
                                            else
                                                yield (!start_serial, !continuousElems )
                                                start_serial := serial
                                                continuousElems := numElems
                                        yield (!start_serial, !continuousElems )
                                    } |> Seq.toArray
                                yield ( parti, lst )                                
                    } |> Seq.toArray
                outputDictionary.Item(peeri) <- res
        outputDictionary

    /// Replication Analysis
    /// Return: true: no error
    ///         false: some error
    member internal x.ReplicationAnalysis() = 
        /// Let's check on the writing status.
        let mapping = x.GetMapping() 
        let missPart = 
            seq {
                for parti=0 to partitionSerialConfirmed.Length-1 do 
                    if partitionSerialConfirmed.[parti].Count>0 || ( partitionSerial.[parti]>0L && partitionSerial.[parti]<>Int64.MaxValue ) then 
                        // Required peer
                        let partimapping = mapping.[parti] |> Array.sort
                        let sortedConfirmation = 
                            partitionSerialConfirmed.[parti].ToArray() 
                            |> Array.sortBy( fun (serial, numElems, peerIdx) -> serial )
                            |> Seq.groupBy( fun (serial, numElems, peerIdx) -> (serial, numElems) )
                            |> Seq.toArray
        
                        let first, _ = if sortedConfirmation.Length>0 then sortedConfirmation.[0] else ((0L, 0), Seq.empty<_>)
                        let first_serial, first_num = first
                        let lastUnmatchPeerList = List<int>()
                        let lastSerial = ref first_serial
                        let accNumElems = ref 0
                        for j = 0 to sortedConfirmation.Length-1 do
                            let sn, peerIdx_seq = sortedConfirmation.[j]
                            let startSerial, numElems = sn
                            let peerArray = peerIdx_seq |> Seq.map( fun (_, _, peerIdx) -> peerIdx ) |> Seq.toArray |> Array.sort
                            let curUnmatchPeerList = List<int>()
                            if not (Enumerable.SequenceEqual( partimapping, peerArray )) then 
                                for peerIdx in peerArray do
                                    let bFind = partimapping |> Array.fold ( fun s v -> s || v=peerIdx ) false
                                    if not bFind then 
                                        // A peer is over replicated (no need to replicate, but does. 
                                        curUnmatchPeerList.Add( -peerIdx - 100 )
                                for peerIdx in partimapping do
                                    let bFind = peerArray |> Array.fold ( fun s v -> s || v=peerIdx ) false
                                    if not bFind then 
                                        // A peer is missed to be replicated. 
                                        curUnmatchPeerList.Add( peerIdx )    
                            curUnmatchPeerList.Sort()
                            let bMatch = Enumerable.SequenceEqual( lastUnmatchPeerList, curUnmatchPeerList )

                            if bMatch && (startSerial - !lastSerial)=int64 !accNumElems then 
                                // Continuous of the current segment
                                accNumElems := !accNumElems + numElems
                            else
                                // Output a unique unmatched instance 
                                if lastUnmatchPeerList.Count>0 then 
                                    for peeri in lastUnmatchPeerList do
                                        yield ( peeri, parti, !lastSerial, !accNumElems )                                        
                                if (startSerial - !lastSerial) <>int64 !accNumElems then 
                                    for peeri in partimapping do
                                        let eserial = ( (!lastSerial) + int64 (!accNumElems) )
                                        let eElems =  int (startSerial - !lastSerial - int64 !accNumElems)
                                        yield ( peeri, parti, eserial, eElems )
                                lastUnmatchPeerList.Clear()
                                lastUnmatchPeerList.AddRange( curUnmatchPeerList )
                                lastSerial := startSerial
                                accNumElems := numElems
                        if lastUnmatchPeerList.Count>0 then 
                            for peeri in lastUnmatchPeerList do
                                yield ( peeri, parti, !lastSerial, !accNumElems )
                        if (partitionSerial.[parti] - !lastSerial) <> int64 !accNumElems && partitionSerial.[parti]<>Int64.MaxValue then 
                            for peeri in partimapping do
                                yield ( peeri, parti, (!lastSerial + int64 !accNumElems), int (partitionSerial.[parti] - !lastSerial - int64 !accNumElems) )
            }
        let missDictionary = DSet.AggregateSegmentByPeer( missPart )
        let mutable bSomeError = false
        for pair in missDictionary do
            let peeri, missingPeer = pair.Key, pair.Value
            if missingPeer.Length>0 then 
                bSomeError <- true
                let sb = Text.StringBuilder()
                if peeri>=0 then 
                     sb.Append( sprintf "Failed replication for peer %d..." peeri ) |> ignore
                else 
                     let realpeeri = -( peeri + 100 )
                     sb.Append( sprintf "Over replication for peer %d..." realpeeri ) |> ignore
                for missingPart in missingPeer do
                    let parti, slist = missingPart
                    sb.Append( sprintf "part %d " parti ) |> ignore
                    for sItem in slist do
                        let serial, numElems = sItem
                        sb.Append( sprintf "%d:%d " serial numElems ) |> ignore
                Logger.Log( LogLevel.Info, sb.ToString())
            ()
        (not bSomeError)
        
    // For task ReadOne
    // Try to call parents' ResetForRead for PassthroughDSet. 
    // If no parent exist, the operation is ()

    member val internal ResetForRead: NetworkCommandQueue -> unit = thisDSet.ResetForReadImpl with get, set
    member private x.ResetForReadImpl( queue ) = 
        match x.Dependency with 
        | MixFrom dep 
        | Passthrough dep 
        | Bypass ( dep, _ ) ->
            let parentDSet = dep.TargetDSet
            if Utils.IsNotNull parentDSet then 
                parentDSet.ResetForRead( queue )
        | WildMixFrom ( dep, pstream ) -> 
            dep.TargetDSet.ResetForRead( queue ) 
            pstream.TargetStream.ResetForRead(  )
        | CorrelatedMixFrom parents 
        | MergeFrom parents->
            for dep in parents do 
                let parentDSet = dep.TargetDSet
                if Utils.IsNotNull parentDSet then 
                    parentDSet.ResetForRead( queue )
        | HashJoinFrom (parent0, parent1)
        | CrossJoinFrom (parent0, parent1) -> 
            let parent0DSet = parent0.TargetDSet
            if Utils.IsNotNull parent0DSet then 
                parent0DSet.ResetForRead( queue )    
            let parent1DSet = parent1.TargetDSet
            if Utils.IsNotNull parent1DSet then 
                parent1DSet.ResetForRead( queue )    
        | DecodeFrom pstream -> 
            let st = pstream.TargetStream
            st.ResetForRead()
        | _ ->
            ()    
    member val internal MappingPartitionToParent = null with get, set
    member val internal ParentDSets = null with get, set
    member internal  x.FreeDSetResource() = 
        x.SeenPartition <- null            
    member private x.ResetAllImpl( jbInfo ) = 
        x.FreeBaseResource( jbInfo )
        x.ResetForRead( null )
        x.FreeDSetResource()
        x.ResetCache() 
        x.MappingPartitionToParent <- null
        x.ParentDSets <- null 
        if Utils.IsNotNull x.FunctionObj then 
            x.FunctionObj.Reset()
    member private x.PreBeginImpl( jbInfo, direction ) = 
        x.BasePreBegin( jbInfo, direction )
        match x.Dependency with 
        | MergeFrom parents -> 
            for pa in parents do 
                let parentDSet = pa.TargetDSet
                parentDSet.GetMapping() |> ignore
            let mapping = x.GetMapping() 
            x.MappingPartitionToParent <- Array.zeroCreate<_> mapping.Length
            let mutable parenti = 0
            let mutable parentpart = 0 
            for parti = 0 to mapping.Length - 1 do 
                while parentpart >= parents.[parenti].TargetDSet.Mapping.Length do
                    parenti <- parenti + 1
                    parentpart <- 0
                x.MappingPartitionToParent.[parti] <- parenti, parentpart
                parentpart <- parentpart + 1
        | CorrelatedMixFrom parents -> 
            // This preallocation reduce # of allocation in inner execution loop
            x.ParentDSets <- parents.ToArray() |> Array.map ( fun pa -> pa.TargetDSet )
        | _ -> 
            ()
    member val internal SyncReadChunk: JobInformation -> int ->  ( BlobMetadata*StreamBase<byte> -> unit ) -> ManualResetEvent * bool = thisDSet.SyncReadChunkImpl with get, set
    member private x.SyncReadChunkImpl jbInfo parti pushChunkFunc = 
        x.SyncEncode jbInfo parti pushChunkFunc  

    member val internal SeenPartition = null with get, set
    member val internal CachedPartition : PartitionCacheBase[] = null with get, set
    member val internal AllCachedPartition : bool[] = null with get, set
    /// Encode a collection of data
    member val internal SyncEncode: JobInformation -> int -> ( BlobMetadata*StreamBase<byte> -> unit ) -> ManualResetEvent * bool = thisDSet.SyncEncodeImpl with get, set
    /// Encode a collection of data
    member private x.SyncEncodeImpl jbInfo parti ( pushChunkFunc:(BlobMetadata*StreamBase<byte>)->unit  ) = 
        // A pass through type, we will call its  
        let wrapperFunc( meta, elemArray ) = 
            if Utils.IsNotNull elemArray then 
                let currentFunc = x.FunctionObj
                let newMeta, msSend = currentFunc.Encode( meta, elemArray )
                if Utils.IsNotNull msSend then 
                    pushChunkFunc( newMeta, msSend ) 
                    (msSend :> IDisposable).Dispose()
            else
                // End of stream mask
                pushChunkFunc( meta, null )
        x.SyncIterate jbInfo parti wrapperFunc

        // Use as small async as possible
    member internal  x.SyncIterateParent (jbInfo:JobInformation) (parti:int) func = 
        // Pass through type  
        using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF ( LogLevel.WildVerbose, fun _ -> sprintf "[Job %A cancelled for SyncIterateParent], DSet %s:%s, partition %d"
                                                                jbInfo.JobID x.Name x.VersionString parti
                            )
                null, true
            else
                match x.Dependency with 
                | StandAlone -> 
                    let wrapperFunc( meta, ms:StreamBase<byte> ) = 
                        if not (jobAction.IsCancelled) then 
                            func( meta, ms :> Object )
                    /// trigger read from stream
                    x.SyncReadChunk jbInfo parti wrapperFunc 
                | Passthrough oneParent  -> 
                    let parentDSet = oneParent.TargetDSet
                    // a Decoder will be automatically installed if it is of type MemStream
                    let wrapperFunc( meta, elemObject ) = 
                        if not (jobAction.IsCancelled) then 
                            let currentFunc = x.FunctionObj
                            if Utils.IsNotNull elemObject then 
                                // Pass to a DSet, so should code to Object
                                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                                for newMeta, newElemObject in seqs do 
                                    if Utils.IsNotNull newElemObject then 
                                        func( newMeta, newElemObject )
                                    else
                                        // The entire data segment is filtered out, don't generate a call in this case. 
                                        ()
                            else
                                // Encounter the end, MapFunc is called with keyArray being null only once in the execution
                                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
                                let lastTuple = ref Unchecked.defaultof<_>
                                for tuple in seqs do
            //                        lastTuple := tuple
                                    let newMeta, newElemObject = tuple
                                    if Utils.IsNotNull newElemObject then 
                                        func( newMeta, newElemObject )
                                    else
                                        // The entire data segment is filtered out, don't generate a call in this case. 
                                        ()
            //                    let finalMeta, _ = !lastTuple
                                // Final meta will be passed through
                                func( meta, null )                        
                    parentDSet.SyncIterate jbInfo parti wrapperFunc
                | Bypass ( oneParent, brothers ) -> 
                    let parentDSet = oneParent.TargetDSet           
                    // parentDSet has Decoder/Encoder installed 
                    let wrapperFunc( meta, elemObject ) = 
                        if not (jobAction.IsCancelled) then 
                            let currentFunc = x.FunctionObj
                            if Utils.IsNotNull elemObject then 
                                // Push down original ojbect to the brother DSet in downstream direction. 
                                for bro in brothers do 
                                    let broDSet = bro.TargetDSet
                                    broDSet.SyncExecuteDownstream jbInfo parti meta elemObject
                                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                                for newMeta, newElemObject in seqs do 
                                    if Utils.IsNotNull newElemObject then 
                                        func( newMeta, newElemObject )
                                    else
                                        // The entire data segment is filtered out, don't generate a call in this case. 
                                        ()
                            else
                                func( meta, null )  
                                for bro in brothers do 
                                    let broDSet = bro.TargetDSet
                                    broDSet.SyncExecuteDownstream jbInfo parti meta null                
                                // Encounter the end, MapFunc is called with keyArray being null only once in the execution
                                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
            //                    let lastTuple = ref Unchecked.defaultof<_>
                                for tuple in seqs do
            //                        lastTuple := tuple
                                    let newMeta, newElemObject = tuple
                                    if Utils.IsNotNull newElemObject then 
                                        func( newMeta, newElemObject )
                                    else
                                        // The entire data segment is filtered out, don't generate a call in this case. 
                                        ()
            //                    let finalMeta, _ = !lastTuple
                    parentDSet.SyncIterate jbInfo parti wrapperFunc
                | Source -> 
                    x.SyncInit jbInfo parti func
                | MixFrom oneParent ->  
                    x.InitializeCache( true )
                    let bInitialied = x.InitializeCachePartitionWStatus( parti ) 
                    let cache = x.CachedPartition.[parti]
                    let retVal = cache.RetrieveNonBlocking( )
                    match retVal with 
                    | CacheDataRetrieved (meta, elemObject ) -> 
                        func( meta, elemObject )
                        if not (Utils.IsNull elemObject) then 
                            null, false
                        else
                            null, true
                    | CacheSeqRetrieved seq -> 
                        let mutable bFinalObjectSeen = false
                        let mutable curMeta = BlobMetadata( parti, 0L, 0, 0 )
                        for (meta, elemObject) in seq do 
                            if Utils.IsNull elemObject then 
                                if not bFinalObjectSeen then 
                                    bFinalObjectSeen <- true
                                    curMeta <- meta
                                    func( meta, elemObject)
                                else
                                    // Filter out, final object already seen
                                    ()
                            else
                                curMeta <- meta
                                func( meta, elemObject)
                        if not bFinalObjectSeen then 
                            let finalMeta = BlobMetadata( curMeta, 0 )
                            func( finalMeta, null )
                        null, true
                    | CacheBlocked handle -> 
                        handle, false
                | WildMixFrom (oneParent, parentS) -> 
                    x.InitializeCache( true )
                    let bInitialied = x.InitializeCachePartitionWStatus( parti ) 
                    let cache = x.CachedPartition.[parti]
                    let retVal = cache.RetrieveNonBlocking( )
                    match retVal with 
                    | CacheDataRetrieved (meta, elemObject ) -> 
                        func( meta, elemObject )
                        if not (Utils.IsNull elemObject) then 
                            null, false
                        else
                            null, true
                    | CacheSeqRetrieved seq -> 
                        let mutable bFinalObjectSeen = false
                        let mutable curMeta = BlobMetadata( parti, 0L, 0, 0 )
                        for (meta, elemObject) in seq do 
                            if Utils.IsNull elemObject then 
                                if not bFinalObjectSeen then 
                                    bFinalObjectSeen <- true
                                    curMeta <- meta
                                    func( meta, elemObject)
                                else
                                    // Filter out, final object already seen
                                    ()
                            else
                                curMeta <- meta
                                func( meta, elemObject)
                        if not bFinalObjectSeen then 
                            let finalMeta = BlobMetadata( curMeta, 0 )
                            func( finalMeta, null )
                        null, true
                    | CacheBlocked handle -> 
                        handle, false
                | MergeFrom parents -> 
                    let parenti, parentpart = x.MappingPartitionToParent.[ parti ] 
                    let parentDSet = parents.[parenti].TargetDSet
                    let wrapperFunc( meta, elemObject ) = 
                        if not (jobAction.IsCancelled) then 
                            let currentFunc = x.FunctionObj
                            if Utils.IsNotNull elemObject then 
                                // Pass to a DSet, so should code to Object
                                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                                for newMeta, newElemObject in seqs do 
                                    if Utils.IsNotNull newElemObject then 
                                        let outMeta = BlobMetadata( newMeta, parti, newMeta.Serial, newMeta.NumElems )
                                        func( outMeta, newElemObject )
                                    else
                                        // The entire data segment is filtered out, don't generate a call in this case. 
                                        ()
                            else
                                // Encounter the end, MapFunc is called with keyArray being null only once in the execution
                                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
                                let lastTuple = ref Unchecked.defaultof<_>
                                for tuple in seqs do
            //                        lastTuple := tuple
                                    let newMeta, newElemObject = tuple
                                    if Utils.IsNotNull newElemObject then 
                                        let outMeta = BlobMetadata( newMeta, parti, newMeta.Serial, newMeta.NumElems )
                                        func( outMeta, newElemObject )
                                    else
                                        // The entire data segment is filtered out, don't generate a call in this case. 
                                        ()
            //                    let finalMeta, _ = !lastTuple
                                // Final meta will be passed through
                                let outMeta = BlobMetadata( meta, parti, meta.Serial, meta.NumElems )
                                func( outMeta, null )                        
                    parentDSet.SyncIterate jbInfo parentpart wrapperFunc 
                | CorrelatedMixFrom parents -> 
                    let currentFunc = x.FunctionObj
                    currentFunc.InitAll()            
                    let parentFunctions = x.ParentDSets |> Array.mapi ( fun parenti parentDSet -> ( fun _ -> parentDSet.SyncIterate jbInfo parti (currentFunc.DepositFunc parenti)) )
                    x.LaunchForkedThreadsFunction (x.NumParallelExecution) jbInfo parti ( fun pi -> sprintf "Thread for DSet %s:%s CorrelatedMixFrom partition %d parent %d" x.Name x.VersionString parti pi) parentFunctions
                    // Execute action of parents
                    let mutable bEnd = false
                    while not (jobAction.IsCancelled) && not bEnd do
                            // Pass to a DSet, so should code to Object
                            let seqs = currentFunc.ExecuteFunc parti 
                            for newMeta, newElemObject in seqs do 
                                func( newMeta, newElemObject )
                                if Utils.IsNull newElemObject then 
                                    bEnd <- true
                    null, true
        
                | CrossJoinFrom (parent0, parent1) -> 
                    let parent0DSet = parent0.TargetDSet
                    let parent1DSet = parent1.TargetDSet
                    let curfunc = x.FunctionObj
                    curfunc.InitAll()
                    let wrappedFunc (meta:BlobMetadata, o:Object) =
                        if not (jobAction.IsCancelled) then 
                            if Utils.IsNotNull o then 
                                curfunc.DepositFunc meta.Partition (meta, o) 
                                // Filter out null object from CrossJoin
                                let innerWrappedFunc (innerMeta:BlobMetadata, innerObject:Object) =
                                    if not (jobAction.IsCancelled) then 
                                        if Utils.IsNotNull innerObject then 
                                            let combinedobj = ( meta.Partition, innerObject ) 
                                            let seqs = curfunc.MapFunc( innerMeta, combinedobj :> Object, MapToKind.OBJECT )
                                            for newMeta, newElemObject in seqs do 
                                                if Utils.IsNotNull newElemObject then  
                                                    func( newMeta, newElemObject )
                                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "CrossJoin DSet %s:%s with DSet %s:%s, compute part %A x %A yield %A" 
                                                                                                           parent0DSet.Name parent0DSet.VersionString parent1DSet.Name parent1DSet.VersionString meta innerMeta newMeta )        )
                                for p1parti = 0 to parent1DSet.NumPartitions - 1 do
                                    let mutable bDone = false
                                    while not bDone do 
                                        let ev, bTerminate = parent1DSet.SyncIterate jbInfo p1parti innerWrappedFunc
                                        bDone <- bTerminate
                                        if not bDone && Utils.IsNotNull ev then 
                                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "CrossJoin DSet %s:%s with DSet %s:%s, part %A, wait for part %d" 
                                                                                           parent0DSet.Name parent0DSet.VersionString parent1DSet.Name parent1DSet.VersionString meta p1parti ))
                                            ThreadPoolWaitHandles.safeWaitOne( ev ) |> ignore
                                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "CrossJoin DSet %s:%s with DSet %s:%s, part %A, done waiting for part %d" 
                                                                                           parent0DSet.Name parent0DSet.VersionString parent1DSet.Name parent1DSet.VersionString meta p1parti ))
                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "CrossJoin DSet %s:%s with DSet %s:%s, done part %A x part %d" 
                                                                                           parent0DSet.Name parent0DSet.VersionString parent1DSet.Name parent1DSet.VersionString meta p1parti ))
                                // All of the object is done execution. 
                                let seqs = curfunc.ExecuteFunc meta.Partition
                                for newMeta, newElemObject in seqs do 
                                    if Utils.IsNotNull newElemObject then  
                                        func( newMeta, newElemObject )
                                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "CrossJoin DSet %s:%s with DSet %s:%s, final ops on part %A yield %A" 
                                                                                               parent0DSet.Name parent0DSet.VersionString parent1DSet.Name parent1DSet.VersionString meta newMeta ))
                            else
                                let finalMeta = curfunc.GetFinalMetadata parti
                                func( finalMeta, null )
                                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "CrossJoin DSet %s:%s with DSet %s:%s, part %d reaches final with %A" 
                                                                                       parent0DSet.Name parent0DSet.VersionString parent1DSet.Name parent1DSet.VersionString parti finalMeta ))

                    parent0DSet.SyncIterate jbInfo parti wrappedFunc            
                | HashJoinFrom (parent0, parent1) -> 
                    let parent0DSet = parent0.TargetDSet
                    let parent1DSet = parent1.TargetDSet
                    if parent1DSet.CacheType &&& CacheKind.ConcurrectDictionary <> CacheKind.None && 
                        parent1DSet.CacheType &&& CacheKind.UnifiedCache <> CacheKind.None then 
                        // Concurrent Dictionary & Unified cache
                        let curfunc = x.FunctionObj
                        curfunc.InitAll()
                        let cache = parent1DSet.InitializeCachePartitionWStatus
                        /// Unified cache if in use
                        /// The cache support a concurrent dictionary type 
                        parent1DSet.InitializeCache( true )
                        let cache = parent1DSet.CachedPartition.[0]
                        let mutable bCacheReady = false
                        /// Wait for cache to be ready
                        while not bCacheReady do 
                            let cacheStatus = cache.RetrieveNonBlocking()
                            match cacheStatus with 
                            | CacheBlocked ev -> 
                                bCacheReady <- ThreadPoolWaitHandles.safeWaitOne( ev ) 
                            | _ -> 
                                bCacheReady <- true
                        /// Deposit cache 
                        curfunc.DepositFunc parti (BlobMetadata(), cache :> Object )
                        let wrappedFunc (meta:BlobMetadata, o:Object) =
                            if not (jobAction.IsCancelled) then 
                                if Utils.IsNotNull o then 
                                    // Pass to a DSet, so should code to Object
                                    let seqs = curfunc.MapFunc( meta, o, MapToKind.OBJECT )
                                    for newMeta, newElemObject in seqs do 
                                        if Utils.IsNotNull newElemObject then 
                                            func( newMeta, newElemObject )
                                        else
                                            // The entire data segment is filtered out, don't generate a call in this case. 
                                            ()
                                else
                                    // Encounter the end, MapFunc is called with keyArray being null only once in the execution
                                    let seqs = curfunc.MapFunc( meta, null, MapToKind.OBJECT )
                                    let lastTuple = ref Unchecked.defaultof<_>
                                    for tuple in seqs do
                //                        lastTuple := tuple
                                        let newMeta, newElemObject = tuple
                                        if Utils.IsNotNull newElemObject then 
                                            func( newMeta, newElemObject )
                                        else
                                            // The entire data segment is filtered out, don't generate a call in this case. 
                                            ()
                //                    let finalMeta, _ = !lastTuple
                                    // Final meta will be passed through
                                    func( meta, null )                       
                        parent0DSet.SyncIterate jbInfo parti wrappedFunc  
                    else
                        let msg = sprintf "Error in DSet.SyncIterateParent, DSet %s:%s HashJoinFrom need the joined DSet %s:%s to support ConcurrentDictionary Cache type, but type %A is found " x.Name x.VersionString parent1DSet.Name parent1DSet.VersionString parent1DSet.CacheType
                        Logger.Log( LogLevel.Error, msg )
                        failwith msg
                | _ ->
                    let msg = sprintf "Exception in DSet.SyncIterateParent, Job %A, DSet %s:%s has an unsupported dependency type %A" jbInfo.JobID x.Name x.VersionString (x.Dependency)
                    jobAction.ThrowExceptionAtContainer( msg )
                    null, true
        )
    /// Implement init for source
    member internal  x.SyncInit (jbInfo:JobInformation) (parti:int) func = 
            let meta = ref (BlobMetadata( parti, 0L, 0 ))
            let bEndReached = ref false          
            while not (!bEndReached) do 
                let seqs = x.FunctionObj.MapFunc( !meta, Object(), MapToKind.OBJECT )
                let lastmeta = ref (BlobMetadata( parti, 0L, 0 ))
                for newMeta, newObj in seqs do 
                    Logger.LogF( jbInfo.JobID, LogLevel.MediumVerbose, ( fun _ -> sprintf "DSet.Init %s:%s for partition %d, Serial %d, %d Elems" x.Name x.VersionString newMeta.Parti newMeta.Serial newMeta.NumElems ) )
                    func( newMeta, newObj ) 
                    lastmeta := newMeta
                    bEndReached := (Utils.IsNull newObj)
                // BlobMetadata should not be changed. 
                let newInitMeta = BlobMetadata( !lastmeta, (!lastmeta).Serial + int64 (!lastmeta).NumElems, (!lastmeta).NumElems )
                meta := newInitMeta
            null, true

    /// Implement init for source
    member internal  x.AsyncInit (jbInfo:JobInformation) (parti:int) func = 
        // Use as small async as possible
        async { 
            let bEndReached = ref false
            while not (!bEndReached) do
                let handle, bEnd = x.SyncInit jbInfo parti func
                bEndReached := bEnd
                if not (!bEndReached) && Utils.IsNotNull handle then 
                    let! bWait = Async.AwaitWaitHandle( handle )
                    ()
                ()
        }
    
    /// Iterate Children, in downstream direction. 
    /// The downstream iteration has a push model, in which BlobMetadata*Object are pushed down for result
    member private x.SyncExecuteDownstreamImpl jbInfo parti meta elemObject = 
        match x.DependencyDownstream with 
        | Discard -> 
            let currentFunc = x.FunctionObj
            if Utils.IsNotNull elemObject then 
                // content push forward will be discard here. 
                currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT ) |> ignore
            ()
        | MixTo oneChild -> 
            // Operations here are sync, 
            let childDSet = oneChild.TargetDSet
            // a Decoder will be automatically installed if it is of type MemStream
            let currentFunc = x.FunctionObj
            if Utils.IsNotNull elemObject then 
                // Pass to a DSet, so should code to Object
                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                for newMeta, newElemObject in seqs do 
                    if Utils.IsNotNull newElemObject then 
                        // Task in Child DSet will be put in an async queue to be executed through Async.StartWithContinuation
                        childDSet.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newElemObject
            else
                // Encounter the end, MapFunc is called with keyArray being null only once in the execution
                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
//                let lastTuple = ref Unchecked.defaultof<_>
                for tuple in seqs do
//                    lastTuple := tuple
                    let newMeta, newElemObject = tuple
                    if Utils.IsNotNull newElemObject then 
                        // Task in Child DSet will be put in an async queue to be executed through Async.StartWithContinuation
                        childDSet.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newElemObject
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "SyncExecuteDownstream MixTo DSet %s:%s reach the last of object for meta with null object for %A" 
                                                               x.Name x.VersionString (meta.ToString()) ))
        //                let newMeta, newElemObject = !lastTuple
                        // !!! Note !!! MixTo doesn't flush partition. the partition flush will need to be executed after all streams have arrived. 
        | WildMixTo (oneChild, oneChildS) -> 
            // Operations here are sync, 
            let childDSet = oneChild.TargetDSet
            let networkStream = oneChildS.TargetStream
            // a Decoder will be automatically installed if it is of type MemStream
            let currentFunc = x.FunctionObj
            let multicast = 
                match networkStream.DependencyDownstream with
                | MulticastToNetwork _  -> true
                | _ -> false
            let networkDownStream = 
                match networkStream.DependencyDownstream with
                | PassTo s
                | SendToNetwork s
                | MulticastToNetwork s
                    -> s.TargetStream 
                | _ -> networkStream

            if Utils.IsNotNull elemObject then 
                // Pass to a DSet, so should code to Object
                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                for newMeta, newElemObject in seqs do 
                    if Utils.IsNotNull newElemObject then 
                        if not multicast && (networkDownStream.IsTowardsCurrentPeer jbInfo newMeta.Partition) then 
                            // true towards the current peer
                            // Task in Child DSet will be put in an async queue to be executed through Async.StartWithContinuation
                            childDSet.SyncDecodeToDownstream jbInfo newMeta.Partition newMeta newElemObject
                        else
                            // networkStream -> SendOverNetwork -> DecodeTo will merge to childDSet on iterateExecuteDownstream
                            let encMeta, encStream = currentFunc.Encode( newMeta, newElemObject )
                            networkStream.SyncExecuteDownstream jbInfo encMeta.Partition encMeta (encStream :> Object)
                            (encStream :> IDisposable).Dispose()
            else
                // Encounter the end, MapFunc is called with keyArray being null only once in the execution
                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
//                let lastTuple = ref Unchecked.defaultof<_>
                for tuple in seqs do
//                    lastTuple := tuple
                    let newMeta, newElemObject = tuple
                    if Utils.IsNotNull newElemObject then 
                        if not multicast && (networkDownStream.IsTowardsCurrentPeer jbInfo newMeta.Partition) then 
                            // Task in Child DSet will be put in an async queue to be executed through Async.StartWithContinuation
                            childDSet.SyncDecodeToDownstream jbInfo newMeta.Partition newMeta newElemObject
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "SyncExecuteDownstream WildMixTo To Local DSet %s:%s reach the last of object for meta with null object for %A" 
                                                                           x.Name x.VersionString (meta.ToString()) ))
                        else
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "SyncExecuteDownstream WildMixTo To Network DSet %s:%s reach the last of object for meta with null object for %A" 
                                                                           x.Name x.VersionString (meta.ToString()) ))
                            // The entire data segment is filtered out, don't generate a call in this case. 
                            let encMeta, encStream = currentFunc.Encode( newMeta, newElemObject )
                            networkStream.SyncExecuteDownstream jbInfo encMeta.Partition encMeta (encStream :> Object)
        //                let newMeta, newElemObject = !lastTuple
        // !!! Note !!! MixTo doesn't flush partition. the partition flush will need to be executed after all streams have arrived. 
        | Passforward oneChild  -> 
            // Operations here are sync, 
            let childDSet = oneChild.TargetDSet
            // a Decoder will be automatically installed if it is of type MemStream
            let currentFunc = x.FunctionObj
            if Utils.IsNotNull elemObject then 
                // Pass to a DSet, so should code to Object
                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                for newMeta, newElemObject in seqs do 
                    if Utils.IsNotNull newElemObject then 
                        // Task in Child DSet will be put in an async queue to be executed through Async.StartWithContinuation
                        childDSet.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newElemObject
            else
                // Encounter the end, MapFunc is called with keyArray being null only once in the execution
                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
//                let lastTuple = ref Unchecked.defaultof<_>
                for tuple in seqs do
//                    lastTuple := tuple
                    let newMeta, newElemObject = tuple
                    if Utils.IsNotNull newElemObject then 
                        // Task in Child DSet will be put in an async queue to be executed through Async.StartWithContinuation
                        childDSet.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newElemObject
                    else
                        // The entire data segment is filtered out, don't generate a call in this case. 
                        ()
//                let newMeta, newElemObject = !lastTuple
                // Final object always use original meta
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "SyncExecuteDownstream Passforward DSet %s:%s reach the last of object for meta with null object for %A" 
                                                               x.Name x.VersionString (meta.ToString()) ))
                childDSet.SyncExecuteDownstream jbInfo parti meta null
            // Seq.singleton asyncTask
        | DistributeForward children -> 
            // Multiple distributed job can be started in parallel, thus we allow each children to setup its own AsyncExecutionQueue
            let currentFunc = x.FunctionObj
            if Utils.IsNotNull elemObject then 
                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                for newMeta, newElemObject in seqs do 
                    if Utils.IsNotNull newElemObject then 
                        for child in children do
                            let childDSet = child.TargetDSet
                            childDSet.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newElemObject 
                    else
                        // The entire data segment is filtered out, don't generate a call in this case. 
                        ()
            else
                // Last object
                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
                for newMeta, newElemObject in seqs do 
                    if Utils.IsNotNull newElemObject then 
                        for child in children do
                            let childDSet = child.TargetDSet
                            childDSet.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newElemObject 
                    else
                        // The entire data segment is filtered out, don't generate a call in this case. 
                        ()
                for child in children do 
                    let childDSet = child.TargetDSet
                    childDSet.SyncExecuteDownstream jbInfo parti meta null 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "SyncExecuteDownstream Passforward DSet %s:%s reach the last of object for meta with null object for %A" 
                                                                   x.Name x.VersionString (meta.ToString()) ))
        | SaveTo cstream 
        | EncodeTo cstream -> 
            let currentFunc = x.FunctionObj
            if Utils.IsNotNull elemObject then 
                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.MEMSTREAM )
                let childStream = cstream.TargetStream
                for newMeta, newStreamObject in seqs do 
                    if Utils.IsNotNull newStreamObject then 
                        childStream.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newStreamObject
            else
                // Last object
                let seqs = currentFunc.MapFunc( meta, null, MapToKind.MEMSTREAM ) 
                let childStream = cstream.TargetStream
                for newMeta, newStreamObject in seqs do 
                    if Utils.IsNotNull newStreamObject then 
                        childStream.SyncExecuteDownstream jbInfo newMeta.Partition newMeta newStreamObject 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "SyncExecuteDownstream SaveTo/EncodeTo DSet %s:%s reach the last of object for meta with null object for %A" 
                                                                   x.Name x.VersionString (meta.ToString()) ))
                // Signal the end of the stream. 
                childStream.SyncExecuteDownstream jbInfo parti meta null
        | MergeTo oneChild
        | CorrelatedMixTo oneChild 
        | HashJoinTo oneChild 
        | CrossJoinTo oneChild -> 
            failwith (sprintf "SyncExecuteDownstream To be imeplemented for %A" x.DependencyDownstream)
    member internal  x.AllCloseReceived() = 
        true
//        | _ -> 
//            let msg = sprintf "Error in DSet.AsyncIterateParent, DSet %s:%s has an unsupported dependency type %A" x.Name x.VersionString (x.Dependency)
//            Logger.Log(LogLevel.Error, msg)
//            failwith msg
//        | _ -> 
//            let msg = sprintf "Error in DSet.AsyncIterateParent, DSet %s:%s has an unsupported dependency type %A" x.Name x.VersionString (x.Dependency)
//            Logger.Log(LogLevel.Error, msg)
//            failwith msg
    member private x.SyncPreCloseAllStreamsImpl ( jbInfo ) = 
        match x.DependencyDownstream with 
        | Discard 
        | Passforward _ 
        | DistributeForward _ 
        | SaveTo _
        | EncodeTo _ 
        | CorrelatedMixTo _
        | MergeTo _ 
        | HashJoinTo _ 
        | CrossJoinTo _ ->
            () 
        | MixTo child -> 
            // ToDo: Add a final null object to all partitions. 
            ()
        | WildMixTo (oneChild, oneChildS ) -> 
            ()
        x.BaseSyncPreCloseAllStreams jbInfo
     
        
    member val internal ReachCacheLimit = false with get, set
    member val internal TrackSeenKeyValue = DeploymentSettings.TrackSeenKeyValue with get, set
    member val internal Lock = SpinLockSlim(true) with get
    member val internal Locks = null with get, set
    // Return: true if this initialized the cache, false otherwise
    member  internal x.InitializeCacheWStatus( bForWrite ) = 
        if Utils.IsNull x.AllCachedPartition then 
            let mutable lockTaken = false
            let bInitialized = ref false
            while not (lockTaken) || Utils.IsNull x.AllCachedPartition do 
                lockTaken <- x.Lock.TryEnter()
                if lockTaken then 
                    if Utils.IsNull x.AllCachedPartition then
                        if x.CacheType &&& CacheKind.UnifiedCache = CacheKind.None then 
                            x.CachedPartition <- Array.zeroCreate<_>  x.NumPartitions
                        else
                            // Construct one cache for all 
                            let oneCache = x.FunctionObj.ConstructPartitionCache( x.CacheType, 0, x.SerializationLimit )
                            x.CachedPartition <- Array.create x.NumPartitions oneCache
                        bInitialized := true
                    if Utils.IsNull x.Locks then
                        x.Locks <- Array.init x.NumPartitions ( fun _ -> SpinLockSlim(true) )
                    // x.AllCachePartition should be populated last, so when another thread finds it is 
                    // not null, x.CachedPartition must have been allocated. So it's safe for that thread
                    // to continue with x.InitializeCachePartitionWStatus 
                    x.AllCachedPartition <- Array.create x.NumPartitions false
            if lockTaken then
                x.Lock.Exit() 
            if x.TrackSeenKeyValue then 
                if bForWrite && Utils.IsNull x.SeenPartition then 
                    let mutable lkTaken = false
                    while not (lkTaken ) && Utils.IsNull x.SeenPartition do 
                        lkTaken <- x.Lock.TryEnter()
                        if lkTaken then 
                            x.SeenPartition <- ConcurrentDictionary<int,ConcurrentDictionary<_,_>>()
                    if lkTaken then 
                        x.Lock.Exit()
            (!bInitialized)
        else
            false
    member internal x.InitializeCache( bForWrite ) =
        x.InitializeCacheWStatus( bForWrite ) |> ignore
    member internal x.InitializeCachePartitionWStatus( parti ) = 
        if Utils.IsNull x.CachedPartition.[parti] then 
            let mutable lockTaken = false
            let bInitialized = ref false
            while not (lockTaken) || Utils.IsNull (x.CachedPartition.[parti]) do
                lockTaken <- x.Locks.[parti].TryEnter() 
                if ( lockTaken ) then 
                    if Utils.IsNull x.CachedPartition.[parti] then 
                        x.CachedPartition.[parti] <- x.FunctionObj.ConstructPartitionCache( x.CacheType, parti, x.SerializationLimit )
                        bInitialized := true
            if ( lockTaken ) then 
                x.Locks.[parti].Exit() 
            if x.TrackSeenKeyValue then 
                x.SeenPartition.GetOrAdd( parti, fun _ -> ConcurrentDictionary<_, _>() ) |> ignore
            (!bInitialized)
        else
            false
    member internal x.InitializeCachePartition( parti ) = 
        x.InitializeCachePartitionWStatus( parti ) |> ignore
    member internal x.ResetCache() = 
        if Utils.IsNotNull x.CachedPartition then 
            for parti = 0 to x.CachedPartition.Length - 1 do 
                let cache = x.CachedPartition.[parti]
                if Utils.IsNotNull cache then 
                    cache.Reset()        

    member internal x.SyncIterateProtected jbInfo parti func = 
        try
            x.SyncIterate jbInfo parti func
        with 
        | e -> 
            let msg = sprintf "Error in DSet.AsyncIterateProtected, DSet %s:%s with exception %A" x.Name x.VersionString  e
            Logger.Log( LogLevel.Error, msg )
            Logger.Flush()
            failwith msg
    member private x.SyncIterateImpl jbInfo parti func = 
        using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF ( LogLevel.WildVerbose, fun _ -> sprintf "[Job %A cancelled for SyncIterateImpl], DSet %s:%s, partition %d"
                                                                jbInfo.JobID x.Name x.VersionString parti
                            )
                null, true
            else
                let ty = x.StorageType &&& StorageKind.StorageMediumMask
                if ty<>StorageKind.RAM then 
                    x.SyncIterateParent jbInfo parti func
                else
                    // Need in RAM cache or index
                    let wrapperFunc( meta, elemObject ) = 
                        if not jobAction.IsCancelled then 
                            let currentFunc = x.FunctionObj
                            let cache( newMeta:BlobMetadata, newElemObject:Object ) = 
                                if Utils.IsNotNull newElemObject then 
                                    if not x.ReachCacheLimit then  
                                        // Install cache 
                                        // This assumes partition i of the upstream maps to the same partition i, 
                                        // If we assume .Persist() is always installed after an identity mapping operation, this is OK. 
                                        let newParti = newMeta.Partition
                                        let newSerial = newMeta.Serial
                                        let newNumElems = newMeta.NumElems
                                        match ty with 
                                        | StorageKind.RAM ->
                                            x.InitializeCachePartition( newParti )
                                            if not x.TrackSeenKeyValue || not ( x.SeenPartition.[newParti].ContainsKey( newSerial, newNumElems ) ) then 
                                                if x.TrackSeenKeyValue then 
                                                    x.SeenPartition.[newParti].Item( (newSerial, newNumElems) ) <- true
                    //                            let bMemoryPressure = MemoryStatus.CheckMemoryPressure() 
                    //                            if bMemoryPressure then 
                    //                                Logger.LogF( LogLevel.WildVerbose,  fun _ -> sprintf "Partition %d, set %d, memory usage increased to %dMB" parti (x.CachedPartition.[parti].Count + 1) MemoryStatus.MaxUsedMemoryInMB  )
                                                let curMemoryUsage = MemoryStatus.CurMemoryInMB
                                                if curMemoryUsage >= DeploymentSettings.MaxMemoryLimitInMB then 
                                                    x.ReachCacheLimit <- true
                                                    x.CachedPartition.[newParti] <- null
                                                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Turn off cache for partition %d, memory usage increased to %dMB" parti curMemoryUsage )                               )
                                                else
                                                    x.CachedPartition.[newParti].Add( newMeta, newElemObject, false )                            
                                            else
                                                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "!!! Filter out !!! DSet %s:%s %s" x.Name x.VersionString (meta.ToString()) ))
                                        | _ ->
                                            ()
                                    func( newMeta, newElemObject )
                                else
                                    // The entire data segment is filtered out, don't generate a call in this case. 
                                    ()
                            if Utils.IsNotNull elemObject then 
                                let seqs = currentFunc.MapFunc( meta, elemObject, MapToKind.OBJECT )
                                for tuple in seqs do 
                                    cache( tuple )    
                            else
                                // Calling MapFunc with null once at the end. 
                                let seqs = currentFunc.MapFunc( meta, null, MapToKind.OBJECT )
                                let lastTuple = ref Unchecked.defaultof<_>
                                for tuple in seqs do
                                    lastTuple := tuple
                                    cache( tuple )
                                // This assumes partition i of the upstream maps to the same partition i, 
                                // If we assume .Persist() is always installed after an identity mapping operation, this is OK. 
                                let lastMeta, _ = !lastTuple
                                let newParti = lastMeta.Partition
                                if not x.ReachCacheLimit then 
                                    x.InitializeCachePartition( newParti )
                                    // Mark partition i as complete.                     
                                    x.AllCachedPartition.[newParti] <- true
                                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Cache DSet %s:%s partition %d" x.Name x.VersionString parti ))
                                else
                                    if Utils.IsNotNull x.CachedPartition.[newParti] then 
                                        x.CachedPartition.[newParti] <- null
                                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Release cache DSet %s:%s partition %d" x.Name x.VersionString parti ))
                                // guarantee func is called with null only one time at last
                                func( meta, null )
                        else
                            // Receive cancellation, we will clear caches
                            x.AllCachedPartition.[meta.Partition] <- false
                            x.CachedPartition.[meta.Partition] <- null    
            
                    x.InitializeCache( false ) 
                    if not x.AllCachedPartition.[parti] then 
                        // Reinitialize cache to deal with write
                        x.InitializeCache( true ) 
                        match x.Dependency with 
                        | WildMixFrom (_,_) -> 
                            // No cache, as add cache is performed by network
                            x.SyncIterateParent jbInfo parti func
                        | _ -> 
                            x.SyncIterateParent jbInfo parti wrapperFunc
                    else
                        x.CachedPartition.[parti].ToSeq() 
                        |> Seq.filter ( fun _ -> not jobAction.IsCancelled ) 
                        |> Seq.iter func
                        null, true
                        // Cancel operation if cancellationToken is flag as true
        )

    /// Turn a local or network folder into seq<string, byte[]> to be fed into DSet.store
    /// sPattern, sOption is the search pattern and option used in Directory.GetFiles
    static member FolderRecursiveSeq( localFolderName, sPattern, sOption ) = 
        let mutable files = Array.create<string> 0 ""

        try 
            files <- Directory.GetFiles(localFolderName, sPattern, sOption)
        with
            | _ as ex -> Logger.LogF( LogLevel.Error,  fun _ -> sprintf "Exception: Directory.GetFiles: %A" ex  )
        let total = ref 0UL
        let fseq = files 
                    |>  Seq.map ( fun file -> 
                            let bytes = ReadBytesFromFile( file )
                            let idx = file.ToUpper().IndexOf( localFolderName.ToUpper() )
                            let useName1 = 
                                if idx>=0 then file.Substring( idx + localFolderName.Length ) else file
                            let useName = useName1.TrimStart( @"\/".ToCharArray())
                            lock (total) ( fun () -> total := !total + (uint64 useName.Length) + (uint64 bytes.Length ) )
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Processing %s ... %dB " useName (useName.Length+bytes.Length) ))
                            ( useName, bytes) 
                            )
        fseq, total, files
    /// store a seq<string, byte[]> (e.g., that is retrieved from DSet.toSeq() 
    /// to a local folder. 
    static member RetrieveFolderRecursive( localFolderName, o: seq<string*byte[]> ) = 
        let numFiles = ref 0
        let total = ref 0UL
        let milestone = ref 0UL
        o |> Seq.iter( fun (filename, bytes) -> 
                            let f1 = ref (filename.Trim())
                            let removePrefixes = [| @"http://" |]
                            let removeAfterSurfixes = [| @".jpg"; @".bmp"; @".png"; @".gif" |]
                            let bDone = ref false
                            while not (!bDone) do 
                                bDone := true
                                for prefix in removePrefixes do 
                                let nHttp = (!f1).ToLower().IndexOf( prefix )
                                if nHttp >=0 then
                                    f1 := (!f1).Substring( prefix.Length ).Trim()
                                    bDone := false
                            bDone := false
                            while not (!bDone) do 
                                bDone := true
                                for surfix in removeAfterSurfixes do 
                                let nHttp = (!f1).ToLower().LastIndexOf( surfix )
                                if nHttp >=0 then
                                    let f2 = (!f1).Substring( 0, nHttp + surfix.Length ).Trim()
                                    if f2.Length < (!f1).Length then 
                                        f1:=f2
                                        bDone := false
                            let usefilename = (!f1)
                            try
                                let fullname = Path.Combine( localFolderName, usefilename )
                                let dirname = Path.GetDirectoryName( fullname ) 
                                DirectoryInfoCreateIfNotExists dirname |> ignore
                                use fstream = CreateFileStreamForWrite( fullname )
                                fstream.Write( bytes, 0, bytes.Length ) 
                                fstream.Flush()
                                fstream.Close()
                            with 
                            | e -> 
                                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Exception %A encountered when trying to write file %s with %dB" e usefilename (bytes.Length) ) )
                            total := !total + (uint64 filename.Length) + (uint64 bytes.Length ) 
                            numFiles := !numFiles + 1
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Saving %s ... %dB " filename (filename.Length+bytes.Length) ))
                            if !total - !milestone >= 10000000UL then 
                                milestone := !milestone + 10000000UL
                                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "%d files saved ... %d MB " !numFiles (!total>>>20) )                               )
                            )
        !numFiles, !total

    // Always placed object in queue for sync mode 
    member private x.SyncDecodeToDownstreamImpl jbInfo parti (newMeta:BlobMetadata) newElemObject  = 
        x.InitializeCache( true ) 
        x.InitializeCachePartition( newMeta.Partition )
        let cache = x.CachedPartition.[newMeta.Partition]
        let decMeta, decodedElemObject = 
            if Utils.IsNull newElemObject then 
                newMeta, newElemObject
            else
                match newElemObject with 
                | :? StreamBase<byte> as ms -> 
                    let func = x.FunctionObj
                    func.Decode( newMeta, ms )
                | _ -> 
                    newMeta, newElemObject
        cache.Add( decMeta, decodedElemObject, true )
        if Utils.IsNull decodedElemObject then 
            // Last Object in 
            x.AllCachedPartition.[ parti ] <- true
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DSet %s:%s, SyncDecodeToDownstream called with null, parti %d is all cached" x.Name x.VersionString parti ))
        else
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DSet %s:%s, SyncDecodeToDownstream called, add parti %d blob %A" x.Name x.VersionString parti decMeta ))
    // Start a thread to execute down stream
    // These actions may throw independent exception and needs to be wrapped in cancellation. 
    member internal x.NewThreadToExecuteDownstream (jbInfo:JobInformation) parti () = 
        using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "[NewThreadToExecuteDownstream, Job %A has already been cancelled] DSet %s:%s partition %d"
                                                                        jbInfo.JobID x.Name x.VersionString parti
                           )
                null, true
            else
                try 
                    let cache = x.CachedPartition.[parti]
                    let ret = cache.RetrieveNonBlocking()
                    match ret with 
                    | CacheDataRetrieved ( meta, o ) -> 
                        x.SyncExecuteDownstream jbInfo parti meta o
                        if Utils.IsNull o then 
                            null, true
                        else
                            null, false
                    | CacheSeqRetrieved seq -> 
                        let mutable bFinalObjectSeen = false
                        let mutable curMeta = BlobMetadata( parti, 0L, 0, 0 )
                        for (meta, elemObject) in seq do 
                            if Utils.IsNull elemObject then 
                                if not bFinalObjectSeen then 
                                    bFinalObjectSeen <- true
                                    curMeta <- meta
                                    x.SyncExecuteDownstream jbInfo parti meta elemObject
                                else
                                    // Filter out, final object already seen
                                    ()
                            else
                                curMeta <- meta
                                x.SyncExecuteDownstream jbInfo parti meta elemObject
                        if not bFinalObjectSeen then 
                            let finalMeta = BlobMetadata( curMeta, 0 )
                            x.SyncExecuteDownstream jbInfo parti finalMeta null
                        null, true      
                    | CacheBlocked handle -> 
                        handle, false     
                with
                | ex -> 
                    // Recoverable failures 
                    jbInfo.PartitionFailure( ex, "___ NewThreadToExecuteDownstream ___ ", parti )
                    null, true
        )

//    Note: make it disposable affect usablity, currently rely on finalizer
//    interface IDisposable with
//        member x.Dispose() = 
//            x.MappingInfoUpdatedEvent.Dispose()
//            // dispose remaining items in deliveryQueue
//            // walk the dependence graph to dispose DStreams
//            base.DisposeResource()

// Data structure related to an instance of the job 
// E.g., what partition has been read, remapped, etc.. 
type internal DJobInstance(curDSet: DSet, getSingleJobAction: unit -> SingleJobActionApp ) as thisInstance =  
    let remappingInterval = int64 DeploymentSettings.RemappingIntervalInMillisecond * TimeSpan.TicksPerMillisecond
    let mutable clockLastRemapping = (PerfADateTime.UtcNowTicks())
    do
        // Make sure memory release upon cancellation
        using ( getSingleJobAction() ) ( fun jobAction -> 
            if Utils.IsNotNull jobAction then 
                jobAction.LifeCycleObject.OnDisposeFS( thisInstance.ClearConnectionState )
        )
    member val JobID = Guid.Empty with get, set
    /// allow information to be captured on peer failure in the DSet 
    member val internal bMetaDataSet = false with get, set
    member val internal bDSetMetaRead : bool[] = null with get, set
    member val internal bPeerFailed : bool[] = null with get, set
    member val internal PeerDSet : DSet[] = null with get, set
    member val internal bLastPeerFailedPattern : bool[] = null with get, set
    member val internal peersTriedForPartition = null with get, set
    member val internal peersFailedForPartition = null with get, set
    member val internal peersNonExistPartition = null with get, set
    member val internal partitionReadFromPeers = null with get, set
    member val internal bFailingPartitionReported = null with get, set
    member val internal bPartitionReadSent = null with get, set
    member val internal numErrorPartition = null with get, set
    member val internal numDSetMetadataRead = 0 with get, set
    member val internal numPeerRespond = 0 with get, set
    member val internal bFirstReadCommand = null with get, set
    member val internal numPeerPartitionCmdSent = null with get, set
    member val internal numPeerPartitionCmdRcvd = null with get, set
    member internal x.TriggerRemapping() = 
        clockLastRemapping <- (PerfADateTime.UtcNowTicks()) - remappingInterval 
    member internal x.IsRemapping() =
        let curTicks = (PerfADateTime.UtcNowTicks())
        // Remapping is rather expensive, so we trigger every 100ms  
        curTicks - clockLastRemapping >= remappingInterval && (Utils.IsNotNull x.bPeerFailed)
    /// Connection state
    member internal x.ClearConnectionState() = 
        // These are mainly for DSet read. 
        x.bDSetMetaRead <- null
        x.bPeerFailed <- null
        x.bLastPeerFailedPattern <- null
        x.peersTriedForPartition <- null
        x.peersFailedForPartition <- null
        x.peersNonExistPartition <- null
        x.partitionReadFromPeers <- null
        x.bFailingPartitionReported <- null
        x.bPartitionReadSent <- null
        x.numErrorPartition <- null
        x.numDSetMetadataRead <- 0
        x.numPeerRespond <- 0
    /// Peer availability and fail pattern, bMetaRead, bPeerF should be an array of the size of the cluster 
    member internal x.SetMetaDataAvailability( bMetaRead, bPeerF ) = 
        x.bDSetMetaRead <- bMetaRead
        x.bPeerFailed <- bPeerF
        x.bLastPeerFailedPattern <- Array.copy x.bPeerFailed 
    /// Initiate Partition Status to ready to communicate to other peer owned by DSet
    member internal x.InitiatePartitionStatus() = 
//        let mapping = curDSet.GetMapping()
//        curDSet.NumPartitions <- mapping.Length
        x.bPartitionReadSent <- Array.create curDSet.NumPartitions false
        x.numErrorPartition <- Array.create curDSet.NumPartitions Int32.MaxValue
        x.peersFailedForPartition <- Array.init curDSet.NumPartitions ( fun _ -> List<int>() )
        x.peersNonExistPartition <- Array.init curDSet.NumPartitions ( fun _ -> List<int>() )
        x.peersTriedForPartition <- Array.init curDSet.NumPartitions ( fun _ -> List<int>() )
        x.partitionReadFromPeers <- Array.init curDSet.NumPartitions ( fun _ -> List<int>() )
        x.bFailingPartitionReported <- Array.create curDSet.NumPartitions false

    member internal x.InitiateCommandStatus () =
        // We will use a common remapping function to deal with partition mapping. 
        x.bFirstReadCommand <- Array.create curDSet.Cluster.NumNodes true
        x.numPeerPartitionCmdSent <- Array.create curDSet.Cluster.NumNodes (ref 0)
        x.numPeerPartitionCmdRcvd <- Array.create curDSet.Cluster.NumNodes (ref 0)

    member internal x.PartitionAnalysis( ) =
        // Has all partition been processed? 
        if Utils.IsNotNull x.numErrorPartition then 
            let partitionNonExist = Dictionary<_,_>()
            let partitionError = Dictionary<_,_>()
            let peerNonExist = Dictionary<_,_>()
            let peerError = Dictionary<_,_>()
            for parti = 0 to x.numErrorPartition.Length - 1 do 
                if x.numErrorPartition.[parti] <> 0 then 
                    if x.peersNonExistPartition.[parti].Count = x.peersFailedForPartition.[parti].Count then 
                        partitionNonExist.Item(parti) <- true
                        for peeri in x.peersTriedForPartition.[parti] do
                            peerNonExist.Item( peeri ) <- true
                    else
                        partitionError.Item( parti ) <- true
                        for peeri in x.peersTriedForPartition.[parti] do
                            peerError.Item( peeri ) <- true
            let partitionNonExistArr = partitionNonExist.Keys |> Seq.toArray |> Array.sort
            let peerNonExistArr = peerNonExist.Keys |> Seq.toArray |> Array.sort
            let partitionErrorArr = partitionError.Keys |> Seq.toArray |> Array.sort
            let peerErrorArr = peerError.Keys |> Seq.toArray |> Array.sort
            if partitionErrorArr.Length > 0 then 
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "The following partition encounter error in processing %A" (partitionErrorArr) ))
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "The following peer may encounter error in processing %A" (peerErrorArr) ))
                
            if partitionNonExistArr.Length > 0 then 
                // Non Existing partition can be caused by partitioning function, in which no data is allocated to a certain 
                // partition, that may be OK. 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "The following partition of DSet %s:%s doesn't exist: %A" curDSet.Name curDSet.VersionString partitionNonExistArr ))
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DSet %s:%s the following peer may have nonexisting partitions in cluster %A" curDSet.Name curDSet.VersionString peerNonExistArr ))

    member internal x.CheckMetaData() =
        for i = 0 to curDSet.Cluster.NumNodes-1 do
            if (x.bDSetMetaRead.[i]) then
                if Utils.IsNotNull x.PeerDSet.[i] then
                     if (x.PeerDSet.[i].Version > curDSet.Version ||
                         (x.PeerDSet.[i].Version = curDSet.Version && x.PeerDSet.[i].MetaDataVersion > curDSet.MetaDataVersion)) then
                         // reset
                         curDSet.CopyMetaData(x.PeerDSet.[i], DSetMetadataCopyFlag.Copy)
        // check other versions for matching
        x.numDSetMetadataRead <- 0
        for j = 0 to curDSet.Cluster.NumNodes-1 do
            if Utils.IsNotNull x.PeerDSet.[j] then
                if (x.PeerDSet.[j].Version = curDSet.Version &&
                    x.PeerDSet.[j].MetaDataVersion = curDSet.MetaDataVersion) then
                    x.bPeerFailed.[j] <- false
                    x.numDSetMetadataRead <- x.numDSetMetadataRead + 1
                else
                    x.bPeerFailed.[j] <- true
                    Logger.LogF( LogLevel.Info, (fun _ -> sprintf "RetrieveMetaDataCallback: Failed peer %d because of inconsistent metadata" j))
            else
                x.bPeerFailed.[j] <- false

    /// Retrieve meta data of DSet
    /// Please note that if there are metadata of multiple DSet to be retired, the RetrieveMetaData call for job should be used instead. 
    member internal x.RetrieveOneMetaData( jobID ) =
        if Utils.IsNull curDSet.Cluster then 
            let msg = sprintf "Failed to load Source DSet %s:%s \n. Details: the program can't locate local metadata, it attempts to load remote metadata, but the cluster parameter has not been specified." curDSet.Name curDSet.VersionString
            Logger.Log( LogLevel.Error, msg )
            failwith msg
        curDSet.Cluster.RegisterCallback( jobID, curDSet.Name, 0L, [| ControllerCommand( ControllerVerb.Set, ControllerNoun.Metadata);
                                                             ControllerCommand( ControllerVerb.NonExist, ControllerNoun.DSet); 
                                                             ControllerCommand( ControllerVerb.Exception, ControllerNoun.DSet); |],
                { new NetworkCommandCallback with 
                    member this.Callback( cmd, peeri, ms, jobID, name, verNumber, cl ) = 
                        x.RetrieveMetaDataCallback( cmd, peeri, ms, jobID, name, verNumber )
                } )
        x.bDSetMetaRead <- Array.create curDSet.Cluster.NumNodes false
        x.bPeerFailed <- Array.create curDSet.Cluster.NumNodes false
        x.PeerDSet <- Array.zeroCreate curDSet.Cluster.NumNodes
        x.numPeerRespond <- 0
        x.numDSetMetadataRead <- 0
        x.bMetaDataSet <- false

        curDSet.Cluster.ConnectAll()
        Cluster.Connects.Initialize()

        let bSentGetDSet = Array.create curDSet.Cluster.NumNodes false
        let bMetaDataRetrieved = ref false
        let clock_start = curDSet.Clock.ElapsedTicks
        let maxWait = ref (clock_start + curDSet.ClockFrequency * int64 curDSet.TimeoutLimit)
        using ( new MemStream( 1024 ) ) ( fun msSend -> 
            msSend.WriteGuid( x.JobID )
            msSend.WriteString( curDSet.Name )
            msSend.WriteInt64( curDSet.Version.Ticks )
            msSend.WriteInt64( curDSet.Cluster.Version.Ticks )
        
            // Reset curDSet version so that we can read in the latest DSet version. 
            curDSet.Version <- DateTime.MinValue   
        
            // Calculated required number of response for metadata
            let numRequiredPeerRespond = curDSet.RequiredNodes( curDSet.MinNodeResponded )
            let numRequiredVlidResponse = curDSet.RequiredNodes( curDSet.MinValidResponded )
            // Make sure memory release upon cancellation
            using ( getSingleJobAction() ) ( fun jobAction -> 
                if Utils.IsNotNull jobAction then 
                    while not !bMetaDataRetrieved && curDSet.Clock.ElapsedTicks<(!maxWait) && not jobAction.IsCancelledAndThrow do
                        // Try send out Get, DSet request. 
                        for peeri=0 to curDSet.Cluster.NumNodes-1 do
                            if not bSentGetDSet.[peeri] then 
                                let queue = curDSet.Cluster.QueueForWrite( peeri )
                                if Utils.IsNotNull queue && queue.CanSend && not jobAction.IsCancelledAndThrow then 
                                    queue.ToSend( ControllerCommand( ControllerVerb.Get, ControllerNoun.DSet ), msSend )
                                    bSentGetDSet.[peeri] <- true
                        x.CheckMetaData()
                        if x.numPeerRespond>=numRequiredPeerRespond && x.numDSetMetadataRead>=numRequiredVlidResponse then 
                            bMetaDataRetrieved := true    
                        else if x.numPeerRespond>=curDSet.Cluster.NumNodes then 
                            // All peer responded, timeout
                            maxWait := clock_start
                        else if x.numDSetMetadataRead + ( curDSet.Cluster.NumNodes - x.numPeerRespond ) < numRequiredVlidResponse then 
                            // Enough failed response gathered, we won't be able to succeed. 
                            maxWait := clock_start

                        ThreadPoolWaitHandles.safeWaitOne( jobAction.WaitHandle, 5 ) |> ignore 
                )
        )

        // by setting bMetaDataSet, we stop update metadata, all further peer response with different DSet version will be considered as a failed peer. 
        x.PeerDSet <- null
        x.bMetaDataSet <- true
        x.bLastPeerFailedPattern <- Array.copy x.bPeerFailed 

        curDSet.Cluster.ConnectAll()
        if (not !bMetaDataRetrieved) then
            Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Failed to load metadata for DSet %s:%A" curDSet.Name curDSet.Version))
        !bMetaDataRetrieved

    member internal x.ToClose() = 
//        let curDSet = x.CurDSet
        if (Utils.IsNotNull curDSet.Cluster) then
            curDSet.Cluster.UnRegisterCallback( x.JobID, [| ControllerCommand( ControllerVerb.Set, ControllerNoun.Metadata);
                                                                    ControllerCommand( ControllerVerb.NonExist, ControllerNoun.DSet);
                                                                    ControllerCommand( ControllerVerb.Exception, ControllerNoun.DSet); |] )
                
    /// Callback function used during metadata retrieval phase
    member internal x.RetrieveMetaDataCallback( cmd, peeri, msRcvd, jobID, name, verNumber ) = 
        using ( getSingleJobAction() ) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                try
                    match ( cmd.Verb, cmd.Noun ) with 
                    | ( ControllerVerb.Exception, ControllerNoun.DSet ) -> 
                        let ex = msRcvd.ReadException()
                        let showMsg = sprintf "[RetrieveMetaDataCallback, Job already cancelled] Job %A, DSet %s, receive exception from peer %d message: %A" jobID name peeri ex
                        Logger.Log( LogLevel.Info, showMsg )
                    | _ -> 
                        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "[(may be OK)RetrieveMetaDataCallback, Job already cancelled] Job %A, DSet %s, receive cmd %A from peer %d of %dB"
                                                                                jobID name cmd peeri msRcvd.Length ) 
                with
                | ex -> 
                    Logger.Log( LogLevel.Info, ( sprintf "RetrieveMetaDataCallback, Job %A, failed to parse exception message cmd %A, peer %d, with exception %A" jobID cmd peeri ex )    )
            else 
                try
                    let q = curDSet.Cluster.Queue( peeri )
                    match ( cmd.Verb, cmd.Noun ) with 
                    | ( ControllerVerb.Set, ControllerNoun.Metadata ) ->
                        // Set, Metadata usually is used for Src/Destination DSet, in which there should not be a function object (03/13/2014)
                        let readDSet = DSet.Unpack( msRcvd, false )
                        if readDSet.Cluster.Version.Ticks = curDSet.Cluster.Version.Ticks then
                            let peerDSet = x.PeerDSet
                            if Utils.IsNotNull peerDSet then
                                peerDSet.[peeri] <- readDSet
                            x.bDSetMetaRead.[peeri] <- true
                            if x.bMetaDataSet then
                                if (readDSet.Version <> curDSet.Version ||
                                    readDSet.MetaDataVersion <> curDSet.MetaDataVersion) then
                                            x.bPeerFailed.[peeri] <- true
                                            Logger.Log( LogLevel.Info, ( sprintf "RetrieveMetaDataCallback: Failed peer %d because of inconsistent metadata" peeri ))
                        else
                            // Older version, or wrong cluster, peer failed. 
                            x.bPeerFailed.[peeri] <- true
                            Logger.Log( LogLevel.Info, ( sprintf "RetrieveMetaDataCallback: Failed peer %d because a wrong cluster or expired metadata file is encountered" peeri ))
                        x.numPeerRespond <- x.numPeerRespond + 1
                    | ( ControllerVerb.NonExist, ControllerNoun.DSet ) ->
                        if Utils.IsNotNull x.bDSetMetaRead then 
                            x.bDSetMetaRead.[peeri] <- true                  
                        x.bPeerFailed.[peeri] <- true
                        x.numPeerRespond <- x.numPeerRespond + 1   
                        Logger.Log( LogLevel.Info, ( sprintf "RetrieveMetaDataCallback: failed peer %d because it doesn't have DSet %s:%s" peeri curDSet.Name curDSet.VersionString ))
                    | (ControllerVerb.Exception, ControllerNoun.DSet ) -> 
                        let ex = msRcvd.ReadException()
                        let showMsg = sprintf "RetrieveMetaDataCallback, Job %A, exception at peer %d, message: %A" jobID peeri ex
                        jobAction.ReceiveExceptionAtCallback( ex, showMsg )
                    | _ ->
                        Logger.Log( LogLevel.Warning, ( sprintf "RetrieveMetaDataCallback: Unexpected command from peer %d, command %A" peeri cmd ))
                with
                | ex ->
                    let addMsg = sprintf "Error in RetrieveMetaDataCallback, cmd %A, peer %d, exception %A" cmd peeri ex
                    jobAction.EncounterExceptionAtCallback( ex, addMsg )
        )
        true

    member val internal GetPeer = fun (peeri:int) -> curDSet.Cluster.Queue(peeri) with get, set

    /// Find new peers that will be assigned with partitions, send those partition information. 
    /// Return :
    ///      True: there are pending remapping command to be sentout. 
    ///      False: there is no remapping command pending. 
    member internal x.Remapping(  ) = 
        clockLastRemapping <- (PerfADateTime.UtcNowTicks())
//        let curDSet = x.CurDSet
        // Identify new failing peers 
        for peeri = 0 to curDSet.Cluster.NumNodes-1 do 
            let queue = x.GetPeer(peeri)
            if not x.bPeerFailed.[peeri] then 
                if (NetworkCommandQueue.QueueFail(queue)) then 
                    x.bPeerFailed.[peeri] <- true
                    Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Remapping, remove peer %d as its socket failed" peeri ))
        // Identify newly failed peers. 
        let newlyFailedPeer = List<int>()
        let failedPeer = List<int>()
        for peeri = 0 to curDSet.Cluster.NumNodes-1 do 
            if x.bPeerFailed.[peeri] then 
                failedPeer.Add( peeri ) 
                if not x.bLastPeerFailedPattern.[peeri] then 
                    x.bLastPeerFailedPattern.[peeri] <- x.bPeerFailed.[peeri]
                    newlyFailedPeer.Add( peeri )
//        if String.Compare( curDSet.Name, "SortGen_sort1", StringComparison.OrdinalIgnoreCase ) = 0 then 
//            let a = 4
//            ()

        let mapping = curDSet.GetMapping()
        // Regenerating the matrix partitionReadFromPeers that indicate what peer is assigned to job of a certain partition. 
        // We have three lists to manage:
        // peersFailedForPartition: black list, those peers are proven failure 
        // peersTriedForPartition: pending, those peers have been contacted to perform action
        // partitionReadFromPeers: current list, those peers are to be contacted (but haven't, e.g., because connection hasn't been established)
        for parti=0 to x.partitionReadFromPeers.Length-1 do
            let readFromPeerList = x.partitionReadFromPeers.[parti]
            let peerFailedForList = x.peersFailedForPartition.[parti]
            let peerTriedForList = x.peersTriedForPartition.[parti]
            if x.numErrorPartition.[parti]<>0 then 
                // Don't do anything if the partition has already been processed successfully. 

                // Any peer that is in to be replicated partition is proven in failing?
                // Remove the peer and put in in the failed list
                if readFromPeerList.Count>0 then 
                    let peerArray = readFromPeerList |> Seq.toArray
                    for peeri in peerArray do 
                        if failedPeer.Contains( peeri ) then 
                            readFromPeerList.Remove( peeri ) |> ignore
                            if not (peerFailedForList.Contains(peeri)) then 
                                peerFailedForList.Add( peeri )
                if peerTriedForList.Count>0 then 
                    let peerArray = peerTriedForList |> Seq.toArray
                    for peeri in peerArray do 
                        if failedPeer.Contains( peeri ) then 
                            peerTriedForList.Remove( peeri ) |> ignore
                            if not (peerFailedForList.Contains(peeri)) then 
                                peerFailedForList.Add( peeri )

                        (* Deprecated. 
                // The first condition, this partition has not been assigned. 
                // The second condition: there are some error during returned by the peer that is assigned to the partition. 
                //  indicate that this peer hasn't been successfully read yet. 
                // The third condition: the partition is assigned to a peer that just failed. 
    //            if partitionReadFromPeers.[parti].Count <= 0 
    //                || ( numErrorPartition.[parti]<>0 && peersFailedForPartition.[parti].Contains(partitionReadFromPeers.[parti]) )
    //                || failedPeer.Contains( partitionReadFromPeers.[parti] ) then 
    *)
                let partimapping = mapping.[parti]
                // Trigger seeking new peer, 
                // If 1) there is no pending peer in the readFrom List and Tried list 
                // and 2) not all peers in the partition mapping have been tried. 
                if readFromPeerList.Count=0 && peerTriedForList.Count=0 && peerFailedForList.Count < partimapping.Length then 
                    // We need to find a new peer for this partition. 
                    let mutable idx = 0 
                    // find an alternative peer 
                    while idx<partimapping.Length do
                        let peeri = partimapping.[idx]
                        if not (peerFailedForList.Contains(peeri)) then 
                            if not x.bPeerFailed.[peeri] then 
                                // we find a peer that can be tried. 
                                // whether we put multiple peer in the readFromPeerList depending on whether the bit DSetFlag.ReadAllReplica is set
                                if readFromPeerList.Count=0 || ( curDSet.Flag &&& DSetFlag.ReadAllReplica <> DSetFlag.None ) then 
                                    readFromPeerList.Add( peeri )  
                            else
                                peerFailedForList.Add( peeri ) 
                           
                        idx <- idx + 1
                    // If we can't find any peer 
                    if readFromPeerList.Count=0 then 
                        if not x.bFailingPartitionReported.[parti] then 
                            x.bFailingPartitionReported.[parti] <- true
                            // This message is suppressed as it is very possible that there is no partition parti written out to DSet in the process. 
                            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "DSet.SendReadDSetCommand fail, can't find live peer to read partition %d" parti ))
                    else
                        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Partition %d --> Peer %d from %A" parti readFromPeerList.[0] partimapping ))
            else
                // This partition has already been successfully processed, but some peer is still lingering to be read.  
                // Clear the list of to be requested peer, so that we can end the call. 
                if readFromPeerList.Count>0 then 
                    for peeri in readFromPeerList do 
                        if not (peerFailedForList.Contains(peeri)) then 
                            peerFailedForList.Add( peeri )
                    readFromPeerList.Clear()
//                      Allow failing partition to continue the read operation. 
//                    failwith msg
//                else
//                    bPartitionReadSent.[parti] <- false
        // Remapping for some partition that fails to read successfully. 
        x.SeekNewMapping( ) 
    // Find new peers that will be assigned with partitions, send those partition information. 
    // Return :
    //      True: there are pending remapping command to be sentout. 
    //      False: there is no remapping command pending. 
    member internal x.SeekNewMapping(  ) = 
//        let curDSet = x.CurDSet
        let newPartitionForPeers = Array.create curDSet.Cluster.NumNodes null
        let mutable bAnyPending = false
        for parti=0 to curDSet.NumPartitions-1 do 
//            if not bPartitionReadSent.[parti] then 
                let readFromPeerList = x.partitionReadFromPeers.[parti]
                for peeri in readFromPeerList do
                    if peeri>=0 then 
                        if Utils.IsNull newPartitionForPeers.[peeri] then 
                            newPartitionForPeers.[peeri] <- List<int>()
                        newPartitionForPeers.[peeri].Add( parti )
                        bAnyPending <- true
        if bAnyPending then              
            x.DoRemapping( newPartitionForPeers )
        else
            false
    /// an example of a remapping command. 
    /// Parameter: peeri: int, send the command to ith peer
    ///            peeriPartitionArray: int[], the command applies to the following partitions. 
    ///            dset : the command applies to the following DSet
    member internal x.RemappingCommandForRead ( queue:NetworkCommandQueue, peeri, peeriPartitionArray:int[], curDSet:DSet ) = 
        using( new MemStream( 1024 ) ) ( fun msPayload ->
            // Add Job ID to DSet
            msPayload.WriteGuid( x.JobID )
            msPayload.WriteString( curDSet.Name )
            msPayload.WriteInt64( curDSet.Version.Ticks )
            msPayload.WriteVInt32( peeriPartitionArray.Length )
            for parti in peeriPartitionArray do 
                msPayload.WriteVInt32( parti )
            Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Request to peer %d partition %A" peeri peeriPartitionArray ))
            queue.ToSend( ControllerCommand( ControllerVerb.Read, ControllerNoun.DSet), msPayload )
        )
    member val internal DoRemapping = thisInstance.ExecuteNewMapping with get, set
    /// Call back function used by Execute New Mapping, set this call back to have customized command to send to DSet.
    /// Parameter: peeri: int, send the command to ith peer
    ///            peeriPartitionArray: int[], the command applies to the following partitions. 
    ///            dset : the command applies to the following DSet
    member val internal RemappingCommandCallback = thisInstance.RemappingCommandForRead with get, set
    /// Send outgoing DSet command. 
    /// Return :
    ///      True: there are pending remapping command to be sentout. 
    ///      False: there is no remapping command pending. 
    member internal x.ExecuteNewMapping(newPartitionForPeers) =
        let bAnyRemappingRef = ref false
        using ( getSingleJobAction() ) ( fun jobAction -> 
            if Utils.IsNotNull jobAction then 
                for peeri=0 to newPartitionForPeers.Length-1 do
                    //if Utils.IsNotNull newPartitionForPeers.[peeri] then 
                    //let peeriPartitionArray = newPartitionForPeers.[peeri] |> Seq.toArray
                    let peeriPartitionArray = if Utils.IsNull (newPartitionForPeers.[peeri]) then
                                                  [||]
                                              else
                                                  newPartitionForPeers.[peeri] |> Seq.toArray
                    // if peeriPartitionArray.Length>0 then 
                    bAnyRemappingRef := true
                    let queue = curDSet.Cluster.QueueForWrite( peeri ) 
                    if Utils.IsNotNull queue && queue.CanSend then        
                        if x.bFirstReadCommand.[peeri] && not jobAction.IsCancelled then 
                            x.bFirstReadCommand.[peeri] <- false
                            using ( new MemStream( 1024 ) ) ( fun msSend -> 
                                msSend.WriteGuid( x.JobID )
                                msSend.WriteString( curDSet.Name )
                                msSend.WriteInt64( curDSet.Version.Ticks )
                                queue.ToSend( ControllerCommand( ControllerVerb.Use, ControllerNoun.DSet), msSend )
                            )
                        if not jobAction.IsCancelled then 
                            for parti in peeriPartitionArray do 
                                x.SentCmd( peeri, parti ) 
                        if not jobAction.IsCancelled then 
                            x.RemappingCommandCallback( queue, peeri, peeriPartitionArray, curDSet )
                        let node = curDSet.Cluster.Nodes.[peeri]
                        if curDSet.PeerRcvdSpeedLimit < node.NetworkSpeed && not jobAction.IsCancelled then 
                            using ( new MemStream( 1024 ) ) ( fun msSpeed -> 
                                msSpeed.WriteGuid( x.JobID )
                                msSpeed.WriteString( curDSet.Name ) 
                                msSpeed.WriteInt64( curDSet.Version.Ticks )
                                msSpeed.WriteInt64( curDSet.PeerRcvdSpeedLimit )
                                queue.SetRcvdSpeed(curDSet.PeerRcvdSpeedLimit)
                                queue.ToSend( ControllerCommand( ControllerVerb.LimitSpeed, ControllerNoun.DSet), msSpeed )  
                            )
                        // One more Read DSet command outstanding. 
                        Interlocked.Increment( x.numPeerPartitionCmdSent.[peeri] ) |> ignore
            )
        !bAnyRemappingRef
    /// We have sent request to peeri for parti
    member internal x.SentCmd( peeri, parti ) = 
        x.partitionReadFromPeers.[parti].Remove(peeri ) |> ignore
        x.peersTriedForPartition.[parti].Add(peeri)
        x.bPartitionReadSent.[parti] <- true
    member internal x.PartitionFailed( peeri, parti ) = 
        if not (x.peersFailedForPartition.[parti].Contains(peeri)) then 
            x.peersFailedForPartition.[parti].Add( peeri )
        x.peersTriedForPartition.[parti].Remove(peeri ) |> ignore
        x.partitionReadFromPeers.[parti].Remove(peeri ) |> ignore
        x.bPartitionReadSent.[parti] <- false
        
    /// A peer encounter some error in processing parti
    member internal x.ProcessedPartition( peeri, parti, numError ) = 
        if numError = 0 then 
            // Signal a certain partition is succesfully read
            x.numErrorPartition.[parti] <- numError
        else
            // record failing peer for the partition. 
            x.PartitionFailed( peeri, parti )
            if x.numErrorPartition.[parti]<>0 then 
                x.numErrorPartition.[parti] <- numError  
    /// peeri sends feedback that it doesn't have a set of partitions. 
    member internal x.NotExistPartitions( peeri, notFindPartitions ) = 
        let bRemapped = ref false
        for parti in notFindPartitions do 
            let mutable bRemap = true
            if Utils.IsNotNull curDSet.MappingNumElems then 
                if curDSet.MappingNumElems.[parti].[0]<=0 then 
                    bRemap <- false
            if bRemap then 
                bRemapped := true
                x.PartitionFailed( peeri, parti )
                if not (x.peersNonExistPartition.[parti].Contains( peeri) ) then 
                    x.peersNonExistPartition.[parti].Add( peeri )
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> 
           if !bRemapped then 
               sprintf "Received non exist partition %A from peer %d execute remapping" notFindPartitions peeri 
           else
               sprintf "Received non exist partition %A from peer %d, but those partitions are empty during write" notFindPartitions peeri ))
        !bRemapped
// We rewrote the interface to not expose the following member.
//    member x.NumErrorPartition with get() = numErrorPartition             
//    member x.PeersFailedForPartition with get() = peersFailedForPartition
//    member x.PartitionReadFromPeers with get() = partitionReadFromPeers
    /// Have we read all DSets? 
    member internal x.AllDSetsRead( ) = 
        let mutable bEndReached = true
        for peeri=0 to curDSet.Cluster.NumNodes - 1 do
            let numRcvd = Volatile.Read( x.numPeerPartitionCmdRcvd.[peeri])
            let numSent = Volatile.Read( x.numPeerPartitionCmdSent.[peeri])
            if not x.bPeerFailed.[peeri] && numRcvd<numSent then 
                // At least one peer still active
                bEndReached <- false
        bEndReached 
    /// Indicate a Close, Partition or equivalent command has received from a peer, and the job requested from the peer has been completed. 
    member internal x.PeerCmdComplete( peeri ) = 
        Interlocked.Increment(x.numPeerPartitionCmdRcvd.[peeri]) |> ignore 
