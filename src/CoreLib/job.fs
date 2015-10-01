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
        job.fs
  
    Description: 
        Job for a Prajna host, which defines a continuous set of Prajna Operaton to be 
    executed by a host. There is a corresponding sturcture, Task at the client side.  

    Message: 
        Set, Job            Send Metadata
        Acknowledge, Job    Acknowledge receipt of Metadata
        Availability, Blob  Figure out what blobs are available   
        Write, Blob         distribution of blobs (in JobName, JobVersion, blobi format, where hash of the blob is not known [e.g., ClusterInfo, etc..] )
        Acknowledge, Blob   acknowledge the receipt of Blob
        Set, Blob           write blob ( in hash format, where hash of the blob is pre-known ).
        EchoReturn, Blob    echo of hash blob
        Start, Job          Start the Job
        Link, Program       Link back PrajnaProgram to the daemon
        ConfirmStart, Job   Confirm the start of Blob
        Read, Job           DSet.ToSeq
        Fold, Job           DSet.fold
        ReadToNetwork       transfer content across nodes in a job
        InfoNode, Job       Information of portion of job
        NonExist, Job       Error of job
        Close, Job          Inform PrajnaProgram to terminate a job


    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Runtime.Serialization
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.BytesTools
open Prajna.Tools.Network


type internal PerQueueJobStatus = 
    | None = 0
    | NotConnected = 0          // Initial state, nothing has happened yet. 
    | Connected = 1 
    | JobMetaDataSent = 2       // Sent job metadata && availability info to peer 
    | AvailabilitySent = 4      // Send a request of source metadata to this peer
    | SentAllMetadata = 8       // Receive a request of source metadata
    | AllMetadataSynced = 16    // All Metadata synced
    | StartJob = 32             // Start a job request
    | ConfirmStart = 64         // ConfirmStart received. 
    | Shutdown = 128            // Peer connection closed. 
    | Failed = 256              // Peer connection failed. 
    | FailedToStart = 512       // Job fails to start
    | Crashed = 1024            // Job crashed 
    | AnyFailure = 0x780

type internal JobStage = 
    | None = 0                  // Job not started yet. 
    | RequestSrcDSetData = 1     // Request SrcDSet data from peers. 
    | SyncMetaData = 2          // Sync Metadata among peers. 
    | Ready = 3                 // Job is ready to execute
    | Termination = 5           // Terminating stage. 
    | AllStarted = 5            // All jobs have been started
    | AtLeastOneFailed = 6      // At least one job failed. 

type internal BlobSyncMethod = 
    | Unicast = 0               // Sync metadata via Unicast
    | P2P = 1                   // ToDO: Sync blob via P2P protocol

type internal DSetKind =
    | SrcDSet = 0
    | PassthroughDSet = 1
    | DstDSet = 2

/// <summary>
/// Track class that is being cached at PrajnaProgram
/// </summary>
type internal CacheTracker() = 
    static member val Current = CacheTracker() with get
    /// Store what object has been cached 
    member val CachedCollection = ConcurrentDictionary<_,ConcurrentDictionary<_,_>>() with get
    member x.Register( dobj: DistributedObject ) = 
        if dobj.IsCached then 
            let hash = dobj.Hash
            if not (Utils.IsNull hash) then 
                let dic = x.CachedCollection.GetOrAdd( hash, fun _ -> ConcurrentDictionary<_,_>()) 
                dic.GetOrAdd( dobj.Cluster, false ) |> ignore
    member x.Unregister( hash ) = 
        x.CachedCollection.TryRemove( hash ) |> ignore 
        
    

type internal FunctionParam internal (ty:FunctionParamType, o: DistributedObject, blobIndex) = 
    member val internal ParamType = ty with get, set
//    member val Name = name with get, set
//    member val Version = ver with get, set
    member val BlobIndex = blobIndex with get, set
    member val Object = o with get, set
    new () = 
        FunctionParam( FunctionParamType.None, null, -1 ) 
    member x.Pack( ms:Stream ) = 
        ms.WriteByte( byte x.ParamType )
        ms.WriteVInt32( x.BlobIndex ) 
    static member Unpack( ms:Stream ) = 
        let ty = ms.ReadByte()
        let blobIndex = ms.ReadVInt32() 
        FunctionParam( enum<_>(ty), null, blobIndex )

/// Traverse from source, to resolve any dependency issue
type internal JobTraveseFromSource() = 
    member val Traversed = HashSet<_>() with get, set
    member val FutureExamine = List<_>() with get, set
    member val Unresolved = HashSet<_>() with get, set
    member x.AddOneToFutureExamine( obj:DistributedObject ) = 
        // Use reference equal to see if the object is already there. 
        let mutable bFind = false
        for existingObj in x.FutureExamine do 
            if Object.ReferenceEquals( existingObj, obj ) then 
                bFind <- true
        if not bFind then 
            x.FutureExamine.Add( obj ) 
    member x.AddToFutureExamine( objseq: IEnumerable<DistributedObject>) = 
        for obj in objseq do 
            x.AddOneToFutureExamine( obj ) |> ignore
    /// Examine the object in FutureExamine, 
    /// If the object has all information, place it in the Traversed list, otherwise, move it to FutureExamine
    /// For object that is placed onto the traversed list, add its downstream object to FutureExamine
    member x.TraverseOnce( func ) = 
        // Examine future list, any of the object has valid metadata?
        let curExamine = x.FutureExamine
        x.FutureExamine <- List<_>()
        let toExamineDownstream = List<_>()
        let TryResolve (o:DistributedObject) bAllDependentAvailable = 
            func o bAllDependentAvailable
            Some bAllDependentAvailable
        for o in curExamine do
            let resolved = 
                match o with 
                | :? DSet as dset -> 
                    match dset.Dependency with 
                    | StandAlone
                    | Source -> 
                        TryResolve dset (dset.bValidMetadata)
                    | WildMixFrom ( parent, _ )
                    | MixFrom parent 
                    | Passthrough parent -> 
                        if x.Traversed.Contains( parent.Target ) then 
                            TryResolve dset true 
                        elif x.Unresolved.Contains( parent.Target ) then 
                            TryResolve dset false 
                        else 
                            x.AddOneToFutureExamine( parent.Target )
                            None
                    | Bypass (parent, brothers) -> 
                        if x.Traversed.Contains( parent.Target ) then 
                            TryResolve dset true 
                        elif x.Unresolved.Contains( parent.Target ) then 
                            TryResolve dset false 
                        else 
                            x.AddOneToFutureExamine( parent.Target ) 
                            None 
                    | HashJoinFrom ( pa0, pa1) 
                    | CrossJoinFrom ( pa0, pa1) -> 
                        if x.Unresolved.Contains( pa0.Target ) || x.Unresolved.Contains( pa0.Target ) then 
                            TryResolve dset false
                        elif x.Traversed.Contains( pa1.Target ) && x.Traversed.Contains( pa1.Target ) then 
                            TryResolve dset true 
                        else
                            if not (x.Traversed.Contains( pa0.Target )) then 
                                x.AddOneToFutureExamine( pa0.Target )
                            if not (x.Traversed.Contains( pa1.Target )) then 
                                x.AddOneToFutureExamine( pa1.Target )
                            None
                    | DecodeFrom parentStream -> 
                        Some dset.bValidMetadata 
                    | CorrelatedMixFrom parents 
                    | UnionFrom parents -> 
                        let mutable bTraverese = true
                        let mutable bAllDependAvailable = true
                        for pa in parents do 
                            if x.Unresolved.Contains( pa.Target ) then 
                                bAllDependAvailable <- false
                            else
                                if not ( x.Traversed.Contains( pa.Target ))  then 
                                    bTraverese <- false    
                        if bTraverese then 
                            TryResolve dset bAllDependAvailable
                        else
                            for pa in parents do 
                                if not ( x.Traversed.Contains( pa.Target ))  then 
                                    x.AddOneToFutureExamine( pa.Target )    
                            None
                | :? DStream as dstream -> 
                    match dstream.Dependency with 
                    | SourceStream -> TryResolve dstream true
                    | SaveFrom parent
                    | EncodeFrom parent -> 
                        if x.Traversed.Contains( parent.Target ) then 
                            TryResolve dstream true 
                        elif x.Traversed.Contains( parent.Target ) then 
                            TryResolve dstream false 
                        else 
                            x.AddOneToFutureExamine( parent.Target )
                            None
                    | PassFrom parentStream 
                    | ReceiveFromNetwork parentStream 
                    | MulticastFromNetwork parentStream -> TryResolve dstream true
                | _ -> 
                    Logger.Fail(sprintf "JobTraveseFromSource: Unknown object %s:%s to traverse" o.Name o.VersionString)
            match resolved with 
            | Some true -> 
                // The object is resolved, try to examine down stream object
                x.Traversed.Add( o ) |> ignore
                toExamineDownstream.Add( o ) 
            | Some false -> 
                // The object is not resolvable (metadata doesn't exist )
                x.Unresolved.Add( o ) |> ignore
            | None -> 
                // The object waits for certain of its parent to be added before we can determine if it is resolvable
                x.AddOneToFutureExamine( o ) 
        // Examine the toExamineDownstream list, any future object to be resolved? 
        for o in toExamineDownstream do 
            match o with 
            | :? DSet as dset -> 
                match dset.DependencyDownstream with 
                | Discard -> () 
                | MixTo child
                | Passforward child 
                | CorrelatedMixTo child 
                | UnionTo child 
                | HashJoinTo child 
                | CrossJoinTo child -> 
                    x.AddOneToFutureExamine( child.Target )
                | DistributeForward children -> 
                    children |> Seq.map ( fun child -> child.Target ) |> x.AddToFutureExamine
                | WildMixTo (child, childStream ) -> 
                    x.AddOneToFutureExamine( child.Target ) 
                    x.AddOneToFutureExamine( childStream.Target )
                | SaveTo childStream
                | EncodeTo childStream -> x.AddOneToFutureExamine( childStream.Target )
            | :? DStream as dstream -> 
                match dstream.DependencyDownstream with 
                | SinkStream -> () 
                | DecodeTo child -> x.AddOneToFutureExamine( child.Target )
                | PassTo childStream 
                | SendToNetwork childStream 
                | MulticastToNetwork childStream -> x.AddOneToFutureExamine( childStream.Target )
            | _ -> 
                Logger.Fail(sprintf "JobTraveseFromSource, examine downstream: Unknown object %s:%s to traverse" o.Name o.VersionString)
    /// Examine the object in FutureExamine, 
    /// If the object has all information, place it in the Traversed list, otherwise, move it to FutureExamine
    /// For object that is placed onto the traversed list, add its downstream object to FutureExamine
    member x.Traverse( func ) = 
        let mutable bStopIterate = false
        // Iterate traverse, until all object has been resolved. 
        while not bStopIterate do 
            let nTraversed = x.Traversed.Count
            let nFutureExamine = x.FutureExamine.Count
            let nUnresolved = x.Unresolved.Count
            x.TraverseOnce( func ) 
            bStopIterate <- nTraversed = x.Traversed.Count && 0 = x.FutureExamine.Count && nUnresolved = x.Unresolved.Count
    /// Monitor metadata resolution
    member x.Status() = 
        seq {
            for o in x.Traversed do 
                yield sprintf "Metadata resolved: %A %s:%s" o.ParamType o.Name o.VersionString 
            for o in x.Unresolved do 
                yield sprintf "Metadata unavailable: %A %s:%s" o.ParamType o.Name o.VersionString 
            // This is an error
            for o in x.FutureExamine do 
                yield sprintf "Metadata unresolvable as some of its parent is not included in the src list: %A %s:%s" o.ParamType o.Name o.VersionString 
        } |> String.concat Environment.NewLine

/// Put in traverse class here. 
[<AllowNullLiteral>]
type internal JobTraverseBase() = 
    /// Traverse all job object, with a direction
    member internal x.TraverseAllObjectsWDirection direction (allObj:List<_>) (cur:DistributedObject) action= 
        let mutable bFindObj = false
        for obji in allObj do 
            if Object.ReferenceEquals( obji, cur ) then 
                bFindObj <- true
        if not bFindObj then 
            allObj.Add( cur )
            action direction cur
            match cur with 
            | :? DSet as dset -> 
                match direction with
                | TraverseUpstream ->
                    // Traverse upstream
                    match dset.Dependency with 
                    | StandAlone 
                    | Source -> 
                        // Nothing to resolve
                        ()
                    | MixFrom parent
                    | Passthrough parent -> 
                        x.TraverseAllObjectsWDirection direction allObj parent.Target action
                    | WildMixFrom ( parent, parentS ) -> 
                        // Make sure to first traverse the stream path, as there is need to set WaitForHandle
                        x.TraverseAllObjectsWDirection direction allObj parentS.Target action
                        x.TraverseAllObjectsWDirection direction allObj parent.Target action
                    | HashJoinFrom ( pa0, pa1 )
                    | CrossJoinFrom( pa0, pa1 ) -> 
                        x.TraverseAllObjectsWDirection direction allObj pa0.Target action
                        x.TraverseAllObjectsWDirection direction allObj pa1.Target action
                    | CorrelatedMixFrom parents 
                    | UnionFrom parents ->
                        for parent in parents do 
                            x.TraverseAllObjectsWDirection direction allObj parent.Target action
                    | Bypass ( parent, brothers ) -> 
                        x.TraverseAllObjectsWDirection direction allObj parent.Target action
                        for bro in brothers do 
                            x.TraverseAllObjectsWDirection TraverseDownstream allObj bro.Target action
                    | DecodeFrom parent -> 
                        x.TraverseAllObjectsWDirection direction allObj parent.Target action
                | TraverseDownstream ->
                    // Traverse downstream
                    match dset.DependencyDownstream with 
                    | Discard -> 
                        // Nothing to further resolve
                        ()
                    | MixTo child 
                    | Passforward child 
                    | CorrelatedMixTo child 
                    | UnionTo child 
                    | HashJoinTo child
                    | CrossJoinTo child -> 
                        x.TraverseAllObjectsWDirection direction allObj child.Target action
                    | WildMixTo ( child, childS ) -> 
                        x.TraverseAllObjectsWDirection direction allObj child.Target action
                        x.TraverseAllObjectsWDirection direction allObj childS.Target action
                    | DistributeForward children -> 
                        for child in children do
                            x.TraverseAllObjectsWDirection direction allObj child.Target action
                    | SaveTo child
                    | EncodeTo child -> 
                        x.TraverseAllObjectsWDirection direction allObj child.Target action
            | :? DStream as stream -> 
                match direction with 
                | TraverseUpstream ->
                    // Traverse upstream
                    match stream.Dependency with 
                    | SaveFrom parent
                    | EncodeFrom parent -> 
                        x.TraverseAllObjectsWDirection direction allObj parent.Target action
                    | PassFrom parent 
                    | ReceiveFromNetwork parent 
                    | MulticastFromNetwork parent -> 
                        x.TraverseAllObjectsWDirection direction allObj parent.Target action
                    | SourceStream ->
                        ()
                | TraverseDownstream ->
                    // Traverse downstream
                    match stream.DependencyDownstream with 
                    | PassTo child 
                    | SendToNetwork child 
                    | MulticastToNetwork child -> 
                        x.TraverseAllObjectsWDirection direction allObj child.Target action
                    | DecodeTo child -> 
                        x.TraverseAllObjectsWDirection direction allObj child.Target action
                    | SinkStream -> 
                        ()
                    /// Get all dependent DSets
            | _ -> 
                let msg = sprintf "TraverseAllObjects failed, don't know how to traverse type %A object %A" (cur.GetType()) cur
                Logger.Log( LogLevel.Error, msg  )
                failwith msg 
        ()
    member internal x.TraverseAllObjects direction (allObj:List<_>) (cur:DistributedObject) (action:DistributedObject->unit ) = 
        let wrapperAction direction = 
            action    
        x.TraverseAllObjectsWDirection direction allObj cur wrapperAction
    member internal x.FindDObject (direction) (cur:DistributedObject) (action:DistributedObject->bool ) =  
        if action( cur ) then 
            cur
        else
            match cur with 
            | :? DSet as dset -> 
                match direction with 
                | TraverseUpstream -> 
                    match dset.Dependency with 
                    | StandAlone 
                    | Source -> 
                        null
                    | MixFrom parent 
                    | WildMixFrom ( parent, _ )
                    | Bypass ( parent, _ )
                    | Passthrough parent 
                    | HashJoinFrom ( parent, _ )
                    | CrossJoinFrom (parent, _ ) -> 
                         x.FindDObject direction parent.Target action
                    | CorrelatedMixFrom parents 
                    | UnionFrom parents -> 
                         x.FindDObject direction parents.[0].Target action
                    | DecodeFrom parent -> 
                         x.FindDObject direction parent.Target action
                | TraverseDownstream -> 
                    match dset.DependencyDownstream with 
                    | Discard -> 
                        null
                    | MixTo child 
                    | WildMixTo ( child, _ )
                    | Passforward child 
                    | CorrelatedMixTo child 
                    | UnionTo child 
                    | HashJoinTo child 
                    | CrossJoinTo child -> 
                        x.FindDObject direction child.Target action
                    | DistributeForward children -> 
                        x.FindDObject direction children.[0].Target action
                    | SaveTo child
                    | EncodeTo child -> 
                        x.FindDObject direction child.Target action
            | :? DStream as stream -> 
                match direction with 
                | TraverseUpstream ->
                    // Traverse upstream
                    match stream.Dependency with 
                    | SaveFrom parent
                    | EncodeFrom parent -> 
                        x.FindDObject direction parent.Target action
                    | PassFrom parent 
                    | ReceiveFromNetwork parent
                    | MulticastFromNetwork parent -> 
                        x.FindDObject direction parent.Target action
                    | SourceStream ->
                        null
                | TraverseDownstream ->
                    // Traverse downstream
                    match stream.DependencyDownstream with 
                    | PassTo child 
                    | SendToNetwork child 
                    | MulticastToNetwork child -> 
                        x.FindDObject direction child.Target action
                    | DecodeTo child -> 
                        x.FindDObject direction child.Target action
                    | SinkStream -> 
                        null
                    /// Get all dependent DSets
            | _ -> 
                let msg = sprintf "Task.FindDObject failed, don't know how to traverse type %A object %A" (cur.GetType()) cur
                Logger.Log( LogLevel.Error, msg  )
                failwith msg 



/// Prajna Job
/// A Prajna Job is a set of actions to be executed on Prajna. 
type internal JobFactory() = 
    inherit CacheFactory<Job>()

and /// JobTraverse is used to traverse multiple object hosted in a job to figure out
    /// Src, Dst and Passthrough DSet and various objects. 
    /// We define a separate JobTraverse() object to recognize the need to figure out the source/destination DSet involved in a particular action, 
    /// which can be a subset of the source/destination/passthroughDSet involved in Job 
    internal JobTraverse() = 
    inherit JobTraverseBase()
    member val Clusters = List<_>() with get, set
    member val SrcDSet = List<_>() with get, set
    member val DstDSet = List<_>() with get, set
    member val PassthroughDSet = List<_>() with get, set
    member val DStreams = List<_>() with get, set
    member x.AddOneDepentObject( obj:DistributedObject ) =
        if not (Utils.IsNull obj) then 
            match obj with 
            | :? DSet as dset -> 
                let findObject = new Predicate<DSet>( fun o -> Object.ReferenceEquals( o, obj ) )
                // We use ReferenceEquality here to speed up comparison
                let bDSetExist = x.SrcDSet.Exists( findObject ) || x.PassthroughDSet.Exists( findObject ) || x.DstDSet.Exists( findObject )
                if not bDSetExist then 
//                    if not (x.Clusters.Contains( dset.Cluster )) then 
//                        x.Clusters.Add( dset.Cluster )                
                    // JinL: 01/06/2015, this should mirror AddDSet 
                    match ( dset.Dependency, dset.DependencyDownstream) with 
                    | StandAlone, _ 
                    | DecodeFrom _, _ ->
                        // Consider any source DSet with StandAlone tag, source DSet needs to load metadata
                        x.SrcDSet.Add( dset )     
                    | _, SaveTo _  
                    | _, EncodeTo _ -> 
                        // Any destination DSet carries SaveTo, Dst DSet will register for report, and wait for report to come in.
                        x.DstDSet.Add( dset ) 
                    | _, _ -> 
                        x.PassthroughDSet.Add( dset ) 
                    match dset.Dependency with 
                    | Source -> 
                        dset.GetMapping() |> ignore
                    | _ -> 
                        ()
            | :? DStream as stream -> 
                let findStream = new Predicate<DStream>( fun o -> Object.ReferenceEquals( o, obj ) )
                let bStreamExist = x.DStreams.Exists( findStream )
                if not bStreamExist then 
                    x.DStreams.Add( stream ) 
//                    if not (x.Clusters.Contains( stream.Cluster )) then 
//                        x.Clusters.Add( stream.Cluster )         
            | _ -> 
                let msg = sprintf "AddOneDepentObject failed, don't know how to add type %A object %A" (obj.GetType()) obj 
                Logger.Log( LogLevel.Error, msg  )
                failwith msg 
    /// Get all dependent DSets & DStream
    /// obj: the current object to used in traversal
    /// flag: 0, trace upstream
    ///       1, trace downstream
    member x.TraverseDependencyObj direction allObj  cur =
        x.TraverseAllObjects direction allObj cur x.AddOneDepentObject

    member x.TraverseDependency( objList, direction ) = 
        let allObj = List<_>()
        objList |> Seq.iter ( fun obj -> x.TraverseDependencyObj direction allObj obj )
    /// Parsed chained DSet
    /// flag with mask DSetChainFlag.Passthrough, return all passthrough DSet in the chains
    /// flag with mask DSetChainFlag.Source, return all source DSet in the chains
    /// flag with mask DSetChainFlag.Destination, return all destination DSet in the chain. 
    member internal x.GetDSetChain( obj, flag, extract ) = 
        x.TraverseDependencyObj flag (List<_>()) obj
        match extract with 
        | DSetChainFlag.Source -> 
            x.SrcDSet
        | DSetChainFlag.Passthrough -> 
            x.PassthroughDSet
        | DSetChainFlag.Destination -> 
            x.DstDSet
        | _ -> 
            let msg = sprintf "GetDSetChain, unsupported extraction flag %A" extract
            Logger.Log( LogLevel.Error, msg )
            failwith msg

    member x.ClusterExist( cl: Cluster) = 
        let mutable bFind = false
        for o in x.Clusters do 
            if not bFind then 
                bFind <- Object.ReferenceEquals( o, cl ) 
        bFind
    member x.AddOneDepedentCluster(cl) = 
        if (Utils.IsNotNull cl) && (not (x.ClusterExist(cl))) then 
            x.Clusters.Add(cl)
    member x.AddDepedentClusters() = 
        for dset in x.SrcDSet do 
            x.AddOneDepedentCluster( dset.Cluster )
        for dset in x.PassthroughDSet do 
            x.AddOneDepedentCluster( dset.Cluster )
        for dset in x.DstDSet do 
            x.AddOneDepedentCluster( dset.Cluster )
        for dstream in x.DStreams do 
            x.AddOneDepedentCluster( dstream.Cluster )

and
 [<AllowNullLiteral>]
 internal Job() = 
    inherit JobTraverseBase()
    let mutable jobMetadataStream = null
    let mutable numPeerConnected = 0
    let mutable availThis = null 
    let mutable availPeer = List<_>()
    let mutable jobStage = JobStage.None
    // clock for communication 
    let clock = System.Diagnostics.Stopwatch.StartNew()
    // clock frequency
    let clockFrequency = System.Diagnostics.Stopwatch.Frequency 
    let mutable bAllSrcAvailable = false
    let mutable bFailureToGetSrcMetadata = false
    let mutable bAllSynced = false
    let mutable lastLive = clock.ElapsedTicks
    // Is the job ready to be executed? 
    let mutable bReady = false
    // Parameter Set of PrajnaDSets
    let jobParams = List<FunctionParam>()
    static member val ContainerJobCollection = ConcurrentDictionary<_,ContainerJob>(BytesCompare()) with get
    member val JobStartTicks = (PerfADateTime.UtcNowTicks()) with get, set
    /// A Job Holder contains Assembly & dependent files, a non job holder doesn't have those components. 
    member val internal IsJobHolder = false with get, set
    member val Name = DeploymentSettings.GetRandomName () with get, set
    /// Version of Job is a 64bit integer, coded through hash of assemblies. 
    /// The idea is that the Job can persist 
    member val Version = (PerfDateTime.UtcNow()) with get, set
    /// Whether the job needs to be launched, a few action, e.g., "stop service" don't attempt to launch a new job
    member val LaunchMode = DeploymentSettings.DefaultLaunchBehavior with get, set
    member val LaunchIDName = "" with get, set
    member val LaunchIDVersion = 0L with get, set
    /// Job ID, uniquely identify the job, inserted on 9/2015
    /// Job ID is initiated in DSetAction. 
    member val JobID = Guid.Empty with get, set
    /// Obtain a SingleJobActionApp object that is used for controlling the lifecycle of a job
    /// This function is set by DSetAction
    member val TryExecuteSingleJobActionFunc : (unit -> SingleJobActionApp) = ( fun _ -> null ) with get, set
    /// Job port to be used by remote execution container
    member val JobPort = 0us with get, set
    /// Signature Name & Version uniquely define a job that can be loaded into an AppDomain/Exe
    member x.SignatureName with get() =
                                    if x.LaunchMode = TaskLaunchMode.LaunchAndDisregardDifferentVersion then 
                                        x.LaunchIDName + x.LaunchIDVersion.ToString( "X" )
                                    else
                                        x.LaunchIDName
    /// Signature Name & Version uniquely define a job that can be loaded into an AppDomain/Exe
    member x.SignatureVersion with get() = 
                                    if x.LaunchMode = TaskLaunchMode.LaunchAndDisregardDifferentVersion then 
                                        0L
                                    else
                                        x.LaunchIDVersion
    /// Information of the clusters that uses the Job
    member val internal ClustersInfo = List<ClusterJobInfo>() with get, set
//    member val SignatureName = "" with get, set
//    member val SignatureVersion = 0L with get, set
    member val MetadataStream : StreamBase<byte> = null with get, set
    member val OutgoingQueues = List<_>() with get, set
    /// Given a queue information, lookup the peer number
    /// OutgoingQueuesToPeerNumber is constructed one time during job launch, no multithread issue here. 
    member val OutgoingQueuesToPeerNumber = Dictionary<_,_>() with get, set
    member val OutgoingQueueStatuses = List<_>() with get, set
    /// All Clusters to be used in the current job. 
    member val Clusters = List<_>() with get, set   
    /// Prajna Task Type
    member val TypeOf = JobTaskKind.None with get, set
    /// SrcDSet are the set of DSets that exist before the execution of the current job. 
    member val SrcDSet = List<DSet>() with get, set
    /// Number of peer responded with SrcDSet request, as the SrcDSet may differ from peers
    member val SrcDSetNumResponded = List<int>() with get, set
    /// Number of peer responded with SrcDSet request, as the SrcDSet may differ from peers
    member val SrcDSetNumValidResponse = List<int>() with get, set
    /// DstDSet are the set of DSets that has persist content after the completion of the current job
    member val DstDSet = List<DSet>() with get, set
    /// PassthroughDSet are the set that exists for the duration of the job only
    member val PassthroughDSet = List<DSet>() with get, set
    /// DStreams object used for the job
    member val DStreams = List<DStream>() with get, set
    /// Assemblies 
    member val internal Assemblies = List<AssemblyEx>() with get, set
    /// Job File Dependencies
    member val internal JobDependencies = List<JobDependency>() with get
    member val internal UseGlobalJobDependencies = true with get, set
    /// The Job Directory, if empty use default directory
    member val JobDirectory = "" with get, set
    member val RemoteMappingDirectory = "" with get, set
    /// Environment Variables for the job
    member val JobEnvVars = new List<string*string>() with get
    /// Assembly bindings for the job
    member val JobAsmBinding : AssemblyBinding option = None with get, set
    /// Start Blob Serial Number that will define this 
    member val BlobStartSerial = 0L with get, set
    /// Blobs associated with the current job
    member val internal Blobs : Blob[] = null with get, set
    /// Is the job to launch a container 
    member val internal IsContainer = false with get, set
    /// Linked job dependency object. 
    member val internal JobDependecyObjec : JobDependencies = null with get, set
    /// Blob sync method
    member val BlobSync = BlobSyncMethod.Unicast with get, set
    /// Ready Status
    member x.ReadyStatus with get() = bReady
    /// Add one Prajna job parameter
    member x.Param with set( p: Object ) = 
                            match p with 
                            | :? GV as gv -> 
                                if bReady then 
                                    let msg = sprintf "Error at Job.Param, once Job is declared as ready (metadata read), no more parameter can be added to the job class. Fail to add GV %A" gv
                                    Logger.Log( LogLevel.Error, msg )
                                    failwith msg
                                else
                                    jobParams.Add( FunctionParam( FunctionParamType.GV, gv, -1 ) )
                            | :? DSet as dset ->
                                // blob index is not resolved. 
                                let bAdded = x.AddDSet( dset )
                                if bAdded then 
                                    if bReady then 
                                        let msg = sprintf "Error at Job.Param, once Job is declared as ready (metadata read), no more parameter can be added to the job class. Fail to add DSet %s" dset.Name 
                                        Logger.Log( LogLevel.Error, msg )
                                        failwith msg
                                    else
                                        jobParams.Add( FunctionParam( FunctionParamType.DSet, dset, -1 ) )
                            | _ -> 
                                failwith (sprintf "Job.Param, unknown type %A" p )
    /// Get Prajna Job parameters
    member x.ParameterList with get() = jobParams
    /// Set Multiple Parameters
    member x.Params with set( parameters:IEnumerable<Object> ) = 
                        for p in parameters do
                            x.Param <- p
    /// Because it is possible for the DSet version to get updated, so that we will need to get the correct DSet version (if necessary) 
    member x.ResolveDSetAfterMetadataSync( dset:DSet ) = 
        let returnDSet = ref dset
        for parami=0 to x.ParameterList.Count - 1 do 
            let param = x.ParameterList.[parami]
            if Object.ReferenceEquals( param, dset ) then 
                if param.BlobIndex>=0 then 
                    let blob = x.Blobs.[ param.BlobIndex ]
                    if Utils.IsNotNull blob.Object then 
                        returnDSet := blob.Object :?> DSet
//                    else
//                        let msg = sprintf "Error in ResolveDSetAfterMetadataSync, resolved blob.Object is null, possible because error in metadata update"
//                        Logger.Log(LogLevel.Error, msg)
//                        failwith msg
                else
                    let msg = sprintf "Error in ResolveDSetAfterMetadataSync, param.BlobIndex is %d, possible because ResolveDSetAfterMetadataSync is called before Metadata sync" param.BlobIndex
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg
            
        !returnDSet                       
    /// Availability vector for the current job
    member internal x.AvailThis with get() = availThis
                                 and set(v) = availThis <- v
    /// Availability vector for the current job
    member internal x.AvailPeer with get() = availPeer
                                 and set(v) = availPeer <- v
    /// Number of blobs
    member x.NumBlobs with get() = if Utils.IsNull x.Blobs then 0 else x.Blobs.Length
    /// Version String
//    member x.VersionString with get() = VersionToString( x.Version )
    member x.VersionString with get() = VersionToString( x.Version )
    // Metadata Timeout Limit ( in second )
    // deprecated, please use DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit 
    // member val RemoteContainerEstablishmentTimeoutLimit = DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit with get, set
    /// Total length of sending queue that is allowed outstanding. 
    member val SendingQueueLimit = 1024 * 1024 * 100 with get, set
        
    member x.KeepLive() = 
        lastLive = clock.ElapsedTicks
    member x.SecondsLive() = 
        float ( clock.ElapsedTicks - lastLive ) / float clockFrequency
    member x.GetAssemAndRefAssem() =
        let assem = List(AppDomain.CurrentDomain.GetAssemblies())
        let assemArr = assem.ToArray()
        for a in assemArr do
            let assemRefName = a.GetReferencedAssemblies()
            for aRefName in assemRefName do
                let aRef = Reflection.Assembly.ReflectionOnlyLoad(aRefName.FullName)
                if (not (assem.Contains aRef)) then
                    assem.Add(aRef)
        assem
    member val internal AssemblyInJobDependencies = ConcurrentDictionary<_,AssemblyEx>(StringComparer.OrdinalIgnoreCase) with get
    member internal x.AssembliesContains(assem : AssemblyEx) =
        let mutable bFound = false
        for a in x.Assemblies do
            if (not bFound && a.IsSame(assem)) then
                bFound <- true
        // check if job dependencies already has this assembly
        if not bFound then 
            for j in x.JobDependencies do
                if (not bFound) then
                    // JinL: Add a concurrent dictionary lookup to avoid the performance overhead of repeatedly loading assembly and testing IsSame
                    let a = x.AssemblyInJobDependencies.GetOrAdd( j.Location, fun _ -> j.ToAssembly(j.Location) )
                    if (Utils.IsNotNull a && a.IsSame(assem)) then
                        bFound <- true
        bFound
    /// Populate loaded assemblies, 
    /// Extended after similar function in ReflectionTools.fs
    member x.PopulateLoadedAssems() =
        //let assemLoaded = AppDomain.CurrentDomain.GetAssemblies()
        let assemLoaded = AssemblyEx.GetAssemAndRefAssem() |> Seq.toArray
        for pair in AssemblyCollection.Current.PDBCollection do 
            if Interlocked.CompareExchange( pair.Value, 1 , 0 ) = 0 then 
                // First time, add the pdb to job dependencies
                JobDependencies.Current.AddPdbs( [| pair.Key |] )
        let bDebuggable = ref true 
        match DeploymentSettings.LaunchDebugMode with 
        | DebugMode.LaunchInDebug -> 
            bDebuggable := true
        | DebugMode.LaunchInRelease -> 
            bDebuggable := false
        | _ -> 
            let a = assemLoaded.[0]
            try
                let attributes = a.GetCustomAttributes(false)
                for attr in attributes do
                    match attr with 
                    | :? System.Diagnostics.DebuggableAttribute as debuggable -> 
                            bDebuggable := debuggable.IsJITTrackingEnabled
                    | _ -> 
                        ()
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Assembly %s has debuggable attribitute of %A" (a.GetName().Name) !bDebuggable ))
            with
            | e -> 
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Fail to get Custom Attribute for assembly %s, debuggable flag is set to %A" (a.GetName().Name) (!bDebuggable) ))
        for a in assemLoaded do
            try
                let assem = AssemblyCollection.Current.ReflectionOnlyLoadAssemblyCollection.GetOrAdd(
                                a.FullName, 
                                fun _ -> 
                                    let loadAssembly = AssemblyEx( TypeOf = AssemblyKind.ManagedDLL, 
                                                                            Name = a.GetName().Name, 
                                                                            FullName = a.FullName, 
                                                                            Location = a.Location ) 
                                    loadAssembly.ComputeHash() // Compute hash is done when the assembly is added. 
                                    loadAssembly                                                                                    
                                                    )

//              JinL: 03/01/2014, Public Key Token of assembly doesn't change, which is not a good representation of the assembly. 
//                assem.Hash <- a.GetName().GetPublicKeyToken()
                if assem.IsCoreLib && not (!bDebuggable) then 
                    // Detection of linking to release mode
                    assem.TypeOf <- assem.TypeOf ||| AssemblyKind.IsRelease
                if assem.IsSystem then 
                    assem.TypeOf <- assem.TypeOf ||| AssemblyKind.IsSystem
                else 
                    // Only add non system assemblies 
//                    if Utils.IsNull assem.Hash || assem.Hash.Length=0 then 
                    assem.ComputeHash() |> ignore                 
                    if not (x.AssembliesContains( assem )) then 
                        x.Assemblies.Add( assem )
            with
            | e ->
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "System constraint, unable to stream assembly %s as location object is not assessible with exception %A" a.FullName e ))

    /// Get the execution mode needed by the current task. 
    member x.GetExecutionMode() = 
        for assem in x.Assemblies do
            if assem.TypeOf &&& AssemblyKind.IsSystem = AssemblyKind.None then 
                let typeOf = assem.TypeOf &&& AssemblyKind.MaskForExecutionType
                let desiredTypeOf = 
                    match typeOf with
                    | AssemblyKind.ExecutionTypeManaged -> 
                        JobTaskKind.AppDomainMask
                    | AssemblyKind.ExecutionTypeUnmanaged -> 
                        JobTaskKind.ApplicationMask
                    | _ ->
                        JobTaskKind.LightCPULoad // Nothing
                        // failwith (sprintf "Programming Logic error, mode %A hasn't been covered" assem.TypeOf )
                let curTypeOf = x.TypeOf &&& JobTaskKind.ExecActionMask 
                if curTypeOf < desiredTypeOf then 
                    x.TypeOf <- x.TypeOf - curTypeOf + desiredTypeOf
        // Execute in release? 
        for assem in x.Assemblies do
            if assem.IsCoreLib && ( assem.TypeOf &&& AssemblyKind.IsRelease <> AssemblyKind.None ) then 
                x.TypeOf <- x.TypeOf &&& ( ~~~ JobTaskKind.ExecActionMask ) ||| JobTaskKind.ApplicationMask ||| JobTaskKind.ReleaseMask
    /// Unmanaged Assembly, this will be a folder which can contain any kind of data/assemblies to be sent out. 
    member x.AddUnmanagedAssemblyAndComputeHash( name:string, folder, ?fullname ) = 
        let fname = defaultArg fullname name 
        let assem = AssemblyEx( TypeOf = AssemblyKind.UnmanagedDir, 
                                    Name = name, 
                                    FullName = fname, 
                                    Location = folder )
        assem.ComputeHash() |> ignore
        if not (x.Assemblies.Contains( assem )) then 
            x.Assemblies.Add( assem )
    /// Add a source DSet 
    member x.AddSrcDSet( dset ) = 
        x.SrcDSet.Add( dset ) 
        x.SrcDSetNumResponded.Add(0)
        x.SrcDSetNumValidResponse.Add(0) 
    /// Clear Source DSet
    member x.ClearSrcDSet() = 
        x.SrcDSet.Clear()
        x.SrcDSetNumResponded.Clear()
        x.SrcDSetNumValidResponse.Clear()
    /// Add a DSet to a destination DSet (a sink for the job)
    member x.AddDstDSet( dset ) = 
        if not (x.DstDSet.Contains(dset)) then 
                x.DstDSet.Add(dset)
    /// Add DSet to the job, depending on whether the current DSet is a passthrough, it will be added to either the SrcDSet list or PassthroughDSet list.
    /// Return:
    ///     true: dset is added to the list
    ///     false: dset has already been in job (e.g., added via dependancy of other DSets).
    member x.AddDSet( dset:DSet ) = 
        let ty = dset.StorageType &&& StorageKind.StorageMediumMask
        let dep = dset.Dependency
        let category =             
            match ty, dep with 
            | _, StandAlone
            | _, DecodeFrom _ ->
                DSetKind.SrcDSet
            | StorageKind.Passthrough, _ 
            | StorageKind.RAM, _ ->     
                DSetKind.PassthroughDSet
            | _, _ ->
                match dset.DependencyDownstream with 
                | EncodeTo _
                | SaveTo _ ->
                    DSetKind.DstDSet
                | _ -> 
                    DSetKind.PassthroughDSet
        match category with 
        | DSetKind.SrcDSet ->
            // Src DSet, no functions applied to them. 
            if not (x.SrcDSet.Contains(dset)) then 
                x.AddSrcDSet(dset)
                true
            else
                false    
        | DSetKind.PassthroughDSet ->
            if not (x.PassthroughDSet.Contains(dset)) then 
                x.PassthroughDSet.Add( dset ) 
                true
            else
                false
        | DSetKind.DstDSet ->
            if not (x.DstDSet.Contains(dset)) then 
                x.DstDSet.Add( dset ) 
                true
            else
                false
        | _ -> 
            let msg = sprintf "Job.AddDSet logic error, unknown Datatype encountered: %A" category
            Logger.Log( LogLevel.Error, msg )
            failwith msg

    /// Add dependent DSets. 
    /// Deprecated
    member x.AddDependencies() = 
        let seedObjects = List<_>()
        seedObjects.AddRange( x.SrcDSet |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        seedObjects.AddRange( x.DstDSet |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        seedObjects.AddRange( x.PassthroughDSet |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        seedObjects.AddRange( x.DStreams |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        
        let tra = JobTraverse( Clusters = x.Clusters, 
                                     SrcDSet = List<_>( x.SrcDSet ), 
                                     DstDSet = List<_>( x.DstDSet ), 
                                     PassthroughDSet = List<_>( x.PassthroughDSet ), 
                                     DStreams = List<_>(x.DStreams ) )
        // First try upstream, then try downstream
        tra.TraverseDependency( seedObjects, TraverseDirection.TraverseUpstream ) 
        tra.TraverseDependency( seedObjects, TraverseDirection.TraverseDownstream )
        tra.AddDepedentClusters()
        // Update the job
        x.SrcDSet <- tra.SrcDSet
        x.SrcDSetNumResponded <- List<_>( Array.zeroCreate<int> tra.SrcDSet.Count )
        x.SrcDSetNumValidResponse <- List<_>( Array.zeroCreate<int> tra.SrcDSet.Count )
        x.DstDSet <- tra.DstDSet
        x.PassthroughDSet <- tra.PassthroughDSet
        x.DStreams <- tra.DStreams
        x.Clusters <- tra.Clusters
    /// Generate Hash for DSet & DStream, precoding. 
    member x.PrecodeDSets() = 
        // We will always reencode DSet & DStream for each job, there are possible way to bypass coding, but there is always risk (e.g., 
        // certain local variable in closure can change. 
        for dset in x.SrcDSet do 
            let blob = Blob( TypeOf = BlobKind.SrcDSetMetaData, 
                                    Name = dset.Name, 
                                    Version = dset.Version.Ticks, 
                                    Object = dset )
            dset.HashNameVersion()
            Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "Source DSet %s:%s, Hash = %s" dset.Name dset.VersionString (BytesToHex(dset.Hash)) ))
            dset.Blob <- blob
        for dset in x.DstDSet do 
            let blob = Blob( TypeOf = BlobKind.DstDSetMetaData,
                                    Name = dset.Name, 
                                    Version = dset.Version.Ticks, 
                                    Object = dset )
            dset.Blob <- blob
        for dset in x.PassthroughDSet do 
            let blob = Blob( TypeOf = BlobKind.PassthroughDSetMetaData,
                                    Name = dset.Name, 
                                    Version = dset.Version.Ticks, 
                                    Object = dset )
            dset.Blob <- blob
        for dstream in x.DStreams do 
            let blob = Blob( TypeOf = BlobKind.DStream, 
                                    Name = dstream.Name, 
                                    Version = dstream.Version.Ticks, 
                                    Object = dstream ) 
            dstream.Blob <- blob
        /// Assume source has been coded, and use hash of the read operation. 
        let codedObjects = List<_>()
        codedObjects.AddRange( x.SrcDSet |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        let seedObjects = List<_>()
        seedObjects.AddRange( x.DstDSet |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        seedObjects.AddRange( x.PassthroughDSet |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        seedObjects.AddRange( x.DStreams |> Seq.map ( fun dset -> dset :> DistributedObject ) )
        x.RepeatedPrecode codedObjects seedObjects

    member x.RepeatedPrecode codedObjects toEncodeObjects =
        let nCodedObjectBefore = codedObjects.Count
        let toEncodeObjectsNext = List<_>()
        for dobj in toEncodeObjects do 
            let depObjs = dobj.PrecodeDependentObjs()
            // Has all dependent object being coded?
            let mutable bCanCode = true
            for depObj in depObjs do
                if (Utils.IsNull depObj.Target.Hash) then 
                    bCanCode <- false
                else
                    // Populate Hash
                    if not (Object.ReferenceEquals( depObj.Hash, depObj.Target.Hash)) then 
                        depObj.Hash <- depObj.Target.Hash
            if bCanCode then 
                x.EncodeToBlob( dobj.Blob ) |> ignore
                dobj.Hash <- dobj.Blob.Hash
                codedObjects.Add( dobj ) 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Precode %A %s:%s, Hash=%s" dobj.ParamType dobj.Name dobj.VersionString (BytesToHex(dobj.Hash)) ))
            else
                toEncodeObjectsNext.Add( dobj ) 
        if toEncodeObjectsNext.Count = 0 then 
            // Setup downstream dependency
            for dobj in codedObjects do 
                dobj.SetupDependencyHash() 
                match dobj with 
                | :? DSet as dset -> 
                    // For SrcDSet recoding. 
                    if Utils.IsNull dset.Blob.Stream then 
                        x.EncodeToBlob( dobj.Blob ) |> ignore        
                    dset.EncodeDownStreamDependency( dset.Blob.Stream )
                | :? DStream as dstream -> 
                    dstream.EncodeDownStreamDependency( dstream.Blob.Stream )
                | _ -> 
                    ()
                // retrigger hash computation
                dobj.Blob.Hash <- null
                dobj.Blob.StreamToBlob( dobj.Blob.Stream )
                Logger.LogF( x.JobID, LogLevel.MediumVerbose, ( fun _ -> sprintf "Precode %A %s:%s, Object Hash=%s Total=%dB (Hash=%s)" 
                                                                           dobj.ParamType dobj.Name dobj.VersionString (BytesToHex(dobj.Hash)) 
                                                                           dobj.Blob.Stream.Length
                                                                           (BytesToHex(dobj.Blob.Hash)) 
                                                   ))
            // Code everything
            true
        elif codedObjects.Count = nCodedObjectBefore then 
            // Nothing is coded. 
            Logger.LogF( LogLevel.Error, ( fun _ -> "The following object fails to encode due to failing in resolving dependency" ))
            x.ShowAllDObjectsInfo() 
            for dobj in toEncodeObjects do 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "%A %s:%s" dobj.ParamType dobj.Name dobj.VersionString ))
            false 
        else
            x.RepeatedPrecode codedObjects toEncodeObjectsNext

    /// Show all objects (the following format is more debuggable
    member x.ShowAllDObjectsInfo() = 
        let clInfo = x.Clusters |> Seq.map ( fun cl -> cl.Name + ":" + cl.VersionString )
        let srcDSetInfo = x.SrcDSet |> Seq.map ( fun dset -> dset.Name + ":" + dset.VersionString )
        let dstDSetInfo = x.DstDSet |> Seq.map ( fun dset -> dset.Name + ":" + dset.VersionString )
        let passthroughDSetInfo = x.PassthroughDSet |> Seq.map ( fun dset -> dset.Name + ":" + dset.VersionString )
        let allDSets = [| x.SrcDSet; x.DstDSet; x.PassthroughDSet |] |> Seq.concat
        let dStreamInfo = x.DStreams |> Seq.map ( fun stream -> stream.Name + ":" + stream.VersionString )
        Logger.LogF( x.JobID, LogLevel.MildVerbose, fun _ -> ( "Clusters......... " + ( String.concat "," clInfo ) ) )
        Logger.LogF( x.JobID, LogLevel.MildVerbose, fun _ -> ( "SrcDSet .......... " + ( String.concat "," srcDSetInfo ) ))
        Logger.LogF( x.JobID, LogLevel.MildVerbose, fun _ -> ( "DstDSet .......... " + ( String.concat "," dstDSetInfo ) ))
        Logger.LogF( x.JobID, LogLevel.MildVerbose, fun _ -> ( "PassthroughDSet .. " + ( String.concat "," passthroughDSetInfo ) ))
        Logger.LogF( x.JobID, LogLevel.MildVerbose, fun _ -> ( "DStreams ........ " + ( String.concat "," dStreamInfo ) ))
        let except = ref Unchecked.defaultof<_>
        for dset in allDSets do 
            try 
                Logger.LogF( x.JobID, LogLevel.MildVerbose, fun _ -> (dset.ShowDependencyInfo()))
            with e -> 
                Logger.LogF( x.JobID, LogLevel.Warning, ( fun _ -> sprintf "Fail to show dependency information of DSet %s:%s" dset.Name dset.VersionString ) )
                except := e
        for st in x.DStreams do 
            try
                Logger.LogF( x.JobID, LogLevel.MildVerbose, fun _ -> (st.ShowDependencyInfo()))
            with e -> 
                Logger.LogF( x.JobID, LogLevel.Warning, ( fun _ -> sprintf "Fail to show dependency information of DStream %s:%s" st.Name st.VersionString ) )
                except := e
        if Utils.IsNull !except then 
            ()
        else
            raise !except

    /// Specific aid to setup structure
    member x.BypassSetup( curDSet:DSet ) = 
        match curDSet.Dependency with 
        | Bypass ( parent, brothers ) -> 
            let depCur = DependentDSet( curDSet )
            let parentDSet = parent.TargetDSet
            let depParent = List<_>( brothers.Count + 1 )
            depParent.Add( depCur )
            depParent.AddRange( brothers )
            parentDSet.DependencyDownstream <- DistributeForward depParent
            for bro in brothers do 
                let broDSet = bro.TargetDSet
                let broList = List<_>( brothers.Count ) 
                broList.Add( depCur ) 
                broList.AddRange( brothers |> Seq.filter ( fun x -> not (Object.ReferenceEquals(x, bro) ) ) )
                broDSet.Dependency <- Bypass (parent, broList )
        | _ -> 
            let msg = sprintf "BypassSetup called with a non bypass DSet %s:%s" x.Name x.VersionString
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    /// Get All clusters associated with the DSets. 
    member x.GetAllClusters() = 
        let clusters = List<_>()
        let allDSets = seq { yield! x.SrcDSet
                             yield! x.DstDSet
                             yield! x.PassthroughDSet 
                           } |> Seq.toArray
        for dset in allDSets do 
            let cluster = dset.Cluster
            if Utils.IsNotNull cluster && not ( clusters.Contains( cluster) ) then 
                clusters.Add( cluster )
        for stream in x.DStreams do 
            let cluster = stream.Cluster
            if Utils.IsNotNull stream && not ( clusters.Contains( cluster) ) then 
                clusters.Add( cluster )
        x.Clusters <- clusters // |> Seq.toArray
    /// Get All nodes (and queues) that is associated with the current job
    member x.InitializeAllQueues() = 
        for cluster in x.Clusters do
            cluster.InitializeQueues()
            for peeri=0 to cluster.NumNodes-1 do
                let queue = cluster.QueueForWrite(peeri)
                x.AddOutoingQueue( queue, cluster )
    /// Add a new queue to the cluster
    member x.AddOutoingQueue( queue, cluster ) = 
        if not (Utils.IsNull queue) then 
            if not ( x.OutgoingQueuesToPeerNumber.ContainsKey(queue) ) then 
                // Add queue to client to queues. 
                let peeri = x.OutgoingQueues.Count
                x.OutgoingQueues.Add( queue )
                // Add initial availability information. 
                availPeer.Add( BlobAvailability( x.NumBlobs ) )
                // Status
                x.OutgoingQueueStatuses.Add( PerQueueJobStatus.NotConnected )
                // Add reverse lookup table. 
                x.OutgoingQueuesToPeerNumber.Item( queue ) <- peeri
    /// Node Information within a job. 
    member val internal NodeInfo = ConcurrentDictionary<_, _>() with get, set

    /// Try load metadata. 
    member x.TryLoadSrcMetadata() = 
        let mutable bAllLoaded = true
        for srcDSet in x.SrcDSet do 
            if not srcDSet.bValidMetadata then 
                // Don't unpack function. 
                if not ( srcDSet.TryLoadDSetMetadata( false ) ) then 
                    bAllLoaded <- false
        bAllLoaded

    member x.DeterminePersistence() = 
        let allDSet = [ x.SrcDSet; x.PassthroughDSet; x.DstDSet ] |> Seq.concat
        let mutable bPersistence = false
        for dset in allDSet do
            let ty = dset.StorageType &&& StorageKind.StorageMediumMask
            if ty = StorageKind.RAM then 
                bPersistence <- true   
        if bPersistence then 
//            x.TypeOf <- x.TypeOf ||| JobTaskKind.PersistentMask
            x.TypeOf <- x.TypeOf ||| JobTaskKind.InRAMDataMask
    member val internal AssemblyHash = null with get, set
    member internal x.NoSignatureGenerated() = 
        Utils.IsNull JobDependencies.AssemblyHash
    /// Generate a signature that best capture the current set of assemblies. 
    /// The signature is based on the assemblies and their referenced assemblies that are loaded at the time GenerateSignature
    /// function is called. 
    member x.GenerateSignature( depHash: byte[]) = 
        if Utils.IsNull JobDependencies.AssemblyHash then 
            // Recalculate Hash
            let mutable signatureName = ""
            let mutable hash = 0L 
            use hasher = Hash.CreateChecksum() 
            if not (Utils.IsNull depHash) then 
                hasher.TransformBlock( depHash, 0, depHash.Length, depHash, 0 ) |> ignore             
            for assemi=0 to x.Assemblies.Count-1 do
                let assem = x.Assemblies.[assemi]
                if not (Utils.IsNull assem.Hash) then 
                    hasher.TransformBlock( assem.Hash, 0, assem.Hash.Length, assem.Hash, 0 ) |> ignore 
                signatureName <- signatureName + assem.Name
            hasher.TransformFinalBlock( [||], 0, 0 ) |> ignore 
            x.AssemblyHash <- hasher.Hash
            JobDependencies.AssemblyHash <- hasher.Hash
            let mutable hash = BitConverter.ToInt64( x.AssemblyHash, 0 ) 
            JobDependencies.LaunchIDVersion <- hash
            x.LaunchIDVersion <- hash // All Jobs with the same job name share the same signature version, disregard the associated DLLs 
        else
            // Make sure that the LaunchIDVersion does not change if the program reference more DLLs in later execution. 
            x.AssemblyHash <- JobDependencies.AssemblyHash
            x.LaunchIDVersion <- JobDependencies.LaunchIDVersion
        x.SetLaunchIDName()
    member x.SetLaunchIDName() = 
        let curJob = JobDependencies.Current
        if not (Utils.IsNull curJob.JobName) && curJob.JobName.Length > 0 then 
            x.LaunchIDName <- curJob.JobName
        else
            // x.LaunchIDName <- signature name // deprecated, we will use the name of the current executable as job name, if not specified. 
            x.LaunchIDName <- Path.GetFileName( System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName )
        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Generate job launch signature, %d assemblies, hash = %s" 
                                                                           x.Assemblies.Count
                                                                           (x.LaunchIDVersion.ToString("X")) ))
// JinL: 01/09/2015, Signature is now attached to LaunchIDVersion, there is no need to inject part of signature to Version. 
//        // The lower 32bit of the job version will be part of the signature, so that if the signature changes, the job version will change
//        // As version of the job is a datetime structure, this will cause the time shown on the job to change a few minutes (plus or minus)
//        let mutable ticks = x.Version.Ticks
//        ticks <- ( ticks &&& 0xffffffff00000000L ) ||| ( x.SignatureVersion &&& 0xffffffffL )
//        x.Version <- DateTime( ticks )
    member x.ResolveParameter() =
        // Parameter list
        for param in x.ParameterList do
            param.BlobIndex <- -1 
            for blobi = 0 to x.Blobs.Length - 1 do 
                let blob = x.Blobs.[blobi] 
                if Object.ReferenceEquals( blob.Object, param.Object ) then 
                    param.BlobIndex <- blobi
            if param.BlobIndex < 0 then 
                let msg = sprintf "Fail to resolve job parameter %A" param.Object 
                Logger.Log( LogLevel.Error, msg )
                failwith msg 
            ()

    /// Pack to send the job information to client
    /// Define blobs that is used to identify data/metadata to be used during the job. 
    /// Job version is generated during pack, based on assemblies. 
    member x.Pack( ms:StreamBase<byte> ) =
        if not x.IsContainer then 
            x.TryLoadSrcMetadata() |> ignore
            x.DeterminePersistence()
        let blobList = List<_>()
        ms.WriteString( x.Name ) 
        ms.WriteInt64( x.Version.Ticks ) 
        ms.WriteString( x.LaunchIDName )
        ms.WriteInt64( x.LaunchIDVersion )
        ms.WriteGuid( x.JobID )
        ms.WriteBoolean( x.IsContainer )
        // LaunchMode, specifically, Do not launch is needed by job
        ms.WriteVInt32( int x.LaunchMode )

        if x.IsContainer then 
            Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Send Job Information for job %s:%s, of task %s:%s, with launchmode %A"
                                                                           x.Name x.VersionString
                                                                           x.SignatureName (x.SignatureVersion.ToString("X"))
                                                                           x.LaunchMode ))
            x.GetExecutionMode()
            ms.WriteVInt32( int x.TypeOf )
        // Deterministic Blob Start Serial Number, so that a restart job can sync
        x.BlobStartSerial <- int64 x.SignatureVersion
        ms.WriteInt64( x.BlobStartSerial )
//        x.AssembliesArray <- addAssemblyList |> Seq.toArray

        // Send blob sync method
        ms.WriteVInt32( int x.BlobSync )

        if x.IsContainer then 
            // Find list of assemblies that needs to be sent over
            ms.WriteVInt32( x.Assemblies.Count )
            for assemi=0 to x.Assemblies.Count-1 do
                let assem = x.Assemblies.[assemi]
                let useAssemType = assem.TypeOf &&& AssemblyKind.MaskForLoad
                let codeAssemType = int ( useAssemType - AssemblyKind.ManagedDLL ) + int BlobKind.AssemblyManagedDLL
                ms.WriteVInt32( codeAssemType )
                ms.WriteString( assem.Name ) 
                ms.WriteBytesWVLen( assem.Hash )
                let blob = Blob( TypeOf = enum<BlobKind>codeAssemType, 
                                        Name = assem.Name, 
                                        Hash = assem.Hash,
                                        Object = assem, 
                                        Index = assemi )
                blobList.Add( blob ) 

            // Add File Dependencies
            ms.WriteString(x.JobDirectory)
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Packing Remote Execution Container with job directory as %s" x.JobDirectory ))

            ms.WriteVInt32( x.JobDependencies.Count )
            for filei=0 to x.JobDependencies.Count-1 do
                let jobDep = x.JobDependencies.[filei]
                ms.WriteString(jobDep.Name)
                ms.WriteBytesWVLen( jobDep.Hash)
                let blob = Blob( TypeOf = BlobKind.JobDependencyFile,
                                       Name = jobDep.Name,
                                       Hash = jobDep.Hash,
                                       Object = jobDep,
                                       Index = filei )
                blobList.Add( blob )

            // Add Environment Variables
            ms.WriteVInt32( x.JobEnvVars.Count )
            for vari=0 to x.JobEnvVars.Count-1 do
                ms.WriteString(fst x.JobEnvVars.[vari])
                ms.WriteString(snd x.JobEnvVars.[vari])

            // Add asm bindings
            x.JobAsmBinding |> ConfigurationUtils.PackAsmBinding ms

            // Send job port
            ms.WriteUInt16( x.JobPort )

            // Pack customzied function, if any


            /// Add all clusters information for the Job
            ms.WriteVInt32( x.Clusters.Count )
            for clusteri=0 to x.Clusters.Count-1 do
                let cluster = x.Clusters.[clusteri]
                ms.WriteVInt32( int BlobKind.ClusterMetaData )
                let blob = Blob( TypeOf = BlobKind.ClusterMetaData, 
                                        Name = cluster.Name, 
                                        Version = cluster.Version.Ticks, 
                                        Object = cluster, 
                                        Index = clusteri )
                x.EncodeToBlob( blob ) |> ignore
                ms.WriteString( cluster.Name )
                ms.WriteInt64( cluster.Version.Ticks )
                ms.WriteBytesWVLen( blob.Hash ) 
                blobList.Add( blob )
        
        else

            /// Add all clusters information for the Job
            ms.WriteVInt32( x.Clusters.Count )
            for clusteri=0 to x.Clusters.Count-1 do
                let cluster = x.Clusters.[clusteri]
                ms.WriteVInt32( int BlobKind.ClusterMetaData )
                let blob = Blob( TypeOf = BlobKind.ClusterMetaData, 
                                        Name = cluster.Name, 
                                        Version = cluster.Version.Ticks, 
                                        Object = cluster, 
                                        Index = clusteri )
                x.EncodeToBlob( blob ) |> ignore
                ms.WriteString( cluster.Name )
                ms.WriteInt64( cluster.Version.Ticks )
                ms.WriteBytesWVLen( blob.Hash ) 
                blobList.Add( blob )

            /// Add information of the source DSet
            ms.WriteVInt32( x.SrcDSet.Count)
            for dseti=0 to x.SrcDSet.Count-1 do
                let dset = x.SrcDSet.[dseti]
                let blob = dset.Blob
                blob.Index <- dseti 
                ms.WriteVInt32( int BlobKind.SrcDSetMetaData )
                ms.WriteStringV( dset.Name )
                ms.WriteInt64( dset.Version.Ticks )
                ms.WriteBytesWVLen( blob.Hash ) 
                blobList.Add( blob )
            /// Add information of the destination DSet       
            ms.WriteVInt32( x.DstDSet.Count )
            for dseti=0 to x.DstDSet.Count-1 do
                let dset = x.DstDSet.[dseti]
                let blob = dset.Blob
                blob.Index <- dseti
                ms.WriteVInt32( int BlobKind.DstDSetMetaData )
                ms.WriteBytesWVLen( blob.Hash ) 
                blobList.Add( blob )
            /// Add information of the passthrough DSet
            ms.WriteVInt32( x.PassthroughDSet.Count )
            for dseti=0 to x.PassthroughDSet.Count-1 do
                let dset = x.PassthroughDSet.[dseti]
                let blob = dset.Blob
                blob.Index <- dseti 
                ms.WriteVInt32( int BlobKind.PassthroughDSetMetaData )
                ms.WriteBytesWVLen( blob.Hash )
                blobList.Add( blob )
            /// Add information of the Streams
            ms.WriteVInt32( x.DStreams.Count )
            for streami = 0 to x.DStreams.Count-1 do
                let stream = x.DStreams.[streami]
                let blob = stream.Blob
                blob.Index <- streami
                ms.WriteVInt32( int BlobKind.DStream )
                ms.WriteBytesWVLen( blob.Hash )
                blobList.Add( blob )
        // Add node information related to job
        x.ClustersInfo <- List<_> ( x.Clusters.Count ) 
        for clusteri = 0 to x.Clusters.Count - 1 do 
            let cluster = x.Clusters.[clusteri]
            let clusterJobInfo = ClusterJobInfo( cluster.Name, cluster.Version, cluster.NumNodes )
            x.ClustersInfo.Add( clusterJobInfo ) 
            let blob = Blob( TypeOf = BlobKind.ClusterWithInJobInfo, 
                                   Name = x.Clusters.[clusteri].Name, 
                                   Version = x.Clusters.[clusteri].Version.Ticks, 
                                   Object = clusterJobInfo, 
                                   Index = clusteri ) 
            blobList.Add( blob ) 
            
        x.Blobs <- blobList |> Seq.toArray
        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "There are a total of %d blobs for Job %s:%s" x.NumBlobs x.Name x.VersionString ))
        // Resolve parameter, this must be done after x.Blobs has been populated. 
        x.ResolveParameter()
        // Parameter list
        ms.WriteVInt32( x.ParameterList.Count ) 
        for param in x.ParameterList do
            param.Pack( ms )
        // Mark the end 
        ms.WriteVInt32( int BlobKind.Null )
        ms.WriteString( "End-Job" )
        ms.WriteInt64( -1L )
    member val internal CustomizedFuncStream = null with get, set
    /// Peek Serialized Job, extract Name, Version, and Job ID 
    static member internal PeekJob( ms:Stream ) = 
        let pos = ms.Position 
        let name = ms.ReadString( ) 
        let verNumber = ms.ReadInt64()
        ms.ReadString() |> ignore 
        ms.ReadInt64( ) |> ignore 
        let jobID = ms.ReadGuid()
        ms.Seek( pos, SeekOrigin.Begin ) |> ignore 
        jobID, name, verNumber
    /// Deserialize Job description to Blob, 
    member x.UnpackToBlob( ms:StreamBase<byte> ) = 
        x.MetadataStream <- ms.Replicate()
        x.MetadataStream.Info <- sprintf "ReplicatedStream:%s" ms.Info
        let blobList = List<_>()
        x.Name <- ms.ReadString( ) 
        x.Version <- DateTime( ms.ReadInt64() )
        x.LaunchIDName <- ms.ReadString()
        x.LaunchIDVersion <- ms.ReadInt64( )
        x.JobID <- ms.ReadGuid()
        x.IsContainer <- ms.ReadBoolean()
        x.LaunchMode <- enum<_> (ms.ReadVInt32())
        if x.IsContainer then 
            x.TypeOf <- enum<_> ( ms.ReadVInt32() ) 

        // Information on the start serial number, which is used to identify jobs. 
        x.BlobStartSerial <- ms.ReadInt64()
        // Read blob sync method
        x.BlobSync <- enum<_> (ms.ReadVInt32())

        if x.IsContainer then 
            
            // # of assemblies. 
            let lenAssem = ms.ReadVInt32( )
    //        x.AssembliesArray <- Array.zeroCreate<_> lenAssem
            x.Assemblies.Clear()
            for assemi=0 to lenAssem-1 do
                x.Assemblies.Add(null)
                let blobtype = enum<_> (ms.ReadVInt32())
                let name = ms.ReadString()
                let hash = ms.ReadBytesWVLen()
                if blobtype<BlobKind.AssemblyManagedDLL || blobtype>BlobKind.GV then 
                    failwith ( sprintf "Expect Assemblies blob type, but get blob type %A" blobtype )
                let blob = Blob( TypeOf = blobtype, 
                                        Name = name, 
                                        Hash = hash, 
                                        Object = null, 
                                        Index = assemi )
                blobList.Add( blob ) 
            // Job Directory
            let jobDirectory = ms.ReadString()
            x.JobDirectory <- Path.Combine(DeploymentSettings.LocalFolder, DeploymentSettings.JobFolder + DeploymentSettings.ClientPort.ToString(), jobDirectory )
            x.RemoteMappingDirectory <- x.JobDirectory
            // # of file dependencies
            let lenFileDep = ms.ReadVInt32()
            
            for filei=0 to lenFileDep-1 do
                x.JobDependencies.Add(null)
                let name = ms.ReadString()
                let hash = ms.ReadBytesWVLen()
                let blob = Blob( TypeOf = BlobKind.JobDependencyFile,
                                       Name = name,
                                       Hash = hash,
                                       Object = null,
                                       Index = filei )
                blobList.Add( blob )
            // environment variables
            let lenEnvVar = ms.ReadVInt32()
            let evInfo = List<_>()
            for vari=0 to lenEnvVar-1 do
                let envvar = ms.ReadString()
                let value = ms.ReadString()

                if String.Compare( envvar, DeploymentSettings.EnvStringSetJobDirectory, StringComparison.OrdinalIgnoreCase )=0 then 
                    // Set Job Directory
                    let bIsRelative = FileTools.IsRelativePath value
                    if bIsRelative then 
                        x.JobDirectory <- Path.Combine( x.JobDirectory, value )
                    else
                        x.JobDirectory <- value
                else
                    evInfo.Add( (envvar, value ) )
    

            for ev in evInfo do
                let envvar, value = ev
    //  JinL: Failwith at remote node may not be a very good idea. Regex.Replace has issue, use a custom replacestring. 
    //            if (Text.RegularExpressions.Regex.Match(value, DeploymentSettings.EnvStringCurrentJobDirectory).Success &&
    //                Utils.IsNotNull (System.Environment.GetEnvironmentVariable(DeploymentSettings.EnvStringCurrentJobDirectory ))) then
    //                failwith "Perhaps ambiguous meaning of job directory"
                let result = ReplaceString value DeploymentSettings.EnvStringGetJobDirectory x.JobDirectory StringComparison.OrdinalIgnoreCase
                Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Set Environment Variable %s to %s [(%s)]" envvar result value))
                x.JobEnvVars.Add((envvar, result))
            
            x.JobAsmBinding <- ConfigurationUtils.UnpackAsmBinding ms
            
            Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Try to use directory %s for Job %s:%s" x.JobDirectory x.Name x.VersionString ))
            // Create Job Directory if not exist
            FileTools.DirectoryInfoCreateIfNotExists x.JobDirectory |> ignore 
            // # of clusters 

            // Read job port
            x.JobPort <- ms.ReadUInt16( )



        let blobIndexClusters = blobList.Count
        let lenCluster = ms.ReadVInt32( )
        x.Clusters <- List<_>( Array.zeroCreate lenCluster )
        for clusteri=0 to lenCluster-1 do
            let blobtype = enum<_> (ms.ReadVInt32())
            let name = ms.ReadString()
            let verNumber = ms.ReadInt64()
            let hash = ms.ReadBytesWVLen( ) 
            if blobtype<>BlobKind.ClusterMetaData then 
                failwith ( sprintf "Expect Cluster blob type, but get blob type %A" blobtype )
            let blob = Blob( TypeOf = blobtype, 
                                    Name = name, 
                                    Version = verNumber, 
                                    Hash = hash, 
                                    Object = null, 
                                    Index = clusteri )
            blobList.Add( blob )

        if not x.IsContainer then 
            // # of clusters 
            /// Add information of the source DSet
            let lenSrcDSet = ms.ReadVInt32()
            x.ClearSrcDSet()
            for dseti=0 to lenSrcDSet-1 do
                x.AddSrcDSet( null )
                let blobtype = enum<_> (ms.ReadVInt32())
                let name = ms.ReadStringV()
                let ticks = ms.ReadInt64()
                let hash = ms.ReadBytesWVLen()
                if blobtype<>BlobKind.SrcDSetMetaData then 
                    failwith ( sprintf "Expect SrcDSet blob type, but get blob type %A" blobtype )
                let blob = Blob( TypeOf = blobtype, 
                                        Hash = hash, 
                                        Name = name, 
                                        Version = ticks,
                                        Object = null, 
                                        Index = dseti )
                blobList.Add( blob )
            /// Add information of the destination DSet
            let lenDstDSet = ms.ReadVInt32()
            x.DstDSet.Clear()
            for dseti=0 to lenDstDSet-1 do
                x.DstDSet.Add( null )
                let blobtype = enum<_> (ms.ReadVInt32())
                let hash = ms.ReadBytesWVLen()
                if blobtype<>BlobKind.DstDSetMetaData then 
                    failwith ( sprintf "Expect DstDSet blob type, but get blob type %A" blobtype )
                let blob = Blob( TypeOf = blobtype, 
                                        Hash = hash,
                                        Object = null, 
                                        Index = dseti )
                blobList.Add( blob )
            /// Add information of the passthrough DSet
            let lenPassthroughDSet = ms.ReadVInt32()
            x.PassthroughDSet.Clear()
            for dseti=0 to lenPassthroughDSet-1 do
                x.PassthroughDSet.Add(null) 
                let blobtype = enum<_> (ms.ReadVInt32())
                let hash = ms.ReadBytesWVLen()
                if blobtype<>BlobKind.PassthroughDSetMetaData then 
                    failwith ( sprintf "Expect PassthroughDSet blob type, but get blob type %A" blobtype )
                let blob = Blob( TypeOf = blobtype, 
                                        Hash = hash, 
                                        Object = null, 
                                        Index = dseti )
                blobList.Add( blob )
            /// Add information on streams
            let lenStreams = ms.ReadVInt32()
            x.DStreams.Clear()
            for streami = 0 to lenStreams - 1 do
                x.DStreams.Add(null)
                let blobtype = enum<_> (ms.ReadVInt32())
                let hash = ms.ReadBytesWVLen()
                if blobtype<>BlobKind.DStream then 
                    failwith ( sprintf "Expect DStream blob type, but get blob type %A" blobtype )
                let blob = Blob( TypeOf = blobtype, 
                                        Hash = hash,
                                        Object = null, 
                                        Index = streami )
                blobList.Add( blob )
        // Add node information related to job
        x.ClustersInfo <- List<_> ( x.Clusters.Count ) 
        for clusteri = 0 to x.Clusters.Count - 1 do 
            // refering to an earlier cluster information. 
            let blobcluster = blobList.[ blobIndexClusters + clusteri ]
            let clusterJobInfo = null // ClusterJobInfo( blobcluster.Name, DateTime( blobcluster.Version), 0 )
            x.ClustersInfo.Add( clusterJobInfo ) 
            let blob = Blob( TypeOf = BlobKind.ClusterWithInJobInfo, 
                                   Name = blobcluster.Name, 
                                   Version = blobcluster.Version, 
                                   Object = clusterJobInfo, 
                                   Index = clusteri ) 
            blobList.Add( blob ) 
        // Parameter list
        let lenParameterLists = ms.ReadVInt32()
        x.ParameterList.Clear()
        for parami = 0 to lenParameterLists - 1 do
            let param = FunctionParam.Unpack( ms )
            x.ParameterList.Add( param )
        let blobtype = ms.ReadVInt32()
        let name = ms.ReadString()
        let verNumber = ms.ReadInt64()
        let bEnd = String.Compare( name, "End-Job")=0 && verNumber = -1L && enum<_>(blobtype)=BlobKind.Null
        /// The job is properly decoded. 
        if bEnd then 
            x.Blobs <- blobList |> Seq.toArray
        else
            x.TypeOf <- JobTaskKind.None
        bEnd
    member internal x.StreamForWrite( blob: Blob ) = 
        let stream = blob.StreamForWrite( -1L )
        stream

    /// Encode metadata/content to blob
    member internal x.EncodeToBlob( blob: Blob ) = 
        if not blob.IsAllocated then 
            // Create buffer if the blob has not been allocated. 
            match blob.TypeOf with 
            | BlobKind.ClusterMetaData ->
                let stream = x.StreamForWrite( blob ) 
                let cluster = blob.Object :?> Cluster
                cluster.ClusterInfo.Pack( stream ) 
                blob.Hash <- null // Force recalculation of hash
                blob.StreamToBlob( stream )
            | BlobKind.SrcDSetMetaData 
            | BlobKind.DstDSetMetaData
            | BlobKind.PassthroughDSetMetaData ->
                let stream = x.StreamForWrite( blob ) 
                let dset = blob.Object :?> DSet
                // Send password, if it is in DSet
                dset.Pack( stream, DSetMetadataStorageFlag.All, false ) 
                dset.Blob <- blob
                blob.StreamToBlob( stream ) 
                // This flag is encoded after the blob's hash being calculated,
                // so downstream dependencies would not affect the hash value
                dset.PackHasDownStreamDependencyFlag (stream)
            | BlobKind.DStream -> 
                let writeStream = x.StreamForWrite( blob ) 
                let dstream = blob.Object :?> DStream
                dstream.Pack( writeStream, DStreamMetadataStorageFlag.ALL )
                dstream.Blob <- blob
                blob.StreamToBlob( writeStream ) 
            | BlobKind.AssemblyManagedDLL 
            | BlobKind.AssemblyUnmanagedDir ->
                let stream = x.StreamForWrite( blob ) 
                let assem = blob.Object :?> AssemblyEx
                assem.Pack( stream ) 
                blob.Hash <- null // Force recalculation of hash
                blob.StreamToBlob( stream )
                let cmp = BytesCompare() :> IEqualityComparer<byte[]>
                if not (cmp.Equals( blob.Hash, assem.Hash )) then 
                    failwith (sprintf "Assembly hash %s doesn't match the hash generated by Prajna %s" 
                                        (BytesToHex(assem.Hash)) (BytesToHex(blob.Hash)) )

            | BlobKind.ClusterWithInJobInfo -> 
                let stream = x.StreamForWrite( blob ) 
                let clusterJobInfo = blob.Object :?> ClusterJobInfo
                clusterJobInfo.Pack( stream ) 
                blob.StreamToBlob( stream )
            | BlobKind.JobDependencyFile ->
                let stream = x.StreamForWrite( blob )
                let jobDep = blob.Object :?> JobDependency
                jobDep.Pack( stream )
                blob.Hash <- null // Force recalculation of hash
                blob.StreamToBlob( stream )
                let cmp = BytesCompare() :> IEqualityComparer<byte[]>
                if not (cmp.Equals( blob.Hash, jobDep.Hash )) then 
                    failwith (sprintf "Job dependency hash %s doesn't match the hash generated by Prajna %s" 
                                        (BytesToHex(jobDep.Hash)) (BytesToHex(blob.Hash)) )

            | _ ->
                let msg = sprintf "EncodeToBlob: Unknown Blob type %A" blob.TypeOf
                Logger.Log( LogLevel.Error, msg )
                failwith msg
        if blob.Stream.Position=blob.Stream.Length then 
            blob.Stream
        else
            // Form a properly formated stream
            blob.Stream.Seek( 0L, SeekOrigin.Begin ) |> ignore
            blob.Stream
    /// Release the memory of a particular blob
    member x.UnallocateBlob( blobi ) = 
        let blob = x.Blobs.[blobi]
        if blob.IsAllocated then 
            blob.Release()
//            blob.Buffer <- null
    /// Release the memory of all blobs
    member x.UnallocateAllBlobs( ) = 
        for blobi=0 to x.Blobs.Length - 1 do
            x.UnallocateBlob( blobi ) 
    /// When a blob is written, the blob name & version can change (especially for DSet). We thus need to update the associated blob information. 
    member x.UpdateBlobInfo( blobi ) = 
        let blob = x.Blobs.[blobi]
        if blob.IsAllocated then 
            match blob.TypeOf with 
            | BlobKind.SrcDSetMetaData 
            | BlobKind.DstDSetMetaData
            | BlobKind.PassthroughDSetMetaData ->
                if Utils.IsNotNull blob.Object then 
                    let dset = blob.Object :?> DSet
                    blob.Name <- dset.Name
                    blob.Version <- dset.Version.Ticks
                else
                    let stream = blob.StreamForRead()
                    let name, verNumber = DSet.Peek( stream )
                    blob.Name <- name
                    blob.Version <- verNumber
            | BlobKind.DStream -> 
                let dobjUpdate = 
                    if Utils.IsNotNull blob.Object then 
                        blob.Object :?> DistributedObject
                    else
                        let stream = blob.StreamForRead()
                        let dobj = DistributedObject() 
                        dobj.PeekBase( stream ) 
                        dobj
                blob.Name <- dobjUpdate.Name
                blob.Version <- dobjUpdate.Version.Ticks    
            | _ ->
                ()
    /// Decode from Blob
    /// For Assemblies, the received assemblies is put into file (directory), but is not loaded. 
    /// For Cluster, the received cluster is decoded, loaded into ClusterFactory, and saved (persisted)
    /// For DSet, the received DSet is decoded. It isn't loaded into DSetFactory. 
    ///     For DSet that needs special assemblies, it is important to call DecodeFromBlob only in AppDomain, as the necessary assemblies may not be loaded. 
    member x.DecodeFromBlob( blobi, peeri ) = 
        let blob = x.Blobs.[blobi]
        let mutable bDecodeSuccessful = true
        let stream, pos, streamlen = 
            if blob.IsAllocated then 
                // Create buffer if the blob has not been allocated. 
                let streamRead = blob.StreamForRead()
                streamRead, streamRead.Position, streamRead.Length
            else
                null, 0L, 0L
        try
            match blob.TypeOf with 
            | BlobKind.ClusterMetaData ->
                let clOpt = ClusterInfo.Unpack( stream )
                match clOpt with 
                | Some( clusterinfo ) ->
                    clusterinfo.Persist()
                    let cluster = ClusterFactory.FindCluster( clusterinfo.Name, clusterinfo.Version.Ticks )
                    blob.Object <- cluster
                    x.Clusters.[blob.Index] <- cluster
                | None ->
                    let msg() = sprintf "DecodeFromBlob: Failed to decode ClustermetaData %s:%d" blob.Name blob.Version
                    Logger.LogF(LogLevel.Info, msg)
                    bDecodeSuccessful <- false
            | BlobKind.SrcDSetMetaData 
            | BlobKind.DstDSetMetaData
            | BlobKind.PassthroughDSetMetaData ->
                let decodeDSet = DSet.Unpack( stream, true )
                // This should not be used for DSetPeer
                let dset = decodeDSet // DSetFactory.CacheDSet( decodeDSet )
                match blob.TypeOf with
                | BlobKind.SrcDSetMetaData ->
                    // Multiple peers may respond to SrcDSet request, and may not have the same metadata, 
                    // so we need to check for SrcDSet validity. 
                    x.SrcDSetNumResponded.[blob.Index] <- x.SrcDSetNumResponded.[blob.Index] + 1
                    let mutable bValidResponse = true
                    if Utils.IsNull dset.Cluster then bValidResponse <- false
                    if bValidResponse then 
                        if Utils.IsNotNull blob.Object then 
                            let curDSet = blob.Object :?> DSet
                            if dset.Cluster=curDSet.Cluster && 
                                dset.Version > curDSet.Version
                                || ( dset.Version=curDSet.Version && dset.MetaDataVersion > curDSet.MetaDataVersion ) then 
                                let bJustNewmetadataVersion = ( dset.Version=curDSet.Version && dset.MetaDataVersion > curDSet.MetaDataVersion ) 
                                if not curDSet.bValidMetadata then 
                                    curDSet.CopyMetaData( dset, DSetMetadataCopyFlag.Copy )
                                    if bJustNewmetadataVersion then 
                                        x.SrcDSetNumValidResponse.[blob.Index] <- x.SrcDSetNumValidResponse.[blob.Index] + 1
                                    else
                                        // mark all other peers metadata as outdated. 
                                        x.SrcDSetNumValidResponse.[blob.Index] <- 1
                                        for i=0 to x.OutgoingQueues.Count-1 do
                                            if i<>peeri then 
                                                x.AvailPeer.[i].AvailVector.[blobi] <- byte BlobStatus.Outdated
                                else
                                    bValidResponse <- false
                            else if dset.Cluster=curDSet.Cluster && dset.Version=curDSet.Version then 
                                x.SrcDSetNumValidResponse.[blob.Index] <- x.SrcDSetNumValidResponse.[blob.Index] + 1
                            else
                                bValidResponse <- false
                        else
                            blob.Object <- dset 
                            x.SrcDSet.[blob.Index] <- dset
                    bDecodeSuccessful <- bValidResponse
                    x.AvailPeer.[peeri].AvailVector.[blobi] <- byte (if bValidResponse then BlobStatus.AllAvailable else BlobStatus.Outdated)
                    x.AvailPeer.[peeri].CheckAllAvailable()
                | BlobKind.DstDSetMetaData ->
                    blob.Object <- dset 
                    x.DstDSet.[blob.Index] <- dset
                    bDecodeSuccessful <- Utils.IsNotNull dset
                | BlobKind.PassthroughDSetMetaData ->
                    blob.Object <- dset 
                    x.PassthroughDSet.[blob.Index] <- dset
                    bDecodeSuccessful <- Utils.IsNotNull dset
                | _ ->
                    ()
            | BlobKind.DStream ->
                let decodeDStream = DStream.Unpack( stream, true )
                // This should not be used for DSetPeer
                if Utils.IsNotNull decodeDStream then 
                    let useDStream = decodeDStream 
                    blob.Object <- useDStream
                    x.DStreams.[blob.Index] <- useDStream 
                    bDecodeSuccessful <- Utils.IsNotNull useDStream
                else
                    bDecodeSuccessful <- false
            | BlobKind.AssemblyManagedDLL 
            | BlobKind.AssemblyUnmanagedDir ->
                let assemTypeOf = enum<_>( int( blob.TypeOf - BlobKind.AssemblyManagedDLL ) + int AssemblyKind.ManagedDLL )
                let assem = AssemblyEx.GetAssembly( blob.Name, blob.Hash, assemTypeOf, true )
                assem.Unpack( stream )
                x.Assemblies.[blob.Index] <- assem
            | BlobKind.ClusterWithInJobInfo -> 
                let clusterJobInfo = ClusterJobInfo.Unpack( stream ) 
                clusterJobInfo.bValidMetadata <- clusterJobInfo.Validate( x.Clusters.[ blob.Index ] )
                Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Decode cluster job information success:%A \n %s" clusterJobInfo.bValidMetadata (clusterJobInfo.ToString()) ))
                if clusterJobInfo.bValidMetadata then 
                    x.ClustersInfo.[blob.Index] <- ClusterJobInfoFactory.CacheClusterJobInfo( x.SignatureName, x.SignatureVersion, clusterJobInfo ) 
                    x.ClustersInfo.[blob.Index].LinkedCluster <- x.Clusters.[ blob.Index ]
                    // Add DNS entries
                    let cluster = x.Clusters.[ blob.Index ]
                    for peeri = 0 to cluster.NumNodes - 1 do 
                        let cl = cluster.ClusterInfo.ListOfClients.[peeri]
                        let nodeInfo = clusterJobInfo.NodesInfo.[peeri]
                        if Utils.IsNotNull nodeInfo then 
                            LocalDNS.AddEntry( cl.MachineName, nodeInfo.IPAddresses )
                bDecodeSuccessful <- clusterJobInfo.bValidMetadata  
            | BlobKind.JobDependencyFile ->
                let jobDep = JobDependency.GetDependency(blob.Name, blob.Hash, true)
                jobDep.Unpack( stream )
                x.JobDependencies.[blob.Index] <- jobDep
            | _ ->
                let msg = sprintf "DecodeFromBlob: Unknown Blob type %A" blob.TypeOf
                Logger.Log( LogLevel.Error, msg )
                failwith msg
            stream.Seek( pos, SeekOrigin.Begin ) |> ignore // Reset the pointer in case the stream needs to be read again. 
            bDecodeSuccessful
        with
        | e -> 
            let msg = sprintf "DecodeFromBlob: blob %d (%s) failed to be decoded, blob stream is of length %dB, exception %A " blobi blob.Name streamlen e
            Logger.Log( LogLevel.Error, msg )
            failwith msg
            false
    /// Send blob to host, always use non hash version here.  
    member x.SendBlobToHost( queue:NetworkCommandQueue, blobi ) = 
        let blob = x.Blobs.[blobi]
        let stream = x.EncodeToBlob( blob ) 
        let buf, pos, count = stream.GetBufferPosLength()
        let msSend = new MemStream() 
        msSend.WriteGuid( x.JobID )
        msSend.WriteString( x.Name ) 
        msSend.WriteInt64( x.Version.Ticks ) 
        msSend.WriteVInt32( blobi ) 
        msSend.Append(buf, int64 pos, int64 count)
        //msSend.WriteBytesWithOffset( buf, pos, count ) 
        Logger.LogF( x.JobID, LogLevel.MediumVerbose, ( fun _ ->  let blob = x.Blobs.[blobi]
                                                                  sprintf "Write, Blob %d type %A, name %s to %s" 
                                                                             blobi blob.TypeOf blob.Name (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) ))
        queue.ToSend( ControllerCommand( ControllerVerb.Write, ControllerNoun.Blob), msSend )
        msSend.DecRef()
    /// Send blob to peer 
    member x.SendBlobToJob( queue:NetworkCommandQueue, blobi ) = 
        let blob = x.Blobs.[blobi]
        let stream = x.EncodeToBlob( blob )
        match blob.TypeOf with 
        |  BlobKind.ClusterWithInJobInfo -> 
            Logger.LogF( x.JobID, DeploymentSettings.TraceLevelBlobIO, ( fun _ -> sprintf "send Write, Blob of blob %d type %A, name %s to program %s, pos %d, count %d length %d" 
                                                                                               blobi blob.TypeOf blob.Name (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) 
                                                                                               stream.Position (stream.Length-stream.Position)
                                                                                               stream.Length ))
            queue.ToSend( ControllerCommand( ControllerVerb.Write, ControllerNoun.Blob), stream )
            stream.DecRef()
        |  _ -> 
            Logger.LogF( x.JobID, DeploymentSettings.TraceLevelBlobIO, ( fun _ -> sprintf "send Set, Blob of blob %d type %A to program %s, pos %d, count %d length %d" 
                                                                                       blobi blob.TypeOf (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) 
                                                                                       stream.Position (stream.Length-stream.Position)
                                                                                       stream.Length ))
            queue.ToSend( ControllerCommand( ControllerVerb.Set, ControllerNoun.Blob), stream )
            stream.DecRef()
    /// Send blob to peer i, used by client
    member x.SendBlobPeeri (peeri:int) (queue:NetworkCommandQueue) blobi = 
        // Do we need to send different function to each peer? 
        let blob = x.Blobs.[blobi]
        let stream = x.EncodeToBlob( blob )
        let buf, pos, count = stream.GetBufferPosLength()
        let msSend = new MemStream( count + 1024 )
        match blob.TypeOf with 
        | BlobKind.ClusterWithInJobInfo -> 
            // Hash not known during job creation time. 
            msSend.WriteGuid( x.JobID )
            msSend.WriteString( x.Name ) 
            msSend.WriteInt64( x.Version.Ticks ) 
            msSend.WriteVInt32( blobi )
            msSend.Append(buf, int64 pos, int64 count)
            //msSend.WriteBytesWithOffset( buf, pos, count )
            Logger.LogF( DeploymentSettings.TraceLevelBlobIO, ( fun _ -> sprintf "Write, Blob to peer %s blob %d type %A, name %s length %d for job %s:%s (blob %d)" 
                                                                          (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) blobi blob.TypeOf blob.Name msSend.Length
                                                                          x.Name x.VersionString blobi ))
            queue.ToSend( ControllerCommand( ControllerVerb.Write, ControllerNoun.Blob), msSend )
            msSend
        | _ -> 
            /// Hashed blob doesn't have associated job name & ticks information, as they may be reused across jobs. 
            //msSend.WriteBytesWithOffset( buf, pos, count )
            msSend.Append(buf, int64 pos, int64 count)
            Logger.Do( DeploymentSettings.TraceLevelBlobValidateHash, ( fun _ -> 
                //let vHash = HashByteArrayWithLength( buf, pos, count ) 
                let vHash = blob.GetHashForBlobType ( buf, pos, count )
                let cmp = BytesCompare() :> IEqualityComparer<_>
                if not(cmp.Equals( vHash, blob.Hash )) then 
                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Set Blob %d type %A, name %s length %d (hash %d, %s) does not match blobhash %s" 
                                                                           blobi blob.TypeOf blob.Name msSend.Length
                                                                           count 
                                                                           (BytesToHex(vHash))
                                                                           (BytesToHex(blob.Hash)) ))
                else
                    Logger.LogF( DeploymentSettings.TraceLevelBlobIO, ( fun _ -> sprintf "Set Blob to peer %s blob %d type %A, name %s length %d (hash %s)" 
                                                                                  (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) blobi blob.TypeOf blob.Name msSend.Length
                                                                                  (BytesToHex(blob.Hash)) ))
                ))
            queue.ToSend( ControllerCommand( ControllerVerb.Set, ControllerNoun.Blob), msSend )
            msSend

    /// Set metadata availability fo a DSet according to job availability. 
    member x.SetMetaDataAvailability( curDSet: DSet ) = 
        // Find all source DSet
        let curCluster = curDSet.Cluster
        let traverse = JobTraverse() 
        let srcDSets = traverse.GetDSetChain( curDSet, TraverseUpstream, DSetChainFlag.Source )
        // Assume all bDSetMetaData is read, and no peer is failed, flip flag otherwise later. 
        let bDSetMetaRead = Array.create curCluster.NumNodes true
        let bPeerFailed = Array.create curCluster.NumNodes false
        
        for srcDSet in srcDSets do 
            if srcDSet.Cluster <> curCluster then 
                // If dset lies in another cluster, we assume that each peer is available, as reliability should be provided between cluster. 
                ()
            else
                // TODO: the code will need to be modified if the partition relationship in dependency has other relationship. 
                let findindex = ref -1
                for blobi = 0 to x.NumBlobs - 1 do 
                    let blob = x.Blobs.[blobi] 
                    if System.Object.ReferenceEquals( blob.Object, srcDSet ) then 
                        findindex := blobi
                if !findindex = -1 then 
                    let msg = sprintf "Job.SetMetaDataAvailability: Job %s:%s Can't find DSet %s:%s in the current job" x.Name x.VersionString curDSet.Name curDSet.VersionString
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg
                else
                    let blobi = !findindex
                    for i = 0 to curDSet.Cluster.NumNodes - 1 do
                        // Convert peer number in a cluster to peeri
                        let queue = curCluster.Queue( i )
                        if Utils.IsNull queue || queue.HasFailed || not (x.OutgoingQueuesToPeerNumber.ContainsKey(queue)) then 
                            bDSetMetaRead.[i] <- false
                            bPeerFailed.[i] <- true
                        else
                            let peeri = x.OutgoingQueuesToPeerNumber.Item(queue)
                            let status = LanguagePrimitives.EnumOfValue ( x.AvailPeer.[peeri].AvailVector.[blobi] )
                            match status with 
                            | BlobStatus.DependentNeeded 
                            | BlobStatus.Initial ->
                                bDSetMetaRead.[i] <- false
                            | BlobStatus.AllAvailable 
                            | BlobStatus.LatestVersion -> 
                                () 
                            | BlobStatus.NotAvailable
                            | BlobStatus.PartialAvailable 
                            | BlobStatus.Outdated
                            | BlobStatus.Error
                            | _ ->
                                bDSetMetaRead.[i] <- false
                                bPeerFailed.[i] <- true   
        ( bDSetMetaRead, bPeerFailed )
    static member val StartJob = null with get, set
    static member val StartedClusters = ConcurrentDictionary<_,_>(StringTComparer<int64>(StringComparer.Ordinal)) with get, set

    /// Prepare Job Metadata to be sent to other peers. 
    member x.PrepareMetaData() = 
        let curJob = JobDependencies.Current
        // Debugging, try to see if new objects gets populated
//        Logger.LogF( LogLevel.WildVerbose,  fun _ -> x.AllDObjectsInfo()  )
//        x.AddDependencies()
//        Logger.LogF( LogLevel.WildVerbose,  fun _ -> x.AllDObjectsInfo()  )
        x.AddDependencies()
        Logger.Do( LogLevel.MildVerbose, ( fun _ -> x.ShowAllDObjectsInfo() ))
        let mutable bSuccess = x.PrecodeDSets()
        Logger.LogF( x.JobID, 
                     LogLevel.MildVerbose, ( fun _ -> let t1 = (PerfADateTime.UtcNowTicks())
                                                      sprintf "Job %s, Precoded DSet in %.2f ms"
                                                               x.Name
                                                               (float (t1 - x.JobStartTicks) / float TimeSpan.TicksPerMillisecond) ))
        if bSuccess then 
            bSuccess <- 
        /// Start cluster, if needed 
//                for cluster in x.Clusters do 
//                    let tuple = new ManualResetEvent(false), ref true
//                    let tupleReturn = Job.StartedClusters.GetOrAdd( (cluster.Name, cluster.Version.Ticks), tuple )
//                    if Object.ReferenceEquals( tuple, tupleReturn ) then 
//                        // Initialize the cluster
//                        let startJob = Job( TypeOf=(JobTaskKind.Computation|||JobDependencies.DefaultTypeOfJobMask), Name = "StartContainer", Version = (PerfADateTime.UtcNow()), IsJobHolder = true)
//                        ()
                true
        if bSuccess then 
            // add job dependencies first as they may already include referenced assemblies
            if not x.IsContainer then 
                match x.LaunchMode with 
                | TaskLaunchMode.DonotLaunch -> 
                    // No need to calculate job signature if we only need to kill a job
                    x.SetLaunchIDName()
                | _ -> 
                    // Get signature of the current job 
                    if x.NoSignatureGenerated() then 
                        x.PopulateLoadedAssems()
                        x.GenerateSignature( curJob.JobDependencySHA256() )
                    else
                        x.PopulateLoadedAssems()
                        x.GenerateSignature( null )
                    /// Create a job to launch a container. 
                    let y = Job.ContainerJobCollection.GetOrAdd( x.AssemblyHash, fun _ -> let containerJob = ContainerJob( )
                                                                                          containerJob
                                                               )
                    y.PrepareRemoteExecutionRoster( curJob, x )
                    bSuccess <- y.ReadyStatus
        bSuccess      
    member val private nRefCreateRemoteExecutionRoster = ref 0 with get
    member val private evCreateRemoteExecutionRoster = new ManualResetEvent(false) with get
    /// Prepare a remote execution roster. 
    member internal x.PrepareRemoteExecutionRoster( curJob: JobDependencies, srcJob:Job ) = 
        let mutable bDepChange = true
        while bDepChange do 
            let depChangedTo = !curJob.nDependencyChanged
            let oldValue = !x.nRefCreateRemoteExecutionRoster
            bDepChange <- ( depChangedTo > oldValue )
            if bDepChange then 
                // Some process need to wait 
                x.evCreateRemoteExecutionRoster.Reset() |> ignore // we will need to wait 
                if Interlocked.CompareExchange( x.nRefCreateRemoteExecutionRoster, depChangedTo, oldValue) = oldValue then 
                    /// We have the lock in switching 
                    x.PrepareRemoteExecutionRosterOnce( curJob, srcJob )
                    x.evCreateRemoteExecutionRoster.Set() |> ignore
        x.evCreateRemoteExecutionRoster.WaitOne() |> ignore 
    /// Prepare a remote execution roster. 
    member private x.PrepareRemoteExecutionRosterOnce( curJob, srcJob ) = 
        // Reform job signature 
        x.Assemblies <- srcJob.Assemblies
        x.LaunchMode <- srcJob.LaunchMode
        x.JobID <- Guid.NewGuid()
        x.Name <- "ContainerJob"
        x.Version <- (PerfADateTime.UtcNow())
        x.TypeOf <- srcJob.TypeOf
        x.AssemblyHash <- srcJob.AssemblyHash
        x.LaunchIDName <- srcJob.LaunchIDName
        x.LaunchIDVersion <- srcJob.LaunchIDVersion
        // Launch job in all the srcJob clusters. 
        x.Clusters.Clear() |> ignore 
        x.Clusters.AddRange( srcJob.Clusters )
        x.JobPort <- srcJob.JobPort
        if (srcJob.UseGlobalJobDependencies) then
            x.JobDependencies.Clear()
            for depi = 0 to curJob.FileDependencies.Count-1 do
                x.JobDependencies.Add(curJob.FileDependencies.[depi])
            x.JobEnvVars.Clear()
            for vari = 0 to curJob.EnvVars.Count-1 do
                x.JobEnvVars.Add(curJob.EnvVars.[vari])

            // Get the current exe's asm binding information
            x.JobAsmBinding <- ConfigurationUtils.GetAssemblyBindingsForCurrentExe()

            // JinL: This logic of computing Job Directory needs to change & include assemblies. 
            // add remaining assemblies that are not in job dependencies
            let jobDirectory = if StringTools.IsNullOrEmpty curJob.JobDirectory then 
                                    Path.GetFileNameWithoutExtension( System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName )
                                else
                                    curJob.JobDirectory                                   
            x.JobDirectory <- Path.Combine(jobDirectory, BytesToHex(x.AssemblyHash))
            x.JobDependecyObjec <- curJob
            let bContainerReady = x.Ready() 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  let t1 = (PerfADateTime.UtcNowTicks())
                                                           sprintf "With Job dependencies, Remote Job Directory is ..... %s, Job dependencies handled & remote container launched in %.2f ms and ready is %A"
                                                                   x.JobDirectory 
                                                                   (float (t1 - x.JobStartTicks) / float TimeSpan.TicksPerMillisecond) 
                                                                   bContainerReady ))
    /// Initialize the avaliablity vector
    member x.InitializeAvailability() = 
        // current peer availability vector 
        availThis <- BlobAvailability( x.NumBlobs )
        // Initialize availability vector 
        for blobi = 0 to x.NumBlobs - 1 do
            availThis.AvailVector.[blobi] <- byte BlobStatus.Error
        // Check Src metadata availability 
        bAllSrcAvailable <- true
        for blobi = 0 to x.NumBlobs - 1 do 
            let blob = x.Blobs.[blobi]
            match blob.TypeOf with 
            | BlobKind.ClusterMetaData 
            | BlobKind.AssemblyManagedDLL 
            | BlobKind.AssemblyUnmanagedDir
            | BlobKind.JobDependencyFile ->
                availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
            | BlobKind.SrcDSetMetaData ->
                let dset = x.Blobs.[blobi].Object :?> DSet
                if dset.bValidMetadata then 
                    availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                else
                    availThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable
                    bAllSrcAvailable <- false
            | BlobKind.ClusterWithInJobInfo -> 
                availThis.AvailVector.[blobi] <- byte BlobStatus.DependentNeeded
            | _ ->
                ()
//        x.CalculateDependantDSet() |> ignore
        x.UpdateMetadata() |> ignore
        for blobi = 0 to x.NumBlobs - 1 do 
            let blob = x.Blobs.[blobi]
            match blob.TypeOf with 
            | BlobKind.PassthroughDSetMetaData 
            | BlobKind.DstDSetMetaData -> 
                let dset = x.Blobs.[blobi].Object :?> DSet
                if dset.bValidMetadata then 
                    availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                else
                    availThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable
                    bAllSrcAvailable <- false
            | BlobKind.DStream -> 
                let dstream = x.Blobs.[blobi].Object :?> DStream
                if dstream.bValidMetadata then 
                    availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                else
                    availThis.AvailVector.[blobi] <- byte BlobStatus.NotAvailable
                    bAllSrcAvailable <- false
            | _ ->
                ()

    /// Try to Update node information within Job
    member x.UpdateClusterJobInfo() = 
        for clusteri = 0 to x.Clusters.Count - 1 do 
            let mutable bNewClusterJobInfoAvailable = false
            let clusterJobInfo = x.ClustersInfo.[clusteri]
            if not clusterJobInfo.bValidMetadata then 
                let mutable bClusterJobInfoAvailable = true
                // Not all Node Job Information is available
                let cluster = x.Clusters.[clusteri]
                for peeri = 0 to cluster.NumNodes - 1 do
                    let queue = cluster.Queue(peeri)
                    if Utils.IsNotNull queue && not queue.Shutdown then 
                        let refValue = ref Unchecked.defaultof<_>
                        if x.NodeInfo.TryGetValue( queue.RemoteEndPointSignature, refValue ) then 
                            clusterJobInfo.NodesInfo.[peeri] <- !refValue
                        else
                            let node = cluster.ClusterInfo.ListOfClients.[peeri]
                            let nodeInfo = NodeConnectionFactory.Current.ResolveNodeInfo( node.MachineName, node.MachinePort, x.LaunchIDName, x.LaunchIDVersion ) 
                            if Utils.IsNull nodeInfo then 
                                match x.LaunchMode with 
                                | TaskLaunchMode.DonotLaunch -> 
                                    // If DonotLaunch flag is marked, there is no need to wait for the NodeInfo
                                    ()
                                | _ -> 
                                    // Waiting for job information. 
                                    let machineStore = NodeConnectionFactory.Current.Resolve( (node.MachineName, node.MachinePort) )
                                    if not (Utils.IsNull machineStore) then 
                                        bClusterJobInfoAvailable <- false
                            else
                                clusterJobInfo.NodesInfo.[peeri] <- nodeInfo
                    else
                        // queue is either null or queue is shutdown. 
//
//                        if Utils.IsNotNull queue then 
                            // queue shutdown marked 
                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Peer %d is considered as failed without JobInfo filled as it is %s" peeri (if Utils.IsNull queue then "unconnected" else "shutdown") ))
                            clusterJobInfo.NodesInfo.[peeri] <- null
                clusterJobInfo.bValidMetadata <- bClusterJobInfoAvailable
                if bClusterJobInfoAvailable then 
                    bNewClusterJobInfoAvailable <- true
                    // Transition Once, set availability tab
                    for blobi = 0 to x.Blobs.Length - 1 do 
                        let blob = x.Blobs.[blobi]
                        if Object.ReferenceEquals( blob.Object, clusterJobInfo ) then 
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                    x.AvailThis.CheckAllAvailable()

    /// Calculate dependent DSet & mark availability. 
    /// Return: true: if DSet availability has changed, false: no change
    member x.CalculateDependantDSet() = 
        let mutable bReturn = false
        if not bAllSrcAvailable then 
            // Some source is not available, marked the dependent DSet as unavailable as well. 
            // This is the call of CalculateDependantDSet before source DSet metadata is retrieved from cluster
            for blobi = 0 to x.NumBlobs - 1 do 
                let blob = x.Blobs.[blobi]
                match blob.TypeOf with 
                | BlobKind.DstDSetMetaData 
                | BlobKind.PassthroughDSetMetaData ->
                    // Needed SrcDSet to execute
                    let dset = x.Blobs.[blobi].Object :?> DSet
                    if dset.bValidMetadata then 
                        availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                    else
                        availThis.AvailVector.[blobi] <- byte BlobStatus.DependentNeeded
                | _ ->
                    ()

            // Any error for metadata setting?
            for blobi = 0 to x.NumBlobs - 1 do 
                if availThis.AvailVector.[blobi]=byte BlobStatus.Error || 
                    availThis.AvailVector.[blobi]=byte BlobStatus.Outdated then 
                    let msg = sprintf "Error or Outdated DSet, Job.CalculateDependantDSet for blob %d with type %A" blobi x.Blobs.[blobi].TypeOf
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg
            bReturn <- true
        else
            // All source is available. 
            // This is the call of CalculateDependantDSet after source DSet metadata is retrieved
            for blobi = 0 to x.NumBlobs - 1 do 
                let blob = x.Blobs.[blobi]
                match blob.TypeOf with 
                | BlobKind.DstDSetMetaData 
                | BlobKind.PassthroughDSetMetaData ->
                    // Needed SrcDSet to execute
                    let dset = x.Blobs.[blobi].Object :?> DSet
                    if dset.bValidMetadata then 
                        availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                    else
                        dset.Initialize()
                        availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                        bReturn <- true
                | _ ->
                    ()
        availThis.CheckAllAvailable()
        bReturn
    member x.PropagateOneMetadata (curObj:DistributedObject) (bAllDependentAvailable) = 
        if bAllDependentAvailable then 
            match curObj with 
            | :? DSet as dset -> 
                match dset.Dependency with 
                | StandAlone
                | Source 
                | DecodeFrom _ -> 
                    // No need to propagate metadata here
                    () 
                | MixFrom parent
                | WildMixFrom (parent, _ )
                | Passthrough parent 
                | Bypass (parent, _ ) 
                | HashJoinFrom (parent, _ )
                | CrossJoinFrom( parent, _ ) -> 
                    dset.ReplicateBaseMetadata( parent.TargetDSet )
                    dset.bValidMetadata <- true
                | CorrelatedMixFrom parents 
                | UnionFrom parents ->
                    let parent = parents.[0]
                    dset.ReplicateBaseMetadata( parent.TargetDSet )
                    dset.bValidMetadata <- true
            | :? DStream as dstream -> 
                match dstream.Dependency with 
                | SourceStream -> 
                    ()
                | SaveFrom parent 
                | EncodeFrom parent -> 
                    dstream.ReplicateBaseMetadata( parent.Target ) 
                    dstream.bValidMetadata <- true
                | PassFrom parentStream 
                | ReceiveFromNetwork parentStream 
                | MulticastFromNetwork parentStream -> 
                    dstream.ReplicateBaseMetadata( parentStream.Target ) 
                    dstream.bValidMetadata <- true
            | _ ->         
                Logger.Fail(sprintf "PropagateOneMetadata: Unknown DObject type %A (%s:%s) to propagate metadata" curObj.ParamType curObj.Name curObj.VersionString )
        for blobi=0 to x.Blobs.Length - 1 do 
            let blob = x.Blobs.[blobi] 
            if Object.ReferenceEquals( blob.Object, curObj ) then 
                // Find object
                if bAllDependentAvailable then 
                    availThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                else
                    availThis.AvailVector.[blobi] <- byte BlobStatus.DependentNeeded
    
    /// Propagate metadata
    /// Return:
    ///     true: all metadata becomes available
    ///     false: some metadata is still not available. 
    member x.PropagateMetadata() = 
        let tra = JobTraveseFromSource() 
        tra.AddToFutureExamine( x.SrcDSet |> Seq.map ( fun dset -> dset :> DistributedObject ) )      
        // Add as source those DSet with Source Depedency type, and mark them as having valid metadata. 
        tra.AddToFutureExamine( x.PassthroughDSet |> Seq.choose ( fun dset -> if dset.Dependency = Source then 
                                                                                dset.bValidMetadata <- true
                                                                                Some ( dset :> DistributedObject ) 
                                                                              else 
                                                                                None ) )      
        tra.Traverse( x.PropagateOneMetadata )
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Propagate Metadata for Job %s .... %s%s" x.Name Environment.NewLine (tra.Status()) ))
        availThis.CheckAllAvailable()
        tra.Unresolved.Count = 0 && tra.FutureExamine.Count = 0 
    /// Update metadata
    /// Return: 
    ///     true:  all metadata becomes available
    ///     false: some metadata is still not available. 
    member x.UpdateMetadata() = 
        x.PropagateMetadata() 

    /// Calculate dependent DSet & mark availability. 
    member x.UpdateSourceAvailability() = 
        if bFailureToGetSrcMetadata then 
            ()
        else
            if not bAllSrcAvailable then 
                let mutable bAllAvaialble = true
                // Examine Src DSet availability. 
                for blobi = 0 to x.NumBlobs - 1 do 
                    let blob = x.Blobs.[blobi]
                    match blob.TypeOf with 
                    | BlobKind.SrcDSetMetaData ->
                        if Utils.IsNull blob.Object then 
                            bAllAvaialble <- false
                        else
                            let curDSet = blob.Object :?> DSet
                            if not curDSet.bValidMetadata && x.AvailThis.AvailVector.[blobi]<>byte BlobStatus.Outdated then 
                                let index = blob.Index
                                let numRequiredPeerRespond = curDSet.RequiredNodes( curDSet.MinNodeResponded )
                                let numRequiredVlidResponse = curDSet.RequiredNodes( curDSet.MinValidResponded )
                                if x.SrcDSetNumResponded.[index] >= numRequiredPeerRespond && 
                                    x.SrcDSetNumValidResponse.[index] >=  numRequiredVlidResponse then 
                                    curDSet.bValidMetadata <- true
                                    x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.LatestVersion
                                else if x.SrcDSetNumResponded.[index]>=curDSet.Cluster.NumNodes || 
                                    x.SrcDSetNumValidResponse.[index] + ( curDSet.Cluster.NumNodes - x.SrcDSetNumResponded.[index] ) < 
                                        numRequiredVlidResponse then 
                                    x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.Outdated
                                    bFailureToGetSrcMetadata <- true
                                    bAllAvaialble <- false
                                else
                                    bAllAvaialble <- false
                            else 
                                if x.AvailThis.AvailVector.[blobi]=byte BlobStatus.Outdated then 
                                    bFailureToGetSrcMetadata <- true    
                    | _ ->
                        ()
                bAllSrcAvailable <- bAllAvaialble
                if bAllSrcAvailable then 
                    Logger.LogF(DeploymentSettings.TraceLevelBlobRcvd, ( fun _ -> sprintf "Job %s:%s all local metadata available .... %A" x.Name x.VersionString x.AvailThis ))
                    // let bReturn = x.CalculateDependantDSet()
                    let bReturn = x.UpdateMetadata() 
                    if bReturn then 
                        for peeri=0 to x.OutgoingQueues.Count-1 do 
                            x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] &&& (~~~ PerQueueJobStatus.AvailabilitySent)
                    x.CheckSync()
            else
                // Add CheckSync() to Check on peer failures. 
                x.CheckSync()

    member x.BeginSendMetaData() = 
//        x.OutgoingQueueStatuses <- x.OutgoingQueues |> Seq.map ( fun queue -> 
//                                                    if Utils.IsNotNull queue && queue.CanSend 
//                                                        then PerQueueJobStatus.Connected 
//                                                        else PerQueueJobStatus.NotConnected ) |> List<_>.AddRange
        jobMetadataStream <- new MemStream( 4096 ) 
        x.Pack( jobMetadataStream ) 
        for cluster in x.Clusters do 
            cluster.RegisterCallback( x.JobID, x.Name, x.Version.Ticks, 
                [| ControllerCommand( ControllerVerb.Set, ControllerNoun.Metadata ); 
                   ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Job ); 
                   ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Blob ) |], 
                { new NetworkCommandCallback with 
                    member this.Callback( cmd, peeri, ms, jobID, name, verNumber, cl ) = 
                        x.JobCallback( cmd, peeri, ms, jobID, name, verNumber, cl )
                } )
    member val private nExecutorForDisposer = ref 0 with get 
    /// Free all resource related to the Job
    member x.FreeJobResource() = 
        if Interlocked.Increment( x.nExecutorForDisposer )= 1 then 
            x.UnallocateAllBlobs()
            if Utils.IsNotNull jobMetadataStream then 
                jobMetadataStream.Dispose()
                jobMetadataStream <- null 
            for cluster in x.Clusters do 
                cluster.UnRegisterCallback( x.JobID, [| ControllerCommand( ControllerVerb.Set, ControllerNoun.Metadata ); 
                                                           ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Job ); 
                                                           ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Blob ) |] )
    /// For those source metadata that needs to be read, set their version to minimum value to trigger receiving latest version 
    member x.PrepareToReceiveSourceMetadata() = 
        for dset in x.SrcDSet do 
            if not dset.bValidMetadata then 
                dset.Version <- DateTime.MinValue
        ()
    member val private CheckSyncTimer = (PerfDateTime.UtcNow()) with get, set
    member x.CheckSync() = 
        if not bAllSynced then 
            let mutable bAllAvailable = x.AvailThis.AllAvailable
            if bAllAvailable then 
                let unavailLst = List<_>()
                for peeri = 0 to x.OutgoingQueues.Count - 1 do 
                    let queue = x.OutgoingQueues.[peeri] 
                    if not (NetworkCommandQueue.QueueFail(queue)) then 
                        if (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.FailedToStart)<>PerQueueJobStatus.None then 
                            match x.LaunchMode with 
                            | TaskLaunchMode.DonotLaunch -> 
                                // Mark peer as all available. 
                                x.AvailPeer.[peeri].AllAvailable <- true
                            | _ -> 
                                // Mark peer as fails. 
                                ()
                        else
                        // Peer not failed, for peer failed, no need to check its sync status 
                            if not x.AvailPeer.[peeri].AllAvailable then 
                                bAllAvailable <- false  
                                unavailLst.Add( peeri ) 
                if not bAllAvailable then 
                    for peeri in unavailLst do 
                        Logger.LogF(DeploymentSettings.TraceLevelBlobAvailability, ( fun _ -> sprintf "CheckSync: peer %d is not all available .... %A " peeri x.AvailPeer.[peeri] ))
            else
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "CheckSync: current app doesnot has all blobs .... %A " x.AvailThis )                            )
            bAllSynced <- bAllAvailable
            Logger.Do( LogLevel.MildVerbose, ( fun _ ->    let curTime = (PerfDateTime.UtcNow())
                                                           if curTime.Subtract( x.CheckSyncTimer ).TotalSeconds > DeploymentSettings.CheckSyncStatusInterval then 
                                                               x.CheckSyncTimer <- curTime
                                                               let curStatus = if x.AvailThis.AllAvailable then "Client Available" else "Client unavailable"
                                                               let peerStatus = ref ""
                                                               let peerInfo = ref ""
                                                               for peeri = 0 to x.OutgoingQueues.Count - 1 do 
                                                                   let queue = x.OutgoingQueues.[peeri] 
                                                                   let info = LocalDNS.GetShowInfo( queue.RemoteEndPoint ) 
                                                                   if not (NetworkCommandQueue.QueueFail(queue)) then 
                                                                       if not x.AvailPeer.[peeri].AllAvailable then
                                                                           peerInfo := ""
                                                                           for blobi = 0 to x.Blobs.Length - 1 do 
                                                                               let blobstatus = x.AvailPeer.[peeri].AvailVector.[blobi]
                                                                               if blobstatus <> byte BlobStatus.AllAvailable then 
                                                                                   peerInfo := !peerInfo + ( sprintf " %A %s:%s" x.Blobs.[blobi].TypeOf x.Blobs.[blobi].Name (BlobAvailability.StatusString( blobstatus )) )
                                                                           peerInfo := !peerInfo + "(no)" + Environment.NewLine
                                                                       else
                                                                           peerInfo := "yes"
                                                                   else
                                                                       peerInfo := "failed"
                                                                   peerStatus := !peerStatus + " " + info + ":" + !peerInfo
                                                               Logger.LogF( x.JobID, LogLevel.MediumVerbose, ( fun _ -> sprintf "%s, %s" curStatus (!peerStatus) ))
                                               ))

    member x.TrySyncMetaDataHost() = 
        let jobStage = JobStage.None
        let bIOActivity = ref false
        let clock_start = clock.ElapsedTicks
        let maxWait = ref (clock_start + clockFrequency * DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit)

        using ( x.TryExecuteSingleJobActionFunc()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                failwith "Fail to secure job Action object, job already cancelled?"
            else
                try 
                    while not bAllSynced && clock.ElapsedTicks < (!maxWait) && not bFailureToGetSrcMetadata && not jobAction.IsCancelledAndThrow do 
                        bIOActivity := false
                        // Process feedback

                        let mutable outstandingSendingQueue = 0
                        for peeri=0 to x.OutgoingQueues.Count-1 do 
                            let queue = x.OutgoingQueues.[peeri]
                            if Utils.IsNotNull queue && not queue.Shutdown then 
                                outstandingSendingQueue <- outstandingSendingQueue + int queue.UnProcessedCmdInBytes
                        for peeri=0 to x.OutgoingQueues.Count-1 do 
                            let queue = x.OutgoingQueues.[peeri]
                            if Utils.IsNotNull queue && queue.CanSend && not jobAction.IsCancelledAndThrow then 
                                if (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.JobMetaDataSent)=PerQueueJobStatus.None then 
                                    // Set, Job
                                    queue.ToSend( ControllerCommand( ControllerVerb.Set, ControllerNoun.Job ), jobMetadataStream )
                                    Logger.LogF( x.JobID, DeploymentSettings.TraceLevelBlobSend, ( fun _ -> sprintf "Job: %s:%s Send Set, Job to peer %d " x.Name x.VersionString peeri))
                                    x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.JobMetaDataSent
                                    bIOActivity := true
                                if (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.AvailabilitySent)=PerQueueJobStatus.None then 
                                    // Availability, Blob
                                    Logger.LogF( x.JobID, DeploymentSettings.TraceLevelBlobSend, ( fun _ -> sprintf "Job: %s:%s Send Availability, Blob to peer %d " x.Name x.VersionString peeri))
                                    // Send out job metadata & availability information. 
                                    // Move Availability stream out, to make sure that sourceDSet is received from every peer. 
                                    using( new MemStream( 1024 ) ) ( fun availStream -> 
                                        availStream.WriteGuid( x.JobID )
                                        availStream.WriteString( x.Name )
                                        availStream.WriteInt64( x.Version.Ticks )
                                        x.AvailThis.Pack( availStream )
                                        queue.ToSend( ControllerCommand( ControllerVerb.Availability, ControllerNoun.Blob ), availStream ) 
                                    )
                                    x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.AvailabilitySent
                                    bIOActivity := true
                                if x.BlobSync=BlobSyncMethod.Unicast && // bAllSrcAvailable && 
                                    (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.SentAllMetadata)=PerQueueJobStatus.None then  
                                    // outstandingSendingQueue < x.SendingQueueLimit then 
                                    // Calculate outstanding queue length 
                                    // Sent all metadata 
                                    let peerAvail = x.AvailPeer.[peeri]
                                    let mutable bAllSent = peerAvail.AllAvailable
                                    if not bAllSent then 
                                        bAllSent <- true
                                        for blobi=0 to x.NumBlobs-1 do
                                            if x.AvailThis.AvailVector.[blobi]=byte BlobStatus.AllAvailable then  
                                                let peerBlobAvail = peerAvail.AvailVector.[blobi]
                                                if peerBlobAvail = byte BlobStatus.NotAvailable || 
                                                    peerBlobAvail = byte BlobStatus.Initial then 
                                                    // Not sent other wise, the status will change to PartialAvailable 
                                                    //if queue.SendQueueLength<5 && int queue.UnProcessedCmdInBytes<=x.SendingQueueLimit / x.OutgoingQueues.Count && 
                                                    //    outstandingSendingQueue<x.SendingQueueLimit then 
                                                    if queue.CanSend then 
                                                        // Send blob to peeri to start the job
                                                        let stream = x.SendBlobPeeri peeri queue blobi 
                                                        outstandingSendingQueue <- outstandingSendingQueue + (int stream.Length)
                                                        /// Assume blob is delivered. 
                                                        peerAvail.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                                                        Logger.LogF( x.JobID, DeploymentSettings.TraceLevelBlobSend, ( fun _ -> sprintf "Send Blob %d (%s) to peer %d, mark the blob as available %A" blobi (x.Blobs.[blobi].Name ) peeri peerAvail) )
                                                        bIOActivity := true
                                                        stream.DecRef()
                                                    else
                                                        // Exceeding limit, need to wait. 
                                                        bAllSent <- false
                                                elif peerBlobAvail = byte BlobStatus.NoInformation then 
                                                    // Wait for first availability before try to send anything. 
                                                    bAllSent <- false
                                                else 
                                                    ()
                                            else
                                                bAllSent <- false 
                                                Logger.LogF( x.JobID, LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Host: Blob %d of %d is still not available" blobi x.NumBlobs ) )
                                        peerAvail.CheckAllAvailable() // Selectively mark all available flag 
                                        if peerAvail.AllAvailable then 
                                            Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "peer %d of %s, All metadata send and received.... vector %A" 
                                                                                                           peeri (LocalDNS.GetShowInfo( x.OutgoingQueues.Item(peeri).RemoteEndPoint ))
                                                                                                           x.AvailPeer.[peeri]
                                                                                                            ) )
                                            x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.SentAllMetadata
                                                                                                                // assume that Metadata is received, 
                                                                                                               ||| PerQueueJobStatus.AllMetadataSynced

                        // Check if job related information is available. 
                        x.UpdateClusterJobInfo() 
                        // Check if Src DSet information is received. 
                        x.UpdateSourceAvailability()
                        if !bIOActivity then 
                            // Reset clock for any io activity
                            maxWait := clock.ElapsedTicks + clockFrequency * DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit    
                        elif not jobAction.IsCancelledAndThrow then 
                            Threading.Thread.Sleep(5)
                with
                | :? AggregateException as ex -> 
                    reraise()
                | ex -> 
                    jobAction.EncounterExceptionAtCallback( ex, "___TrySyncMetaDataHost___" )
        )
        ()
    member x.TryStartJob() = 
        if bAllSynced then 
            let mutable bIOActivity = false
            let clock_start = clock.ElapsedTicks
            let mutable maxWait = clock_start + clockFrequency * DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit
            jobStage <- JobStage.Ready

            // mask on confirm job received. 
            let mutable bAllPeerConfirmed = false
            let mutable numConfirmedStart = 0

            while jobStage < JobStage.Termination && clock.ElapsedTicks<maxWait && not bAllPeerConfirmed do 
                bIOActivity <- false
                bAllPeerConfirmed <- true
                numConfirmedStart <- 0
                for peeri=0 to x.OutgoingQueues.Count-1 do 
                    let queue = x.OutgoingQueues.[peeri]
                    if not (NetworkCommandQueue.QueueFail(queue)) then 
                        // No need to start failed peer
                        if (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.StartJob)=PerQueueJobStatus.None then 
                            // Start, Job hasn't sent yet. 
                            if Utils.IsNotNull queue && queue.CanSend then 
                                if (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.AllMetadataSynced)<>PerQueueJobStatus.None 
                                    && (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.FailedToStart)=PerQueueJobStatus.None then 
                                    // Start, Job
                                    Logger.LogF( x.JobID, DeploymentSettings.TraceLevelStartJob, ( fun _ -> sprintf "Start, Job send to peer %d" peeri ))
                                    // Call start, job on each client. 
                                    using ( new MemStream( 1024 ) ) ( fun jobStream -> 
                                        jobStream.WriteGuid( x.JobID )
                                        jobStream.WriteString( x.Name )
                                        jobStream.WriteInt64( x.Version.Ticks )
                                        queue.ToSend( ControllerCommand( ControllerVerb.Start, ControllerNoun.Job ), jobStream )
                                    )
                                    x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.StartJob
                                    bIOActivity <- true
                        if (x.OutgoingQueueStatuses.[peeri] &&& PerQueueJobStatus.ConfirmStart)<>PerQueueJobStatus.None || 
                            x.LaunchMode = TaskLaunchMode.DonotLaunch then 
                            numConfirmedStart <- numConfirmedStart + 1
                        else
                            if (x.OutgoingQueueStatuses.[peeri] &&& (PerQueueJobStatus.Failed|||PerQueueJobStatus.Shutdown|||PerQueueJobStatus.FailedToStart)) = PerQueueJobStatus.None then 
                                // At least one peer can't start
                                bAllPeerConfirmed <- false
                            else
                                // peer failed, job may still start
                                ()
                if bIOActivity then 
                    // Reset clock for any io activity
                    maxWait <- clock.ElapsedTicks + clockFrequency * DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit    
                else
                    Threading.Thread.Sleep(5)
            Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Job %s:%s of task %s is ready to execute" x.Name x.VersionString x.SignatureName ))
            // bAllSynced is always true, so should be bAllPeerConfirmed
            bAllSynced && bAllPeerConfirmed && numConfirmedStart>0
        else
            bAllSynced 
    member x.Error( queue:NetworkCommandQueue, msg ) = 
        Logger.Log( LogLevel.Error, msg )
        if Utils.IsNotNull queue && queue.CanSend then 
            use msError = new MemStream( 1024 )
            msError.WriteString( msg )
            queue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message), msError )
            queue.Close()
    member x.JobCallback( cmd, inClusterPeeri, ms, jobID, name, verNumber, cluster ) = 
        using ( x.TryExecuteSingleJobActionFunc()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "[May be OK] Job.JobCallback, Received command %A, payload of %dB, but job has already been closed/cancelled. Message will be discarded. " 
                                                                                cmd ms.Length ) )
            else

                let queue = cluster.Queue(inClusterPeeri)
                try
                    ms.Info <- ms.Info + ":Job:" + x.Name
                    let peeri = x.OutgoingQueuesToPeerNumber.Item(queue)
                    match (cmd.Verb, cmd.Noun) with 
                    | ControllerVerb.Unknown, _ -> 
                        ()
                    | ControllerVerb.Availability, ControllerNoun.Blob ->
                        // Decode availability information. 
                        let availInfo = availPeer.[peeri]
                        availInfo.Unpack( ms )
                        availInfo.CheckAllAvailable()
                        if availInfo.AllAvailable then 
                            Logger.LogF( jobID, DeploymentSettings.TraceLevelBlobRcvd,  ( fun _ -> sprintf "Availability, Blob (all available ) for job %s:%s node %d peer %d peer %A this %A" 
                                                                                                        x.Name x.VersionString inClusterPeeri peeri
                                                                                                        availInfo x.AvailThis ))
                            x.CheckSync()
                            x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.AllMetadataSynced
                        else
                            Logger.LogF( jobID, DeploymentSettings.TraceLevelBlobRcvd, ( fun _ -> sprintf "Availability, Blob for job %s:%s node %d peer %d peer %A this %A" 
                                                                                                        x.Name x.VersionString inClusterPeeri peeri
                                                                                                        availInfo x.AvailThis ))

                    | ControllerVerb.Acknowledge, ControllerNoun.Job ->
                        // Acknowledge 
                        ()
                    | ControllerVerb.InfoNode, ControllerNoun.Job ->
                        // retrieve node related information
                        let nodeInfo = NodeWithInJobInfo.Unpack( ms )  
                        let ep = queue.RemoteEndPoint :?> Net.IPEndPoint
                        if ep.AddressFamily = Net.Sockets.AddressFamily.InterNetwork then 
                            nodeInfo.AddExternalAddress( ep.Address.GetAddressBytes() )  
                        let node = cluster.ClusterInfo.ListOfClients.[peeri]
                        LocalDNS.AddEntry( node.MachineName, nodeInfo.IPAddresses )   
                        NodeConnectionFactory.Current.StoreNodeInfo( node.MachineName, node.MachinePort,  x.LaunchIDName, x.LaunchIDVersion, nodeInfo ) 
                        x.NodeInfo.Item( queue.RemoteEndPointSignature ) <- nodeInfo
                        Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Rcvd node info from peer %d:%s : %s" inClusterPeeri queue.EPInfo (nodeInfo.ToString()) ))
                        ()
                    | ControllerVerb.NonExist, ControllerNoun.Job ->
                        // Fail to find job
                        let node = cluster.ClusterInfo.ListOfClients.[peeri]
                        NodeConnectionFactory.Current.StoreNodeInfo( node.MachineName, node.MachinePort, x.LaunchIDName, x.LaunchIDVersion, null ) 
                        x.NodeInfo.Item( queue.RemoteEndPointSignature ) <- null
                        x.OutgoingQueueStatuses.[inClusterPeeri] <- x.OutgoingQueueStatuses.[inClusterPeeri] ||| PerQueueJobStatus.FailedToStart
                        Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Fail to start task %s on peer %d:%s" x.SignatureName inClusterPeeri (LocalDNS.GetShowInfo( queue.RemoteEndPoint)) ))
                    | ControllerVerb.Write, ControllerNoun.Blob ->                
                        let blobi = ms.ReadVInt32()
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Rcvd Write, Blob %d from peer %d:%s" blobi inClusterPeeri queue.EPInfo ))
                        if blobi<0 || blobi >= x.NumBlobs then 
                            let msg = sprintf "Error: Job %s:%s Write, Blob with idx %d that is outside of range of valid blob index (0-%d)" x.Name x.VersionString blobi x.NumBlobs
                            x.Error( queue, msg )
                        else
                            let blob = x.Blobs.[blobi]
                            blob.Stream <- ms
                            blob.Stream.AddRef()
                            x.AvailThis.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            try 
                                let bSuccess = x.DecodeFromBlob( blobi, peeri )
                                if bSuccess then 
                                    x.UpdateBlobInfo( blobi ) 
                                    use msFeedback = new MemStream( 1024 )
                                    msFeedback.WriteGuid( jobID )
                                    msFeedback.WriteString( x.Name ) 
                                    msFeedback.WriteInt64( x.Version.Ticks ) 
                                    msFeedback.WriteVInt32( blobi ) 
                                    msFeedback.WriteBoolean( bSuccess )
                                    queue.ToSend( ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.Blob ), msFeedback )
                                else
                                    let msg = sprintf "Error: Failed to decode blob %d, please check if the right DSet name is used, and if the DSet is associated with this cluster." blobi
                                    x.Error( queue, msg )
                            with 
                            | e -> 
                                let msg = sprintf "JobCallback:Error in DecodeBlob %A" e
                                x.Error( queue, msg )
                    | ControllerVerb.Acknowledge, ControllerNoun.Blob ->
                        let blobi = ms.ReadVInt32()
                        if blobi<0 || blobi >= x.NumBlobs then 
                            let msg = sprintf "Error: Acknowledge, Blob with idx %d that is outside of range of valid serial number %d:%d" blobi x.BlobStartSerial x.NumBlobs
                            x.Error( queue, msg )
                        else
                            Logger.LogF( jobID, DeploymentSettings.TraceLevelBlobAvailability, ( fun _ -> sprintf "Acknowledge, Blob for job %s:%s node %d peer %d blob %d" 
                                                                                                                (x.Blobs.[blobi].Name) x.VersionString inClusterPeeri peeri
                                                                                                                blobi ))
                            let peerAvail = x.AvailPeer.[peeri]
                            peerAvail.AvailVector.[blobi] <- byte BlobStatus.AllAvailable
                            let bAvailBefore = peerAvail.AllAvailable
                            peerAvail.CheckAllAvailable()
                            if not bAvailBefore && peerAvail.AllAvailable then 
                                x.CheckSync()
                                x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.AllMetadataSynced
                    | ControllerVerb.Echo, ControllerNoun.Job ->
                        // Confirm Start, Job command received
                        ()
                    | ControllerVerb.ConfirmStart, ControllerNoun.Job ->
                        let bSuccess = ms.ReadBoolean()
                        Logger.LogF( jobID, DeploymentSettings.TraceLevelStartJob, ( fun _ -> sprintf "ConfirmStart, Job for job %s:%s node %d peer %d received, status is %A" 
                                                                                                 x.Name x.VersionString inClusterPeeri peeri
                                                                                                 bSuccess ))

                        if bSuccess then 
                            x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.ConfirmStart
                            // Job started    
                        else
                            // Some failure
                            x.OutgoingQueueStatuses.[peeri] <- x.OutgoingQueueStatuses.[peeri] ||| PerQueueJobStatus.Failed
                            ()
                    | ControllerVerb.Exception, ControllerNoun.Job -> 
                        let ex = ms.ReadException()
                        jobAction.ReceiveExceptionAtCallback( ex, sprintf  "___ Job.JobCallback (received from %s) ___" (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) )
                    | _ ->
                        let msg = sprintf "JobCallback: Unknown cmd %A" cmd 
                        jobAction.ThrowExceptionAtCallback( msg )
                with 
                | ex ->
                    let msg = sprintf "Exception in JobCallback, cmd %A, %s:%d, %A" cmd name verNumber ex
                    jobAction.EncounterExceptionAtCallback( ex, "___ Job.JobCallback (throw) ___" )
            true
        )

    /// Exchange with peer on metadata content, get all metadata ready. 
    member x.ReadyMetaData() = 
        let bSuccess = x.PrepareMetaData()
        Logger.LogF( x.JobID, LogLevel.MildVerbose, ( fun _ ->    let t1 = (PerfADateTime.UtcNowTicks())
                                                                  sprintf "Job %s, PrepareMetaData in %.2f ms"
                                                                           x.Name
                                                                           (float (t1 - x.JobStartTicks) / float TimeSpan.TicksPerMillisecond) ))
        if bSuccess then 
            x.BeginSendMetaData()
            x.PrepareToReceiveSourceMetadata()
            x.InitializeAllQueues()
            x.InitializeAvailability()
            x.TrySyncMetaDataHost()
            x.TryStartJob()
        else
            false
    /// Is the job ready to be executed?
    member x.Ready() = 
        if not bReady then 
            bReady <- x.ReadyMetaData()
            Logger.LogF( x.JobID, 
                         LogLevel.MildVerbose, ( fun _ -> let t1 = (PerfADateTime.UtcNowTicks())
                                                          sprintf "Job %s, ready in %.2f ms"
                                                                   x.Name
                                                                   (float (t1 - x.JobStartTicks) / float TimeSpan.TicksPerMillisecond) ))
        bReady
    /// Blob Status
    member x.BlobiPeerStatusString( blobi ) = 
        let blob = x.Blobs.[blobi]
        let peerStatusSeq = 
            seq {
                for peeri = 0 to x.AvailPeer.Count - 1 do 
                    let peeriAvail = x.AvailPeer.[peeri]
                    yield sprintf "%d:%s" peeri (BlobAvailability.StatusString(peeriAvail.AvailVector.[blobi]))
            }
        let blobNameInfo = 
            match blob.TypeOf with 
            | BlobKind.SrcDSetMetaData ->          sprintf "Src DSet %s:%s" blob.Name (Version64ToString(blob.Version)) 
            | BlobKind.PassthroughDSetMetaData ->  sprintf "Passthrough DSet %s:%s" blob.Name (Version64ToString(blob.Version)) 
            | BlobKind.DstDSetMetaData ->          sprintf "Dst DSet %s:%s" blob.Name (Version64ToString(blob.Version)) 
            | BlobKind.AssemblyManagedDLL ->      sprintf "Managed DLL %s:%s" blob.Name (blob.Version.ToString("X")) 
            | BlobKind.AssemblyUnmanagedDir ->    sprintf "Unmanged DIR %s:%s" blob.Name (blob.Version.ToString("X")) 
            | BlobKind.JobDependencyFile ->       sprintf "Job Dependency File %s:%s" blob.Name (blob.Version.ToString("X")) 
            | BlobKind.ClusterMetaData ->         sprintf "Cluster %s:%s" blob.Name (Version64ToString(blob.Version))
            | _ -> sprintf "Blob Type %A, %s:%s" blob.TypeOf blob.Name (Version64ToString(blob.Version))

        blobNameInfo + " Peer Status ..." + ( peerStatusSeq |> String.concat ", " )
    member internal x.BlobPeerStatusString( showTypes:IEnumerable<_> ) = 
        let showTypesList = List<_>( showTypes )
        let blobStatusSeq = 
            seq { 
                for blobi = 0 to x.NumBlobs - 1 do 
                    let blob = x.Blobs.[blobi]
                    if showTypesList.Contains( blob.TypeOf ) then 
                        yield x.BlobiPeerStatusString( blobi )
            }
        blobStatusSeq |> String.concat Environment.NewLine
    static member PeerQueueString( status ) = 
        let stSeq = 
            seq { 
                if status &&& PerQueueJobStatus.Failed <> PerQueueJobStatus.None then yield "Failed"
                elif status &&& PerQueueJobStatus.Shutdown <> PerQueueJobStatus.None then yield "Shutdown"
                elif status &&& PerQueueJobStatus.ConfirmStart <> PerQueueJobStatus.None then yield "ConfStart"
                elif status &&& PerQueueJobStatus.StartJob <> PerQueueJobStatus.None then yield "StartSent"
                elif status &&& PerQueueJobStatus.AllMetadataSynced <> PerQueueJobStatus.None then yield "MetaSynced"
                elif status &&& PerQueueJobStatus.SentAllMetadata <> PerQueueJobStatus.None then yield "MetaSent"
                elif status &&& PerQueueJobStatus.AvailabilitySent <> PerQueueJobStatus.None then yield "AvailSent"
                elif status &&& PerQueueJobStatus.JobMetaDataSent <> PerQueueJobStatus.None then yield "JobInfoSent"
                elif status &&& PerQueueJobStatus.Connected <> PerQueueJobStatus.None then yield "Conn"
                else 
                    yield "Unknown"
            }
        stSeq |> String.concat ":"
    member x.PeerQueueStatusString() = 
        let peerQueueStatusSeq = 
            seq {
                for peeri=0 to x.OutgoingQueues.Count-1 do 
                    let queue = x.OutgoingQueues.[peeri]
                    if (NetworkCommandQueue.QueueFail(queue)) then 
                        yield sprintf "%d:FAILED" peeri 
                    else
                        let status = x.OutgoingQueueStatuses.[peeri] 
                        yield sprintf "%d:%s" peeri (Job.PeerQueueString( status ))
            }
        "PeerQueue Status : " + ( peerQueueStatusSeq |> String.concat ", " )
    /// Return a (status, string) explaining the status of the job
    member x.JobStatus() = 
        if bFailureToGetSrcMetadata then 
            false, "Job fails as source metadata is outdated." + Environment.NewLine + x.BlobPeerStatusString( Blob.AllSourceStatus ) + Environment.NewLine + x.PeerQueueStatusString() 
        elif not bAllSrcAvailable then 
            false, "Job fails as some source is not available." + Environment.NewLine + x.BlobPeerStatusString( Blob.AllSourceStatus ) + Environment.NewLine + x.PeerQueueStatusString() 
        elif not bAllSynced then 
            false, "Job fails as some peer fails to complete the sync operation." + Environment.NewLine + x.BlobPeerStatusString( Blob.AllStatus ) + Environment.NewLine + x.PeerQueueStatusString() 
        elif not bReady then 
            false, "Job fails as some peer fails to confirm job start." + Environment.NewLine + x.PeerQueueStatusString() 
        else 
            true, "Job is ready to execute."
    /// Is all metadata available for execution
    member x.AllAvailable with get() = 
                                if Utils.IsNull x.AvailThis then 
                                    true
                                else
                                    x.AvailThis.CheckAllAvailable()
                                    x.AvailThis.AllAvailable
    /// Send Current Job Metadata to each peer. 



    override x.ToString() = 
        seq {
            yield "JobID: %s" + x.JobID.ToString("D")
            for cluster in x.Clusters do 
                yield "Cluster " + cluster.ToString()
            for dsetSrc in x.SrcDSet do
                yield (sprintf "Src DSet: %s, %s, %A " dsetSrc.Name dsetSrc.VersionString dsetSrc.StorageType )
            for dsetDst in x.DstDSet do
                yield (sprintf "Dst DSet: %s, %s, %A " dsetDst.Name dsetDst.VersionString dsetDst.StorageType )
            for dsetPassthrough in x.PassthroughDSet do
                yield (sprintf "Passthrough DSet: %s, %s, %A:%A" dsetPassthrough.Name dsetPassthrough.VersionString dsetPassthrough.Function.FunctionType dsetPassthrough.Function.TransformType )
            yield "Assemblies" 
            for assem in x.Assemblies do   
                if not assem.IsSystem then              
                    yield "  " + assem.ToString()
            yield " Job File Dependencies"
            for file in x.JobDependencies do
                yield " " + file.ToString()
        } |> String.concat Environment.NewLine

and /// Create a job for remote execution roster
    internal ContainerJob() as thisInstance = 
        inherit Job() 
        let jobLifecycleRef = ref ( JobLifeCycleCollectionApp.BeginJob() )
        do 
            thisInstance.IsContainer <- true 
            let jobLifecycle = Volatile.Read( jobLifecycleRef )
            jobLifecycle.OnDisposeFS( thisInstance.EndContainerJob )
            thisInstance.TryExecuteSingleJobActionFunc <- thisInstance.TryExecuteSingleJobAction
        /// Grab a single job action object, when secured, the cancellation of the underlying jobLifeCycle object will be delayed 
        /// until this action completes. 
        member internal x.TryExecuteSingleJobAction() = 
            let writeObj = Volatile.Read( jobLifecycleRef )
            SingleJobActionApp.TryEnterAndThrow( writeObj )
        /// End container job
        member x.EndContainerJob() = 
            x.FreeJobResource()
            let objLifeCyle = Volatile.Read( jobLifecycleRef )
            /// Free resource associated with the jobLifecyle object 
            Volatile.Write( jobLifecycleRef, null )
            if Utils.IsNotNull objLifeCyle then 
                JobLifeCycleCollectionApp.UnregisterJob( objLifeCyle )  
                ( objLifeCyle :> IDisposable ).Dispose()
