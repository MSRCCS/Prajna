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
        DSetAction.fs
  
    Description: 
        Perform action on DSet 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Threading
open System.IO
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Linq
open Microsoft.FSharp.Collections
open Prajna.Tools
open Prajna.Tools.Network
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

/// DSetAction is a wrapper class to perform action on one set of DSet. 
[<AbstractClass;AllowNullLiteral>]
type internal DSetAction() = 
    // clock 
    let clock = System.Diagnostics.Stopwatch.StartNew()
    // clock frequency
    let clockFrequency = System.Diagnostics.Stopwatch.Frequency
    // clock start
    let mutable clockstart = clock.ElapsedTicks
    // DSet used in this particular DSet Action
    let dsetParams = List<_>()
    // DSet that is seen, so may need to be thrown away
    // Only if the action output to Write, DSet
    let mutable seenWriteDSet = null


    /// Add one Prajna job parameter
    member x.Param with set( dset:DSet ) = 
                            if not ( dsetParams.Contains( dset ) ) then 
                                dsetParams.Add( dset ) 
    /// Get Prajna Job parameters
    member x.ParameterList with get() = dsetParams
    /// Set Multiple Parameters
    member x.Params with set( parameters:IEnumerable<_> ) = 
                        for p in parameters do
                            x.Param <- p
    /// Hold a Job object
    member val Job  = null with get, set
    /// Prajna command argument
    member val Verb : ControllerVerb = ControllerVerb.Error with get, set
    /// Prajna function argument, in a serialized memory stream
    member val FuncArgument : MemStream = null with get, set
//    static member CurrentJob with get() = DSetAction.JobGlobal
    /// InitializeGlobalJob set up one Prajna Job to be used across multiple DSetActions. 
    /// In such a case, the programmer should define all actions to be executed, before actually calling the action to maximize execution efficiency
//    static member InitializeGlobalJob( jobname ) = 
//        if Utils.IsNull (!(DSetAction.JobGlobal)) then 
//            // Perform initialization if the job has not been started. 
//            let jobHolder = Job( TypeOf=(JobTaskKind.Computation|||JobDependencies.DefaultTypeOfJobMask), Name = jobname, Version = (PerfADateTime.UtcNow()) )
//            if Interlocked.CompareExchange( DSetAction.JobGlobal, jobHolder, null ) = null then 
//                DSetAction.JobGlobal <- 
    /// Initialize Job 
    member x.InitializeJob() =
        if Utils.IsNull x.Job then 
            let jobName = x.ParameterList |> Seq.map ( fun dset -> dset.Name ) |> String.concat "+"
            x.Job <- Job( TypeOf=(JobTaskKind.Computation|||JobDependencies.DefaultTypeOfJobMask), Name = jobName, Version = (PerfADateTime.UtcNow()) )
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Created Job %s:%s ... " x.Job.Name x.Job.VersionString ))
        // populate job parameter
        for dset in dsetParams do 
            x.Job.Param <- dset
        x.Job.Version <- (PerfADateTime.UtcNow()) // Get a new job version every time of execution
    /// Retrieve Metadata of all source DSetS. 
    member x.RetrieveMetaData() =
        // Make Metadata ready 
        let bSuccess = x.Job.Ready()
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Job %s:%s metadata & start: %A" x.Job.Name x.Job.VersionString bSuccess ))
        bSuccess 
    member x.InitializePeerAvailability() =
    /// Intialize Peer Availability 
        // Set peer availability 
        for dset in x.RemappedDSet do  
            let bMetaRead, bPeerF = x.Job.SetMetaDataAvailability( dset )
            dset.SetMetaDataAvailability( bMetaRead, bPeerF) 
            dset.InitiatePartitionStatus()
            dset.InitiateCommandStatus()
    abstract member BeginAction : unit -> unit
    default x.BeginAction() = 
        x.BeginActionWithLaunchMode( DeploymentSettings.DefaultLaunchBehavior )
    abstract member BeginActionWithLaunchMode : TaskLaunchMode -> unit
    default x.BeginActionWithLaunchMode( nLaunchNewTaskMode ) = 
        x.BaseBeginAction( nLaunchNewTaskMode )
    /// This is used as an example if DSetAction is to be derived. 
    member x.BaseBeginAction(nLaunchNewTaskMode) = 
        clockstart <- clock.ElapsedTicks
        x.InitializeJob()
        x.Job.LaunchMode <- nLaunchNewTaskMode
        let priorDSetLists = x.ParameterList.ToArray() 
        let bReady = x.RetrieveMetaData()
        // Update DSet with a new List of DSets with updated metadata information. 
        let updatedDSetList = priorDSetLists |> Array.map ( fun dset -> x.Job.ResolveDSetAfterMetadataSync( dset ) )
        dsetParams.Clear()
        dsetParams.AddRange( updatedDSetList )
        if bReady then 
            x.FindRemappedDSets()
            x.InitializePeerAvailability()
            x.BeginRegister()
            x.RegisterGVCallback()
            if DeploymentSettings.bSaveInitialMetadata then 
                for dst in x.Job.DstDSet do 
                    // Save an initial version of metadata. 
                    if not (dst.bIsClusterReplicate()) then 
                        dst.bEncodeMapping <- true
                    dst.SaveMetadata()
//                dst.WriteFirstMetadataToAllPeers() // First send a 1st copy of DSet to all peers. 
    /// Timeout value 
    member val TimeoutVal = DeploymentSettings.TimeOutAction with get, set
    member x.Timeout() = 
        let cur = clock.ElapsedTicks
        cur - clockstart >= clockFrequency * x.TimeoutVal
    /// Additional Callback 
    member val FurtherDSetCallback = 
        let noFurtherCallback( cmd, peeri, ms, name, verNumber, cl ) =
            // Command processed, no need to block
            true
        noFurtherCallback with get, set
    member val internal ReferencedDSet = ConcurrentDictionary<_,_>( (StringTComparer<int64>(StringComparer.Ordinal)) ) with get
    /// Register call back for all DSet involved
    member x.BeginRegister( ) = 
        let registerDSet = seq { yield! x.RemappedDSet
                                 yield! x.Job.DstDSet }
        // Initial peer to read from. 
        for curDSet in registerDSet do 
            x.ReferencedDSet.Item( (curDSet.Name, curDSet.Version.Ticks) ) <- curDSet
            curDSet.Cluster.RegisterCallback( curDSet.Name, curDSet.Version.Ticks, 
                        [| ControllerCommand( ControllerVerb.Unknown, ControllerNoun.DSet);      // All DSet command
                           ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Service);  // All Service command
                           ControllerCommand( ControllerVerb.Close, ControllerNoun.Partition);  // Close, Partition call 
                         |],
                            { new NetworkCommandCallback with 
                                member this.Callback( cmd, peeri, ms, name, verNumber, cl ) = 
                                    let bProcessed, bNotBlocked = x.DSetCallback( cmd, peeri, ms, name, verNumber, cl ) 
                                    if not bProcessed then 
                                        x.FurtherDSetCallback( cmd, peeri, ms, name, verNumber, cl ) 
//                                        match DSetCallback with 
//                                        | Some ( callbackFunc ) ->
//                                            callbackFunc( cmd, peeri, ms, name, verNumber, cl )
//                                        | None ->
//                                            ()
                                    else
                                        bNotBlocked
                            } )
    member internal x.ResolveDSetByName tuple =            
        let bExist, dset = x.ReferencedDSet.TryGetValue( tuple )
        if bExist then 
            dset
        else
            null
    member val RemappedDSet = List<_>() with get, set
    member x.FindRemappedDSets() = 
        x.RemappedDSet.Clear()
        x.RemappedDSet.AddRange( dsetParams )
        for dset in dsetParams do 
            x.Job.TraverseAllObjectsWDirection TraverseUpstream (List<_>()) dset x.FindOneRemappedDSet
    member x.FindOneRemappedDSet direction dobj = 
        match dobj with 
        | :? DSet as dset -> 
            // Only DSet is remapped
            let mutable bRemapping = false
            match direction with 
            | TraverseUpstream -> 
                match dset.DependencyDownstream with 
                | MixTo _ ->
                    bRemapping <- true
                | WildMixTo _ -> 
// not if proceed by MixTo, otherwise, use this. 
                    match dset.Dependency with 
                    | MixFrom _ -> 
                        ()
                    | _ -> 
                        bRemapping <- true
                | _ ->
                    ()
            | _ -> 
                () 
            if bRemapping then 
                // Is the object already contained in the list? ReferenceContains
                for obji in x.RemappedDSet do 
                    if Object.ReferenceEquals( obji, dset ) then 
                        bRemapping <- false     
            if bRemapping then 
                x.RemappedDSet.Add( dset ) 
                dset.RemappingCommandCallback <- x.RemappingCommandForReadToNetwork
        | _ ->
            // No need of remapping for downstream objects 
            ()

    member x.RemappingDSet() = 
        if x.RemappedDSet.Count > 0 then 
            let mutable bRemapping = true
            for curDSet in x.RemappedDSet do  
                let ret = curDSet.Remapping()
                bRemapping <- bRemapping && ret
            bRemapping
        else
            false

    member x.RemappingCommandForReadToNetwork( peeri, peeriPartitionArray:int[], curDSet:DSet ) = 
        let msPayload = new MemStream( 1024 )
        msPayload.WriteString( curDSet.Name )
        msPayload.WriteInt64( curDSet.Version.Ticks )
        msPayload.WriteVInt32( peeriPartitionArray.Length )
        for parti in peeriPartitionArray do 
            msPayload.WriteVInt32( parti )
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DSet %s:%s ReadToNetwork, DSet issued to peer %d partition %A" curDSet.Name curDSet.VersionString peeri peeriPartitionArray ))
        ControllerCommand( ControllerVerb.ReadToNetwork, ControllerNoun.DSet), msPayload
    /// Register call back for GV
    member x.RegisterGVCallback( ?GVCallback ) = 
        // Initial peer to read from. 
        for cluster in x.Job.Clusters do 
            // Catch all calls to the GV.
            cluster.RegisterCallback( null, 0L, 
                        [| ControllerCommand( ControllerVerb.Unknown, ControllerNoun.GV);      // All DSet command
                         |],
                            { new NetworkCommandCallback with 
                                member this.Callback( cmd, peeri, ms, name, verNumber, cl ) = 
                                    let bProcessed, bNotBlocked = x.GVCallback( cmd, peeri, ms, name, verNumber, cl ) 
                                    if not bProcessed then 
                                        match GVCallback with 
                                        | Some ( callbackFunc ) ->
                                            callbackFunc( cmd, peeri, ms, name, verNumber, cl )
                                        | None ->
                                            true
                                    else
                                        bNotBlocked
                            } )
        
        
                                
       

    /// Called to shutdown the reading data from Prajna, note that normally, the read operation is 
    /// graceful shutdown automatically. EndRead is not absolutely necessary to be called. 
    member x.EndRegister() = 
        let registerDSet = seq { yield! x.RemappedDSet
                                 yield! x.Job.DstDSet }
        // Initial peer to read from. 
        for curDSet in registerDSet do 
            curDSet.ToClose()
            curDSet.PartitionAnalysis()
            curDSet.Cluster.UnRegisterCallback( curDSet.Name, curDSet.Version.Ticks,  [| ControllerCommand( ControllerVerb.Unknown, ControllerNoun.DSet);
                                                                                      ControllerCommand( ControllerVerb.Close, ControllerNoun.Partition);
                                                                                      |] )
        for cluster in x.Job.Clusters do 
            // Catch all calls to the GV.
            cluster.UnRegisterCallback( null, 0L, [| ControllerCommand( ControllerVerb.Unknown, ControllerNoun.GV);      // All DSet command
                         |] )
        x.ReferencedDSet.Clear()
    abstract member EndAction : unit -> unit
    default x.EndAction() = 
        x.BaseEndAction()
    /// This is used as an example if DSetAction is to be derived. 
    member x.BaseEndAction() = 
        x.CloseAndUnregister()
    /// This is used as an example if DSetAction is to be derived. 
    member x.CloseAndUnregister() = 
        if Utils.IsNotNull x.Job then 
            let msCloseMsg = new MemStream( 1024 )
            msCloseMsg.WriteString( x.Job.Name ) 
            msCloseMsg.WriteInt64( x.Job.Version.Ticks ) 
            for cluster in x.Job.Clusters do 
                for peeri = 0 to cluster.NumNodes - 1 do 
                    let queue = cluster.Queue( peeri )
                    if Utils.IsNotNull queue && queue.CanSend then 
                        queue.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.Job), msCloseMsg ) 
            x.EndRegister()

    /// Process DSet Call back 
    /// Return: 1st param true -> Command has been parsed. 
    ///                   false -> Command has not been parsed, subsequent callback should handle the command. 
    ///         2nd param true -> Command has been processed
    ///                   false -> Command has been blocked. 
    member x.DSetCallback( cmd, peeri, msRcvd, name, verNumber, cl ) = 
        try
            let curDSet = x.ResolveDSetByName( name, verNumber )
            if Utils.IsNull curDSet then 
                let msg = sprintf "DSetActionCallback: Unable to resolve DSet %s" name
                Logger.Log( LogLevel.Error, msg )
                false, true
            else 
                let q = cl.Queue( peeri )
                let mutable bParsed = true
                match ( cmd.Verb, cmd.Noun ) with 
                | ( ControllerVerb.Acknowledge, ControllerNoun.DSet ) ->
                    ()
                | ( ControllerVerb.Get, ControllerNoun.ClusterInfo ) ->
    //                let cluster = curDSet.Cluster.ClusterInfo :> ClusterInfoBase
                    let msSend = new MemStream( 10240 ) 
    //                msSend.Serialize( cluster )
                    cl.ClusterInfo.Pack( msSend )
                    let cmd = ControllerCommand( ControllerVerb.Set, ControllerNoun.ClusterInfo ) 
                    // Expediate delivery of Cluster Information to the receiver
                    q.ToSend( cmd, msSend, true ) 
                | ( ControllerVerb.Close, ControllerNoun.Partition) ->
                    let parti = msRcvd.ReadVInt32()
                    let numError = msRcvd.ReadVInt32()
                    if numError = 0 then 
                        // Signal a certain partition is succesfully read
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "From peer %d, DSet %s:%s partition %d has been sucessfully read without error" peeri curDSet.Name curDSet.VersionString parti ))
                    else
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "From peer %d, DSet %s:%s partition %d reaches an end with %d errors" peeri curDSet.Name curDSet.VersionString parti numError ))
                    curDSet.ProcessedPartition( peeri, parti, numError )
                | ( ControllerVerb.Error, ControllerNoun.DSet ) ->
                    let errorMsg = msRcvd.ReadString() 
                    Logger.Log( LogLevel.Info, ( sprintf "RetrieveMetaDataCallback: peer %d(%s), error message %s" peeri (LocalDNS.GetShowInfo(q.RemoteEndPoint)) errorMsg )    )
                | ( ControllerVerb.Info, ControllerNoun.DSet ) ->
                    // Missing DSet partition can be due to the reason that no key has been ever written to the partition. 
                    let errorCode = enum<DSetErrorType>( msRcvd.ReadByte() )
                    match errorCode with 
                    | DSetErrorType.NonExistPartition ->
                        let numMissedPartitions = msRcvd.ReadVInt32() 
                        let notFindPartitions = Array.zeroCreate<int> numMissedPartitions
                        for i = 0 to notFindPartitions.Length-1 do
                            let parti = msRcvd.ReadVInt32( ) 
                            notFindPartitions.[i] <- parti
                        let bRemapped = curDSet.NotExistPartitions( peeri, notFindPartitions )
                        if bRemapped then 
                            curDSet.Remapping() |> ignore    
                        Logger.Log( LogLevel.WildVerbose, ( sprintf "RetrieveMetaDataCallback: peer %d, doesn't have partition %A" peeri numMissedPartitions )    )
                    | e ->
                        Logger.Log( LogLevel.WildVerbose, ( sprintf "RetrieveMetaDataCallback: Unexpected Error,DSet errorcode %A from peer %d, command %A" errorCode peeri cmd )    )
                | ( ControllerVerb.Close, ControllerNoun.DSet ) ->
                    Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Close DSet received for DSet %s:%s from peer %d" curDSet.Name curDSet.VersionString peeri ))
                    // Don't actively close queue, as it may be used by following command. 
                    // curDSet.Cluster.CloseQueueAndRelease( peeri )
                    curDSet.PeerCmdComplete( peeri ) 
                | ( ControllerVerb.ReportPartition, ControllerNoun.DSet ) ->
                    let nPartitions = msRcvd.ReadVInt32()
                    let activePartitions = Array.zeroCreate<_> nPartitions
                    for i = 0 to nPartitions - 1 do 
                        let parti = msRcvd.ReadVInt32()
                        let numElems = msRcvd.ReadVInt32()
                        let streamLength = msRcvd.ReadInt64()
                        activePartitions.[i] <- parti, numElems, streamLength
                    Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "ReportPartition DSet received for DSet %s:%s from peer %d with partitions information %A" curDSet.Name curDSet.VersionString peeri activePartitions ))
                    // Save peer reports
                    curDSet.ReportPartition( peeri, activePartitions )
                | ( ControllerVerb.ReportClose, ControllerNoun.DSet ) ->
                    
                    // Final command after all streams have received. 

                    curDSet.ReportClose( peeri )                
                | _ ->
                    bParsed <- false
                    // Warning message is suppressed as further callback can be hooked to resolve those missing command. 
                    Logger.Log( LogLevel.ExtremeVerbose, ( sprintf "Unparsed command in processing DSetAction.DSetCallback, cmd %A, peer %d" cmd peeri )    )
                if curDSet.IsRemapping() then 
                    curDSet.Remapping() |> ignore
                bParsed, true
        with
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "Error in processing DSetAction.DSetCallback, cmd %A, peer %d, with exception %A" cmd peeri e )    )
            false, true
    /// Process GV Call back 
    /// Return: true -> Command has been parsed. 
    ///         false -> Command has not been parsed, subsequent callback should handle the command. 
    member x.GVCallback( cmd, peeri, msRcvd, name, verNumber, cl ) = 
        false, true
    /// ToDo:
    ///     Specifically release all resource that is hold by the current job. 
    static member ReleaseAllJobResource() = 
        ()

[<AllowNullLiteral>]
type internal DSetFoldAction<'U, 'State >()=
    inherit DSetAction()
    member val ReturnResultFromPeer = ref None with get
    member val FoldFunc: FoldFunction = null with get, set
    member val AggreFunc: AggregateFunction = null with get, set
    member val GVSerializeFunc = GVSerialize<'State>()
    member val InitialParam = None  with get, set
    member x.DoFold( s: 'State ) = 
        if x.ParameterList.Count<>1 then 
            let msg = sprintf "DSetFoldAction should take a single DSet parameter, while %d parameters are given" x.ParameterList.Count
            Logger.Log( LogLevel.Error, msg )
            failwith msg
        x.InitialParam <- Some( s ) 
        x.FurtherDSetCallback <- x.DSetFoldCallback
        
        x.BeginAction()
        let useDSet = x.ParameterList.[0]
        if x.Job.ReadyStatus && useDSet.NumPartitions>0 then 
            useDSet.RemappingCommandCallback <- x.RemappingCommandForRead
            // Send out the fold command. 
            x.ReturnResultFromPeer := None
            x.RemappingDSet() |> ignore        
            while not (x.Timeout()) && not (useDSet.AllDSetsRead()) do
                x.RemappingDSet() |> ignore
                // Wait for result to come out. 
                Thread.Sleep( 3 )
            if x.Timeout() then 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Timeout for DSetFoldAction ............." ))
            x.EndAction()
            match (!x.ReturnResultFromPeer) with
            | Some result -> 
                result
            | None -> 
                // Return initial state if fails. 
                s
//            if returnResultFromPeer.Count > 0 then 
//                returnResultFromPeer.[0]
//            else
//                useDSet.PartitionAnalysis() 
//                let msg = sprintf "Fold fail in some peers"
//                Logger.Log(LogLevel.Error, msg)
//                failwith msg
        else
            x.CloseAndUnregister()
            let msg = x.Job.JobStatusString()
            Logger.Log( LogLevel.Info, msg )
            s                            
    member x.RemappingCommandForRead( peeri, peeriPartitionArray:int[], curDSet:DSet ) = 
        let msPayload = new MemStream( 1024 )
        msPayload.WriteString( curDSet.Name )
        msPayload.WriteInt64( curDSet.Version.Ticks )
        msPayload.WriteVInt32( peeriPartitionArray.Length )
        for parti in peeriPartitionArray do 
            msPayload.WriteVInt32( parti )
        msPayload.Serialize( x.FoldFunc ) // Don't use Serialize From
        msPayload.Serialize( x.AggreFunc )
        msPayload.Serialize( x.GVSerializeFunc )
        let stateTypeName =  typeof<'State>.FullName
        msPayload.WriteString( stateTypeName )
        match x.InitialParam with 
        | Some param -> 
            if not (Object.ReferenceEquals( param, Unchecked.defaultof<'State>) ) then 
                msPayload.CustomizableSerializeFromTypeName( param, stateTypeName ) 
            else
                ()
        | None _ ->
            ()
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Fold, DSet issued to peer %d partition %A" peeri peeriPartitionArray ))
        ControllerCommand( ControllerVerb.Fold, ControllerNoun.DSet), msPayload
    member x.DSetFoldCallback( cmd, peeri, msRcvd, name, verNumber, cl ) = 
        try
            let curDSet = x.ResolveDSetByName( name, verNumber )
            if Utils.IsNotNull curDSet then 
                let q = cl.Queue( peeri )
                match ( cmd.Verb, cmd.Noun ) with 
                | ( ControllerVerb.WriteGV, ControllerNoun.DSet ) ->
                    let parts = msRcvd.ReadVInt32()
                    let partitionArray = Array.zeroCreate<_> parts
                    for i = 0 to partitionArray.Length - 1 do 
                        let parti = msRcvd.ReadVInt32()
                        partitionArray.[i] <- parti
                        curDSet.ProcessedPartition( peeri, parti, 0 )
                    let s = msRcvd.DeserializeTo<'State>() 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Receive %A from peer %d with partition %A" s peeri partitionArray )    )
//                    let s = o :?> 'State
                    // Merge Result
//                    if returnResultFromPeer.Count = 0 then 
//                        returnResultFromPeer.Add( s )
//                    else
//                        let aggre = x.AggreFunc :?> AggregateFunction<'State>
//                        let foldFunc = aggre.FoldStateFunc
//                        let aggResult = returnResultFromPeer |> Seq.fold foldFunc s
//                        returnResultFromPeer.Clear()
//                        returnResultFromPeer.Add( aggResult )
                    lock ( x.ReturnResultFromPeer ) ( fun _ -> 
                        let oldResult = !(x.ReturnResultFromPeer)
                        match oldResult with 
                        | None -> 
                            x.ReturnResultFromPeer := Some s
                        | Some prevResult -> 
                            let aggre = x.AggreFunc :?> AggregateFunction<'State>
                            let foldFunc = aggre.FoldStateFunc
                            let result = 
                                if not (Utils.IsNull prevResult) && Utils.IsNotNull s then 
                                    foldFunc prevResult s
                                elif (Utils.IsNull prevResult) then 
                                    s
                                else
                                    prevResult
                            x.ReturnResultFromPeer := Some result
                        )                                                        
                    // curDSet.PeerCmdComplete( peeri ) 
                | _ ->
                    ()
        with
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "Error in processing DSetFoldAction.DSetFoldCallback, cmd %A, peer %d, with exception %A" cmd peeri e )    )
        true    

