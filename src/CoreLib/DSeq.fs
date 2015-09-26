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
        DSeq.fs
  
    Description: 
        The concept of a discrete sequence operator. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        April. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Diagnostics
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Security.Cryptography
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.BytesTools
open Prajna.Tools.Network

type internal DStreamMetadataCopyFlag =
    | Copy = 0x00           // Construct the DSet by copying the core parameter, no relationship is setup between the DSet being constructed. 
    | Passthrough = 0x01    // One-to-One pass through (like seq.filter, seq.map, in which the target DSet performs certain function but doesn't retain in memory )

type internal DStreamMetadataStorageFlag =
    | None = 0x00
    | ALL = 0x7f


[<AllowNullLiteral>]
/// Dependency between DStream and DSet
type internal DependentDStream internal( target, hash ) =
    inherit DependentDObject( target, hash, ParamType = FunctionParamType.DStream )
    member x.TargetStream with get() = x.Target :?> DStream 
                                    and set( p:DStream ) = ( x.Target <- p )
    new ( target:DStream ) = 
        DependentDStream( target, if Utils.IsNull target.Blob then null else target.Blob.Hash )
    new ( hash ) = 
        DependentDStream( null, hash )
    new () = 
        DependentDStream( null, null )
    new ( depDObject:DependentDObject ) = 
        DependentDStream( null, depDObject.Hash )  
and internal DStreamDependencyType = 
    /// SourceStream: the current DStream doesn't depend on other DSet, it is a source 
    | SourceStream
    /// Encode from another object
    | EncodeFrom of DependentDObject
    /// Save to another stream, the DSet is a destination DSet
    | SaveFrom of DependentDObject
    /// Passthrough
    | PassFrom of DependentDStream
    /// Get a blob from network
    | ReceiveFromNetwork of DependentDStream
    /// Get a blob via multicast from network, compared with ReceiveFromNetwork, the main difference is that the Mapping is Full at the receiver
    | MulticastFromNetwork of DependentDStream
and internal DStreamForwardDependencyType = 
    /// SourceStream: the current DStream doesn't depend on other DSet, it is a source 
    | SinkStream 
    /// DecodeTo another object
    | DecodeTo of DependentDObject
    /// Passthrough to another stream
    | PassTo of DependentDStream
    /// Send a blob to network
    | SendToNetwork of DependentDStream
    /// Multicast a blob to all destinations in the Mapping
    | MulticastToNetwork of DependentDStream
and internal DStreamFactory() = 
    static let collectionByJob = ConcurrentDictionary<Guid,(int64 ref)*ConcurrentDictionary<_,_>>() 
    // Cache a DStream, if there is already a DStream existing in the factory with the same name and version information, then use it. 
    static member CacheDStream( jobID, newDStream: DStream ) = 
        let refTicks, jobCollection = collectionByJob.GetOrAdd( jobID, fun _ -> (ref DateTime.UtcNow.Ticks), ConcurrentDictionary<_,_>(StringTComparer<int64>(StringComparer.Ordinal)) ) 
        refTicks := DateTime.UtcNow.Ticks
        let retDStream = jobCollection.GetOrAdd((newDStream.Name, newDStream.Version.Ticks), newDStream ) 
        retDStream
    static member RemoveDStream( jobID ) = 
        collectionByJob.TryRemove( jobID ) |> ignore
    static member ResolveDStreamByName tuple = 
        let jobID, name, ver = tuple
        let bExist, t = collectionByJob.TryGetValue( jobID )
        if bExist then 
            let refTicks, jobCollection = t
            refTicks := DateTime.UtcNow.Ticks
            let bExist, dstream = jobCollection.TryGetValue( (name, ver ))
            if bExist then 
                dstream
            else
                null
        else
            null
/// DStream is a distributed byte[] stream, a central entity in DSet. 
and [<AllowNullLiteral>]
    [<System.Diagnostics.DebuggerDisplay("{DebuggerDisplay()}")>]
    DStream internal ( cl, assignedName, ver ) as this = 
    inherit DistributedObject( cl, FunctionParamType.DStream, assignedName, ver ) 

    do
        this.WaitForCloseAllStreamsViaHandle <- this.WaitForCloseAllStreamsViaHandleImpl
        this.PrecodeDependentObjs <- this.PrecodeDependentObjsImpl
        this.SetupDependencyHash <- this.SetupDependencyHashImpl
        this.GetParentMapping <- this.GetParentMappingImpl
        this.SyncExecuteDownstream <- this.SyncExecuteDownstreamImpl
        this.SyncPreCloseAllStreams <- this.SyncPreCloseAllStreamsImpl
        this.PostCloseAllStreams <- this.PostCloseAllStreamsImpl
        this.ResetAll <- this.ResetAllImpl
        this.PreBegin <- this.PreBeginImpl
        this.NetworkReady <- this.NetworkReadyImpl

    /// Dependency
    member val internal Dependency = DStreamDependencyType.SourceStream with get, set
    /// Forward Dependency
    member val internal DependencyDownstream = DStreamForwardDependencyType.SinkStream with get, set
    /// Construct a DStream spanned across a cluster, the name and version is randomly generated. 
    internal new ( cl ) = 
        DStream( cl, DeploymentSettings.GetRandomName(), (PerfADateTime.UtcNow()) )
    /// Construct a DStream using the default cluster, the name and version is randomly generated. 
    internal new () = 
        DStream( Cluster.GetCurrent(), DeploymentSettings.GetRandomName(), (PerfADateTime.UtcNow()) )
    // inherit from a DStream, assume that the inherited DStream is an upstream DStream
    internal new ( ds:DStream ) as x = 
        DStream( ds.Cluster, ds.Name, ds.Version )
        then 
            x.CopyFromUpStream( ds )
    /// inherit dstream from another DistributedObject 
    internal new ( dobj:DistributedObject ) as x = 
        DStream( dobj.Cluster, dobj.Name, dobj.Version )    
        then 
            x.Hash <- dobj.Hash
            x.bEncodeMapping <- false
            x.ReplicateBaseMetadata( dobj )
    
    override x.ToString () =
        x.Display false
    member private x.DebuggerDisplay() =
        x.Display true
    member private x.Display showHash =
        if showHash then
            sprintf "%s (DStream) (Hash: %s)" x.Name (BytesToHex(x.Hash))
        else
            sprintf "%s (DStream)" x.Name

    /// Copy DSet meta data from another DSet
    member internal x.CopyFromUpStream( ds ) = 
        x.Hash <- ds.Hash
        x.ReplicateBaseMetadata( ds )
        x.Dependency <- PassFrom ( DependentDStream( ds ) )
        ds.DependencyDownstream <- PassTo( DependentDStream( x ) ) 
    /// Serialization of DStream metadata to Memory Stream. 
    member internal x.Pack( ms: StreamBase<byte>, ?flagPack ) = 
        let mutable flag = defaultArg flagPack DStreamMetadataStorageFlag.None
        // Only take input of HasPassword flag, other flag is igonored. 
        x.PackBase( ms ) 
        // DStream, upstream dependency is the same as UpStreamDependentObjs
        x.EncodeUpStreamDependency( ms )        
    // Encode Dependency 
    member internal x.EncodeUpStreamDependency( ms )= 
        let dependencyCode, depObjects = x.UpStreamDependentObjs()
        let depDSetArray = depObjects |> Seq.toArray
        ms.WriteVInt32( dependencyCode )
        DependentDObject.Pack( depDSetArray, ms ) 
    member internal x.UpStreamDependentObjs() = 
            match x.Dependency with 
            | SourceStream ->
                0, Seq.empty
            | SaveFrom parent -> 
                4, Seq.singleton ( parent  )
            /// Encode from another object
            | EncodeFrom parent ->
                1, Seq.singleton ( parent  )
            /// Passthrough
            | PassFrom parentStream ->
                2, Seq.singleton ( parentStream :> DependentDObject )
            | ReceiveFromNetwork parentStream ->
                3, Seq.singleton ( parentStream :> DependentDObject )
            | MulticastFromNetwork parentStream ->
                5, Seq.singleton ( parentStream :> DependentDObject )
    member private x.PrecodeDependentObjsImpl() =
        let _, dobjs = x.UpStreamDependentObjs()
        dobjs 
    member internal x.DecodeUpStreamDependency( ms:StreamBase<byte> ) = 
        let dependencyCode = ms.ReadVInt32()
        let depDObjectArray = DependentDObject.Unpack( ms )
        match dependencyCode with 
        | 0 -> 
            x.Dependency <- SourceStream
        | 4 -> 
            x.Dependency <- SaveFrom depDObjectArray.[0]
        | 1 -> 
            x.Dependency <- EncodeFrom depDObjectArray.[0]
        | 2 -> 
            x.Dependency <- PassFrom (DependentDStream( depDObjectArray.[0] ))
        | 3 -> 
            x.Dependency <- ReceiveFromNetwork (DependentDStream( depDObjectArray.[0] ))
        | 5 -> 
            x.Dependency <- MulticastFromNetwork (DependentDStream( depDObjectArray.[0] ))
        | _ -> 
            let msg = sprintf "Fail in DSet.DecodeUpStreamDependency, unsupported dependency code %d " dependencyCode
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    member internal x.EncodeDownStreamDependency( ms : StreamBase<byte> ) =
        let dependencyCode, depObjects = x.DownStreamDependentObjs()
        let depDSetArray = depObjects |> Seq.toArray
        ms.WriteVInt32( dependencyCode )
        DependentDObject.Pack( depDSetArray, ms )
    member internal x.DownStreamDependentObjs() = 
            match x.DependencyDownstream with 
            | SinkStream -> 
                0, Seq.empty
            | DecodeTo childObj -> 
                1, Seq.singleton ( childObj )
            /// Passthrough to another stream
            | PassTo childStream -> 
                2, Seq.singleton( childStream :> DependentDObject )
            | SendToNetwork childStream -> 
                3, Seq.singleton( childStream :> DependentDObject )
            | MulticastToNetwork childStream -> 
                4, Seq.singleton( childStream :> DependentDObject )
    member internal x.DecodeDownStreamDepedency( ms:StreamBase<byte> ) = 
        let dependencyCode = ms.ReadVInt32()
        let depDObjectArray = DependentDObject.Unpack( ms )
        match dependencyCode with 
        | 0 -> 
            x.DependencyDownstream <- SinkStream 
        | 1 -> 
            x.DependencyDownstream <- DecodeTo depDObjectArray.[0] 
        | 2 -> 
            x.DependencyDownstream <- PassTo (DependentDStream( depDObjectArray.[0] ))
        | 3 -> 
            x.DependencyDownstream <- SendToNetwork (DependentDStream( depDObjectArray.[0] ))
        | 4 -> 
            x.DependencyDownstream <- MulticastToNetwork (DependentDStream( depDObjectArray.[0] ))
        | _ -> 
            let msg = sprintf "Fail in DStream.DecodeDownStreamDepedency, unsupported dependency code %d " dependencyCode
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    static member val internal UpStreamInfo = [| "SourceStream"; 
                                         "EncodeFrom";
                                         "PassFrom"; 
                                         "ReceiveFromNetwork";
                                         "SaveFrom"; 
                                         "MulticastFromNetwork" |] with get
    static member val internal DownStreamInfo = [| "SinkStream"; 
                                           "DecodeTo"; 
                                           "PassTo"; 
                                           "SendToNetwork"; 
                                           "MulticastToNetwork" |] with get
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
        sprintf "DStream %s:%s %d partitions: %s %s %s %s" 
            x.Name x.VersionString x.NumPartitions
            DStream.UpStreamInfo.[code_upstream] 
            ( objs_upstream |> Seq.map ( fun o -> o.ToString() ) |> String.concat "," )
            DStream.DownStreamInfo.[code_downstream] 
            ( objs_downstream |> Seq.map ( fun o -> o.ToString() ) |> String.concat "," )
    member internal x.DependencyUpstreamInfo()=
        let code_upstream, objs_upstream = x.UpStreamDependentObjs()
        sprintf "%s %s" DStream.UpStreamInfo.[code_upstream] 
            ( objs_upstream |> Seq.map ( fun o -> o.Target.Name ) |> String.concat "," )
    member internal x.DependencyDownstreamInfo()=
        let code_downstream, objs_downstream = x.DownStreamDependentObjs()
        sprintf "%s %s" DStream.DownStreamInfo.[code_downstream] 
            ( objs_downstream |> Seq.map ( fun o -> o.Target.Name ) |> String.concat "," )
    /// Deserialization of DStream 
    static member internal Unpack( readStream: StreamBase<byte>, bUnpackFunc ) = 
        let curStream = DStream()
        //let buf = readStream.GetBuffer()
        let startpos = readStream.Position
        curStream.UnpackBase( readStream ) 
        curStream.DecodeUpStreamDependency( readStream ) 
        let endpos = readStream.Position
        // Recover hash
        curStream.Hash <- readStream.ComputeSHA256(startpos, endpos-startpos)
        //curStream.Hash <- HashByteArrayWithLength( buf, int startpos, int (endpos-startpos) )
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Decode DStream %s:%s, Hash = %s" curStream.Name curStream.VersionString (BytesToHex(curStream.Hash)) ))
        curStream.DecodeDownStreamDepedency( readStream ) 
        curStream.bValidMetadata <- true
        curStream
    member val internal StorageProvider : StorageStream ref = ref null with get
    member val internal StoreStreamArray : StorageStream[] ref = ref null with get
    member val internal BufferedStreamArray = null with get, set
    member val internal StreamForWriteArray: bool[] = null with get, set 
    member internal x.GetStorageProvider() = 
        if Utils.IsNull (!x.StorageProvider) then 
            Interlocked.CompareExchange( x.StorageProvider, StorageStreamBuilder.Create( x.StorageType ), null ) |> ignore
        (!x.StorageProvider)
    member val internal CountFunc = MetaFunction() with get
    member val internal SyncCloseSentCollection = ConcurrentDictionary<int,int>() with get
    /// Reset the stream to be read again
    member internal x.ResetForRead() = 
        x.SyncCloseSentCollection.Clear()
        ()
    member private x.PostCloseAllStreamsImpl( jbInfo ) = 
        x.PostCloseAllStreamsBaseImpl( jbInfo )
        x.CloseAllStreamsThis()
        x.FreeNetwork( jbInfo )
    member internal x.GetStoreStreamArray() = 
        if (Utils.IsNull !x.StoreStreamArray) then 
            x.GetStorageProvider() |> ignore
            if Interlocked.CompareExchange( x.StoreStreamArray, Array.zeroCreate<StorageStream> x.NumPartitions, null ) = null then 
                x.StreamForWriteArray <- Array.create x.NumPartitions false
                x.BufferedStreamArray <- (!x.StoreStreamArray)
        (!x.StoreStreamArray)
    member internal x.GetBufferedStreamArray() = 
        while Utils.IsNull x.BufferedStreamArray do
            // Spin until buffered array is created
            x.GetStoreStreamArray() |> ignore
        x.BufferedStreamArray
    member internal x.BeginWrite( parti ) = 
        // Beginning of the blob, with a GUID of PrajnaBlobBeginMarket
        x.BufferedStreamArray.[parti].Write( DeploymentSettings.BlobBeginMarker.ToByteArray(), 0, 16 )
        // A 4B coding of the format of the blob
        x.BufferedStreamArray.[parti].Write( BitConverter.GetBytes( x.EncodeStorageFlag() ), 0, 4 )
        x.StreamForWriteArray.[parti] <- true
    member internal x.PartitionStreamForWrite( parti ) = 
        let bufferedStreamArray = x.GetBufferedStreamArray() 
        if Utils.IsNull bufferedStreamArray.[parti] then 
            let storeStreamArray = x.GetStoreStreamArray()
            let storageProvider = x.GetStorageProvider()
                // First time a certain partition is written. 
            let fname = x.PartitionFileName( parti ) /// parti.ToString("00000000")+".dat"
            storeStreamArray.[parti] <- storageProvider.Create( x.ConstructDSetPath(), fname )
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Start to write stream %s:%s parti %d" x.Name x.VersionString parti ))
            x.BeginWrite( parti )
        bufferedStreamArray.[parti]
    member internal x.CloseStreamForWrite( parti ) = 
        let bufferedStreamArray = x.GetBufferedStreamArray()
        if Utils.IsNotNull bufferedStreamArray.[parti] then 
            let streamWrite = bufferedStreamArray.[parti]
            streamWrite.Write( BitConverter.GetBytes( 16 ), 0, 4 )
            streamWrite.Write( DeploymentSettings.BlobCloseMarker.ToByteArray(), 0, 16 )
            let endPos = streamWrite.Position
            streamWrite.Flush()
            streamWrite.Close() 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Close stream %s:%s parti %d" x.Name x.VersionString parti ))
            bufferedStreamArray.[parti] <- null
            endPos
        else
            -1L
    member internal x.CloseAllStreamsThis() = 
        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Close All isseud to DStream %s:%s" x.Name x.VersionString ))
        if Utils.IsNotNull (!x.StoreStreamArray) then 
            let bufferedStreamArray = x.GetBufferedStreamArray()
            let unclosedPartition = List<_>()
            for parti = 0 to x.NumPartitions - 1  do 
                if Utils.IsNotNull bufferedStreamArray.[parti] then 
                    unclosedPartition.Add( parti, x.CloseStreamForWrite( parti ) )
            if unclosedPartition.Count > 0 then 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "DStream %s:%s, @PostCloseStream, these partitions are unclosed at location %A" x.Name x.VersionString (unclosedPartition.ToArray)))
        x.StoreStreamArray := null
    /// Get the buffer of write object 
    member internal x.GetWriteBufferAndPos  (writeObj:Object) = 
        match writeObj with 
        | :? StreamBase<byte> as ms -> 
            if ms.Position=ms.Length then
                ms, 0, int ms.Length
            else
                ms, int ms.Position, int (ms.Length-ms.Position)
        | _ -> 
            let msg = sprintf "DStream.SyncWriteChunk has received an unsupported object with type %A" writeObj
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    member val internal SyncWriteLocks = ConcurrentDictionary<int, SpinLockSlim>() with get, set
    member val internal SHA512Provider = ConcurrentDictionary<int,SHA512Managed>() with get, set
    member val internal AesAlg = ConcurrentDictionary<int,AesCryptoServiceProvider >() with get, set
    /// Setup Hash Provider
    member internal x.GetHashProvider( parti: int) = 
        x.SHA512Provider.GetOrAdd( parti, fun _ -> new SHA512Managed() )
    /// Setup Hash Provider
    member internal x.GetAesAlg( parti: int) = 
        x.AesAlg.GetOrAdd( parti, fun _ -> new AesCryptoServiceProvider() )
    /// Setup Crytography provider if not there. 
    member internal x.GetCryptoProvider parti password = 
        let tdes = x.GetAesAlg parti
        let enc = new System.Text.UTF8Encoding( true, true )
        let bytePassword = enc.GetBytes( x.Password )
        let hashPassword = 
            x.GetHashProvider(parti).ComputeHash( bytePassword )
        let trucHash = Array.zeroCreate<byte> ( tdes.KeySize / 8 )
        Buffer.BlockCopy( hashPassword, 0, trucHash, 0, trucHash.Length )
        tdes.Key <- trucHash
        tdes
    /// If password is not null, apply encryption 
    member internal x.EncryptBuffer parti (password:string) (msRcvd:StreamBase<byte>) curBufPos (bufRcvdLen:int) = 
        if Utils.IsNotNull password && password.Length>0 then 
            // Encryption content when save to disk
            let tdes = x.GetAesAlg parti
            tdes.IV <- BitConverter.GetBytes( x.Version.Ticks )
            let msCrypt = new MemStream( bufRcvdLen )
            let cStream = new CryptoStream( msCrypt, tdes.CreateEncryptor(tdes.Key,tdes.IV), CryptoStreamMode.Write)
            let sWriter = new BinaryWriter( cStream ) 
            sWriter.Write( msRcvd.GetBuffer(), curBufPos, (bufRcvdLen - curBufPos) )
            sWriter.Close()
            cStream.Close()
            msRcvd.DecRef()
            ( msCrypt :> StreamBase<byte>, 0, int msCrypt.Length )
        else
            ( msRcvd, curBufPos, bufRcvdLen )    
    // Need to be multithread safe, as it may be called by more than one thread. 
    member internal x.SyncWriteChunk (jbInfo:JobInformation) parti meta writeObj = 
        let lock = x.SyncWriteLocks.GetOrAdd( parti, fun _ -> SpinLockSlim(true))
        try
            lock.Enter()
            if Utils.IsNotNull writeObj then 
                let msRcvd, curBufPos, bufRcvdLen = x.GetWriteBufferAndPos( writeObj )
                let streamPartWrite = x.PartitionStreamForWrite( parti ) 
                let writeMs, writepos, writecount = x.EncryptBuffer parti x.Password msRcvd curBufPos bufRcvdLen

                // Each blob is a certain # of key/value pairs (serialized), and written to stream with the following format:
                //  4B length information. ( Position: there is no hash, negative: there is hash )
                //  Stream to write
                //  optional: hash of the written stream (if x.CurDSet.ConfirmDelivery is true )

                // Calculate hash of the message, if ConfirmDelivery = true
                let resHash = 
                    if x.ConfirmDelivery then 
                        let hash = x.GetHashProvider( parti) 
                        writeMs.ComputeHash(hash, int64 writepos, int64 writecount)
                        //hash.ComputeHash( writeBuf, writepos, writecount )
                    else
                        null
                // JINL, WriteDSet Note, Notice the writeout logic, in which how additional hash are coded. 
                let writeLen = if x.ConfirmDelivery then writecount + resHash.Length else writecount
                let outputLen = if  x.ConfirmDelivery then -writeLen else writeLen
                streamPartWrite.Write( BitConverter.GetBytes( outputLen ), 0, 4 )
                //streamPartWrite.Write( writeBuf, writepos, writecount )
                writeMs.ReadToStream(streamPartWrite, int64 writepos, int64 writecount)
                writeMs.DecRef()
                if x.ConfirmDelivery then 
                    streamPartWrite.Write( resHash, 0, resHash.Length )
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Write out to stream %s:%s parti %d a blob of %dB %s" x.Name x.VersionString parti outputLen (meta.ToString()) ))
            else
                let endPos = x.CloseStreamForWrite( parti ) 
                if endPos>=0L then 
                    // Empty flush, don't report
                    jbInfo.ReportClosePartition( jbInfo, x :> DistributedObject, meta, endPos )
            lock.Exit()
        with
        | e ->
            lock.Exit()
            let msg = sprintf "DStream.SyncWriteChunk %s, write error with exception %A" (meta.ToString()) e 
            Logger.Log( LogLevel.Error, msg )
            failwith msg 
        

    member val private whoInitializeNetwork = ref 0 with get, set
    // Will be used when command is received from network stream. 
    // Change to lock implementation as significant resource is involved. 
    member internal x.IsTowardsCurrentPeer (jbInfo:JobInformation ) ( parti ) = 
        x.NetworkReady(jbInfo)
        if x.CurPeerIndex < 0 then 
            false
        else
            let curmapping = x.GetCurrentMapping()
            curmapping.[parti] = x.CurPeerIndex
    member internal x.FreeNetwork ( jbInfo ) =
        if x.bNetworkInitialized then 
            x.SyncCloseSentCollection.Clear()
            x.Cluster.UnRegisterCallback( jbInfo.JobID, [| ControllerCommand( ControllerVerb.Unknown, ControllerNoun.DStream ) |] )
            x.bSentPeer <- null
            x.bRcvdPeer <- null
            x.bRcvdPeerCloseCalled <- null
            x.bSentPeerCloseConfirmed <- null
            x.DefaultJobInfo <- null
            x.NumActiveConnections <- 0 
            x.bConnected <- null
            x.whoInitializeNetwork := 0 
            x.bNetworkInitialized <- false
            
    member val internal WaitForAllPeerClose = false with get, set
    member val internal AllPeerCloseRcvdEvent = new ManualResetEvent(true) with get, set
    member internal x.SetToWaitForAllPeerCloseRcvd() = 
        x.WaitForAllPeerClose <- true
        x.AllPeerCloseRcvdEvent.Reset() |> ignore

    member private x.ResetAllImpl( jbInfo ) = 
        x.FreeBaseResource( jbInfo )
        x.FreeNetwork( jbInfo )
        x.CountFunc.Reset()
        if x.WaitForAllPeerClose then 
            x.AllPeerCloseRcvdEvent.Reset() |> ignore
        else
            x.AllPeerCloseRcvdEvent.Set() |> ignore  
        x.SyncWriteLocks.Clear()
        x.SHA512Provider.Clear() 
        x.AesAlg.Clear()

    member private x.NetworkReadyImpl( jbInfo: JobInformation ) =     
        if not x.bNetworkInitialized then
            let bExecuteInitialization = x.BaseNetworkReady(jbInfo)
            // This function may be repeated called during a concurrent execution
            if bExecuteInitialization then 
                Debug.Assert( jbInfo.JobID <> Guid.Empty )
                x.Cluster.RegisterCallback( jbInfo.JobID, x.Name, x.Version.Ticks, [| ControllerCommand( ControllerVerb.Unknown, ControllerNoun.DStream ) |],
                    { new NetworkCommandCallback with 
                        member this.Callback( cmd, peeri, ms, jobID, name, verNumber, cl ) = 
                            x.DStreamCallback( cmd, peeri, ms, jobID, name, verNumber )
                    } )
                
    member internal x.CloseNetwork() = 
        ()
    /// Process incoming command
    /// Return: 
    ///     false: blocking, need to resend the command at a later time
    ///     true: command processed. 
    member internal x.ProcessJobCommand( jobAction:SingleJobActionContainer, queuePeer:NetworkCommandQueue, cmd:ControllerCommand, ms:StreamBase<byte>, jobID: Guid ) = 
//        using ( SingleJobActionContainer.TryFind(jobID)) ( fun jobAction -> 
//            if Utils.IsNull jobAction then 
//                Logger.LogF( jobID, LogLevel.Info, ( fun _ -> sprintf "[May be OK, Job cancelled] DStream.Callback, received command %A of payload %dB, but can't find job lifecycle object" 
//                                                                        cmd ms.Length )    )
//                true
//            else
//                try
            let peeri = x.Cluster.SearchForEndPoint( queuePeer )
            Logger.LogF( jobID, LogLevel.WildVerbose, ( fun _ -> sprintf "ProcessJobCommand, Map %A to peer %d" queuePeer.RemoteEndPoint peeri ))
            // Outgoing queue for the reply 
            match ( cmd.Verb, cmd.Noun ) with 
            | ( ControllerVerb.Acknowledge, ControllerNoun.DStream ) ->
                true
            | ( ControllerVerb.SyncWrite, ControllerNoun.DStream ) ->
                let meta = BlobMetadata.Unpack( ms )
                Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Rcvd %A command %A from peer %d with %dB" (meta.ToString()) cmd peeri (ms.Length-ms.Position) ))
                let bRet = x.SyncReceiveFromPeer meta ms peeri 
//                let ev = new ManualResetEvent(false)
//                ev.Reset() |> ignore
//                ev.WaitOne() |> ignore
                if bRet && (DateTime.UtcNow - queuePeer.LastSendTicks).TotalMilliseconds > 5000. then 
//                if bRet then 
                    use msEcho = new MemStream( 128 )
                    msEcho.WriteGuid( jobID )
                    msEcho.WriteString( x.Name )
                    msEcho.WriteInt64( x.Version.Ticks )
                    queuePeer.ToSendNonBlock( ControllerCommand( ControllerVerb.Echo, ControllerNoun.DStream ), msEcho )
                bRet 
            | ( ControllerVerb.SyncClosePartition, ControllerNoun.DStream ) ->
                let meta = BlobMetadata.Unpack( ms )
                Logger.LogF( jobID, LogLevel.WildVerbose, ( fun _ -> sprintf "Rcvd %A command %A from peer %d" (meta.ToString()) cmd peeri ))
                let bRet = x.SyncReceiveFromPeer meta null peeri
                if bRet then 
                    use msEcho = new MemStream( 128 )
                    msEcho.WriteGuid( jobID )
                    msEcho.WriteString( x.Name )
                    msEcho.WriteInt64( x.Version.Ticks )
                    queuePeer.ToSendNonBlock( ControllerCommand( ControllerVerb.ConfirmClosePartition, ControllerNoun.DStream ), msEcho )
                bRet 
            | ( ControllerVerb.SyncClose, ControllerNoun.DStream ) ->
                Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Rcvd command %A from peer %d" cmd peeri ))
                x.SyncCloseFromPeer queuePeer peeri jobID |> ignore
                true

            | _ ->
                let msg = sprintf "DStream.Callback(%s:%s), Unexpected command %A, peer %d" x.Name x.VersionString cmd peeri
                jobAction.ThrowExceptionAtContainer( msg )
                true
//                with 
//                | ex -> 
//                    jobAction.EncounterExceptionAtContainer( ex, sprintf "Error in processing DStream.ProcessJobCommand cmd %A, with payload of %dB" 
//                                                                        cmd ms.Length )
//                    true
//        )
    /// Parse echoing from DStream from a container to other container
    member internal x.DStreamCallback( cmd, peeri, ms, jobID, name, verNumber ) =
        using ( SingleJobActionContainer.TryFind(jobID)) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF( jobID, LogLevel.Info, ( fun _ -> sprintf "[May be Ok, Job cancelled?] DStream.DStreamCallback, received command %A of payload %dB, but can't find job lifecycle object" 
                                                                        cmd ms.Length )    )
                true
            else
                try
                    // Outgoing queue for the reply 
                    let q = x.Cluster.Queue( peeri )
                    match ( cmd.Verb, cmd.Noun ) with 
                    | ( ControllerVerb.Echo, ControllerNoun.DStream ) ->
                        ()
                    | ( ControllerVerb.ConfirmClosePartition, ControllerNoun.DStream ) ->
                        ()
                    | ( ControllerVerb.ConfirmClose, ControllerNoun.DStream ) ->
                        x.bSentPeerCloseConfirmed.[peeri] <- true
                        Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "DStream %s:%s, ConfirmClose received from peer %d" x.Name x.VersionString peeri )    )
                        x.CheckAllPeerClosed()
                    | _ ->
                        Logger.LogF( jobID, LogLevel.Info, ( fun _ -> sprintf "DStream.Callback(%s:%s), Unexpected command %A, peer %d" x.Name x.VersionString cmd peeri )    )
                with 
                | e -> 
                        Logger.LogF( jobID, LogLevel.Info, ( fun _ -> sprintf "Error in processing DStream.Callback(%s:%s) cmd %A, peer %d, with exception %A" x.Name x.VersionString cmd peeri e )    )
                true
        )
    // Parent Mapping
    member private x.GetParentMappingImpl() = 
        match x.Dependency with 
        | SourceStream -> 
            UndefinedMapping
        /// Encode from another object
        | SaveFrom parent 
        | EncodeFrom parent ->
            UseParent parent
        | PassFrom parentS 
        | ReceiveFromNetwork parentS -> 
            UseParent parentS
        | MulticastFromNetwork parentS -> 
            FullMapping
    member val private QueueNumberForOutgoingTasks = Int32.MinValue with get, set
    member val private QueueNumberForIncomingTasks = Int32.MinValue with get, set
        
    /// For Sync End wait
    member private x.SyncPreCloseAllStreamsImpl (jbInfo) = 
        match x.DependencyDownstream with 
        | _ -> 
            if not x.WaitForAllPeerClose then 
                x.BaseSyncPreCloseAllStreams (jbInfo) 
            else
                // Need to wait 
                Logger.LogF( jbInfo.JobID, LogLevel.WildVerbose, (fun _ -> sprintf "SyncPreCloseAllStreams %A %s ReSet CanCloseDownStreamEvent" x.ParamType x.Name))
                x.CanCloseDownstreamEvent.Reset() |> ignore
    member private x.WaitForCloseAllStreamsViaHandleImpl ( waithandles, jbInfo, start ) =
        let contSendCloseDStream() = 
            match x.DependencyDownstream with
            | SinkStream -> 
                x.SyncSendCloseDStreamToAll(jbInfo)
                Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "%A %s:%s SinkStream, WaitForCloseAllStreamsViaHandle Send SyncClose, DStream to all peers " 
                                                                                        x.ParamType x.Name x.VersionString )     )
            | SendToNetwork child 
            | MulticastToNetwork child -> 
                match x.ExecutionDirection with 
                | TraverseDirection.TraverseDownstream -> 
                    x.SyncSendCloseDStreamToAll(jbInfo)
                    Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "%A %s:%s SinkStream, WaitForCloseAllStreamsViaHandle Send SyncClose, DStream to all peers " 
                                                                                            x.ParamType x.Name x.VersionString )     )
                | _ -> 
                    ()
            | _ ->
                ()

        let contReportClose() = 
            Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "%A %s:%s SinkStream, WaitForCloseAllStreamsViaHandle network done" 
                                                                                    x.ParamType x.Name x.VersionString ))
            let finalMetadata = BlobMetadata( Int32.MinValue, Int64.MaxValue, 0 )
            jbInfo.ReportClosePartition( jbInfo, x:> DistributedObject , finalMetadata, 0L )
            Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "%A %s:%s SinkStream, WaitForCloseAllStreamsViaHandle send close report, can close downstream" 
                                                                                    x.ParamType x.Name x.VersionString ))
        let showInfo() = 
            sprintf "Waiting for SyncClose, DStream from & to peers for DStream %s:%s %s " x.Name x.VersionString (x.DependencyDownstreamInfo()) 
        match x.DependencyDownstream with 
        | SinkStream -> 
            x.BaseWaitForUpstreamEvents waithandles contSendCloseDStream
            waithandles.EnqueueWaitHandle showInfo x.AllPeerClosedEvent contReportClose x.CanCloseDownstreamEvent
        | PassTo _ ->
            if not (x.CanCloseDownstreamEvent.WaitOne(0)) then 
                x.BaseWaitForUpstreamEvents waithandles ( fun _ -> () )
                waithandles.EnqueueWaitHandle showInfo x.AllPeerClosedEvent (fun _ -> ()) x.CanCloseDownstreamEvent
                ()
        | DecodeTo _ -> 
            // Something to wait
            if not (x.CanCloseDownstreamEvent.WaitOne(0)) then 
                x.BaseWaitForUpstreamEvents waithandles ( fun _ -> () )
                waithandles.EnqueueWaitHandle showInfo x.AllPeerClosedEvent (fun _ -> ()) x.CanCloseDownstreamEvent
                ()
        | SendToNetwork childStream 
        | MulticastToNetwork childStream -> 
            x.BaseWaitForUpstreamEvents waithandles contSendCloseDStream
            waithandles.EnqueueWaitHandle showInfo x.AllPeerClosedEvent (fun _ -> ()) x.CanCloseDownstreamEvent

    
    member internal x.SyncSendCloseDStreamTask peeri (jbInfo: JobInformation) = 
        let lockValue = x.SyncCloseSentCollection.AddOrUpdate( peeri, (fun k -> 1), (fun k v -> v + 1) )
        if lockValue=1 then 
            let peerQueue = x.CurClusterInfo.QueueForWriteBetweenContainer(peeri)
            if Utils.IsNotNull peerQueue && (not peerQueue.Shutdown) then 
                use msClose = new MemStream( 128 )
                msClose.WriteGuid( jbInfo.JobID )
                msClose.WriteString( x.Name ) 
                msClose.WriteInt64( x.Version.Ticks )
                peerQueue.ToSend( ControllerCommand( ControllerVerb.SyncClose, ControllerNoun.DStream), msClose )
                Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "DStream %s:%s, SyncClose, DStream send to peer %d" x.Name x.VersionString peeri )    )
            else
                let msg = sprintf "DStream.SyncClose, DStream %s:%s, attempt to send SyncClose, DStream to peer %d, but the peer queue has already been shutdown" 
                            x.Name x.VersionString peeri 
                Logger.LogF( jbInfo.JobID, LogLevel.Warning, fun _ -> msg )

    member internal x.SyncSendCloseDStreamToAll(jbInfo: JobInformation) = 
        match x.DependencyDownstream with 
        | SendToNetwork child 
        | MulticastToNetwork child -> 
            let childStream = child.TargetStream
            childStream.NetworkReady( childStream.DefaultJobInfo )
            for peeri = 0 to childStream.Cluster.NumNodes - 1 do 
                if peeri<>childStream.CurPeerIndex then 
                    childStream.SyncSendCloseDStreamTask peeri jbInfo 
            if childStream.CurPeerIndex >=0 then 
                childStream.SyncCloseFromPeer null childStream.CurPeerIndex jbInfo.JobID 
        | SinkStream -> 
            x.NetworkReady( x.DefaultJobInfo )
            for peeri = 0 to x.Cluster.NumNodes - 1 do 
                if peeri<>x.CurPeerIndex then 
                    x.SyncSendCloseDStreamTask peeri jbInfo
            if x.CurPeerIndex >=0 then 
                x.SyncCloseFromPeer null x.CurPeerIndex jbInfo.JobID
        | _ -> 
            ()
            

    member private x.PreBeginImpl( jbInfo, direction ) = 
        x.BasePreBegin( jbInfo, direction ) 
        match x.DependencyDownstream with 
        | SinkStream -> 
            let bClusterReplicate = (x.Cluster.ReplicationType = ClusterReplicationType.ClusterReplicate )
            if not bClusterReplicate then 
                // We will establish 1 queue for each stream to write to, and 1 queue for each network peer at least
                x.NetworkReady( jbInfo ) 
            else
                // No network
                ()
        | PassTo childS
        | SendToNetwork childS 
        | MulticastToNetwork childS -> 
            let childStream = childS.TargetStream
            childStream.NetworkReady( jbInfo ) 
        | DecodeTo child ->
            ()        
    /// Push down operation
//            let msg = sprintf "IterateExecuteDownstream, DecodeTo hasn't been implemented in DStream "
//            Logger.Log(LogLevel.Error, msg) 
//            failwith msg 
        
//        | _ ->
//            x.BaseIterateExecuteDownstream jbInfo parti meta o
    member internal x.AsyncPackageToSend (meta:BlobMetadata) (streamObject:Object) jobID = 
        let metaStream = new MemStream( 128 ) 
        metaStream.WriteGuid( jobID )
        metaStream.WriteString( x.Name )
        metaStream.WriteInt64( x.Version.Ticks ) 
        meta.Pack( metaStream ) 
        if Utils.IsNotNull streamObject then 
            match streamObject with 
            | :? StreamBase<byte> as ms -> 
                ms.InsertBefore( metaStream ) |> ignore
                let msSend = metaStream
                ControllerCommand( ControllerVerb.Write, ControllerNoun.DStream), msSend 
            | _ -> 
                Logger.Fail( sprintf "DStream.AsyncPackageToSend, object pushed is of unknonw type %A, %A" (streamObject.GetType()) streamObject )
        else
            ControllerCommand( ControllerVerb.ClosePartition, ControllerNoun.DStream), metaStream 
    member internal x.SyncPackageToSend (meta:BlobMetadata) (streamObject:Object) jobID = 
        if Utils.IsNotNull streamObject then 
            match streamObject with 
            | :? StreamBase<byte> as ms -> 
                let metaStream = ms.GetNew()
                metaStream.Info <- sprintf "SyncPackageToSend"
                metaStream.WriteGuid( jobID )
                metaStream.WriteString( x.Name )
                metaStream.WriteInt64( x.Version.Ticks ) 
                meta.Pack( metaStream ) 
                ms.InsertBefore( metaStream ) |> ignore
                let msSend = metaStream
                ms.DecRef() // done with this now, metaStream contains  information
                ControllerCommand( ControllerVerb.SyncWrite, ControllerNoun.DStream), msSend 
            | _ -> 
                Logger.Fail( sprintf "DStream.SyncPackageToSend, object pushed is of unknonw type %A, %A" (streamObject.GetType()) streamObject )
        else
            let metaStream = new MemStream( 128 ) 
            metaStream.WriteGuid( jobID )
            metaStream.WriteString( x.Name )
            metaStream.WriteInt64( x.Version.Ticks ) 
            meta.Pack( metaStream ) 
            ControllerCommand( ControllerVerb.SyncClosePartition, ControllerNoun.DStream), metaStream :> StreamBase<byte>
    member internal x.UnpackageFromReceive (bClose) (ms:MemStream)  = 
        let meta = BlobMetadata.Unpack( ms )
        if ( bClose ) then 
            meta, null 
        else
            meta, ms
     /// Flush all write, and then confirmed to the source 
    /// Lock is installed so even if closeFromPeer is called multiple times, it is only flushed once. 
    member internal x.SyncCloseFromPeer queuePeer peeri jobID = 
        if not x.bRcvdPeerCloseCalled.[peeri] then 
            if not x.WaitForAllPeerClose then 
                // Flush partitions associated with peer when a peer clsoe received. 
                let flushPart = List<_>( x.NumPartitions )
                x.bRcvdPeer.[peeri] |> Array.iteri( fun i bWrite -> if ( bWrite) then flushPart.Add(i) )
                let flushArr = flushPart.ToArray()
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DStream %s:%s Rcvd SyncClose, DStream from peer %d, flush partition %A" x.Name x.VersionString peeri flushArr))
                if flushArr.Length > 0 then 
                    for parti in flushArr do 
                    x.SyncFlushPartitionTask parti peeri
                    flushArr |> Array.iter ( fun parti -> x.bRcvdPeer.[peeri].[parti] <- false )
                x.bRcvdPeerCloseCalled.[peeri] <- true
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DStream %s:%s Rcvd SyncClose, DStream from peer %d" x.Name x.VersionString peeri ))
                x.bRcvdPeerCloseCalled.[peeri] <- true
                // Flush all partitions when all all close received. 
                if x.CanFlush() then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "============= Milestone ==================== DStream %s:%s SyncClose, DStream has received from all peers, flush all partitions..." x.Name x.VersionString ))
                    let flushArr = [| 0 .. x.NumPartitions - 1 |] 
                    for parti in flushArr do 
                        x.SyncFlushPartitionTask parti peeri 
                    // Signal all peer has closed. 
                    if Utils.IsNotNull x.AllPeerCloseRcvdEvent then 
                        x.AllPeerCloseRcvdEvent.Set() |> ignore
                    else
                        let msg = sprintf "DStream.CloseFromPeer %s:%s has WaitForAllPeerClose flag, it should have a AllPeerCloseRcvdEvent to be flagged on" x.Name x.VersionString
                        Logger.Log( LogLevel.Error, msg )
                        let jbInfo = x.DefaultJobInfo
                        use msError = new MemStream( 1024 )
                        msError.WriteString( msg )
                        jbInfo.ToSendHost( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
                
            x.CheckAllPeerClosed()
            if Utils.IsNotNull queuePeer then 
                // queuePeer<>null, this is called from same peer 
                use msConfirmClose = new MemStream( 128 )
                msConfirmClose.WriteGuid( jobID )
                msConfirmClose.WriteString( x.Name ) 
                msConfirmClose.WriteInt64( x.Version.Ticks ) 
                queuePeer.ToSend( ControllerCommand( ControllerVerb.ConfirmClose, ControllerNoun.DStream ), msConfirmClose )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DStream %s:%s Send ConfirmClose, DStream to peer %d" x.Name x.VersionString peeri ))

        else
             Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "DStream %s:%s Rcvd SyncClose, DStream from peer %d, but status variable indicated SyncClose has been received from the peer, no action is done" x.Name x.VersionString peeri ))
    member internal x.CanFlush() = 
        if not x.bNetworkInitialized then 
            let msg = sprintf "DStream.CanFlush %s:%s should be called after network is initialized" x.Name x.VersionString
            failwith msg
        else
            let mutable bAllPeerClosed = true
            let peerCloseStatus = Array.create x.Cluster.NumNodes true
            for peeri = 0 to x.Cluster.NumNodes - 1 do 
                if not x.bRcvdPeerCloseCalled.[peeri] then 
                    if peeri<>x.CurPeerIndex then 
                        let peerQueue = x.CurClusterInfo.QueueForWriteBetweenContainer(peeri)
                        if Utils.IsNotNull peerQueue && (not peerQueue.Shutdown) then 
                            bAllPeerClosed <- false    
                            peerCloseStatus.[peeri] <- false
                    else
                        bAllPeerClosed <- false    
                        peerCloseStatus.[peeri] <- false
            let bShowAllPeerClosed = bAllPeerClosed
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Peer CanFlush status %A, peers that rcvd SyncClose %A" bShowAllPeerClosed peerCloseStatus ))
            bAllPeerClosed          
                // A match statement is used as there is some stream which do not flush parti when a final object is encountered. 
    member internal x.SyncReceiveFromPeer (meta:BlobMetadata) (ms:StreamBase<byte>) peeri =
        if not (Utils.IsNull ms) then 
            if x.bRcvdPeerCloseCalled.[peeri] then 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "SyncRcvd %A from peer %d but (SyncClose, DStream) has already been received" (meta.ToString()) peeri ))
            x.bRcvdPeer.[peeri].[meta.Parti] <- true
        // Received packet is fed into the downstream queue of a particular partition
        let jbInfo = x.DefaultJobInfo
        match x.DependencyDownstream with 
        | SinkStream 
        | _ ->
            x.SyncIterateDownstream jbInfo meta.Parti meta ms peeri
            if Utils.IsNull ms then 
                Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "Rcvd %A, received the final object from peer %d" (meta.ToString()) peeri ))
            true
    /// Flush task 
    member internal x.SyncFlushPartitionTask (parti:int) peeri = 
        let jbInfo = x.DefaultJobInfo
        let meta = BlobMetadata( parti, Int64.MinValue, 0 )
        x.SyncReceiveFromPeer meta null peeri |> ignore
    member val private blockSendTime = null with get, set
    member internal x.MonitorSendPeerStatus peeri (peerQueue:NetworkCommandQueue) bCansend = 
        if Utils.IsNull x.blockSendTime then 
            x.blockSendTime <- Array.create x.Cluster.NumNodes (PerfDateTime.UtcNow())
        if bCansend then 
            x.blockSendTime.[peeri] <- (PerfDateTime.UtcNow())
        else
            let t1 = (PerfDateTime.UtcNow())
            let elapseMS = t1.Subtract( x.blockSendTime.[peeri] ).TotalMilliseconds
            if elapseMS > DeploymentSettings.WarningLongBlockingSendSocketInMilliseconds then 
                x.blockSendTime.[peeri] <- t1
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "peer %d Socket %s has been blocked, conection status %A, queuelimit %dB, unprocessed %dB, CanSend:%A, sendQueueSize: %d " 
                                                           peeri (LocalDNS.GetShowInfo(peerQueue.RemoteEndPoint)) peerQueue.ConnectionStatus (x.SendingQueueLimit) peerQueue.UnProcessedCmdInBytes peerQueue.CanSend  peerQueue.SendQueueSize)       )
        ()

    /// Sync Send Peer operation 
    /// This may have problem as if there is no flow control to slow down the sending client, very long queue can pile up at network, which 
    /// will exhuast memory of the machine. 
    /// SyncSendPeer needs to be threadsafe, and we assume that synchronization is done via peerQueue.ToSend
    /// HZ Li: make SyncSendPeer to be threadsafe to ensure flow control works correctly
    /// if there is no flow control, receiver's receive TCP buffer may be full and sender need to retransmit packages and reset the connection eventally. 
    member internal x.SyncSendPeer (jbInfo:JobInformation) parti (meta:BlobMetadata) (streamObject:Object) peeri = 
            let peerQueue = x.CurClusterInfo.QueueForWriteBetweenContainer(peeri)
            if Utils.IsNotNull peerQueue && (not peerQueue.Shutdown) then 
                x.bSentPeer.[peeri] <- true
                // Peer is available 
                let bSend = ref false
                let mutable bForceSend = false

                let cmd, ms = x.SyncPackageToSend meta streamObject (jbInfo.JobID)

                while not (!bSend) && peerQueue.CanSend do 
//                    if Interlocked.CompareExchange (peerQueue.flowcontrol_lock,1,0) = 0 then
////                        #tag: disable flowcontrol
////                         JinL: 06/19/2015, leave flow control to the network layer. 
//                        let bCansend = peerQueue.CanSend && 
//                                         ( 
//                                            (int64(x.SendingQueueLimit) > peerQueue.UnProcessedCmdInBytes + ms.Length)  ||
//                                            ( peerQueue.UnProcessedCmdInBytes = 0L && (int64(x.SendingQueueLimit) < ms.Length  )  ) // to send huge command
//                                         ) 
                        let bCansend = peerQueue.CanSend 

                        x.MonitorSendPeerStatus peeri peerQueue bCansend
                        if ( bCansend || (peerQueue.CanSend && bForceSend) ) then 
                            peerQueue.ToSend( cmd, ms )
                            if (Utils.IsNotNull ms) then
                                ms.DecRef()
                            Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> let len = if Utils.IsNull ms then 0L else ms.Length
                                                                                        sprintf "DStream.SyncSendPeer, Send %A command %A to peer %d with %dB" (meta.ToString()) cmd peeri len ))
                            bSend := true
                        else
                            // Wait for sending window to open 
                            Logger.LogF( jbInfo.JobID, LogLevel.MildVerbose, ( fun _ -> sprintf "DStream.SyncSendPeer, Block on Send %A to peer %d Socket %s status %A, queuelimit %dB, unprocessed %dB, sendqueue Length: %d" 
                                                                                                   (meta.ToString()) peeri 
                                                                                                   (LocalDNS.GetShowInfo(peerQueue.RemoteEndPoint)) peerQueue.ConnectionStatus (x.SendingQueueLimit) peerQueue.UnProcessedCmdInBytes
                                                                                                   peerQueue.SendCommandQueueLength
                                                                                                   ))
                            let blockedTime = ( (PerfADateTime.UtcNow()).Subtract(!peerQueue.flowcontrol_lastack) ).TotalMilliseconds
                            if blockedTime >= 1000. && peerQueue.CanSend &&  peerQueue.RcvdCommandSerial <> !peerQueue.flowcontrol_lastRcvdCommandSerial then
                                Logger.LogF( jbInfo.JobID, LogLevel.Warning, ( fun _ -> sprintf "Send cmd to peer %s has been blocked for %f ms, send an unknown cmd to prevent deadlock, lastRcvdCommandSerial: %d, peerQueue.RcvdCommandSerial: %d" (LocalDNS.GetShowInfo(peerQueue.RemoteEndPoint)) blockedTime !peerQueue.flowcontrol_lastRcvdCommandSerial peerQueue.RcvdCommandSerial))
                                use msSend = new MemoryStreamB()
                                msSend.Info <- "Nothing"
                                peerQueue.ToSend( new ControllerCommand(ControllerVerb.Unknown,ControllerNoun.Unknown), msSend )
                                //bForceSend <- true
                                peerQueue.flowcontrol_lastack := (PerfADateTime.UtcNow())
                                peerQueue.flowcontrol_lastRcvdCommandSerial := peerQueue.RcvdCommandSerial
                        peerQueue.flowcontrol_lock := 0
            else
                Logger.LogF( jbInfo.JobID, LogLevel.Warning, ( fun _ -> sprintf "Send %A to peer %d will be discarded as no valid outgoing peer is present ........ " (meta.ToString()) peeri ))
                // Is this the first time that the peer shuts down? 
                // Count # of active connections. 
                if x.bConnected.[peeri] then 
                    lock ( x.CurClusterInfo.NodesInfo.[peeri] ) ( fun _ -> 
                        if ( x.bConnected.[peeri] ) then 
                            x.bConnected.[peeri] <- false
                            x.NumActiveConnections <- x.NumActiveConnections - 1 )


    /// Iterate Children, in downstream direction. 
    /// The downstream iteration has a push model, in which BlobMetadata*Object are pushed down for result
    /// No function is attached at the end. 
    member internal x.SyncIterateDownstream jbInfo parti meta streamObject peeri = 
        match x.DependencyDownstream with 
        // Push the write object downward
        | PassTo depStream 
        | SendToNetwork depStream
        | MulticastToNetwork depStream -> 
            let childStream = depStream.TargetStream
            childStream.SyncExecuteDownstream jbInfo parti meta (streamObject :> Object)
        /// Push down the object, seeing the input object as MemStream, the corresponding class (DSet, etc.) will decode the object
        | DecodeTo dobj ->
            let childObj = dobj.Target
            childObj.SyncDecodeToDownstream jbInfo parti meta (streamObject :> Object)
        /// SourceStream: the current DStream doesn't depend on other DSet, it is a source 
        | SinkStream ->
            x.SyncPreWrite jbInfo parti meta streamObject
    /// Push down operation
    member private x.SyncExecuteDownstreamImpl jbInfo parti meta o = 
        match x.DependencyDownstream with 
        | SinkStream -> 
            // Write to local and remote streams. 
            let mutable bWriteLocal = false
            let bClusterReplicate = (x.Cluster.ReplicationType = ClusterReplicationType.ClusterReplicate )
            let partimapping = x.PartiMapping( parti ) 
            for peeri in partimapping do 
                if peeri = x.CurPeerIndex then 
                    // Write to local. 
                    // We expect that the object is converted to memstream beforehand. 
                    let ms = if Utils.IsNull o then null else o :?> StreamBase<byte>
                    x.SyncPreWrite jbInfo parti meta ms
                    if Utils.IsNull o then 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DStream SinkStream %s:%s, reach end of SyncExecuteDownstream for parti %d with null object" x.Name x.VersionString parti ))
                else
                    // Async network queue is used 
                    if not bClusterReplicate then
                            x.SyncSendPeer jbInfo parti meta o peeri 
                        // We don't flush network queue (as the network queue are multiplexed), the execution queue will be flushed at close stream. 
                        // x.SendPeer jbInfo parti meta o peeri 
        | PassTo childS
        | SendToNetwork childS -> 
            let childStream = childS.TargetStream
            let curmapping = childStream.GetCurrentMapping()
            let peeri = curmapping.[parti]
            if peeri = childStream.CurPeerIndex then 
                // Write to local. 
                let ms = o :?> StreamBase<byte>
                // Start execution engine
                childStream.SyncReceiveFromPeer meta ms peeri |> ignore
                if Utils.IsNull o then 
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DStream PassTo.SendTo %s:%s, reach end of SyncExecuteDownstream for parti %d with null object" x.Name x.VersionString parti ))
            else
                // Async network queue is used 
                childStream.SyncSendPeer jbInfo parti meta o peeri
                    // We don't flush network queue (as the network queue are multiplexed), the execution queue will be flushed at close stream. 
                    // x.SendPeer jbInfo parti meta o peeri 
                ()
        | MulticastToNetwork childS -> 
            let childStream = childS.TargetStream
            let mapping = childStream.GetMapping()
            let peers = mapping.[parti]
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DStream %s:%s, Multicast to peers %A with blob %A" x.Name x.VersionString peers meta ))
            let ms = o :?> StreamBase<byte>
            for peeri in peers do 
                // Write to local. 
                if peeri = childStream.CurPeerIndex then 
                    // Start execution engine
                    childStream.SyncReceiveFromPeer meta ms peeri |> ignore
                    if Utils.IsNull o then 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "DStream PassTo.SendTo %s:%s, reach end of SyncExecuteDownstream for parti %d with null object" x.Name x.VersionString parti ))
                else
                    // Async network queue is used 
                    ms.AddRef() // syncsendpeer will dec one ref
                    childStream.SyncSendPeer jbInfo parti meta ms peeri
                    // We don't flush network queue (as the network queue are multiplexed), the execution queue will be flushed at close stream. 
                    // x.SendPeer jbInfo parti meta o peeri 
                ()
            ms.DecRef()
        | DecodeTo child ->
            let childDSet = child.Target 
            childDSet.SyncExecuteDownstream jbInfo parti meta o
//            let msg = sprintf "IterateExecuteDownstream, DecodeTo hasn't been implemented in DStream "
//            Logger.Log(LogLevel.Error, msg) 
//            failwith msg 
    member internal x.SyncPreWrite jbInfo parti meta streamObject = 
        if Utils.IsNull streamObject then 
            let writeMeta = x.CountFunc.GetMetadataForPartition( meta, parti, 0 )
            x.SyncWriteChunk jbInfo parti writeMeta null
        else
//            match streamObject with 
//            | :? MemStream as ms -> 
                let ms = streamObject
                // Add serial and numElems as prefix. 
                //let msPrefix = new MemStream( 128 )
                let msPrefix = ms.GetNew()
                msPrefix.WriteInt64( meta.Serial )
                msPrefix.WriteVInt32( meta.NumElems ) 
                //let msCombine = ms.InsertBefore( msPrefix )
                ms.InsertBefore(msPrefix) |> ignore
                ms.DecRef()
                let msCombine = msPrefix
                let writeMeta = x.CountFunc.GetMetadataForPartition( meta, parti, meta.NumElems )
                x.SyncWriteChunk jbInfo parti writeMeta msCombine
//            | _ -> 
//                let msg = sprintf "DStream.SyncPreWrite, object pushed to SinkStream is not unknonw type %A, %A" (streamObject.GetType()) streamObject
//                Logger.Log(LogLevel.Error, msg)
//                failwith msg

    /// All downstream operation completed? 
//    override x.IterateHasCompletedAllDownstreamTask() = 
//        x.HasCompletedAllDownstreamTask() && 
//            match x.DependencyDownstream with 
//            | PassTo depStream 
//            | SendToNetwork depStream -> 
//                depStream.ParentStream.IterateHasCompletedAllDownstreamTask() 
//            | DecodeTo dobj -> 
//                dobj.Parent.IterateHasCompletedAllDownstreamTask() 
//            | SinkStream -> 
//                true
    /// Process feedback of the outgoing command. 
    member internal x.ProcessCallback( cmd:ControllerCommand, peeri:int, ms:StreamBase<byte>, jobID:Guid ) =
        try
            let queue = x.Cluster.Queue( peeri )
            match ( cmd.Verb, cmd.Noun ) with 
            | ( ControllerVerb.Acknowledge, ControllerNoun.DStream ) ->
                ()
            | _ ->
                Logger.Log( LogLevel.Info, ( sprintf "DStream.ProcessCallback: Unexpected command from peer %d, command %A" peeri cmd ))
                ()
        with
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "Error in processing DSet.Callback, cmd %A, peer %d, with exception %A" cmd peeri e )    )
            ()
        // There is no blocking command in this call back. 
        true
(*---------------------------------------------------------------------------
    Interface functions Below
 ---------------------------------------------------------------------------*)
    /// Constract a receiving DStream, and send the content of the current DStream across network. 
    member internal x.SendAcrossNetwork() =
        let sendStream = DStream( x ) 
        x.DependencyDownstream <- SendToNetwork (DependentDStream(sendStream) )
        sendStream.Dependency <- ReceiveFromNetwork ( DependentDStream(x) )
        sendStream
    static member internal sendAcrossNetwork (x:DStream ) = 
        x.SendAcrossNetwork()
    member internal x.MulticastAcrossNetwork() = 
        let sendStream = DStream( x ) 
        x.DependencyDownstream <- MulticastToNetwork (DependentDStream(sendStream) )
        sendStream.Dependency <- MulticastFromNetwork ( DependentDStream(x) )
        sendStream.Mapping <- sendStream.GetMapping()
        sendStream
    static member internal generate (x:DStream ) = 
        ()
