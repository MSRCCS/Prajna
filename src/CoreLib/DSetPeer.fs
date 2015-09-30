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
        DSetPeer.fs
  
    Description: 
        Code for Prajna Client, mainly on class DSetPeer

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Net
open System.Threading
open System.Diagnostics
open System.IO
open System.Net.Sockets
open System.Text.RegularExpressions
open Microsoft.FSharp.Control
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open System.Security.Cryptography

[<AllowNullLiteral>]
type internal OnPeerClose() = 
    member val CallbackOnClose = fun () -> () with get, set

// Extend DSet to DSetPeer, hold special data structure for operations. 
type internal DSetPeerFactory() = 
    inherit CacheFactory<DSetPeer>()
    static member FullName( name, verNumber ) = 
        name + VersionToString( DateTime(verNumber) )
    static member ResolveDSetPeerFullname( fullname ) = 
        match DSetPeerFactory.Retrieve( fullname ) with
        | Some dset ->
            dset
        | None ->
            null
    // Retrieve a DSet
    static member ResolveDSetPeer( name, verNumber ) = 
        let fullname = DSetPeerFactory.FullName( name, verNumber ) 
        DSetPeerFactory.ResolveDSetPeerFullname( fullname ) 
    // Cache a DSet, if there is already a DSet existing in the factory with the same name and version information, then use it. 
    static member CacheDSetPeer( newDSet: DSetPeer ) = 
        if not ( Utils.IsNull newDSet) then 
            let fullname = DSetPeerFactory.FullName( newDSet.Name, newDSet.Version.Ticks ) 
            let oldDSet = DSetPeerFactory.ResolveDSetPeerFullname( fullname )
            if Utils.IsNotNull oldDSet then 
                oldDSet
            else
                DSetPeerFactory.Store( fullname, newDSet )
                newDSet
        else
            null 
    // Resolve to DSet
and [<AllowNullLiteral>]
    /// DSetPeer acts either as source and/or destination DSet. It has dependancyType of StandAlone. Only DSetPeer is allowed to interact with Source Stream or Sinks.
    /// DSetPeer does not have function attached to it. Function must be installed on an upstream/downstream DSet, which can be an identity DSet. 
    internal DSetPeer( cl ) as this= 
    inherit DSet( cl ) 

    do
        this.CloseAllStreams <- this.CloseAllStreamsImpl
        this.PostCloseAllStreams <- this.PostCloseAllStreamsImpl
        this.ResetForRead <- this.ResetForReadImpl
        this.SyncReadChunk <- this.SyncReadChunkImpl
        this.SyncEncode <- this.SyncEncodeImpl

    let mutable bPeerUseForReplicate : bool[] = null
    let mutable bPeerInReplicate : bool[] = null
    let mutable bBeginReplicationWrite = false
    let mutable bEndReplicateCalled = false
    let mutable bSelfCloseDSetReceived = false
    // Each peer replication has the following item. 
    // ms: memory stream to be replicated. 
    // pos: position in which the replication start
    // List<>: a list of peers to be replicated
    // DateTime: time that the item adds to the replication list. 
    // Partition
    // Serial 
    // numElems. 
    let mutable peerReplicationItems = List<StreamBase<byte>*int*List<int>*int64 ref*int*int64*int>()
    let mutable maxEndReplicationWait = Int64.MaxValue
    // JinL: this class is used when PrajnaNode actively replicate content, 
    // This path is not in current use. 
    let mutable callbackRegister: Dictionary<Object, (Object->bool)> = null
    let mutable bCloseDSetSent: bool[] = null
    let mutable bConfirmCloseRcvd: bool[] = null
    let mutable bAllCloseDSetSent = false
    // Write operation that is seen, to detect duplication write. 
    let mutable seenWriteDSet : ConcurrentDictionary<(int64*int),int>[] = null
    /// peer that has written to DSet
    let mutable peerWriteDSet : ConcurrentDictionary<_, _>[] = null
    /// peer that has sent endPartition command
    let mutable peerEndPartition : ConcurrentDictionary<_, _>[] = null
    /// partition size
    let mutable partitionSize: int[] = null
    /// sream size
    let mutable streamLength: int64[] = null
    let mutable lastReplicationTimeoutMsg = Int64.MinValue
    // buffer used to read in chunks. 
    let mutable bufferRead : byte[] = null
    member val HostQueue: NetworkCommandQueue = null with get, set
    member val AllQueues = ConcurrentDictionary<_,_>() with get
    member val StorageProvider : StorageStream = null with get, set
    member val StoreStreamArray : StorageStream[] = null with get, set
#if USE_BUFFEREDSTREAM
    member val BufferedStreamArray: BufferedStream[] = null with get, set
#else
    member val BufferedStreamArray = null with get, set
#endif
    member val StreamForWriteArray: bool[] = null with get, set 
    // member val SHA512Provider : SHA512CryptoServiceProvider = null with get, set
    member val SHA512Provider = ConcurrentDictionary<int,SHA512Managed>() with get, set
    member val AesAlg = ConcurrentDictionary<int,AesCryptoServiceProvider >() with get, set
    member val CurPeerIndex = -1 with get, set
    member val PeerRcvdSpeed = 40000000000L with get, set
    /// Setup Hash Provider
    member x.GetHashProvider( parti: int) = 
        x.SHA512Provider.GetOrAdd( parti, fun _ -> new SHA512Managed() )
    /// Setup Hash Provider
    member x.GetAesAlg( parti: int) = 
        x.AesAlg.GetOrAdd( parti, fun _ -> new AesCryptoServiceProvider () )
    // When Set, DSet is called by peer, we will reset 
    // replication state. 
    member x.Reset() = 
        bBeginReplicationWrite <- false
        bEndReplicateCalled <- false
        bPeerUseForReplicate <- null
        bPeerInReplicate <- null
        maxEndReplicationWait <-  Int64.MaxValue
        callbackRegister <- null
        bCloseDSetSent <- null
        bConfirmCloseRcvd <- null
        bAllCloseDSetSent <- false
        seenWriteDSet <- null
        peerWriteDSet <- null
        peerEndPartition <- null
        partitionSize <- null
        streamLength <- null
        lastReplicationTimeoutMsg <- Int64.MinValue
        lock ( peerReplicationItems) ( fun _ -> peerReplicationItems.Clear() )
        x.SHA512Provider.Clear()
        x.AesAlg.Clear()
    /// Add a Command Queue, check if this is from host
    member x.AddCommandQueue( queue ) = 
        if Utils.IsNotNull queue then 
            let peerIndex = x.Cluster.SearchForEndPoint( queue )
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Incoming socket %A is known as peer %d" queue.RemoteEndPoint peerIndex ))
            x.AllQueues.Item( queue.RemoteEndPointSignature ) <- (queue, peerIndex)
            if peerIndex<0 then 
                // Host called, reset replication state. 
                x.HostQueue <- queue
    /// Remove a Command Queue
    member x.RemoveCommandQueue( queue: NetworkCommandQueue ) = 
        if not (Utils.IsNull queue) then 
            let refValue = ref Unchecked.defaultof<_>
            if x.AllQueues.TryRemove( queue.RemoteEndPointSignature, refValue ) then 
                let _, peerIdx = !refValue
                peerIdx
            else
                Int32.MinValue
        else
            Int32.MinValue
    /// Check for active quueues. 
    member x.RemoveInactiveQueues() = 
        let examineQueues = x.AllQueues |> Seq.toArray
        for pair in examineQueues do
            let queue, _ = pair.Value
            if not (Utils.IsNull queue) && queue.Shutdown then
                let peeridx = x.RemoveCommandQueue( queue )
                if Object.ReferenceEquals( queue, x.HostQueue) then 
                    x.HostQueue <- null
                    // Search for another host queue 
                    for pair in x.AllQueues do
                        let examineQueue, peerIndex = pair.Value
                        if peerIndex = -1 then 
                            x.HostQueue <- examineQueue
                    if Utils.IsNull x.HostQueue then 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Host queue of DSet %s:%s terminates" x.Name x.VersionString )    )


    /// Based on the connection queue that is still alive, compute active partition 
    /// queue: the calling queue, so that queue is being closed. 
    member x.ComputeActivePartition( queue ) = 
        // Remove inactive queues
        x.RemoveInactiveQueues() 
        // Find all active peers
        let bActivePeer = Array.create x.Cluster.NumNodes false
        for pair in x.AllQueues do
            let pairQueue, pairPeerIdx = pair.Value
            if not (Object.ReferenceEquals( queue, pairQueue )) then 
                let idx = if pairPeerIdx<0 then x.CurPeerIndex else pairPeerIdx
                bActivePeer.[idx] <- true
        let bActivePartitions = Array.create (x.NumPartitions) false
        let mapping = x.GetMapping()
        for parti=0 to bActivePartitions.Length-1 do 
            let partimapping = mapping.[parti]
            for peerIdx in partimapping do
                if peerIdx>=0 && bActivePeer.[peerIdx] then 
                    bActivePartitions.[parti] <- true    
        bActivePartitions
    /// Deserlization of dset
    /// A copy of input stream ms is made so that if the deserialization is unsuccessful with some missing argument, the missing argument be requested from communication partner. 
    /// After that, the ms stream can be attempted to be unpacked again. 
    /// Rewritten to use the DSet.Unpack
    static member Unpack( readStream: StreamBase<byte>, bIncludeSavePassword, hostQueue, jobID: Guid ) = 
        let sendStream = new MemStream( 1024 )
        try 
            // Source DSet ( used by DSetPeer ), always do not instantiate function. 
                let curDSet = DSet.Unpack( readStream, false )
//            let storeDSet = DSetPeerFactory.ResolveDSetPeer( curDSet.Name, curDSet.Version.Ticks )
//            let compVal = if Utils.IsNotNull storeDSet then (storeDSet :> IComparable<DSet> ).CompareTo( curDSet ) else 1
//            if compVal=0 then 
//                    // Use storeDSet, release the memory associated with curDSet
//                    // This is usually happen as multiple peers send DSet to the current node
//                    // Confirmation message: send Name + version
//                if Utils.IsNotNull hostQueue then 
//                    storeDSet.AddCommandQueue( hostQueue )
//                sendStream.WriteString( curDSet.Name )
//                sendStream.WriteInt64( curDSet.Version.Ticks )
//                ( Some( storeDSet) , ClientBlockingOn.None, sendStream )
//            else
//                if Utils.IsNotNull storeDSet then 
//                    // Multiple entries of DSetPeer found with same name, but different content 
//                    // We have received an update of the DSet 
//                    DSetPeerFactory.Remove( curDSet.Name + curDSet.VersionString )
                if ( Utils.IsNull curDSet.Cluster ) then 
                    sendStream.WriteGuid( jobID )
                    sendStream.WriteString( curDSet.Name )
                    sendStream.WriteInt64( curDSet.Version.Ticks )
                    ( None, ClientBlockingOn.Cluster, sendStream )
                else
                    let dsetPeer = DSetPeer( curDSet.Cluster )
                    dsetPeer.CopyMetaData( curDSet, DSetMetadataCopyFlag.Copy )
//                    DSetPeerFactory.CacheDSetPeer( dsetPeer )  |> ignore
                    // Confirmation message: send Name + version
                    sendStream.WriteGuid( jobID )
                    sendStream.WriteString( dsetPeer.Name )
                    sendStream.WriteInt64( dsetPeer.Version.Ticks )
                    if Utils.IsNotNull hostQueue then 
                        dsetPeer.AddCommandQueue( hostQueue )
                    ( Some( dsetPeer) , ClientBlockingOn.None, sendStream )
//                    let msg = (sprintf "Multiple DSetPeer with same name %s, but different content found in DSetPeerFactory" (curDSet.Name + curDSet.VersionString) )
//                    Logger.Log(LogLevel.Error, msg)
//                    // Error message is just a string
//                    sendStream.WriteString( msg )
//                    ( None, ClientBlockingOn.Undefined, sendStream )                   
        with       
        | ex -> 
            let msg = (sprintf "Job %A DSetPeer.Unpack encounter exception %A" jobID ex )
            Logger.Log( LogLevel.Error, msg )
            ex.Data.Add( "@DSetPeer.Unpack", msg )
            // Error message is just a string
            sendStream.WriteGuid( jobID )
            sendStream.WriteString( "Unknown" )
            sendStream.WriteInt64( DateTime.MinValue.Ticks )
            sendStream.WriteException( ex )
            ( None, ClientBlockingOn.Undefined, sendStream ) 
        
    /// Read from Meta Data file
    static member ReadFromMetaData( stream:Stream, useHostQueue, jobID ) = 
        let byte = BytesTools.ReadToEnd stream
        let ms = new MemStream( byte, 0, byte.Length, false, true )
        DSetPeer.Unpack( ms, false, useHostQueue, jobID )
    /// Read from Meta Data file
    static member ReadFromMetaData( name, useHostQueue, jobID ) = 
        let byte = FileTools.ReadBytesFromFile name
        let ms = new MemStream( byte, 0, byte.Length, false, true )
        DSetPeer.Unpack( ms, false, useHostQueue, jobID )
    member x.Setup( ) = 
        let msSend = new MemStream( 1024 )
        let nodes = x.Cluster.Nodes
        let curMachineName = Config().MachineName
        let mutable idx = 0
        let mutable bFound = false
        while (idx < nodes.Length && not bFound) do
            if Regex.Match(nodes.[idx].MachineName, "^"+curMachineName+"""(\.|$)""", RegexOptions.IgnoreCase).Success then
                bFound <- true
            // try host entry
            let machineName = Dns.GetHostEntry(nodes.[idx].MachineName)
            if Regex.Match(machineName.HostName, "^"+curMachineName+"""(\.|$)""", RegexOptions.IgnoreCase).Success then
                bFound <- true
            if (not bFound) then
                idx <- idx + 1
        //while idx < nodes.Length && String.Compare( nodes.[idx].MachineName, curMachineName, true )<>0 do 
        //    idx <- idx + 1
        if idx >= nodes.Length then 
            let msg = sprintf "Failed to find the current machine %s among the cluster list %A" curMachineName nodes
            Logger.Log( LogLevel.Error, msg )
            msSend.WriteString( msg )
            x.CurPeerIndex <- -1
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msSend )    
        else
            x.CurPeerIndex <- idx
            msSend.WriteString( x.Name ) 
            msSend.WriteInt64( x.Version.Ticks )
            ( ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.DSet ), msSend )
    

    /// Construct meta Data name.
    member x.MetaDataName() = 
        sprintf "DSet%08d.meta" x.MetaDataVersion

    /// Construct storage provider if not exist   
    member x.GetStorageProvider() = 
        if Utils.IsNull x.StorageProvider then 
            lock ( x ) ( fun _ -> 
                if Utils.IsNull x.StorageProvider then 
                    x.StorageProvider <- StorageStreamBuilder.Create( x.StorageType )
            )
        x.StorageProvider     
    /// Is a certain partition exist?
    member x.PartitionExist( partitions ) = 
        // True for empty partition
        let bEmptyPartiton = partitions |> Array.map ( fun parti -> x.EmptyPartition( parti ) )
        let nonEmptyPartition = partitions |> Array.filter( fun parti -> not (x.EmptyPartition( parti )) )
        let filenames = nonEmptyPartition |> Seq.map ( fun parti -> x.PartitionFileName( parti ) ) |> Seq.toArray
        let bFileExists = x.GetStorageProvider().Exist( x.ConstructDSetPath(), filenames )
        let mutable j = 0 
        for i=0 to bEmptyPartiton.Length - 1 do 
            if not bEmptyPartiton.[i] then 
                bEmptyPartiton.[i] <- bFileExists.[j]
                j <- j + 1
        bEmptyPartiton
    /// Open a partition for read
    /// Return: True: first time that the store stream is used
    ///         False: not first time. 
    member x.ReadyStoreStreamArray( ) = 
        if ( Utils.IsNull x.StoreStreamArray  || Utils.IsNull x.BufferedStreamArray || Utils.IsNull x.StreamForWriteArray ) then 
            x.GetStorageProvider() |> ignore
            lock ( x ) ( fun _ ->
                if Utils.IsNull x.StoreStreamArray then 
                    x.StoreStreamArray <- Array.zeroCreate<StorageStream> x.NumPartitions 
#if USE_BUFFEREDSTREAM
                    x.BufferedStreamArray <- Array.zeroCreate<BufferedStream> x.NumPartitions     
#else
                    x.BufferedStreamArray <- x.StoreStreamArray
#endif
                    x.StreamForWriteArray <- Array.create x.NumPartitions false
                    true
                else
                    false
            )
        else
            false
    member x.PartitionStreamForRead( parti ) = 
        x.ReadyStoreStreamArray( ) |> ignore
        if Utils.IsNull x.BufferedStreamArray.[parti] then 
            let fname = x.PartitionFileName( parti )
            x.StoreStreamArray.[parti] <- x.StorageProvider.Open( x.ConstructDSetPath(), fname )
            if Utils.IsNotNull x.StoreStreamArray.[parti] then 
#if USE_BUFFEREDSTREAM
                x.BufferedStreamArray.[parti] <- new BufferedStream( x.StoreStreamArray.[parti], x.StorageProvider.RecommendedBufferSize )
#endif
                x.StreamForWriteArray.[parti] <- false
        x.BufferedStreamArray.[parti]
    // Create a stream for read that is job dependent (not shared between job). 
    member x.PartitionStreamForReadInJob jbInfo parti = 
        x.ReadyStoreStreamArray( ) |> ignore
        let fname = x.PartitionFileName( parti )
        x.StorageProvider.Open( x.ConstructDSetPath(), fname )
    // Close partition stream
    member x.ClosePartitionStreamForRead (stream:StorageStream) jbInfo parti = 
        stream.Close()
        stream.Dispose() 
    member val NumReadJobs = 0 with get, set
    member private x.ResetForReadImpl( queue ) = 
        lock( x ) ( fun _ -> 
            x.NumReadJobs <- x.NumReadJobs + 1
            if x.NumReadJobs = 1 then 
                x.HostQueue <- queue 
                x.CloseStorageProvider()
            )
           
    member x.PartitionStreamForWrite( parti ) = 
        if Utils.IsNull x.BufferedStreamArray.[parti] then 
            // First time a certain partition is written. 
            let fname = x.PartitionFileName( parti ) /// parti.ToString("00000000")+".dat"
            x.StoreStreamArray.[parti] <- x.StorageProvider.Create( x.ConstructDSetPath(), fname )
#if USE_BUFFEREDSTREAM
            x.BufferedStreamArray.[parti] <- new BufferedStream( x.StoreStreamArray.[parti], x.StorageProvider.RecommendedBufferSize )
#endif
            // Beginning of the blob, with a GUID of PrajnaBlobBeginMarket
            x.BufferedStreamArray.[parti].Write( DeploymentSettings.BlobBeginMarker.ToByteArray(), 0, 16 )
            // A 4B coding of the format of the blob
            x.BufferedStreamArray.[parti].Write( BitConverter.GetBytes( x.EncodeStorageFlag() ), 0, 4 )
            x.StreamForWriteArray.[parti] <- true
        x.BufferedStreamArray.[parti]
    member x.WriteDSetMetadata() = 
        if x.StorageProvider.IsLocal then 
        // We may create a local stream to persist the MetaData of DSet
            let file = x.StorageProvider.Choose( x.ConstructDSetPath(), x.MetaDataName() )
            x.SaveToMetaData( file, DSetMetadataStorageFlag.None )
        
    member x.WriteDSet( ms: StreamBase<byte>, queue:NetworkCommandQueue, bReplicateWrite ) =
        let msSend = new MemStream( 1024 )
        let retCmd = ControllerCommand( ControllerVerb.EchoReturn, ControllerNoun.DSet )
        msSend.WriteString( x.Name )
        msSend.WriteInt64( x.Version.Ticks )
        try
//            if Utils.IsNull x.StorageProvider then 
//            // This is the first time WriteDSet is called by any parition, we should persist the DSet meta data. 
//                x.StorageProvider <- StorageStreamBuilder.Create( x.StorageType )
//                x.StoreStreamArray <- Array.zeroCreate<StorageStream> x.NumPartitions 
//                x.BufferedStreamArray <- Array.zeroCreate<BufferedStream> x.NumPartitions 
            if x.ReadyStoreStreamArray() then 
                x.WriteDSetMetadata()
            // Get a copy of the message, before it is parsed. If we need to forward the message, we have the pointer. 
            //let bufRcvd = ms.GetBuffer()
            let bufPos = int ms.Position
            let bufRcvdLen = int ms.Length
            // Recover the parti <partition> written out at DSet.Store
            let parti = ms.ReadVInt32()
            let curBufPos = int ms.Position
            // Write DSet format
            // Name, Version, partition, serial, number of KVs
            // serial, number of KVs will be read out & parsed, but still written to the stored stream. 
            let serial = ms.ReadInt64()
            let numElems = ms.ReadVInt32()

            try

                if Utils.IsNull peerWriteDSet then 
                    lock ( x ) ( fun _ -> 
                        if Utils.IsNull peerWriteDSet then 
                            peerWriteDSet <- Array.init x.NumPartitions ( fun _ -> ConcurrentDictionary<_,_>() )
                    )
                peerWriteDSet.[parti].GetOrAdd( queue.RemoteEndPointSignature, true ) |> ignore
                // Written confirmation
                msSend.WriteVInt32( parti ) 
                msSend.WriteInt64( serial )
                msSend.WriteVInt32( numElems )
                msSend.WriteVInt32( x.CurPeerIndex )

                if Utils.IsNull seenWriteDSet then 
                    lock( x ) ( fun _ -> 
                        if Utils.IsNull seenWriteDSet then 
                            seenWriteDSet <- Array.init x.NumPartitions ( fun _ -> ConcurrentDictionary<_,_>() )    
                    )
                if Utils.IsNull seenWriteDSet.[parti] then 
                    let msg = sprintf "Attempt to Write, DSet after CloseStream has been called: partition %d, serial %d:%d" parti serial numElems 
                    Logger.Log( LogLevel.Error, msg )
                    let msError = new MemStream( 1024 )
                    msError.WriteString( (UtcNowToString()) + ":" + msg )
                    ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message), msError ) 
                elif seenWriteDSet.[parti].ContainsKey( serial, bufRcvdLen-curBufPos ) then 
                    // Deduplicate DSet received
                    // supress write & duplicate
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Duplicate DSet encountered for Write, DSet operation: partition %d, serial %d:%d" parti serial numElems ))
                    ( ControllerCommand( ControllerVerb.Duplicate, ControllerNoun.DSet), msSend ) 
                else
                    seenWriteDSet.[parti].Item( (serial, bufRcvdLen-curBufPos) ) <- numElems
    //                if Utils.IsNull x.BufferedStreamArray.[parti] then 
    //                    // First time a certain partition is written. 
    //                    let fname = x.PartitionFileName( parti ) /// parti.ToString("00000000")+".dat"
    //                    x.StoreStreamArray.[parti] <- x.StorageProvider.Create( x.ConstructDSetPath(), fname )
    //                    x.BufferedStreamArray.[parti] <- new BufferedStream( x.StoreStreamArray.[parti], x.StorageProvider.RecommendedBufferSize )
    //                    // Beginning of the blob, with a GUID of PrajnaBlobBeginMarket
    //                    x.BufferedStreamArray.[parti].Write( DeploymentSettings.BlobBeginMarker.ToByteArray(), 0, 16 )
    //                    // A 4B coding of the format of the blob
    //                    x.BufferedStreamArray.[parti].Write( BitConverter.GetBytes( x.EncodeStorageFlag() ), 0, 4 )
                    let streamPartWrite = x.PartitionStreamForWrite( parti ) 
                    // Each blob is a certain # of key/value pairs (serialized), and written to stream with the following format:
                    //  4B length information. ( Position: there is no hash, negative: there is hash )
                    //  Stream to write
                    //  optional: hash of the written stream (if x.CurDSet.ConfirmDelivery is true )
                    let writeMs, writepos, writecount = 
                        if Utils.IsNotNull x.Password && x.Password.Length>0 then 
                            // Encryption content when save to disk
                            let tdes = x.GetAesAlg parti
                            let enc = new System.Text.UTF8Encoding( true, true )
                            let bytePassword = enc.GetBytes( x.Password )
                            let hashPassword = 
                                x.GetHashProvider( parti ).ComputeHash( bytePassword )
                            let trucHash = Array.zeroCreate<byte> ( tdes.KeySize / 8 )
                            Buffer.BlockCopy( hashPassword, 0, trucHash, 0, trucHash.Length )
                            tdes.Key <- trucHash
                            tdes.IV <- BitConverter.GetBytes( x.Version.Ticks )
                            let msCrypt = new MemStream( bufRcvdLen )
                            let cStream = new CryptoStream( msCrypt, tdes.CreateEncryptor(tdes.Key,tdes.IV), CryptoStreamMode.Write)
                            let sWriter = new BinaryWriter( cStream ) 
                            sWriter.Write( ms.GetBuffer(), curBufPos, (bufRcvdLen - curBufPos) )
                            sWriter.Close()
                            cStream.Close()
                            ( msCrypt :> StreamBase<byte>, 0, int msCrypt.Length )
                        else
                            ( ms, curBufPos, bufRcvdLen - curBufPos )
                    // Calculate hash of the message, if ConfirmDelivery = true
                    let resHash = 
                        if x.ConfirmDelivery then 
                            //x.GetHashProvider( parti ).ComputeHash( writeBuf, writepos, writecount )
                            writeMs.ComputeHash(x.GetHashProvider(parti), int64 writepos, int64 writecount)
                        else
                            null
                    // JINL, WriteDSet Note, Notice the writeout logic, in which how additional hash are coded. 
                    let writeLen = if x.ConfirmDelivery then writecount + resHash.Length else writecount
                    let outputLen = if  x.ConfirmDelivery then -writeLen else writeLen
                    streamPartWrite.Write( BitConverter.GetBytes( outputLen ), 0, 4 )
                    //streamPartWrite.Write( writeBuf, writepos, writecount )
                    writeMs.ReadToStream(streamPartWrite, int64 writepos, int64 writecount)
                    // Written confirmation
                    Logger.Log( LogLevel.MediumVerbose, ( sprintf "Write DSet partition %d, serial %d:%d (rep:%A)" parti serial numElems bReplicateWrite))
                    if x.ConfirmDelivery then 
                        streamPartWrite.Write( resHash, 0, resHash.Length )
                    if x.ConfirmDelivery then 
                        msSend.Write( resHash, 0, resHash.Length )
                    // JinL: 02/25/2014, disabled Flush.
                    // streamPartWrite.Flush()
                    if bReplicateWrite then 
                        if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                            let msInfo = new MemStream( 1024 )
                            msInfo.WriteString( x.Name )
                            msInfo.WriteInt64( x.Version.Ticks )
                            msInfo.WriteVInt32( parti ) 
                            msInfo.WriteInt64( serial )
                            msInfo.WriteVInt32( numElems )
                            msInfo.WriteVInt32( x.CurPeerIndex )
                            x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Echo2, ControllerNoun.DSet ), msInfo )
                        ( ControllerCommand( ControllerVerb.EchoReturn, ControllerNoun.DSet), msSend ) 
                    else
                        ( ControllerCommand( ControllerVerb.EchoReturn, ControllerNoun.DSet), msSend ) 
            with
            | e ->
                let msError = new MemStream( 1024 )
                let msg = sprintf "Write DSet Error (inner loop), part %d, serial %d:%d with exception %A" parti serial numElems e 
                Logger.Log( LogLevel.Error, msg )
                msError.WriteString( msg ) 
                ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message), msError ) 
        with
        | e ->
            let msError = new MemStream( 1024 )
            let msg = sprintf "Write DSet Error (outer loop), with exception %A" e 
            Logger.Log( LogLevel.Error, msg )
            msError.WriteString( msg ) 
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message), msError ) 
    member x.CloseStream( parti ) = 
        if Utils.IsNotNull x.BufferedStreamArray then 
            if Utils.IsNotNull x.BufferedStreamArray.[parti] then 
                lock ( x.StoreStreamArray.[parti] ) ( fun _ ->
                    // ToDO: do we need to prevent multiple dispose? just be safe. 
                    if Utils.IsNotNull x.BufferedStreamArray.[parti] then 
                    // Write a blob of Guid of PrajnaBlobCloseMarker to the end of each parition 
                        if x.StreamForWriteArray.[parti] then 
                            x.BufferedStreamArray.[parti].Write( BitConverter.GetBytes( 16 ), 0, 4 )
                            x.BufferedStreamArray.[parti].Write( DeploymentSettings.BlobCloseMarker.ToByteArray(), 0, 16 )
                            let lastpos = x.BufferedStreamArray.[parti].Position
                            x.BufferedStreamArray.[parti].Flush()
                            if Utils.IsNull partitionSize then 
                                partitionSize <- Array.create x.NumPartitions -1 
                            if Utils.IsNull streamLength then 
                                streamLength <- Array.create x.NumPartitions 0L
                            if Utils.IsNull seenWriteDSet || Utils.IsNull (seenWriteDSet.[parti]) then 
                                partitionSize.[parti] <- 0 
                                streamLength.[parti] <- 0L
                            else    
                                // Number of key values in a partition
                                partitionSize.[parti] <- seenWriteDSet.[parti].Values |> Seq.fold ( fun sum v -> sum + v ) 0
                                streamLength.[parti] <- lastpos
                                seenWriteDSet.[parti] <- null
        #if USE_BUFFEREDSTREAM
                            x.StoreStreamArray.[parti].Flush()
                        x.BufferedStreamArray.[parti].Close()
        #endif
                        x.StoreStreamArray.[parti].Close()
                        x.StoreStreamArray.[parti].Dispose() // Free resource )
                        x.StoreStreamArray.[parti] <- null
                        x.BufferedStreamArray.[parti] <- null
                        x.StreamForWriteArray.[parti] <- false
                )    
    member x.CloseStorageProvider() = 
        x.StoreStreamArray <- null
        x.BufferedStreamArray<- null 
        x.SHA512Provider.Clear()
        x.AesAlg.Clear()
        if Utils.IsNotNull x.StorageProvider then 
            x.StorageProvider <- null  
    /// Compute active partitions, close streams that are no longer active (i.e., all peers that potentially can write to the partition has called close)
    /// Return: 
    ///     bool[] indicating whether a partition is still active. 
    member x.CloseNonActiveStreams( queue ) = 
        let bActivePartitions = x.ComputeActivePartition( queue )
        let mutable bAllClosed = true
        for parti=0 to bActivePartitions.Length-1 do
            if not bActivePartitions.[parti] then 
                x.CloseStream( parti ) 
            else
                bAllClosed <- false
        if bAllClosed then 
            x.CloseStorageProvider()
        bActivePartitions
    /// EndPartition: mark the end of partition i. 
    member x.EndPartition( ms: StreamBase<byte>, queue:NetworkCommandQueue, parti, callback ) =
//        Logger.LogF( LogLevel.WildVerbose,  fun _ -> sprintf "Receiving End, Partition on partition %d to peer %s" parti (LocalDNS.GetShowInfo( queue.RemoteEndPoint) ) )
        let msSend = new MemStream( 1024 )
        let retCmd = ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.DSet )
        msSend.WriteString( x.Name )
        msSend.WriteInt64( x.Version.Ticks )
        try 
            if Utils.IsNull peerEndPartition then 
                lock ( x ) ( fun _ -> 
                    if Utils.IsNull peerEndPartition then 
                        peerEndPartition <- Array.init x.NumPartitions ( fun _ -> ConcurrentDictionary<_,_>() )
                )
            peerEndPartition.[parti].Item( queue.RemoteEndPointSignature ) <- true
            // remove entry 
            if Utils.IsNotNull peerWriteDSet && peerWriteDSet.[parti].ContainsKey( queue.RemoteEndPointSignature ) then 
                let bRemove, _ = peerWriteDSet.[parti].TryRemove( queue.RemoteEndPointSignature ) 
                // Close the partition when # of writing queues reach 0 
                if bRemove && peerWriteDSet.[parti].IsEmpty then 
                    x.CloseStream( parti ) 
            ( retCmd, msSend )
        with
        | e ->
            let msError = new MemStream( 1024 )
            let msg = sprintf "EndPartition encounter exception %A " e
            Logger.Log( LogLevel.Error, msg )
            msError.WriteString( msg )
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )

    /// Close DSet
    /// bReplicate = true,  ReplicateClose, DSet is called (from other peer)
    ///            = false, Close, DSet is called (from host) 
    /// no need to further call to ReplicateClose. 
    member x.CloseDSet( jobID, ms: StreamBase<byte> , queue:NetworkCommandQueue, bReplicate, callback ) =
        // the stream will be disposed in the calling App
        let msSend = new MemStream( 1024 )
        let retCmd = ControllerCommand( ControllerVerb.Close, ControllerNoun.DSet )
        msSend.WriteGuid( jobID )
        msSend.WriteString( x.Name )
        msSend.WriteInt64( x.Version.Ticks )
        Logger.LogF( jobID, LogLevel.MildVerbose, ( fun _ -> sprintf "Close DSet received for DSet %s:%s" x.Name x.VersionString ))
        if not bReplicate then 
            bSelfCloseDSetReceived <- true    
        try 
            let bActivePartitions = x.CloseNonActiveStreams( queue )

            if bBeginReplicationWrite && not bReplicate then 
                /// A DSet may have multiple host, the ReplicateClose will only be sent when the last host declares CloseDSet
                x.EndReplicate( queue, x.TimeoutLimit, callback )

            if bReplicate then 
                ( ControllerCommand( ControllerVerb.Close, ControllerNoun.DSet ), msSend )
            else
                // Close, DSet to send after all replicating stream has been confirmed closed. 
                if bBeginReplicationWrite then 
                    ( ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.DSet ), msSend )
                else
                    // Write a summary report 
                    if Utils.IsNull partitionSize then 
                        msSend.WriteVInt32( 0 ) 
                    else
                        // Find valid partition that is written or closed. 
                        let activePartitions = List<_>() 
                        for i = 0 to x.NumPartitions - 1 do 
                            if partitionSize.[i]>=0 then 
                                // written or closed. 
                                activePartitions.Add( i, partitionSize.[i], streamLength.[i] )
                        msSend.WriteVInt32( activePartitions.Count ) 
                        for tuple in activePartitions do 
                            let parti, partsize, streamLength = tuple
                            msSend.WriteVInt32( parti ) 
                            msSend.WriteVInt32( partsize ) 
                            msSend.WriteInt64( streamLength )
                    x.Reset()
                    ( ControllerCommand( ControllerVerb.Report, ControllerNoun.DSet ), msSend )
        with
        | e ->
            let msError = new MemStream( 1024 )
            let msg = sprintf "Close DSet encounter exception %A " e
            Logger.Log( LogLevel.Error, msg )
            msError.WriteString( msg )
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
    member private x.PostCloseAllStreamsImpl( jbInfo ) = 
        x.PostCloseAllStreamsBaseImpl( jbInfo )
        x.CloseAllStreams( true )
    member private x.CloseAllStreamsImpl( bUpStream ) = 
        lock( x ) ( fun _ -> 
        x.NumReadJobs <- Math.Max( x.NumReadJobs - 1, 0 )
        if x.NumReadJobs = 0 then 
            for parti=0 to x.NumPartitions-1 do
                x.CloseStream( parti )
            x.CloseStorageProvider()   
        )

//    interface OnPeerClose with
//        member x.CallbackOnClose() = 
//            x.CloseAllStreams( true )

    member x.CallbackOnClose() = 
        x.CloseAllStreams( true )

    /// BeginReplicationWrite: called to begin replication operation of a DSet
    member x.PrepareReplicationWrite (parti) = 
        // First call, preparing data structure for replicating DSet
        if not bBeginReplicationWrite then 
            bBeginReplicationWrite <- true
            bPeerUseForReplicate <- Array.create x.Cluster.NumNodes false
            bPeerInReplicate <- Array.create x.Cluster.NumNodes false
            let mapping = x.GetMapping()
            for parti = 0 to x.NumPartitions-1 do
                let partimapping = mapping.[parti]
                // is current peer exist in this partition?
                let bCurPeerExist = partimapping |> Array.fold ( fun state n -> state || (n=x.CurPeerIndex) ) false
                if bCurPeerExist then 
                    partimapping |>  Array.iter ( fun n -> bPeerUseForReplicate.[n] <- true )
            // Current peer can't be counted
            let peerList = List<int>()
            bPeerUseForReplicate.[x.CurPeerIndex] <- false
            x.NumActiveConnection <- 0
            for i = 0 to bPeerUseForReplicate.Length-1 do
                if bPeerUseForReplicate.[i] then 
                    // This triggers a connect to the corresponding peer, even nothing has been written out yet. 
                    x.Cluster.QueueForWrite( i ) |> ignore
                    peerList.Add(i) 
                    x.NumActiveConnection <- x.NumActiveConnection + 1
            x.FirstCommand <- Array.create bPeerUseForReplicate.Length true
            // Clear all replication items
            lock ( peerReplicationItems) ( fun _ -> peerReplicationItems.Clear() )
            // Register call back 
            if x.NumActiveConnection > 0 then 
                using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
                    if Utils.IsNotNull jobObj then 
                        let currentWriteID = jobObj.JobID
                        x.Cluster.RegisterCallback( currentWriteID, x.Name, x.Version.Ticks, x.CallbackCommand, 
                            { new NetworkCommandCallback with 
                                member this.Callback( cmd, peeri, ms, jobID, name, ver, cl ) = 
                                    x.ReplicateDSetCallback( cmd, peeri, ms, jobID )
                            } )
                    )

            let extremeTrack() = 
                let msg = sprintf "Peer %d: try connect to ... %A" x.CurPeerIndex peerList
                if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                    using ( new MemStream(1024) ) ( fun msInfo -> 
                    msInfo.WriteString( msg )
                    x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Verbose, ControllerNoun.Message), msInfo )
                    )
                msg
            Logger.LogF( LogLevel.ExtremeVerbose, extremeTrack )
        ()
    // Replicating ms Stream, with start position bufPos to the replicating peers. 
    member x.ReplicateDSet( ms: StreamBase<byte>, bufPos  ) = 
        //let bufRcvd = ms.GetBuffer()
        //let bufRcvdLen = int ms.Length
        //let curPos = int ms.Position
        // Parsing should start at the current position
        // replication need to restart from bufPos. 
        //let peekStream = new MemStream( bufRcvd, curPos, bufRcvdLen-curPos, false, true )
        let peekStream = new MemStream()
        peekStream.AppendNoCopy(ms, ms.Position, ms.Length-ms.Position)
        let parti = peekStream.ReadVInt32()
        let serial = peekStream.ReadInt64()
        let numElems = peekStream.ReadVInt32()
        x.PrepareReplicationWrite(parti)
        if parti>=0 && parti<x.NumPartitions then 
            let partimapping = x.GetMapping().[parti]
            let mutable bCurPeerFind = false
            let replicationPeerList = List<int>()
            for i=0 to partimapping.Length-1 do
                if partimapping.[i]=x.CurPeerIndex then 
                    bCurPeerFind <- true
                else 
                    if partimapping.[i]>=0 && partimapping.[i] < x.Cluster.NumNodes then 
                        // valid peer
                        replicationPeerList.Add( partimapping.[i] )
            // If there is any replication
            if replicationPeerList.Count>0 then 
                lock (peerReplicationItems) ( fun _ -> peerReplicationItems.Add( ms, bufPos, replicationPeerList, ref x.Clock.ElapsedTicks, parti, serial, numElems ) )
            x.TryReplicate()
        else
            let msError = new MemStream( 1024 )
            let msg = sprintf "Replicated partition %d is outside of legal range" parti 
            Logger.Log( LogLevel.Error, msg )
            msError.WriteString( msg )
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
    /// Try to sent out replication stream
    member x.TryReplicate( ) = 
        let peerTimeOut = List<int>() 
        let emptyReplicationItem = List<int>()
        lock (peerReplicationItems) ( fun _ ->
            let peerLastActiveTime = Array.create x.Cluster.NumNodes Int64.MinValue
            for i=0 to peerReplicationItems.Count-1 do 
                let repItem = peerReplicationItems.Item(i)
                let streamToReplicate, streamPos, peerList, refTime, parti, serial, numElems = repItem
                let repPeer = peerList.ToArray()
                for peeri in repPeer do 
                    let peerQueue = x.Cluster.QueueForWrite(peeri)
                    if Utils.IsNotNull peerQueue && (not peerQueue.Shutdown) then 
                        // A certain peer is not active, and replication fails. 
                        // However, this is considered OK in replication, the host should try to repair. 
                        //if peerQueue.CanSend && peerQueue.SendQueueLength<5 && int peerQueue.UnProcessedCmdInBytes <= x.SendingQueueLimit / x.NumActiveConnection then 
                        if peerQueue.CanSend then
                            let bFirstCommand = x.FirstCommand.[peeri] 
                            x.DoFirstWrite( peeri )
                            bPeerInReplicate.[peeri] <- true
                            peerQueue.SetRcvdSpeed(x.PeerRcvdSpeed)
                            if bFirstCommand && Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                                let connectedPeerList = List<int>()
                                for i=0 to x.FirstCommand.Length-1 do
                                    if not x.FirstCommand.[i] && i<>x.CurPeerIndex then 
                                        connectedPeerList.Add(i)    
                                // Connection status check 
                                let extremeTrack() =
                                    let msg = sprintf "Connected from peer %d --> %s" x.CurPeerIndex ( SeqToString( ";", connectedPeerList ) )
                                    let msInfo = new MemStream( 1024 )
                                    msInfo.WriteString( msg )
                                    if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                                        x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Verbose, ControllerNoun.Message ), msInfo )
                                    msg
                                Logger.LogF( LogLevel.ExtremeVerbose, extremeTrack)

                            peerQueue.ToSendFromPos( ControllerCommand( ControllerVerb.ReplicateWrite, ControllerNoun.DSet ), 
                                 streamToReplicate, int64 streamPos )
                            peerLastActiveTime.[peeri] <- x.Clock.ElapsedTicks  
                            let msg = sprintf "Replicate partition %d, serial %d:%d to peer %d" parti serial numElems peeri
                            let msInfo = new MemStream(1024)
                            msInfo.WriteString( msg )
                            Logger.Log( LogLevel.WildVerbose, msg )
                            if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                                x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Info, ControllerNoun.Message ), msInfo )
                            // Make sure we don't replicate again
                            peerList.Remove( peeri ) |> ignore 
                        else
                            // Reset replication clock if we the peer has recently sent out data. 
                            if (!refTime) < peerLastActiveTime.[peeri]  then 
                                refTime := peerLastActiveTime.[peeri]
                            // The peer can't be written, timeout?
                            let t2 = x.Clock.ElapsedTicks
                            if ( t2 - !refTime ) > x.ClockFrequency * (int64 x.TimeoutLimit) then 
                                if not(peerTimeOut.Contains( peeri )) then 
                                    peerTimeOut.Add( peeri )
                    else
                       peerList.Remove( peeri ) |> ignore
                let remainingCount = peerList.Count
                if remainingCount<=0 then 
                    emptyReplicationItem.Add(i)    
            ()
            )
        let emptyReplicationArray = emptyReplicationItem.ToArray()
        // Remove peerReplicationItem that is empty. 
        lock (peerReplicationItems) ( fun _ ->
                for i =  emptyReplicationArray.Length-1 downto 0 do
                    peerReplicationItems.RemoveAt( emptyReplicationArray.[i] )
            )
        if peerTimeOut.Count>0 && x.Clock.ElapsedTicks>lastReplicationTimeoutMsg then 
            lastReplicationTimeoutMsg <- x.Clock.ElapsedTicks + x.ClockFrequency
            let msError = new MemStream( 1024 )
            let msg = sprintf "Replication Timeout, peer %A is not active" (peerTimeOut.ToArray())
            msError.WriteString( msg )
            ( ControllerCommand( ControllerVerb.Error, ControllerNoun.DSet ), msError ) 
        else
            // This message is filtered out. 
            ( ControllerCommand( ControllerVerb.Nothing, ControllerNoun.DSet ), null )
    /// EndStore: called to end storing operation of a DSet to cloud
    member x.EndReplicate( queue:NetworkCommandQueue, timeToWait, callback ) =
        if not bEndReplicateCalled then 
            bEndReplicateCalled <- true

            callbackRegister <- callback
            bCloseDSetSent <- Array.create (x.Cluster.NumNodes) false
            callbackRegister.Item( x ) <- DSetPeer.EndReplicateWait
            maxEndReplicationWait <- x.Clock.ElapsedTicks + (x.ClockFrequency * int64 timeToWait)
            bAllCloseDSetSent <- false
            bConfirmCloseRcvd <- Array.create (x.Cluster.NumNodes) false 

            // Monitoring insert
//            let msg = sprintf "At start of EndReplicate, the state of bConfirmCloseRcvd is %A" bConfirmCloseRcvd
//            Logger.Log(LogLevel.Info, msg)
//            if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then
//                let msInfo = new MemStream(1024)
//                msInfo.WriteString( msg )
//                x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Warning, ControllerNoun.DSet ), msInfo ) 

            // Give it at least some chance to sent close, DSet.
            (* Logic of sending ReplicateClose, merged
            if x.Clock.ElapsedTicks>=maxWait then 
                maxWait <- x.Clock.ElapsedTicks + x.ClockFrequency

            while not bAllCloseDSetSent && x.Clock.ElapsedTicks<maxWait do
                bAllCloseDSetSent <- true
                for i=0 to x.Cluster.NumNodes-1 do 
                    // Send a Close DSet command only for active peer. 
                    let q = x.Cluster.Queue( i )
                    if Utils.IsNotNull q && q.CanSend && ( not q.Shutdown ) then 
                        if Utils.IsNotNull x.RcvdSerialInitialValue && q.RcvdCommandSerial <= x.RcvdSerialInitialValue.[i] then 
                            // No command has every been received from peer i, wait before we send in CloseDSet,
                            // As the peer i may need additional information (e.g., cluster information)
                            // Any of peer that CloseDSet hasn't been sent will cause the EndWrite to Wait
                            bAllCloseDSetSent <- false
                        else
                            if not bCloseDSetSent.[i] then 
                                q.ToSend( cmd, msSend )
                                bCloseDSetSent.[i] <- true
                if not bAllCloseDSetSent then 
                    // Wait for input command. 
                    Threading.Thread.Sleep(10)
                *)
    member x.DoEndReplicateWait() = 
        if Utils.IsNotNull callbackRegister then 
            let cmdReplicateClose = ControllerCommand( ControllerVerb.ReplicateClose, ControllerNoun.DSet ) 
            let mutable msSend : MemStream = null
            let mutable bIOActivity = false
            // Flushing out replication queue. 
            if ( peerReplicationItems.Count>0 || not bAllCloseDSetSent ) && x.Clock.ElapsedTicks<maxEndReplicationWait then
                let cmd, msgInfo = x.TryReplicate( )    
                if cmd.Verb<> ControllerVerb.Nothing then 
                    bIOActivity <- true
                    if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                        x.HostQueue.ToSend( cmd, msgInfo )
                // Find out which peer still has items to replicate. 
                let bActivePeers = Array.create (x.Cluster.NumNodes) false
                if peerReplicationItems.Count>0 then 
                    for i=0 to peerReplicationItems.Count-1 do 
                        let repItem = peerReplicationItems.Item(i)
                        let streamToReplicate, streamPos, peerList, refTime, parti, serial, numElems = repItem
                        for peeri in peerList do
                            bActivePeers.[peeri] <- true
                let mutable bTestAllCloseDSetSent = true
                for i=0 to x.Cluster.NumNodes-1 do 
                    // Send a Close DSet command only for active peer. 
                    let q = x.Cluster.Queue( i )
                    // Peer can be sent, and there is no items to be replicated to peer. 
                    if Utils.IsNotNull q && q.CanSend && ( not q.Shutdown ) then 
                        if bActivePeers.[i] then 
                            // There is item still to be replicated to peer i, wait
                            bTestAllCloseDSetSent <- false
                        else    
                            if Utils.IsNotNull x.RcvdSerialInitialValue && q.RcvdCommandSerial <= x.RcvdSerialInitialValue.[i] then 
                                // No command has ever been received from peer i, wait before we send in CloseDSet,
                                // As the peer i may need additional information (e.g., cluster information)
                                // Any of peer that CloseDSet hasn't been sent will cause the EndWrite to Wait
                                if bPeerUseForReplicate.[i] then 
                                    // It is one of the peer required for replication. 
                                    if not bPeerInReplicate.[i] then 
                                        // never succeed in connecting to that peer i, this is a inactive peer, no need to 
                                        // send Close, DSet
                                        ()
                                    else
                                        bTestAllCloseDSetSent <- false
                            else
                                if not bCloseDSetSent.[i] then 
                                    if Utils.IsNull msSend then 
                                        msSend <- new MemStream( 1024 )
                                        msSend.WriteString( x.Name ) 
                                        msSend.WriteInt64( x.Version.Ticks )
                                    q.ToSend( cmdReplicateClose, msSend )
                                    bIOActivity <- true 
                                    bCloseDSetSent.[i] <- true
                bAllCloseDSetSent <- bTestAllCloseDSetSent

            // Check if all confirmation has received. 
            let bAllConfirmCloseRcvd = Array.fold2 ( fun state v1 v2 -> state && ( v1 || not v2) ) true bConfirmCloseRcvd bPeerInReplicate
            if ( peerReplicationItems.Count=0 && bAllConfirmCloseRcvd ) || x.Clock.ElapsedTicks>=maxEndReplicationWait then 
                // All confirmation received 
                // Remove call back
                callbackRegister.Remove( x ) |> ignore
                callbackRegister <- null 

                use msInfo = new MemStream( 1024 )
                msInfo.WriteGuid( x.WriteIDForCleanUp )
                msInfo.WriteString( x.Name ) 
                msInfo.WriteInt64( x.Version.Ticks )
                if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                    bIOActivity <- true
                    if not bAllConfirmCloseRcvd || peerReplicationItems.Count>0 then 
                        let msgInfo = 
                            seq { 
                                yield (sprintf "Warning at DoEndReplicateWait, graceful shutdown failed.... " )
                                let reorderSeq = 
                                    peerReplicationItems |>
                                    Seq.collect( fun ( streamToReplicate, streamPos, peerList, refTime, parti, serial, numElems ) -> 
                                                    seq {
                                                        for peeri in peerList do 
                                                            yield ( peeri, parti, serial, numElems )
                                                        } 
                                                ) 
                                let missingDictionary = DSet.AggregateSegmentByPeer( reorderSeq )
                                let peerwithMissItems = Dictionary<_,_>()
                                for pair in missingDictionary do
                                    let peeri, missingPeer = pair.Key, pair.Value
                                    if missingPeer.Length>0 then 
                                        let partres = seq {
                                                            for missingPartition in missingPeer do
                                                                let parti, slist = missingPartition
                                                                yield (sprintf "part %d" parti )
                                                                for sitem in slist do
                                                                    let serial, numElems = sitem
                                                                    yield (sprintf "%d:%d" serial numElems )
                                                        } |> String.concat( " " )
                                        yield ( sprintf "Peer %d: missing .......... %s " peeri partres )
                                        peerwithMissItems.Add( peeri, true )
                                // List of inactive peers. 
                                let inactivePeers = List<int>()
                                for i=0 to x.Cluster.NumNodes-1 do
                                    if i<>x.CurPeerIndex && bPeerUseForReplicate.[i] && not bPeerInReplicate.[i] && peerwithMissItems.ContainsKey(i) then 
                                        inactivePeers.Add(i)
                                if inactivePeers.Count>0 then 
                                    yield ( sprintf "    Inactive peer list : %s" (SeqToString( ";", inactivePeers ) ) )
                                for i=0 to x.Cluster.NumNodes-1 do
                                    if i<>x.CurPeerIndex && bPeerUseForReplicate.[i] && bPeerInReplicate.[i] && ( not bCloseDSetSent.[i] || not bConfirmCloseRcvd.[i] ) then 
                                        yield (sprintf "    Peer %d: (In replicate:%A, ReplicateClose sent: %A, rcvd %A)" i bPeerInReplicate.[i] bCloseDSetSent.[i] bConfirmCloseRcvd.[i] )                                
                            }
                            |> String.concat( System.Environment.NewLine )
                        use msSend = new MemStream( 1024 )
                        msSend.WriteString( msgInfo ) 
                        x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Warning, ControllerNoun.Message ) , msSend )
                        Logger.Log( LogLevel.Info, msgInfo )
                    x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.DSet ) , msInfo )
                    using ( x.TryExecuteSingleJobAction() ) ( fun jobObj -> 
                        if Utils.IsNotNull jobObj then 
                            let currentWriteID = jobObj.JobID
                            x.Cluster.UnRegisterCallback( currentWriteID, x.CallbackCommand )
                    )
            bIOActivity 
        else
            false
    static member EndReplicateWait( o: Object ) = 
        match o with 
        | :? DSetPeer as x ->
            x.DoEndReplicateWait()                  
        | _ -> 
            let msg = ( sprintf "Logic error, DSetPeer.EndReplicateWait receives a call with object that is not DSet peer but %A" o )    
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    member x.ReplicateDSetCallback( cmd:ControllerCommand, peeri, msRcvd:StreamBase<byte>, jobID ) = 
        try
            let q = x.Cluster.Queue( peeri )
            match ( cmd.Verb, cmd.Noun ) with 
            | ( ControllerVerb.Close, ControllerNoun.DSet )
            | ( ControllerVerb.ReplicateClose, ControllerNoun.DSet ) ->   

                x.Cluster.CloseQueueAndRelease( peeri )
                // Close DSet received
//                    bPeerInReplicate.[peeri] <- false
                bConfirmCloseRcvd.[peeri] <- true
                let msSend = new MemStream( 1024 )
                msSend.WriteString( x.Name )
                msSend.WriteInt64( x.Version.Ticks )
                msSend.WriteVInt32( peeri )
                if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                    x.HostQueue.ToSend( ControllerCommand( ControllerVerb.ReplicateClose, ControllerNoun.DSet), msSend )
                // Informed peer that Close, DSet received. 
            | ( ControllerVerb.Get, ControllerNoun.ClusterInfo ) ->
                using( new MemStream( 10240 ) ) ( fun msSend -> 
                    x.Cluster.ClusterInfo.Pack( msSend )
                    let cmd = ControllerCommand( ControllerVerb.Set, ControllerNoun.ClusterInfo ) 
                    // Expediate delivery of Cluster Information to the receiver
                    q.ToSend( cmd, msSend, true ) 
                )
            | ( ControllerVerb.Duplicate, ControllerNoun.DSet ) ->
                // Carry echo back to host. 
                let parti = msRcvd.ReadVInt32()
                let serial = msRcvd.ReadInt64()
                let numElems = msRcvd.ReadVInt32()
                let peerIdx = msRcvd.ReadVInt32()
                Logger.Log( LogLevel.WildVerbose, ( sprintf "DSetPeer.Callback: duplicate DSet write reported by peer %d, partition %d, serial %d:%d" peeri parti serial numElems))
            | ( ControllerVerb.EchoReturn, ControllerNoun.DSet ) ->
                // Carry echo back to host. 
                let parti = msRcvd.ReadVInt32()
                let serial = msRcvd.ReadInt64()
                let numElems = msRcvd.ReadVInt32()
                let peerIdx = msRcvd.ReadVInt32()
                let msSend = new MemStream( 1024 )
                msSend.WriteString( x.Name )
                msSend.WriteInt64( x.Version.Ticks )
                msSend.WriteVInt32( parti )
                msSend.WriteInt64( serial )
                msSend.WriteVInt32( numElems )
                msSend.WriteVInt32( peerIdx )
                if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                    x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Echo2Return, ControllerNoun.DSet), msSend )
            | ( ControllerVerb.Acknowledge, ControllerNoun.DSet ) ->
                ()
            | _ ->
                Logger.Log( LogLevel.Info, ( sprintf "DSetPeer.Callback: Unexpected command from peer %d, command %A" peeri cmd ))
        with
        | e ->
            Logger.Log( LogLevel.Info, ( sprintf "Error in processing DSet.Callback, cmd %A, peer %d" cmd peeri )    )
        true
    member val FinalizeAfterDisconnect = false with get, set
    member x.TestForDisconnect() = 
        x.RemoveInactiveQueues()
        let anyConnection = not (x.AllQueues.IsEmpty )
        if not anyConnection then 
            if not x.FinalizeAfterDisconnect then 
                x.FinalizeAfterDisconnect <- true
                x.CloseAllStreams( true )
                x.Reset()
        else
            x.FinalizeAfterDisconnect <- false                
        anyConnection
    static member GetDSet( jobID: Guid, name:string, verNumber, verCluster:int64 ) = 
            let verSearch = DateTime( verNumber )

            let storageProviderList = 
                [| StorageKind.HDD |] // ; StorageType.Azure
                // JinL, Apr. 4, 2014, 
                // The previous behavior of including StorageType.Azure in the maps causes PrajnaClient.exe to crash if the AzureStorage container is not there. 
                // The behavior is not acceptable, and need to be revised. 
                // Performancewise, this causes every open of DSet to wait for access from AzureStorage, that is also an issue. 
                |> Array.map ( fun ty -> StorageStreamBuilder.Create( ty ) )
            let lst = 
                storageProviderList
                |> Seq.collect ( fun sp -> sp.List( name, null ) |> Seq.map( fun (n, e, length) -> (n, e, sp) ) )
                // DSet version is in directory
                |> Seq.filter ( fun ( name, el, provider ) -> 
                                    let ver = 
                                        try 
                                            StringTools.VersionFromString( name )
                                        with
                                        | e ->
                                            DateTime.MaxValue
                                    el=StorageStructureKind.Directory && verSearch.CompareTo(ver)>=0 && ver<>DateTime.MaxValue )
                |> Seq.sortBy( fun ( name, el, length ) -> name )
                |> Seq.toArray
            if lst.Length>0 then 
                let mutable idx = lst.Length-1
                let mutable useDSet :DSetPeer = null
                while Utils.IsNull useDSet && idx >=0 do
                    let useVersionName, _, provider = lst.[idx]
                    let useVersion = StringTools.VersionFromString( useVersionName )
                    let dsetOpt, blockingOn, sendStream = DSetPeer.RetrieveDSetMetadata( jobID, name, useVersion.Ticks, provider )
                    match dsetOpt with 
                    | Some ( s ) ->
                        let sendDSet = Option.get dsetOpt
                        let cl = (sendDSet :> DSet).Cluster
                        if verCluster=0L || verCluster=cl.Version.Ticks then 
                            useDSet <- sendDSet
                    | None ->
                        ()
                    if Utils.IsNull useDSet then 
                        idx <- idx - 1
                DSetPeerFactory.CacheDSetPeer( useDSet ) |> ignore 
                useDSet
            else
                null
    static member ExceptionToClient( jobID, name, verNumber, ex ) = 
        let msSend = new MemStream( 10240 )
        msSend.WriteGuid( jobID )
        msSend.WriteString( name )
        msSend.WriteInt64( verNumber )
        msSend.WriteException( ex )
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Exception in working in PeerDSet, job: %A, name: %s, message: %A" jobID name ex )
        ( ControllerCommand( ControllerVerb.Exception, ControllerNoun.DSet ), msSend )
            
    static member ParsePeerDSet tuple =
        let msSend = new MemStream( 10240 )
        let jobID, name, verNumber, verCluster = tuple 
        try
                let useDSet = DSetPeer.GetDSet tuple
                if Utils.IsNotNull useDSet then 
                    msSend.WriteGuid( jobID )
                    msSend.WriteString( useDSet.Name )
                    msSend.WriteInt64( useDSet.Version.Ticks )            
                    useDSet.Pack( msSend, DSetMetadataStorageFlag.None )
                    ( ControllerCommand( ControllerVerb.Set, ControllerNoun.Metadata ), msSend )
                else
                    // Benign behavior, can't find the DSet on the current peer, the job may still continues
                    msSend.WriteGuid( jobID )
                    msSend.WriteString( name )
                    msSend.WriteInt64( verNumber )            
                    ( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.DSet ), msSend )
        with 
        | ex ->    
            let msg = sprintf "Error in DSetPeer.GetDSet, exception %A" ex
            ex.Data.Add( "@Daemon", msg )
            DSetPeer.ExceptionToClient( jobID, name, verNumber, ex )
    static member RetrieveDSetMetadata( jobID: Guid, name, verNumber, provider ) = 
        let path = DSetPeer.ConstructDSetPath( name, verNumber )
        let metaLst = 
            provider.List( path, "DSet*.meta" )
            |> Seq.filter( fun ( name, el, length ) -> el=StorageStructureKind.Blob && length>0L )
            |> Seq.sortBy( fun ( name, el, length ) -> name )
            |> Seq.toArray
        if metaLst.Length>0 then 
            let useMetaName, _, _ = metaLst.[metaLst.Length-1]
            let stream = provider.Open( path, useMetaName )
            DSetPeer.ReadFromMetaData( stream, null, jobID )
        else
            ( None, ClientBlockingOn.DSet, null )
    static member UseDSet( jobID, name, verNumber ) = 
        try 
            let storageProviderList = 
                [| StorageKind.HDD 
                   //; StorageType.Azure , JinL: Skip Azure 
                |]
                |> Array.map ( fun ty -> StorageStreamBuilder.Create( ty ) )
            let dsetOpt, bl, replyStream = DSetPeer.RetrieveDSetMetadata( jobID, name, verNumber, storageProviderList.[0] )
            match bl with 
            | ClientBlockingOn.None ->
                let msSend = new MemStream( 10240 )
                let dset = Option.get dsetOpt
                msSend.WriteGuid( jobID )
                msSend.WriteString( dset.Name )
                msSend.WriteInt64( dset.Version.Ticks )
                if Utils.IsNotNull replyStream then 
                    replyStream.Dispose()    
                ( ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.DSet ), msSend )
            | ClientBlockingOn.Cluster -> 
                let msg = sprintf "Error, the cluster associated with DSet %s, %s dosn't exist at the peer" name (VersionToString( DateTime(verNumber) ))
                let msSend = new MemStream( 10240 )
                msSend.WriteGuid( jobID )
                msSend.WriteString( name )
                msSend.WriteInt64( verNumber )
                msSend.WriteString( msg )
                if Utils.IsNotNull replyStream then 
                    replyStream.Dispose()    
                ( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.DSet ), msSend )
            | ClientBlockingOn.DSet ->
                // Unknown DSet to be used. 
                let msg = sprintf "Error, DSet %s, %s dosn't exist at the peer" name (VersionToString( DateTime(verNumber) ))
                let msSend = new MemStream( 10240 )
                msSend.WriteGuid( jobID )
                msSend.WriteString( name )
                msSend.WriteInt64( verNumber )
                msSend.WriteString( msg )
                if Utils.IsNotNull replyStream then 
                    replyStream.Dispose()    
                ( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.DSet ), msSend )
            | ClientBlockingOn.Undefined 
            | _ ->
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "UseDSet: DSetPeer.RetrieveDSetMetadata return unknown error" ))
                ( ControllerCommand( ControllerVerb.Exception, ControllerNoun.DSet ), replyStream )                      
        with 
        | ex ->
            let msSend = new MemStream( 10240 )    
            let msg = sprintf "Error in DSetPeer.UseDSet" 
            Logger.LogF( LogLevel.Info, fun _ -> sprintf "%s: Exception: %A" msg ex )
            ex.Data.Add( "@Daemon", msg )
            msSend.WriteGuid( jobID )
            msSend.WriteString( name )
            msSend.WriteInt64( verNumber )
            msSend.WriteException( ex )
            ( ControllerCommand( ControllerVerb.Exception, ControllerNoun.DSet ), msSend ) 
    /// Test for Empty Partition 
    /// Return: true for partition with 0 keys
    member x.EmptyPartition parti =
        let mutable bNullStream = false
        if Utils.IsNotNull x.MappingNumElems && Utils.IsNotNull x.MappingStreamLength then  
            let maxNumElems = Array.max x.MappingNumElems.[parti]
            let maxStreamLength = Array.max x.MappingStreamLength.[parti]
            bNullStream <- maxNumElems = 0 && maxStreamLength=0L        
        bNullStream
    /// Read one chunk in async work format. 
    /// Return: Async<byte[]>, if return null, then the operation reads the end of stream of partition parti. 
    member x.AsyncReadChunk (jbInfo:JobInformation) parti ( pushChunkFunc: (BlobMetadata*MemStream)->unit ) = 
        async {
            use jobAction =  jbInfo.TryExecuteSingleJobAction() 
            if Utils.IsNull jobAction then 
                Logger.LogF ( LogLevel.MildVerbose, fun _ -> sprintf "[AsyncReadChunk, Job %A for DSet %s:%s cancelled before read start on partition %d"
                                                                        jbInfo.JobID x.Name x.VersionString parti )
            else
                    try 
                      let nSomeError = ref 0
                      if x.EmptyPartition( parti ) || jobAction.IsCancelled then 
                        let finalMeta = BlobMetadata( parti, Int64.MaxValue, 0 )
                        pushChunkFunc( finalMeta, null )  
                        // empty stream
                      else                
                        let streamRead = x.PartitionStreamForReadInJob jbInfo parti 
                        if Utils.IsNotNull streamRead then 
                            try
                                let! beginGuid = streamRead.AsyncRead( 16 )
                                if beginGuid.Length=16 && DeploymentSettings.BlobBeginMarker.CompareTo( Guid( beginGuid) )=0 then 
                                    let! flagArray = streamRead.AsyncRead( 4 ) 
                                    if flagArray.Length=4 then 
                                        let flag = BitConverter.ToInt32( flagArray, 0 )
                                        let bPassword, bConfirmDelivery = x.InterpretStorageFlag( flag )
                                        let bEndReached = ref false
                                        while not !bEndReached && not jobAction.IsCancelled do
                                            let! seglenBuffer = streamRead.AsyncRead(4)
                                            if seglenBuffer.Length<4 then 
                                                bEndReached := true
                                                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, failed to find end of file GUID" x.Name x.VersionString parti  ) )
                                            if not !bEndReached && not jobAction.IsCancelled then     
                                                let seglen = BitConverter.ToInt32( seglenBuffer, 0 )
                                                // search for JINL, WriteDSet Note, on length coding
                                                let readlen = Math.Abs( seglen ) 
                                                let sha512provider = 
                                                    if seglen<0 then 
                                                        new SHA512Managed()
                                                    else 
                                                        null

                                                let! bufferPayload = streamRead.AsyncRead(readlen)
                                                if bufferPayload.Length<readlen || 
                                                    ( readlen=16 && DeploymentSettings.BlobCloseMarker.CompareTo( Guid(bufferPayload) )=0 ) then 
                                                    bEndReached := true
                                                    if bufferPayload.Length=readlen && !nSomeError = 0 then 
                                                        // Only this signal the success of the reading of the entire partition. 
                                                        nSomeError := -1
                                                        // Note: JinL, 04/21/2014
                                                        // We can potentially cancel all subsequent read by use cancellationToken
                                                    else
                                                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, partial chunk encountered" x.Name x.VersionString parti  ) )
                                                if not !bEndReached && not jobAction.IsCancelled then 
                                                    let buflen = if seglen>0 then readlen else readlen - (sha512provider.HashSize/8)
                                                    let bVerified = 
                                                        if seglen > 0 then true else
                                                            let computeHash = sha512provider.ComputeHash( bufferPayload, 0, buflen )
                                                            let storeHash = Array.sub( bufferPayload ) buflen (int bufferPayload.Length - buflen)
                                                            System.Linq.Enumerable.SequenceEqual( computeHash, storeHash )
                                                    if bVerified then 
                                                        let ms = new MemStream( bufferPayload, 0, buflen, false, true )
                                                        let serial = ms.ReadInt64()
                                                        let numElems = ms.ReadVInt32()
                                                        let meta = BlobMetadata( parti, serial, numElems, buflen )
                                                        pushChunkFunc( meta, ms )  
                                                    else
                                                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Throw away %dB as stored SHA512 hash doesn't match" readlen ) )
                                                        nSomeError := !nSomeError + 1
                                                        // Note: JinL, 04/21/2014
                                                        // We can potentially cancel all subsequent read by use cancellationToken
                                    else
                                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, failed to read beginning flag" x.Name x.VersionString parti  ) )
                                else
                                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, wrong beginning Guid" x.Name x.VersionString parti  ) )
                            finally 
                                x.ClosePartitionStreamForRead streamRead jbInfo parti 
                        else
                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, failed to find partition file" x.Name x.VersionString parti  ) )
                        // at the end of chunk, the numElems value records the # of error (e.g., inconsistent SHA512 value retrieved during the read operation)
                        // if the push out numElems value is 0, then the entire partition has been successfully retrieved. 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Reach end of DSet %s:%s part %d, with %d read errors" x.Name x.VersionString parti (!nSomeError + 1) ) )
                        let finalMeta = BlobMetadata( parti, Int64.MaxValue, !nSomeError + 1 )
                        pushChunkFunc( finalMeta, null )
                    with 
                    | e ->
                        let msg = sprintf "Error in AsyncReadChunk, with exception %A, stack %A" e (StackTrace (0, true))
                        let bSendHost = Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend 
                        if bSendHost then 
                            let msError = new MemStream( 1024 )
                            msError.WriteString( msg ) 
                            x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
                        Logger.Log( LogLevel.Error, ((sprintf "Send Host: %A" bSendHost) + msg))
        }


    /// Read one chunk in async work format. 
    /// Return: Async<byte[]>, if return null, then the operation reads the end of stream of partition parti. 
    member private x.SyncReadChunkImpl jbInfo parti ( pushChunkFunc: (BlobMetadata*StreamBase<byte>)->unit ) = 
        using ( jbInfo.TryExecuteSingleJobAction()) ( fun jobAction -> 
            if Utils.IsNull jobAction then 
                Logger.LogF ( LogLevel.MildVerbose, fun _ -> sprintf "[SyncReadChunkImpl, Job %A for DSet %s:%s cancelled before read start on partition %d"
                                                                        jbInfo.JobID x.Name x.VersionString parti )
                null, true
            else
                try 
                  let nSomeError = ref 0
                  if x.EmptyPartition( parti ) || jobAction.IsCancelled then 
                    let finalMeta = BlobMetadata( parti, Int64.MaxValue, 0 )
                    pushChunkFunc( finalMeta, null )  
                    // empty stream
                  else                
                    let streamRead = x.PartitionStreamForReadInJob jbInfo parti
                    if Utils.IsNotNull streamRead then 
                        try
                            let beginGuid = Array.zeroCreate<_> 16
                            let readGuidLen = streamRead.Read( beginGuid, 0, 16 )
                            if readGuidLen=16 && DeploymentSettings.BlobBeginMarker.CompareTo( Guid( beginGuid) )=0 then 
                                let flagArray = Array.zeroCreate<_> 4
                                let flagArrayLen = streamRead.Read( flagArray, 0, 4 )
                                if flagArrayLen=4 then 
                                    let flag = BitConverter.ToInt32( flagArray, 0 )
                                    let bPassword, bConfirmDelivery = x.InterpretStorageFlag( flag )
                                    let bEndReached = ref false
                                    while not !bEndReached && not jobAction.IsCancelled do
                                        let seglenBuffer =  Array.zeroCreate<_> 4
                                        let seglenBufferLen = streamRead.Read( seglenBuffer, 0, 4 )
                                        if seglenBufferLen<4 then 
                                            bEndReached := true
                                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, failed to find end of file GUID" x.Name x.VersionString parti  ) )
                                        if not !bEndReached && not jobAction.IsCancelled then     
                                            let seglen = BitConverter.ToInt32( seglenBuffer, 0 )
                                            // search for JINL, WriteDSet Note, on length coding
                                            let readlen = Math.Abs( seglen ) 
                                            let sha512provider = 
                                                if seglen<0 then 
                                                    new SHA512Managed()
                                                else 
                                                    null
                                            let bufferPayload = Array.zeroCreate<_> readlen
                                            let bufferPayloadLen = streamRead.Read( bufferPayload, 0, readlen )
                                            if bufferPayloadLen<readlen || 
                                                ( readlen=16 && DeploymentSettings.BlobCloseMarker.CompareTo( Guid(bufferPayload) )=0 ) then 
                                                bEndReached := true
                                                if bufferPayload.Length=readlen && !nSomeError = 0 then 
                                                    // Only this signal the success of the reading of the entire partition. 
                                                    nSomeError := -1
                                                    // Note: JinL, 04/21/2014
                                                    // We can potentially cancel all subsequent read by use cancellationToken
                                                else
                                                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, partial chunk encountered" x.Name x.VersionString parti  ) )
                                            if not !bEndReached && not jobAction.IsCancelled then 
                                                let buflen = if seglen>0 then readlen else readlen - (sha512provider.HashSize/8)
                                                let bVerified = 
                                                    if seglen > 0 then true else
                                                        let computeHash = sha512provider.ComputeHash( bufferPayload, 0, buflen )
                                                        let storeHash = Array.sub( bufferPayload ) buflen (int bufferPayload.Length - buflen)
                                                        System.Linq.Enumerable.SequenceEqual( computeHash, storeHash )
                                                if bVerified then 
                                                    let ms = new MemStream( bufferPayload, 0, buflen, false, true )
                                                    let serial = ms.ReadInt64()
                                                    let numElems = ms.ReadVInt32()
                                                    let meta = BlobMetadata( parti, serial, numElems, buflen )
                                                    pushChunkFunc( meta, ms )  
                                                else
                                                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Throw away %dB as stored SHA512 hash doesn't match" readlen ) )
                                                    nSomeError := !nSomeError + 1
                                                    // Note: JinL, 04/21/2014
                                                    // We can potentially cancel all subsequent read by use cancellationToken
                                else
                                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, failed to read beginning flag" x.Name x.VersionString parti  ) )
                            else
                                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, wrong beginning Guid" x.Name x.VersionString parti  ) )
                        finally 
                            x.ClosePartitionStreamForRead streamRead jbInfo parti 
                    else
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Reading DSet %s:%s part %d, failed to find partition file" x.Name x.VersionString parti  ) )
                    // at the end of chunk, the numElems value records the # of error (e.g., inconsistent SHA512 value retrieved during the read operation)
                    // if the push out numElems value is 0, then the entire partition has been successfully retrieved. 
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Reach end of DSet %s:%s part %d, with %d read errors" x.Name x.VersionString parti (!nSomeError + 1) ) )
                    let finalMeta = BlobMetadata( parti, Int64.MaxValue, !nSomeError + 1 )
                    pushChunkFunc( finalMeta, null )
                with 
                | e ->
                    let msg = sprintf "Error in SyncReadChunk, with exception %A, stack %A " e (StackTrace (0, true))
                    Logger.Log( LogLevel.Error, msg )
                    if Utils.IsNotNull x.HostQueue && x.HostQueue.CanSend then 
                        let msError = new MemStream( 1024 )
                        msError.WriteString( msg ) 
                        x.HostQueue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
                null, true
        )

    member private x.SyncEncodeImpl jbInfo parti pushChunkFunc = x.SyncReadChunk jbInfo parti pushChunkFunc

