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
        blob.fs
  
    Description: 
        Define A blob in Prajna. 
        A blob is an immutable byte[] that will be broadcast in Prajna. 
        Prajna Global Variable (GV), Assemblies will be packed into blob before being broadcasted to the Cluster.  

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Sept. 2013
    
 ---------------------------------------------------------------------------*)
 // Assemblies wraps around a set of DLL, datas that consists of Prajna
namespace Prajna.Core
open System
open System.IO
open System.Collections.Concurrent
open System.Threading
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

type internal BlobKind =  
    | Null = 0 
    | ClusterMetaData = 1
    | SrcDSetMetaData = 2
    | DstDSetMetaData = 3
    | PassthroughDSetMetaData = 4
    | DStream = 5
    | AssemblyManagedDLL = 16
    | AssemblyUnmanagedDir = 17
    | JobDependencyFile = 18
    | GV = 32
    | ClusterWithInJobInfo = 33

// Transition is only to larger number of client. 
type internal BlobStatus = 
    | NoInformation = 0uy   
    | Initial = 1uy
    | NotAvailable = 2uy
    | DependentNeeded = 3uy
    | PartialAvailable = 4uy
    | LatestVersion = 5uy     // For SrcDSet, SrcGV only
    | Outdated = 6uy        // For SrcDSet, SrcGV only
    | AllAvailable = 7uy
    | Error = 8uy


/// The BlobFactory class implements cache that avoid instantiation of multiple Blob class, and save memory. 
type internal BlobFactory() = 
    static member val Current = BlobFactory() with get
    static member val InactiveSecondsToEvictBlob = 600L with get, set
    member val Collection = ConcurrentDictionary<_,(_*_*_*_)>(BytesCompare()) with get
    /// Register object with a trigger function that will be executed when the object arrives. 
    member x.Register( id:byte[], triggerFunc:(StreamBase<byte>*int64->unit), epSignature:int64 ) = 
        let addFunc _ = 
            ref null, ref (PerfDateTime.UtcNowTicks()), ConcurrentQueue<_>(), ConcurrentDictionary<_,_>()
        let tuple = x.Collection.GetOrAdd( id, addFunc )
        let refMS, refTicks, epQueue, epDic = tuple 
        refTicks := (PerfDateTime.UtcNowTicks())
        epDic.GetOrAdd( epSignature, true ) |> ignore
        if Utils.IsNull !refMS then 
            epQueue.Enqueue( triggerFunc )
            null
        else
            !refMS
    /// Store object info into the Factory class, apply trigger if there are any function waiting to be executed. 
    member x.Store( id:byte[], ms: StreamBase<byte>, epSignature:int64 ) = 
        let addFunc _ = 
            ms.AddRef()
            ref ms, ref (PerfDateTime.UtcNowTicks()), ConcurrentQueue<_>(), ConcurrentDictionary<_,_>()
        let tuple = x.Collection.GetOrAdd( id, addFunc )
        let refMS, refTicks, epQueue, epDic = tuple 
        if (Utils.IsNotNull (!refMS)) then
            (!refMS).DecRef()
        ms.AddRef()
        refMS := ms
        refTicks := (PerfDateTime.UtcNowTicks())
        epDic.GetOrAdd( epSignature, true ) |> ignore
        let refValue = ref Unchecked.defaultof<_>
        while epQueue.TryDequeue( refValue ) do 
            let triggerFunc = !refValue
            triggerFunc( ms, epSignature )
    /// Retrieve object info from the Factory class. 
    member x.Retrieve( id ) = 
        let bExist, tuple = x.Collection.TryGetValue( id )
        if bExist then 
            let blob, clockRef, _, _ = tuple
            clockRef := (PerfDateTime.UtcNowTicks())
            !blob
        else
            null
    /// Cache Information, used the existing object in Factory if it is there already
    member x.ReceiveWriteBlob( id, ms: StreamBase<byte>, epSignature ) = 
        let bExist, tuple = x.Collection.TryGetValue( id )
        if bExist then 
            let refMS, refTicks, epQueue, epDic = tuple 
            if (Utils.IsNotNull (!refMS)) then
                (!refMS).DecRef()
            ms.AddRef()
            refMS := ms
            refTicks := (PerfDateTime.UtcNowTicks())
            epDic.GetOrAdd( epSignature, true ) |> ignore
            let refValue = ref Unchecked.defaultof<_>
            while epQueue.TryDequeue( refValue ) do 
                let triggerFunc = !refValue
                triggerFunc( !refMS, epSignature )
            true
        else
            false
    /// Evict object that hasn't been visited within the specified seconds
    member x.Evict( elapseSeconds ) = 
        let t1 = (PerfDateTime.UtcNowTicks()) - TimeSpan.TicksPerSecond * elapseSeconds
        // Usually don't evict much
        for pair in x.Collection do
            let _, refTime, _, _ = pair.Value
            if !refTime < t1 then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "remove blob %A after %d secs of inactivity"
                                                                       pair.Key BlobFactory.InactiveSecondsToEvictBlob ))
                let mutable tuple = Unchecked.defaultof<(_*_*_*_)>
                let ret = x.Collection.TryRemove( pair.Key, &tuple )
                if (ret) then
                    let refMS, refTicks, epQueue, epDic = tuple
                    if (Utils.IsNotNull (!refMS)) then
                        (!refMS).DecRef()
    /// Remove a certain entry
    member x.Remove( id ) = 
        let mutable tuple = Unchecked.defaultof<(_*_*_*_)>
        let ret = x.Collection.TryRemove( id, &tuple )
        if (ret) then
            let refMS, refTicks, epQueue, epDic = tuple
            if (Utils.IsNotNull (!refMS)) then
                (!refMS).DecRef()
    /// Refresh timer entry of an object
    member x.Refresh( id ) = 
        let bExist, tuple = x.Collection.TryGetValue( id ) 
        if bExist then 
            let _, refTimer, _, _ = tuple
            refTimer := (PerfDateTime.UtcNowTicks())

    /// func: 'T -> bool,   true: when object is still in use
    /// Evict all object if there is elapseSeconds passed when the object is not in use. 
    member x.Refresh( func, elapseSeconds ) = 
        x.Collection |> Seq.iter( fun pair -> 
                                    let blobRef, refTime, _, _ = pair.Value
                                    if ( func !blobRef ) then 
                                        refTime := (PerfDateTime.UtcNowTicks()) )
        x.Evict( elapseSeconds )

    /// Get the list of current members
    member x.ToArray() = 
        x.Collection 
        |> Seq.map( fun pair -> 
                        let blob, _, _, _ = pair.Value
                        !blob )
        |> Seq.toArray
    static member register tuple = 
        BlobFactory.Current.Register tuple
    static member store tuple = 
        BlobFactory.Current.Store tuple
    static member retrieve( id ) = 
        BlobFactory.Current.Retrieve( id )
    static member receiveWriteBlob tuple = 
        BlobFactory.Current.ReceiveWriteBlob tuple
    static member evict( elapseSeconds ) = 
        BlobFactory.Current.Evict( elapseSeconds )
    static member remove( id ) = 
        BlobFactory.Current.Remove( id ) 
    static member refresh( id, elapseSeconds ) = 
        BlobFactory.Current.Refresh( id, elapseSeconds )
    static member toArray() = 
        BlobFactory.Current.ToArray()


and [<AllowNullLiteral>]
    internal Blob() = 
    /// Blob Name, which should correspond to the underlying data name, e.g., Cluster Name, DSet name, etc..
    member val Name = "" with get, set
    /// Version, assoicated with the underlying data version. 
    member val Version = 0L with get, set
    /// Hash of the blob
    member val Hash = null with get, set
    member val TypeOf = BlobKind.Null with get, set
    /// Object that is associated with the blob
    member val Object : System.Object = null with get, set
//    /// Buffers are used to access buffer larger than 2GB, in which subsequent data is put into 
//    /// new byte arrays. 
//    member val Buffers = Array.zeroCreate<byte[]> 1 with get, set
//    /// Base buffer, used for blob with size less than 2GB 
//    member x.Buffer with get() = x.Buffers.[0] 
//                     and set( b ) = x.Buffers.[0] <- b 
//    /// Has the associated Buffer of blob been allocated? 
//    member x.IsAllocated with get() = (Utils.IsNull x.Buffer)
//    /// Release the buffer associated with the blob
//    member x.ReleaseBuffers() = 
//        x.Buffers <- Array.zeroCreate<byte[]> 1
    /// MemStream associated with blob
    member val Stream : StreamBase<byte> = null with get, set
    member x.IsAllocated with get() = Utils.IsNotNull x.Stream
    member x.Release() = 
        if x.IsAllocated then
            x.Stream.DecRef()
            x.Stream.Dispose()
            x.Stream <- null
    /// Index of the array in the specific Blob type. 
    member val Index=0 with get, set
    /// Stream for write
    member x.StreamForWrite( expectedLength:int64 ) = 
        if not x.IsAllocated then 
            if expectedLength >=0L then 
                x.Stream <- new MemStream( int expectedLength )
            else
                x.Stream <- new MemStream( )
        x.Stream
    /// Turn stream to blob
    member x.StreamToBlob( ms:StreamBase<byte> ) = 
        if Utils.IsNull x.Hash then 
            let buf, pos, count = ms.GetBufferPosLength()
            x.Hash <- buf.ComputeSHA256(int64 pos, int64 count)
//            x.Hash <- BytesTools.HashByteArrayWithLength( buf, pos, count )
//        if ms.Length<int64 Int32.MaxValue then
//            x.Buffer <- Array.zeroCreate<byte> (int ms.Length)
//            Buffer.BlockCopy( ms.GetBuffer(), 0, x.Buffer, 0, int ms.Length )
//        else
//            failwith "StreamToBlob ToDo: allow Blob to grow beyond 2GB"
    /// Stream for read
    member x.StreamForRead( ) = 
        x.Stream
//        if x.Buffers.Length=1 then 
//            new MemStream( x.Buffer, 0, x.Buffer.Length, false, true )
//        else
//            failwith "StreamForRead ToDo: allow Blob to grow beyond 2GB"
    static member val AllSourceStatus = [| BlobKind.SrcDSetMetaData |] with get
    static member val AllStatus = [| BlobKind.ClusterMetaData; BlobKind.SrcDSetMetaData; BlobKind.PassthroughDSetMetaData; BlobKind.DstDSetMetaData; 
        BlobKind.AssemblyManagedDLL; BlobKind.AssemblyUnmanagedDir; BlobKind.JobDependencyFile |] with get

        

[<AllowNullLiteral>]
type internal BlobAvailability(numBlobs) = 
    // multithread support
    // Mark the initial availablility vector as NoInformation to prevent premature sending of blob information. 
    member val AvailVector = Array.create numBlobs (byte BlobStatus.NoInformation) with get, set
    member val AllAvailable = false with get, set
    /// Pack to encode availability information
    member x.Pack( ms:MemStream ) = 
        // Write availability vector of fixed length 
        ms.WriteBytes( x.AvailVector )
    member x.CheckAllAvailable() = 
        // one way transition
        if not x.AllAvailable then 
            let bAllAvailable = x.AvailVector |> Array.fold ( 
                                                    fun res s -> 
                                                        res && ( s=byte BlobStatus.AllAvailable || s=byte BlobStatus.LatestVersion) ) true       
            if bAllAvailable then 
                x.AllAvailable <- bAllAvailable
    static member StatusString( status: byte ) = 
        let st =  Microsoft.FSharp.Core.LanguagePrimitives.EnumOfValue<_,_>(status)
        match st with 
        | BlobStatus.NotAvailable -> "NotAvail"
        | BlobStatus.PartialAvailable -> "Partial"
        | BlobStatus.AllAvailable -> "AllAvail"
        | BlobStatus.DependentNeeded -> "NeedDepedent"
        | BlobStatus.Initial -> "Initial"
        | BlobStatus.LatestVersion -> "LatestV"
        | BlobStatus.Outdated -> "Outdated"
        | BlobStatus.Error -> "Error"
        | _ -> "Illegal"
    /// Unpack to decode availability information
    member x.Unpack( ms:StreamBase<byte> ) = 
        // Read availability vector of fixed length
        let readAvailVector = Array.copy x.AvailVector
        ms.ReadBytes( readAvailVector ) |> ignore 
        for i = 0 to x.AvailVector.Length - 1 do 
            // One Way transition to All Available
            x.AvailVector.[i] <- Math.Max( x.AvailVector.[i], readAvailVector.[i] )
        x.CheckAllAvailable()
    override x.ToString() = 
        if x.AllAvailable then 
            "all available"
        else
            x.AvailVector |> Array.map ( fun byt -> byt.ToString() ) |> String.concat ""

