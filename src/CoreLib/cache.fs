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
        cache.fs
  
    Description: 
        Cache module of Prajna Client. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        June. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

/// BlobMetadata used in Job Execution. The class gets exposed because of use in abstract function. 
[<AllowNullLiteral>]
type BlobMetadata internal ( pti:int, serialValue:int64, numElems:int, blobLen:int ) = 
    [<Literal>] static let metadataSize = 16 // 4 (parti) + 8 (serial) + 4 (numElems)
    member val BlobLength = blobLen with get
    member val Parti = pti with get
    member val Serial = serialValue with get
    member val NumElems = numElems with get
    new( parti:int, serial:int64, numElems:int ) = 
        BlobMetadata( parti, serial, numElems, 0 )
    new( parti:int, nError ) = 
        BlobMetadata( parti, Int64.MaxValue, nError, 0 )
    new( y:BlobMetadata ) = 
        BlobMetadata( y.Parti, y.Serial, y.NumElems, y.BlobLength )
    new( y:BlobMetadata, numElems ) = 
        BlobMetadata( y.Parti, y.Serial, numElems, if numElems=0 then 0 else y.BlobLength)
    new( y:BlobMetadata, serial, numElems ) = 
        BlobMetadata( y.Parti, serial, numElems, if numElems=0 then 0 else y.BlobLength )
    new( y:BlobMetadata, parti:int, serial:int64, numElems, blobLen ) =
        BlobMetadata( parti, serial, numElems, blobLen )
    new( y:BlobMetadata, parti:int, serial:int64, numElems ) =
        BlobMetadata( parti, serial, numElems, if numElems=0 then 0 else y.BlobLength )
    new() = 
        BlobMetadata( 0, 0L, 0, 0 )
    member x.Partition with get() = x.Parti
    member x.SerialNumber with get() = x.Serial
    member x.PartSerialNumElems with get() = ( x.Parti, x.Serial, x.NumElems )
    override meta.ToString() = 
        sprintf "partition:%d serial:%d numElems:%d Len:%d" meta.Parti meta.Serial meta.NumElems meta.BlobLength
    member x.Pack( ms:StreamBase<byte>) = 
        ms.WriteVInt32( x.Parti ) 
        ms.WriteInt64( x.Serial ) 
        ms.WriteVInt32( x.NumElems ) 
    static member Unpack( ms:StreamBase<byte> ) = 
        let parti = ms.ReadVInt32() 
        let serial = ms.ReadInt64() 
        let numElems = ms.ReadVInt32()
        let blobLength = if ms.Length >= int64 Int32.MaxValue then Int32.MaxValue else int ms.Length
        BlobMetadata( parti, serial, numElems, blobLength )


type internal CacheRetrieval = 
    | CacheSeqRetrieved of seq<BlobMetadata * Object>
    | CacheDataRetrieved of BlobMetadata * Object
    | CacheBlocked of ManualResetEvent 

/// Cache Structure used to cache a partition of Prajna
[<AllowNullLiteral; AbstractClass>]
type internal PartitionCacheBase( cacheType: CacheKind ) = 
    member val CacheType = cacheType with get, set
    abstract ClearCache : unit -> unit
    // Add a object to partition
    // bToQueue: true, the object is added by pushing downstream, it should be placed on a queue to be retrieved from upstream
    //           false, the object is added by retrieve upstream, no queue is necessary. 
    abstract Add : BlobMetadata * Object * bool -> unit
    abstract Retrieve : unit -> BlobMetadata * Object
    abstract RetrieveNonBlocking : unit -> CacheRetrieval
    abstract Unblock : unit -> unit
    abstract ToSeq : unit -> seq< BlobMetadata * Object >
    abstract Reset: unit -> unit
    abstract HandlesToMonitor: string * IEnumerable<ManualResetEvent> -> unit

/// Cache Structure used to cache a partition of Prajna
[<AllowNullLiteral>]
type internal PartitionCacheQueue<'U>( cacheType, parti:int, serializationLimit:int ) = 
    inherit PartitionCacheBase( cacheType )
    let useLimit = if serializationLimit<=0 then DeploymentSettings.DefaultIOMaxQueue else serializationLimit
    member val CanRead = new ManualResetEvent(false) with get
    member val CanWrite = new ManualResetEvent(true) with get
    member val PartitionI = parti with get
    member val Serial = ref 0L with get
    member val SerializationLimit =  useLimit with get
    member val private ReadUnblockingLimit =  Math.Min( useLimit, int (DeploymentSettings.DefaultIOQueueReadUnblocking) ) with get
    member val private WriteUnblockingLimit = Math.Min( useLimit, int (DeploymentSettings.DefaultIOQueueWriteUnblocking) ) with get
    member val SerializationLimit =  DeploymentSettings.DefaultIOMaxQueue with get
    member val private WriteUnblockingLimit = DeploymentSettings.DefaultIOMaxQueue with get
    member val private IOQueue : ConcurrentQueue<BlobMetadata*Object> = null with get, set
    member val private IOQueueLength = ref 0 with get
    member val private bEndReached = false with get, set
    member val private MonitorHandles = null with get, set
    member val private Name = null with get, set
    member val private LastRead = (PerfDateTime.UtcNow()) with get, set
    member val private LastWrite = (PerfDateTime.UtcNow()) with get, set
    member val private LastMeta = None with get, set
    member x.SetLastMeta (meta:BlobMetadata) = 
        x.LastMeta <- Some meta
    member x.LastMetadata() = 
        match x.LastMeta with 
        | None -> 
            BlobMetadata( x.PartitionI, Int64.MaxValue, 0 )
        | Some meta -> 
            meta
    member private x.ReadyIOQueue() = 
        if Utils.IsNull x.IOQueue then 
            lock ( x ) ( fun _ -> 
                if Utils.IsNull x.IOQueue then 
                    x.IOQueueLength := 0
                    x.CanRead.Reset() |> ignore 
                    x.CanWrite.Set() |> ignore 
                    x.IOQueue <- ConcurrentQueue<_>() 
            )
    // Note: This method cannot be used concurrently with any other operations on the cache
    member x.QueueReset() =
        x.CanRead.Reset() |> ignore 
        x.CanWrite.Set() |> ignore 
        x.IOQueueLength := 0
        x.Serial := 0L
        x.bEndReached <- false
        x.IOQueue <- null   
    override x.ClearCache() = 
        x.QueueReset()
    member x.AddQueue( meta, objarray ) = 
        // Note: the caller ensures that the "null" objarray will be
        // the last item enqueued. 
        x.ReadyIOQueue()
        if (!x.IOQueueLength) >= x.SerializationLimit then 
            
            while (!x.IOQueueLength) >= x.SerializationLimit do
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Partition %d, reaches queue length %d, to block\n" x.PartitionI (!x.IOQueueLength) ))
                x.CanRead.Set() |> ignore
                if x.CanWrite.WaitOne(0) then
                    x.CanWrite.Reset() |> ignore 
                else 
                    ThreadPoolWaitHandles.safeWaitOne( x.CanWrite ) |> ignore
        x.IOQueue.Enqueue( meta, objarray )
        let cnt = Interlocked.Increment( x.IOQueueLength ) 
        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Partition %d, add to queue (len:%d) ...  %s\n" x.PartitionI cnt (meta.ToString()) ))
        if Utils.IsNull objarray then 
            x.bEndReached <- true
            x.SetLastMeta( meta )
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Partition %d end reached %s\n" x.PartitionI (meta.ToString()) ))
            x.CanRead.Set() |> ignore
        elif cnt >= 2 then 
            x.CanRead.Set() |> ignore
    member x.RetrieveQueue( ) = 
        // Note: it's designed to work with a single reader, does not assume multiple concurrent readers
        x.ReadyIOQueue()
        if x.bEndReached && x.IOQueue.IsEmpty then 
            let meta = x.LastMetadata()
            let readMeta = BlobMetadata( meta, (!x.Serial), meta.NumElems )
            x.LastMetadata(), null 
        else
            while x.IOQueue.IsEmpty && not x.bEndReached && x.WaitForRead() do 
                ()
            let retVal = ref Unchecked.defaultof<_>
            let bStillWait = ref true
            while (!bStillWait) && not (x.IOQueue.TryDequeue( retVal ))  do 
                bStillWait := if not x.bEndReached then x.WaitForRead() else false
            if (!x.IOQueueLength)  < x.WriteUnblockingLimit then 
                x.CanWrite.Set() |> ignore
            if not (!bStillWait) then 
                // This is only the case when it is signaling the end of partition. 
                let meta = x.LastMetadata()
                retVal := meta, null   
            else
                Interlocked.Decrement( x.IOQueueLength ) |> ignore
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> let retMeta, _ = !retVal
                                                          sprintf "Partition %d, retrieve %s\n" x.PartitionI (retMeta.ToString()) ))
            let meta, obj = !retVal
            let serial = !x.Serial
            x.Serial := serial + int64 meta.NumElems
            let readMeta = BlobMetadata( meta, serial, meta.NumElems )
            readMeta, obj
    override x.Add( meta, objarray, bToQueue ) = 
        if bToQueue then 
            x.AddQueue( meta, objarray )
    override x.HandlesToMonitor( name, handles ) =  
        x.Name <- name
        if Utils.IsNotNull handles && Seq.length handles > 0 then 
            x.MonitorHandles <- handles 
        else
            x.MonitorHandles <- null
    // Return: true: more to read. 
    //         false: end of stream reached
    // Logic all handles have fired (we don't wait then) or x.CanRead
    member x.WaitForRead( ) = 
        if not x.bEndReached then 
            if Utils.IsNull x.MonitorHandles then 
                // No Upstream signal, just wait
                ThreadPoolWaitHandles.safeWaitOne( x.CanRead ) |> ignore
                true
            else
                let activeHandles = x.MonitorHandles |> Seq.filter ( fun handle -> not (handle.WaitOne(0)) ) |> Seq.toArray
                if activeHandles.Length <= 0 then 
                    // All handles have fired
                    false
                else
                    let waitHandles =  Array.append activeHandles [| x.CanRead |] |> Array.map ( fun o -> o :> WaitHandle )
                    WaitHandle.WaitAny( waitHandles ) |> ignore 
                    true
        else
            true
    override x.RetrieveNonBlocking( ) = 
        x.RetrieveQueueNonBlocking( ) 
    member x.RetrieveQueueNonBlocking( ) =
        // Note: it's designed to work with a single reader, does not assume multiple concurrent readers
        x.ReadyIOQueue()
        let retVal = ref Unchecked.defaultof<_>
        let bSuccess = x.IOQueue.TryDequeue( retVal )
        if bSuccess then 
            let cnt = Interlocked.Decrement( x.IOQueueLength )
            if cnt < x.WriteUnblockingLimit then 
            //if not (x.CanWrite.WaitOne(0)) then 
                x.CanWrite.Set() |> ignore
            let retMeta, obj = !retVal
            let serial = !x.Serial
            x.Serial := serial + int64 retMeta.NumElems
            let retObj = (BlobMetadata( retMeta, serial, retMeta.NumElems ), obj)
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Partition %d, retrieve %s\n" x.PartitionI (retMeta.ToString()) ))
            CacheDataRetrieved (retObj)
        elif x.bEndReached then 
            let meta = x.LastMetadata()
            let retMeta = BlobMetadata( meta, (!x.Serial), meta.NumElems )
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Partition %d, reached end %s\n" x.PartitionI (retMeta.ToString()) ))
            CacheDataRetrieved ( retMeta, null )
        else
            // Always unblock read when write. 
            x.CanRead.Reset() |> ignore
            // recheck - in case other thread has added
            if (not x.IOQueue.IsEmpty) || x.bEndReached then
                x.CanRead.Set() |> ignore
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Partition %d, blocked for read .... \n" x.PartitionI ))
            x.CanWrite.Set() |> ignore
            CacheBlocked x.CanRead
    override x.Unblock() = 
        x.CanRead.Set() |> ignore
    override x.Retrieve( ) = 
        x.RetrieveQueue( ) 
    override x.ToSeq() = 
        let msg = sprintf "PartitionCacheQueue doesnot support ToSeq()"
        Logger.Log( LogLevel.Error, msg )
        let meta = x.LastMetadata()
        Seq.singleton (meta, null )
    static member ConstructPartitionCacheQueue( cacheType:CacheKind, parti:int, serializationLimit:int ) = 
        PartitionCacheQueue<'U>( cacheType, parti, serializationLimit ) :> PartitionCacheBase
    override x.Reset() = 
        x.QueueReset()

/// Cache Structure used to cache a partition of Prajna
[<AllowNullLiteral>]
type internal PartitionCacheEnumerable<'U>( cacheType, parti:int, serializationLimit:int ) = 
    inherit PartitionCacheQueue<'U>( cacheType, parti, serializationLimit )
    member val bReserialization = true with get, set
    member val CachedList : ConcurrentQueue<BlobMetadata*Object> = null with get, set
    member val bAllCached = false with get, set
    member val Pointer = null with get, set
    member x.BaseClearCache() = 
        x.CachedList <- null
        x.bAllCached <- false
        x.QueueReset()
    override x.ClearCache() = 
        x.BaseClearCache()
    override x.Add( meta, objarray, bToQueue ) = 
        if not bToQueue then 
            x.AddBase( meta, objarray ) 
        else 
            x.AddQueue( meta, objarray ) 
    override x.Retrieve() = 
        x.RetrieveQueueOrSeq()
    member x.RetrieveSeq() = 
            if Utils.IsNull x.Pointer then 
                x.Pointer <- (x.ToSeq()).GetEnumerator()
            if x.Pointer.MoveNext() then 
                let meta, elemObject = x.Pointer.Current
                if Utils.IsNull elemObject then 
                    x.Pointer <- null
                meta, elemObject
            else
                x.Pointer <- null
                let meta = x.LastMetadata()
                ( meta, null )

    member x.RetrieveQueueOrSeq() = 
        if not x.bAllCached then 
            x.RetrieveQueue()
        else
            x.RetrieveSeq()        
    member x.AddBase( meta, objarray ) = 
//        if x.CacheType <> CacheKind.EnumerableRetrieve then 
            if Utils.IsNull x.CachedList then 
                lock ( x ) ( fun _ -> 
                    if Utils.IsNull x.CachedList then 
                        x.CachedList <- ConcurrentQueue<_>()
                )    
            x.CachedList.Enqueue( (meta, objarray) )
            if Utils.IsNull objarray then 
                x.bAllCached <- true
                x.SetLastMeta( meta )
    member x.Reserialization( objseq: IEnumerable<'U> ) = 
        if x.bReserialization then 
            if x.SerializationLimit > 0 || Utils.IsNull x.CachedList then 
                let serializationLimit = if x.SerializationLimit > 0 then x.SerializationLimit else DeploymentSettings.DefaultCacheSerializationLimit
                let newList = ConcurrentQueue<_>() 
                let mutable count = 0L
                let mutable countInElemArray = 0
                let mutable ElemArray = null
                for obj in objseq do
                    if countInElemArray = 0 then 
                        ElemArray <- Array.zeroCreate<'U> serializationLimit   
                    ElemArray.[ countInElemArray ] <- obj
                    if countInElemArray = serializationLimit - 1 then 
                        // A full serialization worth of KV objects are generated. 
                        newList.Enqueue( BlobMetadata( x.PartitionI, count - int64 countInElemArray, serializationLimit ), ElemArray :> Object )
                    countInElemArray <- ( countInElemArray + 1 ) % serializationLimit
                    count <- count + 1L
                if countInElemArray > 0 then 
                    let finalElemArray = Array.zeroCreate<'U> countInElemArray
                    Array.Copy( ElemArray, finalElemArray, countInElemArray )
                    newList.Enqueue( BlobMetadata( x.PartitionI, count - int64 countInElemArray, countInElemArray ), finalElemArray :> Object )   
                newList.Enqueue(  BlobMetadata( x.PartitionI, count, 0 ), null )
                x.CachedList <- newList
            x.bReserialization <- false
            x.bAllCached <- true
    member x.ReserializationThis( ) = 
        if x.bReserialization then 
            x.bReserialization <- false
    // Used by derived class, always reserialization once
    member x.ReserializationDerived( objseq ) = 
        if x.bReserialization then 
            x.CachedList <- null 
            let t1 = (PerfDateTime.UtcNow())
            x.Reserialization( objseq )    
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Cached Partition %d serialized to ConcurrentQueue in %f sec" x.PartitionI ((PerfDateTime.UtcNow()).Subtract( t1 ).TotalSeconds) ))
    member x.BaseToSeq( ) = 
        x.CachedList :> seq<_>
    override x.ToSeq() = 
        x.ReserializationThis()
        x.BaseToSeq() 
    static member ConstructPartitionCacheEnumerable( cacheType:CacheKind, parti:int, serializationLimit:int ) = 
        PartitionCacheEnumerable<'U>( cacheType, parti, serializationLimit ) :> PartitionCacheBase

/// Cache Structure used to cache a partition of Prajna
/// This one uses List<_>, we find that it is not observably faster than a ConcurrentQueue, and since ConcurrentQueue has better property, it is used as default. 
[<AllowNullLiteral>]
type internal PartitionCacheList<'U>( cacheType, parti:int, serializationLimit:int ) = 
    inherit PartitionCacheQueue<'U>( cacheType, parti, serializationLimit )
    member val bReserialization = true with get, set
    member val CachedList : List<BlobMetadata*Object> = null with get, set
    member val bAllCached = false with get, set
    member val Pointer = null with get, set
    member x.BaseClearCache() = 
        x.CachedList <- null
        x.bAllCached <- false
        x.QueueReset()
    override x.ClearCache() = 
        x.BaseClearCache()
    override x.Add( meta, objarray, bToQueue ) = 
        if not bToQueue then 
            x.AddBase( meta, objarray ) 
        else 
            x.AddQueue( meta, objarray ) 
    member x.RetrieveSeq() = 
            if Utils.IsNull x.Pointer then 
                x.Pointer <- (x.ToSeq()).GetEnumerator()
            if x.Pointer.MoveNext() then 
                let meta, elemObject = x.Pointer.Current
                if Utils.IsNull elemObject then 
                    x.Pointer <- null
                meta, elemObject
            else
                let meta = x.LastMetadata()
                ( meta, null )
    override x.Retrieve() = 
        x.RetrieveQueueOrSeq()
    member x.RetrieveQueueOrSeq() = 
        if not x.bAllCached then 
            x.RetrieveQueue()
        else
            x.RetrieveSeq()       
    member x.AddBase( meta, objarray ) = 
//        if x.CacheType <> CacheKind.EnumerableRetrieve then 
            if Utils.IsNull x.CachedList then 
                lock ( x ) ( fun _ -> 
                    if Utils.IsNull x.CachedList then 
                        x.CachedList <- List<_>()
                )    
            x.CachedList.Add( (meta, objarray) )
            if Utils.IsNull objarray then 
                x.bAllCached <- true
                x.SetLastMeta( meta )
    member x.Reserialization( objseq: IEnumerable<'U> ) = 
        if x.bReserialization then 
            if x.SerializationLimit > 0 || Utils.IsNull x.CachedList then 
                let serializationLimit = if x.SerializationLimit > 0 then x.SerializationLimit else DeploymentSettings.DefaultCacheSerializationLimit
                let newList = List<_>() 
                let mutable count = 0L
                let mutable countInElemArray = 0
                let mutable ElemArray = null
                for obj in objseq do
                    if countInElemArray = 0 then 
                        ElemArray <- Array.zeroCreate<'U> serializationLimit   
                    ElemArray.[ countInElemArray ] <- obj
                    if countInElemArray = serializationLimit - 1 then 
                        // A full serialization worth of KV objects are generated. 
                        newList.Add( BlobMetadata( x.PartitionI, count - int64 countInElemArray, serializationLimit ), ElemArray :> Object )
                    countInElemArray <- ( countInElemArray + 1 ) % serializationLimit
                    count <- count + 1L
                if countInElemArray > 0 then 
                    let finalElemArray = Array.zeroCreate<'U> countInElemArray
                    Array.Copy( ElemArray, finalElemArray, countInElemArray )
                    newList.Add( BlobMetadata( x.PartitionI, count - int64 countInElemArray, countInElemArray ), finalElemArray :> Object )   
                newList.Add(  BlobMetadata( x.PartitionI, count, 0 ), null )
                x.CachedList <- newList
            x.bReserialization <- false
            x.bAllCached <- true
    member x.ReserializationThis( ) = 
        if x.bReserialization then 
            x.bReserialization <- false
    // Used by derived class, always reserialization once
    member x.ReserializationDerived( objseq ) = 
        if x.bReserialization then 
            x.CachedList <- null 
            x.Reserialization( objseq )    
    member x.BaseToSeq( ) = 
        x.CachedList :> seq<_>
    override x.ToSeq() = 
        x.ReserializationThis()
        x.BaseToSeq() 
    static member ConstructPartitionCacheEnumerable( cacheType:CacheKind, parti:int, serializationLimit:int ) = 
        PartitionCacheList<'U>( cacheType, parti, serializationLimit ) :> PartitionCacheBase

/// Cache Structure used to cache a partition of Prajna
/// The ConcurrentDictionary carries a penalty compared with ConcurrentQueue, 
/// It takes almost 2x to enqueue (even considering processing time), and about 3x to Enumerate, can't figureout why. 
[<AllowNullLiteral>]
type internal PartitionCacheKeyValueStore<'K,'V>(cacheType, parti:int, serializationLimit:int ) = 
    inherit PartitionCacheEnumerable<('K*'V)>(cacheType, parti, serializationLimit)
    member val CachedConcurrentDictionary : ConcurrentDictionary<'K,'V> = null with get, set
    member x.KVClearCache() = 
        x.CachedConcurrentDictionary <- null
    override x.ClearCache() = 
        x.BaseClearCache() 
        x.KVClearCache()
    member val internal Lock = SpinLockSlim(true) with get
    override x.Add( meta, objarray, bToQueue ) = 
//        if x.CacheType &&& CacheKind.ConstructKVCacheMask <> CacheKind.None then 
            if not (Utils.IsNull objarray) then 
                let elemArray = objarray :?> ('K*'V)[]
                let mutable lockTaken = false
                while Utils.IsNull x.CachedConcurrentDictionary do 
                    lockTaken <- x.Lock.TryEnter()
                    if ( lockTaken ) && Utils.IsNull x.CachedConcurrentDictionary then
                        x.CachedConcurrentDictionary <- ConcurrentDictionary<_,_>() 
                if (lockTaken ) then
                    x.Lock.Exit()
                for elem in elemArray do
                    let key, value = elem
                    x.CachedConcurrentDictionary.Item( key ) <- value
            else
                x.SetLastMeta( meta )
                x.bAllCached <- true
                if bToQueue then 
                    x.CanRead.Set() |> ignore
                    if Utils.IsNotNull x.CachedConcurrentDictionary then 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "CacheKeyValue partition %d has received all data ... count = %d" x.PartitionI (x.CachedConcurrentDictionary.Count) ))
                    else
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "CacheKeyValue partition %d has received final object ... count = 0" x.PartitionI ))
    override x.Retrieve() =
        // Not all upstream handles have fired && x.CanRead haven't fired.
        while not (x.CanRead.WaitOne(0)) && x.WaitForRead() do
            ()
        let bCanRead = x.CanRead.WaitOne(0)
        x.RetrieveSeq()
    override x.RetrieveNonBlocking( ) =
        let bCanRead = x.CanRead.WaitOne(0)
        if bCanRead then 
            // This is multi-thread safe, compared with x.RetrieveSeq(), which is not multithread safe. 
            CacheSeqRetrieved (x.ToSeq())
        else
            CacheBlocked x.CanRead
    override x.ToSeq() = 
        x.ReserializationDerived( if Utils.IsNull x.CachedConcurrentDictionary then Seq.empty else x.CachedConcurrentDictionary |> Seq.map ( fun pair -> (pair.Key, pair.Value) ) )
        x.BaseToSeq()        
    static member ConstructPartitionCacheKeyValue( cacheType:CacheKind, parti:int, serializationLimit:int ) = 
        PartitionCacheKeyValueStore<'K,'V>( cacheType, parti, serializationLimit ) :> PartitionCacheBase

// Cache Structure used to cache a partition of Prajna
[<AllowNullLiteral>]
type internal PartitionCacheSortedKeyValueStore<'K,'V>(comparer:IComparer<'K>, cacheType, parti, serializationLimit) = 
    inherit PartitionCacheEnumerable<('K*'V)>(cacheType, parti, serializationLimit)
    member val CachedSortedDictionary : SortedDictionary<'K,'V> = null with get, set
    member x.KVClearCache() = 
        x.CachedSortedDictionary <- null
    override x.ClearCache() = 
        x.BaseClearCache() 
        x.KVClearCache()
    member val internal Lock = SpinLockSlim(true) with get
    override x.Add( meta, objarray, bToQueue ) = 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Add to sorted key, value partition %d ... %s" x.PartitionI (meta.ToString()) ))
            if not (Utils.IsNull objarray) then 
                let elemArray = objarray :?> ('K*'V)[]
                let mutable lockTaken = false
                while Utils.IsNull x.CachedSortedDictionary do 
                    lockTaken <- x.Lock.TryEnter()
                    if ( lockTaken ) && Utils.IsNull x.CachedSortedDictionary then
                        x.CachedSortedDictionary <- SortedDictionary<_,_>(comparer) 
                if not (lockTaken ) then
                    x.Lock.Enter()
                for elem in elemArray do
                    let key, value = elem
                    x.CachedSortedDictionary.Item( key ) <- value
                x.Lock.Exit()
            else
                x.SetLastMeta( meta )
                if bToQueue then 
                    // For sorted queue, the read of queue is only enabled after the entire dataset has been received. 
                    x.CanRead.Set() |> ignore
                    if Utils.IsNotNull x.CachedSortedDictionary then 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "partition %d has received all data ... count = %d" x.PartitionI (x.CachedSortedDictionary.Count) ))
                    else
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "partition %d has received final object ... count = 0" x.PartitionI ))
    override x.Retrieve() =
        // Not all upstream handles have fired && x.CanRead haven't fired.
        while not (x.CanRead.WaitOne(0)) && x.WaitForRead() do
            ()
        let bCanRead = x.CanRead.WaitOne(0)
        x.RetrieveSeq()
    override x.RetrieveNonBlocking( ) =
        let bCanRead = x.CanRead.WaitOne(0)
        if bCanRead then 
            // This is multi-thread safe, compared with x.RetrieveSeq(), which is not multithread safe. 
            CacheSeqRetrieved (x.ToSeq())
        else
            CacheBlocked x.CanRead
    override x.ToSeq() = 
        x.ReserializationDerived( if Utils.IsNull x.CachedSortedDictionary then Seq.empty else x.CachedSortedDictionary |> Seq.map ( fun pair -> (pair.Key, pair.Value) ) )
        x.BaseToSeq()    
    static member ConstructPartitionCacheSortedKeyValue( comparer:IComparer<'K> ) ( cacheType:CacheKind, parti:int, serializationLimit:int ) = 
        PartitionCacheSortedKeyValueStore<'K,'V>( comparer, cacheType, parti, serializationLimit ) :> PartitionCacheBase
    

// Cache Structure used to cache a partition of Prajna
[<AllowNullLiteral>]
type internal PartitionCacheSortedSet<'U>(comparer:IComparer<'U>, cacheType, parti, serializationLimit) = 
    inherit PartitionCacheEnumerable<'U>(cacheType, parti, serializationLimit)
    member val CachedSortedDictionary : SortedDictionary<'U,int> = null with get, set
    member x.KVClearCache() = 
        x.CachedSortedDictionary <- null
    override x.ClearCache() = 
        x.BaseClearCache() 
        x.KVClearCache()
    member val internal Lock = SpinLockSlim(true) with get
    override x.Add( meta, objarray, bToQueue ) = 
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Add to sorted set partition %d ... %s" x.PartitionI (meta.ToString()) ))
//        if x.CacheType &&& CacheKind.ConstructKVCacheMask <> CacheKind.None then 
            if not (Utils.IsNull objarray) then 
                let elemArray = objarray :?> ('U)[]
                if ( Utils.IsNull  x.CachedSortedDictionary ) then 
                    lock( x ) ( fun _ -> 
                        if Utils.IsNull x.CachedSortedDictionary then 
                            x.CachedSortedDictionary <- SortedDictionary<_,_>(comparer) 
                        )
                lock ( x ) ( fun _ -> 
                    for elem in elemArray do
                       x.CachedSortedDictionary.Item( elem ) <- 0
                )
            else
                x.SetLastMeta( meta )
                if bToQueue then 
                // For sorted queue, the read of queue is only enabled after the entire dataset has been received. 
                    x.CanRead.Set() |> ignore
                    if Utils.IsNotNull x.CachedSortedDictionary then 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "partition %d has received all data ... count = %d" x.PartitionI (x.CachedSortedDictionary.Count) ))
                    else
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "partition %d has received final object ... count = 0" x.PartitionI ))
    override x.Retrieve() =
        // Not all upstream handles have fired && x.CanRead haven't fired.
        while not (x.CanRead.WaitOne(0)) && x.WaitForRead() do
            ()
        let bCanRead = x.CanRead.WaitOne(0)
        x.RetrieveSeq()
    override x.RetrieveNonBlocking( ) =
        let bCanRead = x.CanRead.WaitOne(0)
        if bCanRead then 
            // This is multi-thread safe, compared with x.RetrieveSeq(), which is not multithread safe. 
            CacheSeqRetrieved (x.ToSeq())
        else
            CacheBlocked x.CanRead
    override x.ToSeq() = 
        x.ReserializationDerived( if Utils.IsNull x.CachedSortedDictionary then Seq.empty else x.CachedSortedDictionary |> Seq.map ( fun pair -> pair.Key ) )
        x.BaseToSeq()         
    static member ConstructPartitionCacheSortedSet ( comparer:IComparer<'U>) ( cacheType:CacheKind, parti:int, serializationLimit:int ) = 
        PartitionCacheSortedSet<'U>( comparer, cacheType, parti, serializationLimit ) :> PartitionCacheBase

[<AllowNullLiteral>]
type internal PartitionCacheConcurrentQueue<'U>(cacheType, parti, serializationLimit) = 
    inherit PartitionCacheEnumerable<'U>(cacheType, parti, serializationLimit)
    member val CachedQueue : ConcurrentQueue<_> = null with get, set
    member val nEndReceived = ref 0 with get
    member x.ConcurrentQueueClearCache() = 
        x.CanRead.Reset() |> ignore
        x.CachedQueue <- null
        x.nEndReceived := 0 
    override x.ClearCache() = 
        x.BaseClearCache() 
        x.ConcurrentQueueClearCache()
    member val internal Lock = SpinLockSlim(true) with get
    override x.Add( meta, objarray, bToQueue ) = 
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Add to sorted set partition %d ... %s" x.PartitionI (meta.ToString()) ))
            if ( Utils.IsNull x.CachedQueue ) then 
                lock( x ) ( fun _ -> 
                    if Utils.IsNull x.CachedQueue then 
                        x.CachedQueue <- ConcurrentQueue<_>() 
                    )
            if not (Utils.IsNull objarray) then 
                if (!x.nEndReceived)=0 then 
                    x.CachedQueue.Enqueue( (meta, objarray) )
                else
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Partition %d has already received null, but receive %A, it will be discarded " x.PartitionI meta ))
            else
                if Interlocked.CompareExchange( x.nEndReceived, 1, 0)=0 then 
                    x.CachedQueue.Enqueue( (meta, objarray) )
                    x.SetLastMeta( meta )
                    if bToQueue then 
                    // For sorted queue, the read of queue is only enabled after the entire dataset has been received. 
                        x.CanRead.Set() |> ignore
                        if Utils.IsNotNull x.CachedQueue then 
                            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "partition %d has received all data ... count = %d" x.PartitionI (x.CachedQueue.Count) ))
                        else
                            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "partition %d has received final object ... count = 0" x.PartitionI ))
                else
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Partition %d has received multiple end object (null), the current one %A will be discarded " x.PartitionI meta ))
    override x.Retrieve() =
        // Not all upstream handles have fired && x.CanRead haven't fired.
        while not (x.CanRead.WaitOne(0)) && x.WaitForRead() do
            ()
        let bCanRead = x.CanRead.WaitOne(0)
        x.RetrieveSeq()
    override x.RetrieveNonBlocking( ) =
        let bCanRead = x.CanRead.WaitOne(0)
        if bCanRead then 
            // This is multi-thread safe, compared with x.RetrieveSeq(), which is not multithread safe. 
            CacheSeqRetrieved (x.ToSeq())
        else
            CacheBlocked x.CanRead
    override x.ToSeq() = 
        if Utils.IsNull x.CachedQueue then Seq.empty else x.CachedQueue :> seq<_> 
    static member ConstructPartitionCacheConcurrentQueue ( cacheType:CacheKind, parti:int, serializationLimit:int ) = 
        PartitionCacheConcurrentQueue<'U>( cacheType, parti, serializationLimit ) :> PartitionCacheBase
