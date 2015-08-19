(*---------------------------------------------------------------------------
	Copyright 2015 Microsoft

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
		blobcache.fs
  
	Description: 
		A cache of blobs for efficient communication between two service. 
    Each blob represent a byte[] and a typeinfo[], and is hashed to a Guid. 


	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Jan. 2015
	
 ---------------------------------------------------------------------------*)

namespace Prajna.Service.ServiceEndpoint

open System
open System.Collections.Generic
open System.Security.Cryptography
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Prajna.Core
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp

/// <summary> 
/// Blob cache will be used by front-end and back-end to cache object for efficient service. 
/// Each CacheableBuffer is represented by a byte[] and a typeinfo[], and is hashed to a Guid. If the 
/// Guid doesn't exist, it will need to be requested from the other party. 
/// </summary>
type CacheableBuffer = 
    struct
        ///　Buffer
        val Buffer : byte[]
        /// Metadata type of buffer
        val TypeInfo : byte[]
        /// Create a CacheableBuffer of certain content and type 
        new (b, typeinfo ) = { Buffer = b; TypeInfo = typeinfo }
    end
    /// A null buffer
    static member NullBuffer() = 
        CacheableBuffer( null, null )

/// <summary> 
/// Block cache will be used by front-end and back-end to cache object for efficient service. 
/// Each blob is represented by a byte[] and a typeinfo[], and is hashed to a Guid. If the 
/// Guid doesn't exist, it will need to be requested from the other party. 
/// </summary>
type BufferCache() = 
    /// Current system block cache
    static member val Current = BufferCache() with get
    /// <summary> timeout(ms), if the CacheableBuffer is not retrieved by then, it will be considered as null </summary>
    static member val TimeOutRetrieveCacheableBufferInMs = 10000 with get, set
    /// <summary> timeout(ms), if the CacheableBuffer is not used through this interval, it will be released from memory </summary>
    static member val TimeOutReleaseCacheableBufferInMs = 1000000L with get, set
    member val internal BufferCollection = ConcurrentDictionary<_,_>() with get
    member val internal PendingBuffer = ConcurrentDictionary<_,(ManualResetEvent*int64)>() with get
    /// <summary> 
    /// Calculate the hash of a buffer + type 
    /// </summary> 
    static member HashBufferAndType( buffer:byte[], typeBuf:byte[] ) = 
        let hash = new SHA256Managed()
        hash.TransformBlock( buffer, 0, buffer.Length, buffer, 0 ) |> ignore
        hash.TransformFinalBlock( typeBuf, 0, typeBuf.Length ) |> ignore
        Guid( Array.sub hash.Hash 0 16 )
    /// <summary>
    /// Add a Cacheable Buffer to store
    /// If there are any threads that wait for the Cacheable Buffer, they are waken. 
    /// </summary>
    member x.AddCacheableBuffer( y: CacheableBuffer ) = 
        // Only use the first 16B 
        let id = BufferCache.HashBufferAndType( y.Buffer, y.TypeInfo )
        let z = x.BufferCollection.GetOrAdd( id, (y, ref Unchecked.defaultof<_>) )
        let _, ticksRef = z
        ticksRef := (PerfDateTime.UtcNowTicks())
        let bExist, tuple = x.PendingBuffer.TryRemove( id ) 
        if bExist then 
            let ev, _ = tuple
            ev.Set() |> ignore
        id
    /// <summary> 
    /// Get a cachable buffer item by key, if the item exists, it will be retrieved, if not, null will be retrieved. 
    /// </summary>
    member x.GetCacheableBufferById( id ) = 
        let refValue = ref Unchecked.defaultof<_>
        if x.BufferCollection.TryGetValue( id, refValue ) then 
            let buffer, ticksRef = !refValue
            ticksRef := (PerfDateTime.UtcNowTicks())
            Some buffer
        else
            None

    /// <summary>
    /// Try to retrieve a Cacheable Buffer from store, call a retrieveFunc if the buffer does not exist. 
    /// </summary>
    /// <param name="id"> Guid of the Cacheable buffer to be retrieved. </param>
    /// <param name="retrieveFunc"> Callback to be called to retrieve CacheableBuffer according to id. </param>
    /// <param name="timeoutInMs"> Timout value, in milliseconds. </param>
    /// <returns> CacheableBuffer (if successfully retrieved )
    ///         CacheableBuffer.NullBuffer() (if retrieving fails)
    /// </returns>
    member x.RetrieveCacheableBuffer( id: Guid, retrieveFunc: Guid -> bool * ManualResetEvent, timeoutInMs:int ) = 
        let bExist, tuple = x.BufferCollection.TryGetValue( id ) 
        if bExist then 
            let buffer, ticksRef = tuple
            ticksRef := (PerfDateTime.UtcNowTicks()) // mark the blob to be used, so that it won't be cleared away readily
            buffer
        else
            let bInRetrieve, tuple = x.PendingBuffer.TryGetValue( id ) 
            if not bInRetrieve then 
                // Can't find the current cachableBuffer
                let bRetrieved, ev = retrieveFunc( id )
                if not bRetrieved && not (Utils.IsNull ev) then 
                    x.PendingBuffer.Item( id ) <- ( ev, (PerfDateTime.UtcNowTicks()) )
                    let bFire = ev.WaitOne( timeoutInMs ) 
                    ()
            else
                let ev, ticks = tuple 
                ev.WaitOne( timeoutInMs ) |> ignore
            let bExist, tuple = x.BufferCollection.TryGetValue( id ) 
            if bExist then 
                let buffer, ticksRef = tuple
                ticksRef := (PerfDateTime.UtcNowTicks()) // mark the blob to be used, so that it won't be cleared away readily
                buffer
            else
                CacheableBuffer.NullBuffer()

    /// <summary>
    /// Try to retrieve a Cacheable Buffer from store, call a retrieveFunc if the buffer does not exist. 
    /// </summary>
    /// <param name="id"> Guid of the Cacheable buffer to be retrieved. </param>
    /// <param name="retrieveFunc"> Callback to be called to retrieve CacheableBuffer according to id. </param>
    /// <param name="timeoutInMs"> Timout value, in milliseconds. </param>
    /// <returns> Task(CacheableBuffer)
    /// </returns>
    member x.RetrieveCacheableBufferAsync( id: Guid, retrieveFunc: Guid -> bool * ManualResetEvent, timeoutInMs:int ) = 
        let bExist, tuple = x.BufferCollection.TryGetValue( id ) 
        if bExist then 
            let buffer, ticksRef = tuple
            ticksRef := (PerfDateTime.UtcNowTicks()) // mark the blob to be used, so that it won't be cleared away readily
            new Task<_>( fun _ -> buffer )
        else
            let bInRetrieve, tuple = x.PendingBuffer.TryGetValue( id ) 
            let handleToWait =
                if not bInRetrieve then 
                    // Can't find the current cachableBuffer
                    let bRetrieved, ev = retrieveFunc( id )
                    if not bRetrieved && not (Utils.IsNull ev) then 
                        x.PendingBuffer.Item( id ) <- ( ev, (PerfDateTime.UtcNowTicks()) )
                        ev
                    else
                        null 
                    
                else
                    let ev, ticks = tuple 
                    ev
            if Utils.IsNull handleToWait then 
                let returnBuffer = 
                    let bExist, tuple = x.BufferCollection.TryGetValue( id ) 
                    if bExist then 
                        let buffer, ticksRef = tuple
                        ticksRef := (PerfDateTime.UtcNowTicks()) // mark the blob to be used, so that it won't be cleared away readily
                        buffer
                    else
                        CacheableBuffer( null, null )
                new Task<_>( fun _ -> returnBuffer ) 
            else
                let taskSource = TaskCompletionSource<_>()
                let continuationFunc() = 
                    let bExist, tuple = x.BufferCollection.TryGetValue( id ) 
                    if bExist then 
                        let buffer, ticksRef = tuple
                        ticksRef := (PerfDateTime.UtcNowTicks()) // mark the blob to be used, so that it won't be cleared away readily
                        taskSource.SetResult( buffer ) 
                    else
                        taskSource.SetResult( CacheableBuffer( null, null ) )
                ThreadPoolWait.WaitForHandle ( fun _ -> sprintf "Wait to retrieve CacheableBuffer ID %A" id ) handleToWait continuationFunc null
                taskSource.Task

    /// <summary>
    /// Try to retrieve a CacheableBuffer from store. If the Cacheable Buffer identified by the ID doesn't exist, the current thread blocks, 
    /// where a retrievale function is called to retrieve the Cachable Buffer. The retrieveFunc return true if the Cacheable buffer 
    /// is immediately avaialble. If return false, the GetCacheableBuffer will wait till the buffer is deposited by AddCacheableBuffer call. 
    /// </summary>
    member x.RetrieveCacheableBuffer( id: Guid, retrieveFunc ) = 
        x.RetrieveCacheableBuffer( id, retrieveFunc, BufferCache.TimeOutRetrieveCacheableBufferInMs )
    /// Free the resource associated with a CacheableBuffer. 
    member x.FreeCacheableBuffer( id ) = 
        // Release a certain cachable buffer
        x.BufferCollection.TryRemove( id ) |> ignore
        let bExist, tuple = x.PendingBuffer.TryRemove( id ) 
        if bExist then 
            let ev, ticks = tuple
            ev.Set() |> ignore
    /// <summary>
    /// Clean up CacheableBuffer. If a certain buffer hasn't been used for a certain period of time, it will be removed from cache. 
    /// </summary>
    member x.CleanUp( timeOutRetrieveCacheableBufferInMs:int, timeOutReleaseCacheableBufferInMs:int64 ) = 
        let allowTicks = if timeOutRetrieveCacheableBufferInMs<0 then Int64.MaxValue else int64 timeOutRetrieveCacheableBufferInMs * TimeSpan.TicksPerMillisecond
        if allowTicks < Int64.MaxValue then 
            for pair in x.PendingBuffer do 
                let ev, ticks = pair.Value
                let ticksCur = (PerfDateTime.UtcNowTicks())
                if ticks < ticksCur - allowTicks then 
                    ev.Set() |> ignore // Wake up the thread that waits on CacheableBuffer. 
        let allowTicks = 
            if timeOutReleaseCacheableBufferInMs<=0L || timeOutReleaseCacheableBufferInMs > Int64.MaxValue / TimeSpan.TicksPerMillisecond then 
                Int64.MaxValue 
            else 
                timeOutReleaseCacheableBufferInMs * TimeSpan.TicksPerMillisecond
        if allowTicks < Int64.MaxValue then 
            for pair in x.BufferCollection do 
                let _, ticksRef = pair.Value
                let ticksCur = (PerfDateTime.UtcNowTicks())
                if !ticksRef < ticksCur - allowTicks then 
                    x.BufferCollection.TryRemove( pair.Key ) |> ignore
    /// <summary>
    /// Clean up CacheableBuffer. If a certain buffer hasn't been used for a certain period of time, it will be removed from cache. 
    /// </summary>
    member x.CleanUp( ) =
        x.CleanUp( BufferCache.TimeOutRetrieveCacheableBufferInMs, BufferCache.TimeOutReleaseCacheableBufferInMs )
    /// <summary> 
    /// Serialize an array of Guid
    /// </summary> 
    static member PackGuid( arr: Guid[], ms: MemStream ) = 
        let len = if Utils.IsNull arr then 0 else arr.Length
        ms.WriteVInt32( len ) 
        for i = 0 to len - 1 do 
            ms.WriteBytes( arr.[i].ToByteArray() )
    /// <summary> 
    /// Deserialize the guid 
    /// </summary> 
    static member UnPackGuid( ms: MemStream ) = 
        let len = ms.ReadVInt32( ) 
        let arr = Array.zeroCreate<_> len 
        let buf = Array.zeroCreate<_> 16
        for i = 0 to len - 1 do 
            ms.ReadBytes( buf ) |> ignore
            arr.[i] <- Guid(buf)
        arr
    /// <summary> 
    /// Serialize an array of CacheableBuffer 
    /// </summary> 
    static member PackCacheableBuffers( arr: CacheableBuffer[], ms: MemStream ) = 
        let len = if Utils.IsNull arr then 0 else arr.Length
        ms.WriteVInt32( len ) 
        for i = 0 to len - 1 do 
            ms.WriteBytesWVLen( arr.[i].Buffer )
            ms.WriteBytesWVLen( arr.[i].TypeInfo ) 
    /// <summary> 
    /// Deserialize an array of CacheableBuffer 
    /// </summary> 
    static member UnPackCacheableBuffers( ms: MemStream ) = 
        let len = ms.ReadVInt32( ) 
        let arr = Array.zeroCreate<_> len 
        for i = 0 to len - 1 do 
            let buf = ms.ReadBytesWVLen() 
            let typeInfo = ms.ReadBytesWVLen() 
            arr.[i] <- CacheableBuffer( buf, typeInfo)
        arr
    /// <summary> 
    /// Get the collection of Guid of the current CacheableBuffer.
    /// </summary>
    member x.GetGuidCollections( ) =
        let arr = x.BufferCollection.Keys |> Seq.toArray
        arr
    /// <summary>
    /// Is this guid missing? If missing, register in PendingBuffer, and only report true if the registration succeeded. 
    /// </summary> 
    member internal x.IsMissingGuid( id ) =
        if not(x.BufferCollection.ContainsKey( id )) then 
            let tuple = new ManualResetEvent(false), (PerfDateTime.UtcNowTicks())
            let added = x.PendingBuffer.GetOrAdd( id, tuple ) 
            if Object.ReferenceEquals( added, tuple ) then 
                true
            else
                false
        else
            false
    /// <summary> 
    /// Finding those Guids that do not exist in the current CacheableBuffer. 
    /// </summary>
    member x.FindMissingGuids( arr: Guid[] ) =
        arr |> Array.filter ( x.IsMissingGuid )
    /// <summary> 
    /// Pack a collection of CacheableBuffer specified by Guid. 
    /// </summary>
    member x.FindCacheableBufferByGuid( arr: Guid[] ) = 
        let items = arr |> Array.choose( x.GetCacheableBufferById ) 
        items

            

            

