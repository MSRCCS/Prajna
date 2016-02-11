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
        bufferliststream.fs
  
    Description: 
        A stream which is a list of buffers

    Author:
        Sanjeev Mehrotra, Principal Software Architect
    Date:
        July 2015	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading
open System.Runtime.InteropServices

open Prajna.Tools
open Prajna.Tools.Queue
open Prajna.Tools.FSharp

// =====================================================================

/// An array which maintains alignment in memory (for use by native code)
/// <param name="size">The number of elements of type 'T</param>
/// <param name="align">The alignment required as number of bytes</param>
type [<AllowNullLiteral>] internal ArrAlign<'T>(size : int, alignBytes : int) =
    do
        if (alignBytes/sizeof<'T>*sizeof<'T> <> alignBytes) then
            raise (Exception("Invalid alignment size - must be multiple of element size"))
    let align = alignBytes / sizeof<'T>
    let alignSize = (size + align - 1)/align*align
    let mutable arr = Array.zeroCreate<'T>(alignSize + align - 1)
    let handle = GCHandle.Alloc(arr, GCHandleType.Pinned)
    let offsetBytes = handle.AddrOfPinnedObject().ToInt64() % (int64 alignBytes)
    let offset = offsetBytes / int64(sizeof<'T>)
    let mutable dispose = false

    interface IDisposable with
        override x.Dispose() =
            if (not dispose) then
                lock (x) (fun _ ->
                    if (not dispose) then
                        arr <- null
                        handle.Free()
                        GC.SuppressFinalize(x)
                        dispose <- true
                )

    static member AlignSize(size : int, alignBytes : int) =
        let align = (alignBytes + sizeof<'T> - 1) / sizeof<'T>
        let alignBytes = align * sizeof<'T>
        (size + align - 1)/align*align

    member x.Arr with get() = arr
    member x.Offset with get() = int offset
    member x.GCHandle with get() = handle
    member x.Ptr with get() = IntPtr.Add(handle.AddrOfPinnedObject(), int offsetBytes)
    member x.Size with get() = alignSize

// ==============================================

// Helper classes for ref counted objects & shared memory pool
// A basic refcounter interface
type [<AllowNullLiteral>] IRefCounter<'K> =
    interface   
#if DEBUGALLOCS
        abstract Allocs : ConcurrentDictionary<string, string> with get // for debugging allocations
#endif
        abstract DebugInfo : 'K with get, set
        abstract Key : 'K with get
        abstract Release : (IRefCounter<'K>->unit) with get, set
        abstract SetRef : int64->unit
        abstract GetRef : int64 with get
        abstract AddRef : unit->int64
        abstract DecRef : unit->int64
    end

type internal BufferListDebugging =
    static member DebugLeak = false
    static member DumpStreamLogLevel = LogLevel.WildVerbose
    static member DumpPoolLogLevel = LogLevel.WildVerbose
    static member PoolAllocLogLevel = LogLevel.WildVerbose

// A shared pool of RefCounters
//type [<AllowNullLiteral>] internal SharedPool<'K,'T when 'T :> IRefCounter and 'T:(new:unit->'T)> private () =
type [<AllowNullLiteral>] internal SharedPool<'K,'T when 'T :> IRefCounter<'K> and 'T:(new:unit->'T)>() =
    let mutable stack : SharedStack<'T> = null
    let mutable info : 'K = Unchecked.defaultof<'K>

    let usedList = 
#if DEBUG
        if (BufferListDebugging.DebugLeak) then
            new ConcurrentDictionary<'K,'T>()
        else
#endif
            null

    member private x.Stack with get() = stack and set(v) = stack <- v
    member x.GetStack with get() = stack
    member x.StackSize with get() = stack.Count()

    member x.DumpInUse(?level : LogLevel) : unit =
#if DEBUG
        let level = defaultArg level BufferListDebugging.DumpPoolLogLevel
        if (BufferListDebugging.DebugLeak) then
            Logger.LogF(level, fun _ -> sprintf "SharedPool %A has %d objects in use" info usedList.Count)
            Logger.LogF(level, fun _ ->
                let sb = System.Text.StringBuilder()
                for o in usedList do
                    sb.AppendLine(sprintf "Used object %A : %A : %A : %A" o.Key o.Value o.Value.Key o.Value.DebugInfo) |> ignore
#if DEBUGALLOCS
                    for a in o.Value.Allocs do
                        sb.AppendLine(sprintf "Ref from %s : %s" a.Key a.Value) |> ignore
#endif
                sb.ToString()
            )
#endif
        ()

    abstract InitStack : int*int*'K -> unit
    default x.InitStack(initSize : int, maxSize : int, _info : 'K) =
        info <- _info
        x.Stack <- new SharedStack<'T>(initSize, x.Alloc info, info.ToString())
        let lstack : SharedStack<'T> = x.Stack
        if (maxSize > 0) then
            x.Stack.MaxStackSize <- maxSize

    member x.BaseAlloc (info : 'K) (elem : 'T) =
        elem.Release <- x.Release

    abstract Alloc : 'K->'T->unit
    default x.Alloc (info : 'K) (elem : 'T) =
        x.BaseAlloc info elem

    abstract Release : IRefCounter<'K>->unit
    default x.Release(elem : IRefCounter<'K>) =
#if DEBUG
        let key = elem.Key // make copy in case it gets overwritten
#endif
        stack.ReleaseElem(elem :?> 'T)
#if DEBUG
        if (BufferListDebugging.DebugLeak) then
            usedList.TryRemove(key) |> ignore
#endif

    abstract GetElem : 'K->ManualResetEvent*'T
    default x.GetElem(info : 'K) =
        let (event, elem) = stack.GetElem()
#if DEBUG
        if (BufferListDebugging.DebugLeak) then
            elem.DebugInfo <- info
            usedList.[elem.Key] <- elem
#endif
        (event, elem)

    abstract GetElem : 'K*'T ref->ManualResetEvent
    default x.GetElem (info : 'K, elem : 'T ref) =
        let event = stack.GetElem(elem)
#if DEBUG
        if (BufferListDebugging.DebugLeak) then
            (!elem).DebugInfo <- info
            usedList.[(!elem).Key] <- !elem
#endif
        event

[<AllowNullLiteral>]
type SafeRefCnt<'T when 'T:null and 'T :> IRefCounter<string>> (infoStr : string)=
    [<DefaultValue>] val mutable private info : string
    [<DefaultValue>] val mutable private baseInfo : string

    static let g_id = ref -1L
    let mutable id = Interlocked.Increment(g_id) //mutable for GetFromPool
    let bRelease = ref 0
    let mutable elem : 'T = null

    new(infoStr : string, e : 'T) as x =
        new SafeRefCnt<'T>(infoStr)
        then
            x.SetElement(e)

    new(infoStr : string, createNew : unit->SafeRefCnt<'T>) =
        new SafeRefCnt<'T>(infoStr, createNew())

    new(infoStr : string, e : SafeRefCnt<'T>) as x =
        new SafeRefCnt<'T>(infoStr)
        then
            x.Element <- e.Elem // check for released element prior to setting
            x.RC.AddRef() |> ignore
            let e : 'T = x.Element
            x.info <- infoStr + ":" + x.Id.ToString()
#if DEBUGALLOCS
            x.Element.Allocs.[x.info] <- Environment.StackTrace
            Logger.LogF(BufferListDebugging.PoolAllocLogLevel, fun _ -> sprintf "Also using %s for id %d - refcount %d" x.Element.Key x.Id x.Element.GetRef)
#endif

    member x.SetElement(e : 'T) =
        x.Element <- e
        x.RC.AddRef() |> ignore
        x.info <- infoStr + ":" + x.Id.ToString()
#if DEBUGALLOCS
        x.Element.Allocs.[x.info] <- Environment.StackTrace
        Logger.LogF(BufferListDebugging.PoolAllocLogLevel, fun _ -> sprintf "Using element %s for id %d - refcount %d" x.Element.Key x.Id x.Element.GetRef)
#endif
    
    static member internal GetFromPool<'TP when 'TP:null and 'TP:(new:unit->'TP) and 'TP :> IRefCounter<string>>
                           (infoStr : string, pool : SharedPool<string,'TP>, createNew : unit->SafeRefCnt<'T>) : ManualResetEvent*SafeRefCnt<'T> =
        let idGet = Interlocked.Increment(g_id)
        let getInfo = infoStr + ":" + idGet.ToString()
        let (event, poolElem) = pool.GetElem(getInfo)
        if (Utils.IsNotNull poolElem) then
            let x = createNew()
            x.SetId(idGet)
            x.Element <- poolElem :> IRefCounter<string> :?> 'T
            x.InitElem(x.Element)
            x.RC.SetRef(1L)
            x.baseInfo <- infoStr
            x.info <- infoStr + ":" + x.Id.ToString()
#if DEBUGALLOCS
            x.Element.Allocs.[x.info] <- Environment.StackTrace
            Logger.LogF(BufferListDebugging.PoolAllocLogLevel, fun _ -> sprintf "Using pool element %s for id %d - refcount %d" x.Element.Key x.Id x.Element.GetRef)
#endif
            (event, x)
        else
            (event, null)

    abstract InitElem : 'T->unit
    default x.InitElem(poolElem) =
        ()

    abstract ReleaseElem : Option<bool>->unit
    default x.ReleaseElem(bFinalize : Option<bool>) =
        if (Utils.IsNotNull elem) then
            let bFinalize = defaultArg bFinalize false
#if DEBUGALLOCS
            x.Element.Allocs.TryRemove(x.info) |> ignore
            Logger.LogF(BufferListDebugging.PoolAllocLogLevel, fun _ -> sprintf "Releasing %s with id %d elemId %s finalize %b - refcount %d" infoStr id x.RC.Key bFinalize x.Element.GetRef)
#endif
            let newCount = x.RC.DecRef()
            if (0L = newCount) then
                x.RC.Release(x.RC)
            else if (newCount < 0L) then
                failwith (sprintf "RefCount object %s has Illegal ref count of %d" x.RC.Key x.RC.GetRef)

    member private x.ReleaseInternal(?bFinalize : bool) =
        if (Interlocked.CompareExchange(bRelease, 1, 0) = 0) then
            x.ReleaseElem(bFinalize)

    override x.Finalize() =
        x.ReleaseInternal(true)
        // base.Finalize // should be called if deriving from another class which has Finalize

    interface IDisposable with
        member x.Dispose() =
            x.ReleaseInternal(false)
            GC.SuppressFinalize(x)

    member private x.Element with get() = elem and set(v) = elem <- v
    member private x.RC with get() : IRefCounter<string> = (elem :> IRefCounter<string>)

    // use to access the element from outside
    /// Obtain element contained wit
    member x.Elem
        with get() : 'T = 
            if (!bRelease = 1) then
                failwith (sprintf "Already Released %s %d" infoStr id)
            else
                elem

    member x.ElemNoCheck
        with get() =
            elem

    member x.Id with get() = id
    member private x.SetId(v) =
        id <- v

type [<AbstractClass>] [<AllowNullLiteral>] RefCountBase() =
    let mutable key = ""

    member val RefCount = ref 0L with get
    member x.RC with get() = (x :> IRefCounter<string>)
    member x.SetKey(k : string) =
        key <- k

    interface IRefCounter<string> with
#if DEBUGALLOCS
        override val Allocs = ConcurrentDictionary<string, string>() with get
#endif
        override x.Key with get() = key
        override val DebugInfo : string = "" with get, set
        override val Release : IRefCounter<string>->unit = (fun _ -> ()) with get, set
        override x.SetRef(v) =
            x.RefCount := v
        override x.GetRef with get() = !x.RefCount
        override x.AddRef() =
            Interlocked.Increment(x.RefCount)
        override x.DecRef() =
            Interlocked.Decrement(x.RefCount)

// id counter for class 'T
type IdCounter<'T>() =
    static let id = ref -1L
    static member GetNext() =
        Interlocked.Increment(id)

// ======================================

// use AddRef/DecRef to acquire / release resource
// should not directly use this class, as there is no backup resource freeing in destructor (no override Finalize)
// this is because otherwise race condition can cause ReleaseElem to be called incorrectly
// for example:
// 1. in finalize bRelease is set to 1 and element is released back to shared stack
// 2. another bufferliststream picks up this element and sets bRelease back to 0
// 3. then finalize on rbufpart is called (or release via finalize in list is called)
// 4. since bRelease is 0, element gets released again even though it is still in use
// could fix this by making refcntbuf single use and just getting and releasing byte[], but byte[] not supported by SharedStack
// and RBufPart already encapsulates RefCntBuf, so there is no need
type [<AllowNullLiteral>] RefCntBuf<'T>() =
    inherit RefCountBase()

    static let g_id = ref -1
    let id = Interlocked.Increment(g_id)

    let mutable buffer : 'T[] = null
    let mutable offset = 0
    let mutable length = 0
    let mutable readIO : MEvent = null
    let mutable writeIO : MEvent = null
    let disposer = new DoOnce()

    new(size : int) as x =
        new RefCntBuf<'T>()
        then
            x.SetBuffer(Array.zeroCreate<'T>(size), 0, size)

    new(buf : 'T[]) as x =
        new RefCntBuf<'T>()
        then 
            x.SetBuffer(buf, 0, buf.Length)

    abstract Ptr : IntPtr with get
    default x.Ptr with get() = IntPtr.Zero

    member internal x.ReadIO with get() = readIO
    member internal x.WriteIO with get() = writeIO

    member x.InitIOEvent() =
        lock (x) (fun _ ->
            if (Utils.IsNull readIO) then
                readIO <- MEvent(false)
            if (Utils.IsNull writeIO) then
                writeIO <- MEvent(false)
        )

    static member InitForIO(x : RefCntBuf<'T>) =
        x.InitIOEvent()

    member x.Reset() =
        x.RC.SetRef(0L)

    abstract Alloc : int->unit
    default x.Alloc (size : int) =
        x.SetBuffer(Array.zeroCreate<'T>(size), 0, size)

    member internal x.Id with get() = id

    // protected method
    member internal x.SetBuffer(v : 'T[], _offset : int, _length : int) =
        buffer <- v
        offset <- _offset
        length <- _length

    member internal x.Buffer with get() = buffer
    member internal x.Offset with get() = offset
    member internal x.Length with get() = length

    member val UserToken : obj = null with get, set

    abstract DisposeInternal : unit->unit
    default x.DisposeInternal() =
        ()

    interface IDisposable with
        override x.Dispose() =
            disposer.Run(x.DisposeInternal)

[<AllowNullLiteral>]
type internal RefCntBufAlign<'T>() =
    inherit RefCntBuf<'T>()

    let mutable bufAlign : ArrAlign<'T> = null

    new (size : int, alignBytes : int) as x =
        new RefCntBufAlign<'T>()
        then
            x.BufAlign <- new ArrAlign<'T>(size, alignBytes)
            x.SetBuffer(x.BufAlign.Arr, x.BufAlign.Offset, x.BufAlign.Size)

    new (arr : ArrAlign<'T>) as x =
        new RefCntBufAlign<'T>()
        then
            x.BufAlign <- arr
            x.SetBuffer(x.BufAlign.Arr, x.BufAlign.Offset, x.BufAlign.Size)

    override x.Ptr
        with get() =
            IntPtr.Add(bufAlign.GCHandle.AddrOfPinnedObject(), x.Offset*sizeof<'T>)

    static member val AlignBytes = 1 with get, set

    member private x.BufAlign with get() : ArrAlign<'T> = bufAlign and set(v) = bufAlign <- v

    override x.Alloc(size : int) =
        bufAlign <- new ArrAlign<'T>(size, RefCntBufAlign<'T>.AlignBytes)
        x.SetBuffer(bufAlign.Arr, bufAlign.Offset, bufAlign.Size)

    override x.DisposeInternal() =
        if (Utils.IsNotNull bufAlign) then
            (bufAlign :> IDisposable).Dispose()
            bufAlign <- null
        base.DisposeInternal()

[<AllowNullLiteral>]
type RefCntBufChunkAlign<'T>() =
    inherit RefCntBuf<'T>()

    static let mutable currentChunk : ArrAlign<'T> = null
    static let mutable chunkOffset : int = 0
    static let mutable chunkCount : int ref = ref 0
    static let chunkLock = Object()

    let mutable refCount : int ref = ref 0

    new (size : int, alignBytes : int) as x =
        new RefCntBufChunkAlign<'T>()
        then
            x.Alloc(size, alignBytes)

    static member val AlignBytes = 4096 with get, set // sector align
    static member val ChunkSize = 1<<<18 with get, set

    member val private BufAlign : ArrAlign<'T> = null with get, set

    override x.Ptr
        with get() =
            IntPtr.Add(x.BufAlign.GCHandle.AddrOfPinnedObject(), x.Offset*sizeof<'T>)

    member x.Alloc(size : int, alignBytes : int) =
        let align = alignBytes / sizeof<'T>
        let (xBuf, xOffset) =
            lock (chunkLock) (fun _ ->
                chunkOffset <- (chunkOffset + (align - 1))/align*align
                if (null = currentChunk || chunkOffset + size > currentChunk.Size) then
                    currentChunk <- new ArrAlign<'T>(RefCntBufChunkAlign<'T>.ChunkSize, RefCntBufChunkAlign<'T>.AlignBytes)
                    chunkOffset <- 0
                    chunkCount <- ref 1
                else
                    chunkCount := !chunkCount + 1
                let xOffset = chunkOffset
                chunkOffset <- chunkOffset + size
                refCount <- chunkCount
                (currentChunk, xOffset + currentChunk.Offset)
            )
        x.BufAlign <- xBuf
        x.SetBuffer(x.BufAlign.Arr, xOffset, size)

    override x.Alloc(size : int) =
        x.Alloc(size, RefCntBufChunkAlign<'T>.AlignBytes)

    override x.DisposeInternal() =
        if (Interlocked.Decrement(refCount) = 0) then
            if (Utils.IsNotNull x.BufAlign) then
                (x.BufAlign :> IDisposable).Dispose()
                x.BufAlign <- null
        else
            x.BufAlign <- null
        base.DisposeInternal()

type internal RBufPartType =
    | Virtual
    | Valid
    | ValidWrite

type [<AllowNullLiteral>] RBufPart<'T> =
    inherit SafeRefCnt<RefCntBuf<'T>>

#if DEBUGALLOCS
    [<DefaultValue>] val mutable AllocString : string
#endif
    // is virtual RBufPart (without any element)
    [<DefaultValue>] val mutable internal Type : RBufPartType
    [<DefaultValue>] val mutable Offset : int
    [<DefaultValue>] val mutable Count : int64
    // the beginning element's position in the stream 
    [<DefaultValue>] val mutable StreamPos : int64
    // for read/write IO
    [<DefaultValue>] val mutable internal IOEvent : MEvent
    // # of concurrent read/writes happening on this buffer
    [<DefaultValue>] val mutable NumUser : int ref
    [<DefaultValue>] val mutable Finished : bool

    // following is used when getting element from pool
    new() as x =
        { inherit SafeRefCnt<RefCntBuf<'T>>("RBufPart") }
        then
            x.Init()

    new(e : RBufPart<'T>) as x =
        { inherit SafeRefCnt<RefCntBuf<'T>>("RBufPart", e) }
        then
            x.Init()
            x.Offset <- e.Offset
            x.Count <- e.Count
            x.Type <- e.Type

    new(e : RBufPart<'T>, offset : int, count : int64) as x =
        { inherit SafeRefCnt<RefCntBuf<'T>>("RBufPart", e) }
        then
            x.Init()
            x.Offset <- offset
            x.Count <- count

    new (buf : 'T[], offset : int, count : int64) as x =
        { inherit SafeRefCnt<RefCntBuf<'T>>("RBufPart", new RefCntBuf<'T>(buf)) }
        then
            x.Init()
            x.Offset <- offset
            x.Count <- count

    static member inline GetDefaultNew() =
        new RBufPart<'T>() :> SafeRefCnt<RefCntBuf<'T>>

    static member GetVirtual() : RBufPart<'T> =
        let x = new RBufPart<'T>()
        x.Init()
        x.Offset <- 0
        x.Count <- 0L
        x.StreamPos <- 0L
        x.NumUser := 0
        x.Type <- RBufPartType.Virtual
        x

    static member GetVirtual(e : RBufPart<'T>) =
        let x = new RBufPart<'T>()
        x.Init()
        x.Offset <- 0
        x.Count <- e.Count
        x.StreamPos <- e.StreamPos
        x.NumUser := 0
        x.Type <- RBufPartType.Virtual
        x

    static member internal GetFromPool(pool : SharedPool<string,'TP>) =
        let (event, elem) = SafeRefCnt<RefCntBuf<'T>>.GetFromPool("RBufPart:Generic", pool, RBufPart<'T>.GetDefaultNew)
        (event, elem :?> RBufPart<'T>)

    member x.Init() =
#if DEBUGALLOCS
        x.AllocString <- Environment.StackTrace
#endif
        x.Type <- RBufPartType.Valid
        x.IOEvent <- null
        x.NumUser <- ref 1 // self is user
        x.Finished <- false
        ()

    member x.SetIOEvent() =
        if (Utils.IsNull x.IOEvent) then
            x.IOEvent <- MEvent(true)
        else
            x.IOEvent.Set()

    member x.ResetIOEvent() =
        if (Utils.IsNull x.IOEvent) then
            x.IOEvent <- MEvent(false)
        else
            x.IOEvent.Reset()

    override x.InitElem(e) =
        base.InitElem(e)
        x.Offset <- e.Offset
        x.Count <- 0L
        x.StreamPos <- 0L

    override x.ReleaseElem(b) =
        if (x.Type <> RBufPartType.Virtual && x.ElemNoCheck <> null) then
            base.ReleaseElem(b)

type [<AllowNullLiteral>] internal SharedMemoryPool<'T,'TBase when 'T :> RefCntBuf<'TBase> and 'T: (new : unit -> 'T)> =
    inherit SharedPool<string,'T>

    [<DefaultValue>] val mutable InitFunc : 'T->unit
    [<DefaultValue>] val mutable BufSize : int
    [<DefaultValue>] val mutable private disposer : DoOnce

    new (initSize : int, maxSize : int, bufSize : int, initFn : 'T -> unit, infoStr : string) as x =
        { inherit SharedPool<string, 'T>() }
        then
            x.InitFunc <- initFn
            x.BufSize <- bufSize
            x.InitStack(initSize, maxSize, infoStr)
            x.disposer <- new DoOnce()

    static member GetPool(initSize : int, maxSize : int, bufSize : int, infoStr : string) =
        new SharedMemoryPool<'T,'TBase>(initSize, maxSize, bufSize, (fun _ -> ()), infoStr)

    static member GetPoolWithIO(initSize : int, maxSize : int, bufSize : int, initFn : 'T -> unit, infoStr : string) =
        let wrappedFunc(elem : 'T) =
            elem.InitIOEvent()
            initFn(elem)
        let x = new SharedMemoryPool<'T,'TBase>(initSize, maxSize, bufSize, wrappedFunc, infoStr)
        x

    static member GetPoolWithIO(initSize : int, maxSize : int, bufSize : int, infoStr : string) =
        let x = new SharedMemoryPool<'T,'TBase>(initSize, maxSize, bufSize, RefCntBuf<'TBase>.InitForIO, infoStr)
        x

    override x.Alloc (infoStr : string) (elem : 'T) =
        x.BaseAlloc (infoStr) (elem)
        elem.Alloc(x.BufSize)
        elem.SetKey(infoStr + ":" + elem.Id.ToString())
        x.InitFunc(elem)

    override x.GetElem(infoStr : string) =
        let (event, elem) = base.GetElem(infoStr)
        if (Utils.IsNull event) then
            elem.Reset()
        (event, elem)

    override x.GetElem (infoStr: string, elem : 'T ref) =
        let event = base.GetElem (infoStr, elem)
        if (Utils.IsNull event) then
            (!elem).Reset()
        event

    member private x.Dispose() =
        for e in x.GetStack.Stack do
            (e :> IDisposable).Dispose()

    interface IDisposable with
        override x.Dispose() =
            x.disposer.Run(x.Dispose)

// put counter in separate class as classes without primary constructor do not allow let bindings
// and static val fields cannot be easily initialized, also makes it independent of type 'T
type StreamBaseCounter() =
    inherit IdCounter<StreamBaseCounter>()
type [<AllowNullLiteral>] [<AbstractClass>] StreamBase<'T> =
    inherit MemoryStream

    // sufficient for upto GUID
    [<DefaultValue>] val mutable RefCount : int64 ref
    [<DefaultValue>] val mutable ValBuf : byte[]
    [<DefaultValue>] val mutable Writable : bool
    [<DefaultValue>] val mutable Visible : bool
    [<DefaultValue>] val mutable Id : int64
    [<DefaultValue>] val mutable private info : string
    [<DefaultValue>] val mutable debugInfo : string
    [<DefaultValue>] val mutable internal disposer : DoOnce
#if DEBUGALLOCS
    [<DefaultValue>] val mutable allocs : ConcurrentDictionary<string, string>
#endif

    new() as x = 
        { inherit MemoryStream() }
        then
            x.Init()

    new(size : int) as x =
        { inherit MemoryStream(size) }
        then
            x.Init()

    new(buf : byte[]) as x =
        { inherit MemoryStream(buf) }
        then
            x.Init()

    new(buf : byte[], writable : bool) as x =
        { inherit MemoryStream(buf, writable) }
        then
            x.Init()
            x.Writable <- writable

    new(buf : byte[], index, count : int) as x =
        { inherit MemoryStream(buf, index, count) }
        then
            x.Init()
 
    new(buffer, index, count, writable ) as x = 
        { inherit MemoryStream(buffer, index, count, writable)  }
        then
            x.Init()
            x.Writable <- writable

    new(buffer, index, count, writable, publiclyVisible ) as x = 
        { inherit MemoryStream(buffer, index, count, writable, publiclyVisible)  }
        then
            x.Init()
            x.Writable <- writable
            x.Visible <- publiclyVisible

    abstract member Info : string with get, set
    default x.Info
        with get() =
            x.info
        and set(v) =
            x.info <- v

    abstract member GetTotalBuffer : unit -> 'T[]
    abstract member GetMoreBuffer : (int byref)*(int64 byref) -> 'T[]*int*int
    abstract member GetMoreBufferPart : (int byref)*(int64 byref) -> RBufPart<'T>

    abstract member Append : StreamBase<'TS>*int64*int64 -> unit
    abstract member AppendNoCopy : StreamBase<'T>*int64*int64 -> unit
    abstract member AppendNoCopy : RBufPart<'T>*int64*int64 -> unit
    abstract member Append : StreamBase<'TS>*int64 -> unit
    default x.Append(sb, count) =
        x.Append(sb, 0L, count)
    abstract member Append : StreamBase<'TS> -> unit
    default x.Append(sb) =
        x.Append(sb, 0L, sb.Length)

    abstract member ReadToStream : Stream*int64*int64 -> unit
    default x.ReadToStream(s : Stream, offset : int64, count : int64) =
        let finalPos = offset + count
        let offset = Math.Max(0L, Math.Min(offset, x.Length))
        let finalPos = Math.Max(0L, Math.Min(finalPos, x.Length))
        let count = finalPos - offset
        if (count > 0L) then
            s.Write(x.GetBuffer(), int offset*sizeof<'T>, int count*sizeof<'T>)

    abstract member WriteFromStream : Stream*int64 -> unit
    default x.WriteFromStream(s : Stream, count : int64) =
        let buf = Array.zeroCreate<byte>(int count)
        let read = s.Read(buf, 0, int count)
        x.Write(buf, 0, read)

    abstract member ComputeHash : Security.Cryptography.HashAlgorithm*int64*int64 -> byte[]
    default x.ComputeHash(hasher : Security.Cryptography.HashAlgorithm, offset : int64, len : int64) =
        hasher.ComputeHash(x.GetBuffer(), int offset, int len)

    interface IRefCounter<string> with
#if DEBUGALLOCS
        override x.Allocs with get() = x.allocs // for debugging allocations
#endif
        override x.DebugInfo with get() = x.debugInfo and set(v) = x.debugInfo <- v
        override x.Key with get() = "Stream:" + x.info + x.Id.ToString()
        override x.Release with get() = (fun _ -> (x :> IDisposable).Dispose()) and set(v) = ()
        override x.SetRef(r : int64) =
            x.RefCount := r
        override x.GetRef with get() = (!x.RefCount)
        override x.AddRef() =
            Interlocked.Increment(x.RefCount)
        override x.DecRef() =
            Interlocked.Decrement(x.RefCount)

    abstract DisposeInternal : bool->unit
    default x.DisposeInternal(bDisposing) =
        base.Dispose(bDisposing)
    interface IDisposable with
        override x.Dispose() =
            x.disposer.Run(fun () -> x.DisposeInternal(true))
            GC.SuppressFinalize(x)
    // following is needed in case someone calls x.Dispose() method on stream, which calls Close followed
    // by x.Dispose(true)
    override x.Dispose(b) =
        if (b) then
            (x :> IDisposable).Dispose()
        else
            x.Finalize()

    abstract member GetNew : unit -> StreamBase<'T>
    abstract member GetNew : int -> StreamBase<'T>
    abstract member GetNew : 'T[]*int*int*bool*bool -> StreamBase<'T>

    member internal x.GetInfoId() =
        sprintf "%s:%d" x.Info x.Id

    member internal x.GetNewMs() =
        x.GetNew() :> MemoryStream

    member internal x.GetNewMsBuf(buf,pos,len,a,b) =
        x.GetNew(buf,pos,len,a,b) :> MemoryStream

    member internal x.GetNewMsByteBuf(buf : byte[], pos, len, a, b) =
        if (typeof<'T> = typeof<byte[]>) then
            x.GetNew(box(buf) :?> 'T[], pos, len, a, b) :> MemoryStream
        else
            null

    abstract member Replicate : unit->StreamBase<'T>
    // copy and seek to same position
    default x.Replicate() =
        let ms = x.GetNew()
        ms.AppendNoCopy(x, 0L, x.Length)
        ms.Seek(x.Position, SeekOrigin.Begin) |> ignore
        ms

    abstract member Replicate : int64*int64->StreamBase<'T>

    member private x.Init() =
        x.RefCount <- ref 0L
        x.Id <- StreamBaseCounter.GetNext()
        x.ValBuf <- Array.zeroCreate<byte>(32)
        x.Writable <- true
        x.Visible <- true
        x.info <- ""
        x.debugInfo <- ""
        x.disposer <- new DoOnce()

    member x.ComputeSHA512(offset : int64, len : int64) =
        use sha512 = new Security.Cryptography.SHA512Managed() // has dispose
        x.ComputeHash(sha512, offset, len)

    member x.ComputeSHA256(offset : int64, len : int64) =
        use sha256 = new Security.Cryptography.SHA256Managed() 
        x.ComputeHash(sha256, offset, len)

    member x.ComputeChecksum(offset : int64, len : int64) =
        use hasher = Hash.CreateChecksum()
        x.ComputeHash(hasher, offset, len)

    member internal x.WriteUInt128( data: UInt128 ) = 
        x.WriteUInt64( data.Low )
        x.WriteUInt64( data.High ) 
    member internal x.ReadUInt128() = 
        let low = x.ReadUInt64()
        let high = x.ReadUInt64()
        UInt128( high, low )

    /// Write IPEndPoint to bytestream 
    member x.WriteIPEndPoint( addr: Net.IPEndPoint ) =      
        x.WriteBytesWLen( addr.Address.GetAddressBytes() )
        x.WriteInt32( addr.Port )
    /// Read IPEndPoint from bytestream, if the bytestream is truncated prematurely, the later IPAddress and port information will be 0. 
    member x.ReadIPEndPoint( ) = 
        let buf = x.ReadBytesWLen()
        let port = x.ReadInt32() 
        Net.IPEndPoint( Net.IPAddress( buf ), port )

    /// Insert a second MemStream before the current MemStream, and return the resultant MemStream
    member x.InsertBefore( mem2 : StreamBase<'T> ) = 
        let xpos, xlen = if x.Position = x.Length then 0, int x.Length else int x.Position, int ( x.Length - x.Position )
        if mem2.Position < mem2.Length then mem2.Seek( 0L, SeekOrigin.End ) |> ignore
        //mem2.Append(x, int64 xpos, int64 xlen)
        mem2.AppendNoCopy(x, int64 xpos, int64 xlen)
        mem2

    /// <summary>
    /// Return the buffer, position, count as a tuple that captures the state of the current MemStream. 
    /// buffer: bytearray of the underlying bytestream. 
    /// position: the current position if need to write out the bytestream. 
    /// count: number of bytes if need to write out the bytestream. 
    /// </summary>
    member x.GetBufferPosLength() = 
        let xpos, xlen = if x.Position = x.Length then 0, int x.Length else int x.Position, int ( x.Length - x.Position )
        x, xpos, xlen  

    // Write a MemStream at the end of the current MemStream, return the current MemStream after the write
    member x.WriteMemStream( mem2: StreamBase<'T> ) = 
        let xbuf, xpos, xlen = mem2.GetBufferPosLength()
        x.WriteInt32( xlen )
        x.AppendNoCopy(xbuf, int64 xpos, int64 xlen)
        x

    // Read a MemStream out of the current MemStream
    member x.ReadMemStream() = 
        let xlen = x.ReadInt32()
        let xpos = x.Position
        x.Seek(int64 xlen, SeekOrigin.Current) |> ignore
        let ms = x.GetNew()
        ms.AppendNoCopy(x, xpos, int64 xlen)
        ms

    member internal  x.GetValidBuffer() =
        Array.sub (x.GetBuffer()) 0 (int x.Length)

[<AllowNullLiteral>]
type internal StreamBaseRef<'T>() =
    inherit SafeRefCnt<StreamBase<'T>>("StreamRef")
    static member Equals(elem : StreamBase<'T>) : StreamBaseRef<'T> =
        let x = new StreamBaseRef<'T>()
        x.SetElement(elem)
        x

[<AllowNullLiteral>] 
type StreamReader<'T>(_bls : StreamBase<'T>, _bufPos : int64, _maxLen : int64) =
    let bls = _bls
    let mutable elemPos = 0
    let mutable bufPos = _bufPos
    let mutable maxLen = _maxLen

    new (bls, bufPos) =
        new StreamReader<'T>(bls, bufPos, Int64.MaxValue)

    member x.Reset(_bufPos : int64, _maxLen : int64) =
        elemPos <- 0
        bufPos <- _bufPos
        maxLen <- _maxLen

    member x.Reset(_bufPos : int64) =
        x.Reset(_bufPos, Int64.MaxValue)

    member x.GetMoreBufferPart() : RBufPart<'T> =
        if (maxLen > 0L) then
            use rbuf = bls.GetMoreBufferPart(&elemPos, &bufPos)
            let retCnt = Math.Min(int64 rbuf.Count, maxLen)
            maxLen <- maxLen - retCnt
            let ret = new RBufPart<'T>(rbuf, rbuf.Offset, retCnt)
            ret
        else
            null

    member x.GetMoreBuffer() : 'T[]*int*int =
        if (maxLen > 0L) then
            let (buf, pos, cnt) = bls.GetMoreBuffer(&elemPos, &bufPos)
            let retCnt = Math.Min(int64 cnt, maxLen)
            maxLen <- maxLen - retCnt
            (buf, pos, int retCnt)
        else
            (null, 0, 0)

    member x.ApplyFnToBuffers (fn : 'T[]*int*int -> unit) =
        let mutable bDone = false
        while (not bDone) do
            let (buf, pos, cnt) = x.GetMoreBuffer()
            if (Utils.IsNotNull buf) then
                fn(buf, pos, cnt)
            else
                bDone <- true

    member x.ApplyFnToParts (fn : RBufPart<'T> -> unit) =
        let mutable bDone = false
        while (not bDone) do
            let part = x.GetMoreBufferPart()
            if (Utils.IsNotNull part) then
                fn(part)
            else
                bDone <- true

[<AllowNullLiteral>] 
type StreamBaseByte =
    inherit StreamBase<byte>

    new() = 
        { inherit StreamBase<byte>() }

    new(size : int) =
        { inherit StreamBase<byte>(size) }

    new(buf : byte[]) =
        { inherit StreamBase<byte>(buf) }

    new(buf : byte[], writable : bool) =
        { inherit StreamBase<byte>(buf, writable) }

    new(buf : byte[], index : int, count : int) =
        { inherit StreamBase<byte>(buf, index, count) }
 
    new(buffer, index, count, writable ) = 
        { inherit StreamBase<byte>(buffer, index, count, writable)  }

    new(buffer, index, count, writable, publiclyVisible ) = 
        { inherit StreamBase<byte>(buffer, index, count, writable, publiclyVisible)  }

    new(sb : StreamBaseByte, pos : int64, count : int64) =
        new StreamBaseByte(sb.GetBuffer(), int pos, int count, false, true)

    new(sb : StreamBaseByte) =
        let buf = sb.GetBuffer()
        new StreamBaseByte(buf, int sb.Position, buf.Length - (int sb.Position), false, true)

    new(sb : StreamBaseByte, pos : int64) as x =
        let buf = sb.GetBuffer()
        new StreamBaseByte(buf, 0, buf.Length, false, true)
        then
            x.Seek(pos, SeekOrigin.Begin) |> ignore

    override x.GetNew() =
        new StreamBaseByte() :> StreamBase<byte>

    override x.GetNew(size : int) =
        new StreamBaseByte(size) :> StreamBase<byte>

    override x.GetNew(buffer, index, count, writable, publiclyVisible) =
        new StreamBaseByte(buffer, index, count, writable, publiclyVisible) :> StreamBase<byte>

    override x.GetMoreBuffer(elemPos : int byref, pos : int64 byref) =
        if (pos >= x.Length || pos < 0L) then
            (null, 0, 0)
        else
            let origPos = pos
            pos <- x.Length
            (x.GetBuffer(), int origPos, int(x.Length-origPos))

    override x.GetMoreBufferPart(elemPos : int byref, pos : int64 byref) : RBufPart<byte> =
        let (buf, pos, cnt) = x.GetMoreBuffer(&elemPos, &pos)
        new RBufPart<byte>(buf, pos, int64 cnt)

    override x.GetTotalBuffer() =
        x.GetBuffer()

    override x.Append(sb : StreamBase<'TS>, offset : int64, count : int64) =
        x.Write(sb.GetBuffer(), int offset, int count)

    override x.AppendNoCopy(sb : StreamBase<byte>, offset : int64, count : int64) =
        x.Append(sb, offset, count)

    override x.AppendNoCopy(b : RBufPart<byte>, offset : int64, count : int64) =
        x.Write(b.Elem.Buffer, int offset, int count)

    override x.Replicate() =
        let ms = new StreamBaseByte(x.GetBuffer(), 0, int x.Length, false, true)
        ms.Seek(x.Position, SeekOrigin.Begin) |> ignore
        ms :> StreamBase<byte>

    override x.Replicate(pos : int64, cnt : int64) =
        new StreamBaseByte(x.GetBuffer(), int pos, int cnt, false, true) :> StreamBase<byte>

// list is refcounted for easy replication (create replicas for read only)
[<AllowNullLiteral>]
type RefCntList<'T,'TBase when 'T :> SafeRefCnt<'TBase> and 'TBase:null and 'TBase:>IRefCounter<string>>() =
    inherit RefCountBase()
    static let defaultInitNumElem = 8
    let mutable list : List<'T> = List<'T>(defaultInitNumElem)
    let releaseList(_) =
        for l in list do
            (l :> IDisposable).Dispose()
        //list.Clear()

    member x.List with get() = list

    interface IRefCounter<string> with
        override val Release : IRefCounter<string>->unit = releaseList with get, set

// essentially a generic list of buffers of type 'T
// use AddRef/Release to acquire / release resource
[<AllowNullLiteral>] 
type BufferListStream<'T> internal (bufSize : int, doNotUseDefault : bool) =
    inherit StreamBase<'T>()

    static let streamsInUse = ConcurrentDictionary<int64, BufferListStream<'T>>()
    static let streamsInUseCnt = ref 0L

    static let mutable memStack : SharedMemoryPool<RefCntBuf<'T>,'T> = null
    static let memStackInitLock = Object()

    let ioLock = Object()

    let bReleased = ref 0

    let mutable stackTrace = ""

    let mutable bufferSize =
        if (bufSize > 0) then
            bufSize
        else
            BufferListStream<'T>.BufferSizeDefault
    let mutable getNewWriteBuffer : unit->RBufPart<'T> = (fun _ ->
        let buf = Array.zeroCreate<'T>(bufferSize)
        new RBufPart<'T>(buf, 0, 0L)
    )
    let mutable bufList : List<RBufPart<'T>> = null
    let mutable bufListRef : SafeRefCnt<RefCntList<RBufPart<'T>,RefCntBuf<'T>>> = null
    let mutable rbufPart : RBufPart<'T> = null
    let mutable rbuf : RefCntBuf<'T> = null
    let mutable bufBeginPos = 0L // beginning position in current buffer
    let mutable bufPos = 0L // position into current buffer
    let mutable bufRem = 0L // remaining number of elements in current buffer
    let mutable bufRemWrite = 0L // remaining elements that can be written
    let mutable elemPos = 0 // position in list
    let mutable elemLen = 0 // total length of list
    let mutable length = 0L // total length of stream
    let mutable position = 0L // current position in stream
    let mutable bSimpleBuffer = false // simple buffer read (one input buffer)
    let mutable capacity = 0L

    new(size : int) as x =
        new BufferListStream<'T>(size, false)
        then
            x.SetDefaults(true)

    new() =
        new BufferListStream<'T>(BufferListStream<'T>.BufferSizeDefault)

    // use an existing buffer to initialize
    new(buf : 'T[], index : int, count : int) as x =
        new BufferListStream<'T>()
        then
            x.SimpleBuffer <- true
            x.AddExistingBuffer(new RBufPart<'T>(buf, index, int64 count))

    new(bls : BufferListStream<'T>, offset : int64, count : int64) as x =
        new BufferListStream<'T>()
        then
            x.AppendNoCopy(bls, offset, count)

    new(bls : BufferListStream<'T>) =
        new BufferListStream<'T>(bls, bls.Position, bls.Length-bls.Position)

    new(bls : BufferListStream<'T>, offset : int64) as x =
        new BufferListStream<'T>(bls, 0L, bls.Length)
        then
            x.Seek(offset, SeekOrigin.Begin) |> ignore

    member x.IOLock with get() = ioLock

    abstract GetNewNoDefault : unit->BufferListStream<'T>
    default x.GetNewNoDefault() =
        let e = new BufferListStream<'T>(BufferListStream<'T>.BufferSizeDefault, true)
        e.SetDefaults(false)
        e

    member internal x.SetDefaults(bAlloc : bool) =
#if DEBUG
        if (BufferListDebugging.DebugLeak) then
            streamsInUse.[x.Id] <- x
            stackTrace <- Environment.StackTrace
#endif
        Interlocked.Increment(streamsInUseCnt) |> ignore
        if (bAlloc) then
            let newList = RefCntList<RBufPart<'T>,RefCntBuf<'T>>()
            bufList <- newList.List
            bufListRef <- new SafeRefCnt<RefCntList<RBufPart<'T>,RefCntBuf<'T>>>("BufferList", newList)
        getNewWriteBuffer <- x.GetStackElem
        ()

    member internal x.ElemLen with get() = elemLen and set(v) = elemLen <- v
    member internal x.ElemPos with get() = elemPos and set(v) = elemPos <- v
    member internal x.RBufPart with get() = rbufPart
    member private x.SimpleBuffer with get() = bSimpleBuffer
    member internal x.ReplicateInfoFrom(src : BufferListStream<'T>) =
        elemLen <- src.ElemLen
        length <- src.Length
        capacity <- src.Capacity64
        bSimpleBuffer <- src.SimpleBuffer

    override x.Replicate() =
#if DEBUGALLOCS
        let e = x.GetNew() :?> BufferListStream<'T>
        for b in x.BufList do
            e.WriteRBufNoCopy(b)
#else
        let e = x.GetNewNoDefault()
        e.BufListRef <- new SafeRefCnt<RefCntList<RBufPart<'T>,RefCntBuf<'T>>>("BufferList", x.BufListRef)
        e.BufList <- e.BufListRef.Elem.List
        e.ReplicateInfoFrom(x)
#endif
        e.Seek(x.Position, SeekOrigin.Begin) |> ignore
        e :> StreamBase<'T>
        
    override x.Replicate(pos : int64, cnt : int64) =
        let e = x.GetNew() :?> BufferListStream<'T>
        e.AppendNoCopy(x, pos, cnt)
        e :> StreamBase<'T>

    override x.GetNew() =
        new BufferListStream<'T>() :> StreamBase<'T>

    override x.GetNew(size) =
        new BufferListStream<'T>(size) :> StreamBase<'T>

    override x.GetNew(buf : 'T[], offset : int, count : int, a : bool, b : bool) =
        new BufferListStream<'T>(buf, offset, count) :> StreamBase<'T>

    member internal x.InsertIntoList(elem : RBufPart<'T>, i : int) =
        if (i = bufList.Count) then
            bufList.Add(elem)
        else
            bufList.Insert(i, elem)

    member internal x.RemoveFromList(i : int) =
        let itemRemove = bufList.[i]
        bufList.RemoveAt(i)
        (itemRemove :> IDisposable).Dispose()

    override x.Info
        with get() =
            base.Info
        and set(v) =
            base.Info <- v

    member private x.StackTrace with get() = stackTrace

    static member val BufferSizeDefault : int = 64000 with get, set

    static member DumpStreamsInUse() =
#if DEBUG
        if (BufferListDebugging.DebugLeak) then
            Logger.LogF (BufferListDebugging.DumpStreamLogLevel, fun _ ->
                let sb = System.Text.StringBuilder()
                sb.AppendLine(sprintf "Num streams in use: %d" streamsInUse.Count) |> ignore
                for s in streamsInUse do
                    let (key, value) = (s.Key, s.Value)
                    if (Utils.IsNotNull s.Value.BufListRef) then
                        let v : List<RBufPart<'T>> = s.Value.BufListNoCheck
                        sb.AppendLine(sprintf "%d : %s : NumBuffers:%d" s.Key s.Value.Info s.Value.BufListNoCheck.Count) |> ignore
                    sb.AppendLine(sprintf "Alloc From: %s" s.Value.StackTrace) |> ignore
                sb.ToString()
            )
        else
#endif
            Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Num streams in use: %d" !streamsInUseCnt)

    // static memory pool
    static member InitFunc (e : RefCntBuf<'T>) =
        // override the release function
        e.RC.Release <- BufferListStream<'T>.ReleaseStackElem

    static member internal MemStack with get() = memStack
    static member internal InitMemStack(numBufs : int, bufSize : int) =
        if Utils.IsNull memStack then
            lock (memStackInitLock) (fun _ -> 
                if Utils.IsNull memStack then
                    memStack <- new SharedMemoryPool<RefCntBuf<'T>,'T>(numBufs, -1, bufSize, BufferListStream<'T>.InitFunc, "Memory Stream")
#if DEBUG
                if (BufferListDebugging.DebugLeak) then
                    // start monitor timer
                    PoolTimer.AddTimer(BufferListStream<'T>.DumpStreamsInUse, 10000L, 10000L)
#endif
            )

    static member InitSharedPool() =
        BufferListStream<'T>.InitMemStack(128, BufferListStream<'T>.BufferSizeDefault)

    member internal x.GetStackElem() =
        let (event, buf) = RBufPart<'T>.GetFromPool(x.GetInfoId()+":RBufPart", BufferListStream<'T>.MemStack,
                                                    fun () -> new RBufPart<'T>() :> SafeRefCnt<RefCntBuf<'T>>)
        //Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Use Element %d for stream %d" buf.Id x.Id)
        buf.Elem.UserToken <- box(x)
        buf :?> RBufPart<'T>

    static member internal ReleaseStackElem : IRefCounter<string>->unit = (fun e ->
        let e = e :?> RefCntBuf<'T>
        let x = e.UserToken :?> BufferListStream<'T>
        //Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Release Element %d for stream %d" e.Id x.Id)
        //Console.WriteLine("Release Elemement {0}", e.Id)
        BufferListStream<'T>.MemStack.Release(e)
    )

    // return is in units of bytes
    static member internal SrcDstBlkCopy<'T1,'T2,'T1Elem,'T2Elem when 'T1 :> Array and 'T2 :>Array>
        (src : 'T1, srcOffset : int64 byref, srcLen : int64 byref, 
         dst : 'T2, dstOffset : int64 byref, dstLen : int64 byref) =
        let toCopy = Math.Min(int srcLen*sizeof<'T1Elem>, int dstLen*sizeof<'T2Elem>) // in units of bytes
        let numSrc = toCopy / sizeof<'T1Elem>
        let numDst = toCopy / sizeof<'T2Elem>
        if (toCopy > 0) then
            Buffer.BlockCopy(src, int srcOffset*sizeof<'T1Elem>, dst, int dstOffset*sizeof<'T2Elem>, toCopy)
            srcOffset <- srcOffset + int64 numSrc
            srcLen <- srcLen - int64 numSrc
            dstOffset <- dstOffset + int64 numDst
            dstLen <- dstLen - int64 numDst
        (numSrc, numDst)

    static member internal SrcDstBlkReserve<'T1Elem,'T2Elem>(srcOffset : int64 byref, srcLen : int64 byref, dstOffset : int64 byref, dstLen : int64 byref) =
        let toCopy = Math.Min(int srcLen*sizeof<'T1Elem>, int dstLen*sizeof<'T2Elem>) // in units of bytes
        let numSrc = toCopy / sizeof<'T1Elem>
        let numDst = toCopy / sizeof<'T2Elem>
        if (toCopy > 0) then
            srcOffset <- srcOffset + int64 numSrc
            srcLen <- srcLen - int64 numSrc
            dstOffset <- dstOffset + int64 numDst
            dstLen <- dstLen - int64 numDst
        (numSrc, numDst)

    member private x.Release(bFromFinalize : bool) =
        if (Interlocked.CompareExchange(bReleased, 1, 0)=0) then
            if (bFromFinalize) then
                Logger.LogF(LogLevel.WildVerbose, fun _ -> sprintf "List release for %s with id %d %A finalize: %b remain: %d" x.Info x.Id (Array.init bufList.Count (fun index -> bufList.[index].ElemNoCheck.Id)) bFromFinalize streamsInUse.Count)
            else
                Logger.LogF(LogLevel.WildVerbose, fun _ -> sprintf "List release for %s with id %d %A finalize: %b remain: %d" x.Info x.Id (Array.init bufList.Count (fun index -> bufList.[index].Elem.Id)) bFromFinalize streamsInUse.Count)
            let b = ref Unchecked.defaultof<BufferListStream<'T>>
            streamsInUse.TryRemove(x.Id, b) |> ignore
            Interlocked.Decrement(streamsInUseCnt) |> ignore
            (bufListRef :> IDisposable).Dispose() // only truly releases when refcount goes to zero

    override x.DisposeInternal(bDisposing : bool) =
        x.Release(not bDisposing)
        base.DisposeInternal(bDisposing)

    override x.Finalize() =
        x.DisposeInternal(false)

    member x.GetNewWriteBuffer with set(v) = getNewWriteBuffer <- v

    member internal x.SimpleBuffer with set(v) = bSimpleBuffer <- v

    member x.DefaultBufferSize with get() = bufferSize and set(v) = bufferSize <- v

    member x.NumBuf with get() = elemLen

    member private x.BufListNoCheck with get() = bufListRef.ElemNoCheck.List
    member internal x.BufList with get() : List<RBufPart<'T>> = bufList and set(v) = bufList <- v
    member private x.BufListRef with get() : SafeRefCnt<RefCntList<RBufPart<'T>,RefCntBuf<'T>>> = bufListRef and set(v) = bufListRef <- v

    override x.GetTotalBuffer() =
        if (bSimpleBuffer && elemLen = 1) then
            bufList.[0].Elem.Buffer
        else
            // bad to do this
            let arr = Array.zeroCreate<'T>(int length)
            let mutable offset = 0 // in units of 'T
            for l in bufList do
                let off1 = l.Offset*sizeof<'T>
                let off2 =  offset*sizeof<'T>
                let len = Math.Min(int l.Count*sizeof<'T>, (int32) length - off2)
                Buffer.BlockCopy(l.Elem.Buffer, off1, arr,off2, len)
                offset <- offset + int l.Count
            arr

    member x.GetBuffer(arr : byte[], offset : int64, count : int64) =
        let sr = new StreamReader<'T>(x, offset, count)
        let count = Math.Min(count, length-offset)
        let offset = ref 0L
        let rem = ref (int64 arr.Length)
        let copyBuffer (buf : 'T[], soffset : int, scount : int) =
            let mutable scount = int64 scount
            let mutable soffset = int64 soffset
            BufferListStream<'T>.SrcDstBlkCopy<'T[],byte[],'T,byte>(buf, &soffset, &scount, arr, offset, rem) |> ignore
        sr.ApplyFnToBuffers copyBuffer
        arr

    override x.GetBuffer() =
        x.GetBuffer(Array.zeroCreate<byte>(int(length)*sizeof<'T>), 0L, length)

    // get more buffer
    override x.GetMoreBufferPart(elemPos : int byref, pos : int64 byref) : RBufPart<'T> =
        if (pos >= length) then
            null
        else
            if (pos < bufList.[elemPos].StreamPos) then
                elemPos <- 0
            while (bufList.[elemPos].StreamPos + int64 bufList.[elemPos].Count <= pos) do
                elemPos <- elemPos + 1
            let offset = int(pos - bufList.[elemPos].StreamPos)
            let cnt = bufList.[elemPos].Count - int64 offset
            pos <- pos + int64 cnt
            let totalOffset = bufList.[elemPos].Offset + offset
            new RBufPart<'T>(bufList.[elemPos], totalOffset, cnt)

    override x.GetMoreBuffer(elemPos : int byref, pos : int64 byref) : 'T[]*int*int =
        if (pos >= length) then
            (null, 0, 0)
        else
            use part = x.GetMoreBufferPart(&elemPos, &pos)
            let ret = (part.Elem.Buffer, part.Offset, int part.Count)
            ret

    override val CanRead = true with get
    override val CanSeek = true with get
    override x.CanWrite with get() = x.Writable

    override x.Flush() =
        ()

    override x.Length with get() = length
    override x.Position 
        with get() =
            position 
        and set(v) =
            x.Seek(v, SeekOrigin.Begin) |> ignore
    override x.SetLength(l) =
        assert(false)

    member internal x.PositionInternal with get() = position and set(v) = position <- v

    member x.Capacity64
        with get() =
            capacity
        and set(v) =
            while (capacity < v) do
                x.CreateBuffer()

    //override x.Capacity with get() = int x.Capacity64 and set(v) = x.Capacity64 <- int64 v
    override x.Capacity with get() = Int32.MaxValue and set(v) = ()

    member internal x.CreateBuffer() =
        let rbufPartNew = getNewWriteBuffer()
        rbufPartNew.StreamPos <- position
        bufList.Add(rbufPartNew)
        capacity <- capacity + int64 rbufPartNew.Elem.Length

    // add new buffer to pool
    member internal x.AddNewBuffer() =
        if (bufList.Count = elemLen) then
            x.CreateBuffer()
        elemLen <- elemLen + 1

    // add existing buffer to pool, don't adjust position
    member internal x.AddExistingBuffer(rbufAdd : RBufPart<'T>) =
        // if use linked list instead of list, then theoretically could insert if bufRemWrite = 0 also
        // then condition would be if (position <> length && bufRemWrite <> 0) then
        if (position <> length) then
            failwith "Splicing RBuf in middle is not supported"
        else
            let rbufPartNew = new RBufPart<'T>(rbufAdd) // make a copy
            rbufPartNew.StreamPos <- position
            if (elemLen > 0) then
                x.SealWriteBuffer()
            x.InsertIntoList(rbufPartNew, elemLen)
            length <- length + int64 rbufAdd.Count
            capacity <- capacity + int64 rbufAdd.Count
            elemLen <- elemLen + 1
            rbufPart <- rbufAdd
            rbuf <- rbufPart.ElemNoCheck
            bufRem <- 0L
            bufRemWrite <- 0L
            bufPos <- int64 rbufPartNew.Offset + rbufPartNew.Count
            bufBeginPos <- int64 rbufPartNew.Offset
            rbufPartNew.Finished <- true

    member internal x.SetPosToI(i : int) =
        rbufPart <- bufList.[i]
        elemPos <- i + 1
        rbuf <- rbufPart.ElemNoCheck
        bufBeginPos <- int64 rbufPart.Offset
        bufPos <- int64 rbufPart.Offset
        bufRem <- rbufPart.Count
        if (i=0) then
            rbufPart.StreamPos <- 0L
        else
            rbufPart.StreamPos <- bufList.[i-1].StreamPos + int64 bufList.[i-1].Count
        // allow writing at end of last buffer
        if (rbufPart.StreamPos + int64 rbufPart.Count >= length) then
            bufRemWrite <- int64(rbufPart.Elem.Length - (rbufPart.Offset - rbufPart.Elem.Offset))
        else
            // if already written buffer, then don't extend count
            bufRemWrite <- rbufPart.Count

    // move to beginning of buffer i
    abstract MoveToBufferI : bool*int -> bool
    default x.MoveToBufferI(bAllowExtend : bool, i : int) =
        if (bAllowExtend && i >= elemLen) then
            let mutable j = elemLen
            while (j <= i) do
                // extend by grabbing another buffer and appending to list
                x.AddNewBuffer()
                j <- j + 1
        if (i < elemLen) then
            x.SetPosToI(i)
            true
        else
            false

    abstract MoveToNextBuffer : bool*int -> bool
    default x.MoveToNextBuffer(bAllowExtend : bool, rem : int) =
        if (0 = rem) then
            x.MoveToBufferI(bAllowExtend, elemPos)
        else
            true

    // move to end of previous buffer
    member private x.MoveToPreviousBuffer() =
        if (bufBeginPos = bufPos) then
            // elemPos is next buffer, current is elemPos-1
            let ret = x.MoveToBufferI(false, elemPos-2)
            if (ret) then
                // go back by count
                bufPos <- int64 rbufPart.Offset + rbufPart.Count
                bufRem <- 0L
                bufRemWrite <- bufRemWrite - rbufPart.Count
            ret
        else
            true

    member internal x.MoveForwardAfterWriteArr(amt : int64) =
        bufRem <- Math.Max(0L, bufRem - amt)
        rbufPart.Count <- Math.Max(rbufPart.Count, int64(bufPos - bufBeginPos))
        position <- position + amt
        length <- Math.Max(length, position)
        if (0L = bufRemWrite) then
            rbufPart.Finished <- true

    member internal x.MoveForwardAfterWrite(amt : int64) =
        bufPos <- bufPos + amt
        bufRemWrite <- bufRemWrite - amt
        x.MoveForwardAfterWriteArr(amt)

    member internal x.MoveForwardAfterReadArr(amt : int64) =
        bufRemWrite <- bufRemWrite - amt
        position <- position + amt
        if (0L = bufRem) then
            rbufPart.Finished <- true

    member internal x.MoveForwardAfterRead(amt : int64) =
        bufPos <- bufPos + amt
        bufRem <- bufRem - amt
        x.MoveForwardAfterReadArr(amt)

    override x.Seek(offset : int64, origin : SeekOrigin) =
        let mutable offset = offset
        let mutable finalPos =
            match origin with
                | SeekOrigin.Begin -> offset
                | SeekOrigin.Current -> position + offset
                | SeekOrigin.End -> length + offset
                | _ -> Int64.MaxValue
        if (finalPos > length) then
            //failwith "Invalid seek position"
            finalPos <- length
        else if (finalPos < 0L) then
            //failwith "Invalid seek position"
            finalPos <- 0L
        if (elemLen > 0) then
            // at least one buffer
            match origin with
                | SeekOrigin.Begin ->
                    // seek to beginning
                    x.MoveToBufferI(false, 0) |> ignore
                    position <- 0L
                | SeekOrigin.End ->
                    // seek to end
                    x.MoveToBufferI(false, elemLen-1) |> ignore
                    bufPos <- bufPos + rbufPart.Count
                    bufRem <- bufRem - rbufPart.Count
                    bufRemWrite <- bufRemWrite - rbufPart.Count
                    position <- length
                | _ -> ()
        if (finalPos > position) then
            let mutable diff = finalPos - position
            while (diff > 0L) do
                if (x.MoveToNextBuffer(false, int bufRem)) then
                    let amtSeek = int32(Math.Min(diff, int64 bufRem))
                    x.MoveForwardAfterRead(int64 amtSeek)
                    diff <- diff - int64 amtSeek
                else
                    diff <- 0L
        else if (finalPos < position) then
            let mutable diff = position - finalPos
            while (diff > 0L) do
                if (x.MoveToPreviousBuffer()) then
                    let amtSeek = int32(Math.Min(diff, int64 (bufPos - bufBeginPos)))
                    x.MoveForwardAfterRead(int64 -amtSeek)
                    diff <- diff - int64 amtSeek
                else
                    diff <- 0L
        position

    // write functions - ONLY difference between this and AddExistingBuffer is that this adjusts position also
    abstract WriteRBufNoCopy : RBufPart<'T> -> unit
    default x.WriteRBufNoCopy(rbuf : RBufPart<'T>) =
        x.AddExistingBuffer(rbuf)
        position <- position + int64 rbuf.Count
        elemPos <- Math.Max(elemLen, elemPos)
        length <- Math.Max(length, position)

    // write directly into part
    member internal x.GetWritePart() =
        x.MoveToNextBuffer(true, int bufRemWrite) |> ignore
        (rbufPart, bufPos, bufRemWrite)

    member internal x.UpdatePartCount(amt : int64) =
        if (amt > bufRemWrite) then
            failwith "Too much information written beyond count"
        x.MoveForwardAfterWrite(amt)

    member internal x.GetWriteBuffer() =
        x.MoveToNextBuffer(true, int bufRemWrite) |> ignore
        (rbuf.Buffer, bufPos, bufRemWrite)

    member internal x.SealWriteBuffer() =
        // forces move to next buffer
        bufRemWrite <- 0L
        bufRem <- 0L
        rbufPart.Finished <- true

    member internal x.SealWriteBufferConcurrent() =
        lock (ioLock) (fun _ ->
            x.SealWriteBuffer()
        )

    member x.WriteOne(b : 'T) =
        x.MoveToNextBuffer(true, int bufRemWrite) |> ignore
        rbuf.Buffer.[int bufPos] <- b
        x.MoveForwardAfterWrite(1L)

    member x.SealAndGetNextWriteBuffer() =
        x.SealWriteBuffer()
        x.GetWriteBuffer()

    abstract WriteArr<'TS> : 'TS[]*int*int -> unit
    default x.WriteArr<'TS>(buf : 'TS[], offset : int, count : int) =
        let mutable bOffset = int64 offset
        let mutable bCount = int64 count
        while (bCount > 0L) do
            x.MoveToNextBuffer(true, int bufRemWrite) |> ignore
            let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<'TS[],'T[],'TS,'T>(buf, &bOffset, &bCount, rbuf.Buffer, &bufPos, &bufRemWrite)
            x.MoveForwardAfterWriteArr(int64 dstCopy)

    // align in units of 'T
    member x.WriteArrAlign<'TS>(buf : 'TS[], offset : int, count : int, align : int) =
        let mutable bOffset = int64 offset
        let mutable bCount = int64 count
        while (bCount > 0L) do
            x.MoveToNextBuffer(true, int bufRemWrite) |> ignore
            let mutable bufRemAlign = bufRemWrite / (int64 align) * (int64 align)
            if (bufRemAlign = 0L) then
                x.SealWriteBuffer()
            else
                let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<'TS[],'T[],'TS,'T>(buf, &bOffset, &bCount, rbuf.Buffer, &bufPos, &bufRemAlign)
                x.MoveForwardAfterWriteArr(int64 dstCopy)
                bufRemWrite <- bufRemWrite - int64 dstCopy                

    member private x.MoveWriteConcurrent<'TS>(count : int, align : int) =
        let writeList = new List<RBufPart<'T>*int*int>()
        let pCount = ref (int64 count)
        let pOffset = ref 0L
        lock (ioLock) (fun _ ->
            while (!pCount > 0L) do
                x.MoveToNextBuffer(true, int bufRemWrite) |> ignore
                let mutable bufRemAlign = bufRemWrite / (int64 align) * (int64 align)
                if (bufRemAlign = 0L) then
                    x.SealWriteBuffer()
                else
                    let dstPos = bufPos
                    let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkReserve<'TS,'T>(pOffset, pCount, &bufPos, &bufRemAlign)
                    Interlocked.Increment(rbufPart.NumUser) |> ignore
                    writeList.Add(rbufPart, int dstPos, dstCopy)
                    x.MoveForwardAfterWriteArr(int64 dstCopy)
                    bufRemWrite <- bufRemWrite - int64 dstCopy
        )
        writeList

    abstract FlushBuf : RBufPart<'T>->unit
    default x.FlushBuf(buf) = ()

    member x.WriteConcurrent<'TS>(buf : 'TS[], offset : int, count : int, align : int) =
        let list = x.MoveWriteConcurrent(count, align)
        let mutable byteOffset = offset*sizeof<'TS>
        for l in list do
            let (dst, dstOffset, dstCount) = l
            let bytesCopy = dstCount*sizeof<'T>
            Buffer.BlockCopy(buf, byteOffset, dst.Elem.Buffer, dstOffset*sizeof<'T>, bytesCopy)
            byteOffset <- byteOffset + bytesCopy
            // if block copy was async (e.g. from a async file read), then following executes in callback
            let numUser = Interlocked.Decrement(dst.NumUser)
            if (dst.Finished && 0 = numUser) then
                x.FlushBuf(dst)

    member x.WriteArr<'TS>(buf : 'TS[]) =
        x.WriteArr<'TS>(buf, 0, buf.Length)

    override x.Write(buf, offset, count) =
        x.WriteArr<byte>(buf, offset, count)

    // add elements from another type of array - perhaps not needed with previous one being generic
    member x.WriteArrT(buf : System.Array, offset : int, count : int, elemSize : int) =
        let mutable bOffset = int64(offset*elemSize)
        let mutable bCount = int64(count*elemSize)
        while (bCount > 0L) do
            x.MoveToNextBuffer(true, int bufRemWrite) |> ignore
            let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<Array,'T[],byte,'T>(buf, &bOffset, &bCount, rbuf.Buffer, &bufPos, &bufRemWrite)
            x.MoveForwardAfterWriteArr(int64 dstCopy)

    // Read functions
    member internal x.GetReadBuffer() =
        if (x.MoveToNextBuffer(false, int bufRem)) then
            (rbuf, bufPos, bufRem)
        else
            (null, 0L, 0L)

    member x.ReadOne() =
        if (x.MoveToNextBuffer(false, int bufRem)) then
            let b = rbuf.Buffer.[int bufPos]
            x.MoveForwardAfterRead(1L)
            (true, b)
        else
            (false, Unchecked.defaultof<'T>)

    member x.ReadArr<'TD>(buf : 'TD[], offset : int, count : int) =
        let mutable bOffset = int64 offset
        let mutable bCount = int64 count
        let mutable readAmt = 0
        while (bCount > 0L) do
            if (x.MoveToNextBuffer(false, int bufRem)) then
                let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<'T[],'TD[],'T,'TD>(rbuf.Buffer, &bufPos, &bufRem, buf, &bOffset, &bCount)
                x.MoveForwardAfterReadArr(int64 srcCopy)
                readAmt <- readAmt + dstCopy
            else
                bCount <- 0L // quit, no more data
        readAmt

    member private x.MoveReadConcurrent<'TS>(count : int) =
        let readList = new List<RBufPart<'T>*int*int>()
        let pCount = ref (int64 count)
        let pOffset = ref 0L
        lock (ioLock) (fun _ ->
            while (!pCount > 0L) do
                if (x.MoveToNextBuffer(false, int bufRem)) then
                    let srcPos = bufPos
                    let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkReserve<'TS,'T>(&bufPos, &bufRem, pOffset, pCount)
                    Interlocked.Increment(rbufPart.NumUser) |> ignore
                    readList.Add(rbufPart, int srcPos, srcCopy)
                    x.MoveForwardAfterReadArr(int64 srcCopy)
        )
        readList

    member x.ReadConcurrent<'TD>(buf : 'TD[], offset : int, count : int) =
        let list = x.MoveReadConcurrent(count)
        let mutable byteOffset = offset*sizeof<'TD>
        for l in list do
            let (src, srcOffset, srcCount) = l
            let bytesCopy = srcCount*sizeof<'T>
            Buffer.BlockCopy(src.Elem.Buffer, srcOffset*sizeof<'T>, buf, byteOffset, bytesCopy)
            byteOffset <- byteOffset + bytesCopy
            let numUser = Interlocked.Decrement(src.NumUser)
            if (src.Finished && 0 = numUser) then
                x.FlushBuf(src)

    member x.ReadArr<'TD>(buf : 'TD[]) =
        x.ReadArr<'TD>(buf, 0, buf.Length)

    override x.Read(buf, offset, count) =
        x.ReadArr<byte>(buf, offset, count)

    // add elements from another type of array - perhaps not needed with previous one being generic
    member x.ReadArrT(buf : System.Array, offset : int, count : int, elemSize: int) =
        let mutable bOffset = int64(offset*elemSize)
        let mutable bCount = int64(count*elemSize)
        let mutable readAmt = 0
        while (bCount > 0L) do
            if (x.MoveToNextBuffer(false, int bufRem)) then
                let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<'T[],Array,'T,byte>(rbuf.Buffer, &bufPos, &bufRem, buf, &bOffset, &bCount)
                x.MoveForwardAfterReadArr(int64 srcCopy)
                readAmt <- readAmt + dstCopy
            else
                bCount <- 0L
        readAmt

    // append another memstream onto this one
    override x.Append(strmList : StreamBase<'TS>, offset : int64, count : int64) =
        let sr = new StreamReader<'TS>(strmList, offset, count)
        let mutable bDone = false
        while (not bDone) do
            let (buf, pos, cnt) = sr.GetMoreBuffer()
            if (Utils.IsNotNull buf) then
                x.WriteArr<'TS>(buf, pos, cnt)
            else
                bDone <- true

    override x.AppendNoCopy(strmList : StreamBase<'T>, offset : int64, count : int64) =
        let sr = new StreamReader<'T>(strmList, offset, count)
        let mutable bDone = false
        while (not bDone) do
            use rbuf = sr.GetMoreBufferPart()
            if (Utils.IsNotNull rbuf) then
                x.WriteRBufNoCopy(rbuf)
                (rbuf :> IDisposable).Dispose()
            else
                bDone <- true

    override x.AppendNoCopy(rbuf : RBufPart<'T>, offset : int64, count : int64) =
        use rbufAdd = new RBufPart<'T>(rbuf, int offset, count)
        x.WriteRBufNoCopy(rbufAdd)

// MemoryStream which is essentially a collection of RefCntBuf
// Not GetBuffer is not supported by this as it is not useful
[<AllowNullLiteral>]
type MemoryStreamB(defaultBufSize : int, toAvoidConfusion : byte) =
    inherit BufferListStream<byte>(defaultBufSize, false)

    let emptyBlk = [||]

    new(size : int) as x =
        new MemoryStreamB(size, 0uy)
        then
            x.SetDefaults(true)
            while (x.Capacity64 < int64 size) do
                x.CreateBuffer() // add until size reached

    new() as x =
        new MemoryStreamB(BufferListStream<byte>.BufferSizeDefault, 0uy)
        then
            x.SetDefaults(true)

    new(buf : byte[], index : int, count : int) as x =
        new MemoryStreamB()
        then
            x.SimpleBuffer <- true
            use addBuf = new RBufPart<byte>(buf, index, int64 count)
            x.AddExistingBuffer(addBuf)

    new(buf : byte[]) =
        new MemoryStreamB(buf, 0, buf.Length)

    new(buf : byte[], b : bool) as x =
        new MemoryStreamB(buf)
        then
            x.Writable <- b

    new(buf : byte[], index : int, count : int, b : bool) =
        new MemoryStreamB(buf, index, count)

    new(buf : byte[], index : int, count : int, b1, b2) as x =
        new MemoryStreamB(buf, index, count)
        then
            x.Writable <- b1
            x.Visible <- b2

    new(ms : MemoryStreamB, position : int64, count : int64) as x =
        new MemoryStreamB()
        then
            x.AppendNoCopy(ms, position, count)

    new(ms : MemoryStreamB) =
        new MemoryStreamB(ms, ms.Position, ms.Length-ms.Position)

    new(ms : MemoryStreamB, offset : int64) as x =
        new MemoryStreamB(ms, 0L, ms.Length)
        then
            x.Seek(offset, SeekOrigin.Begin) |> ignore

    override x.GetNew() =
        new MemoryStreamB() :> StreamBase<byte>

    override x.GetNew(size) =
        new MemoryStreamB(size) :> StreamBase<byte>

    override x.GetNew(buf, index, count, b1, b2) =
        new MemoryStreamB(buf, index, count, b1, b2) :> StreamBase<byte>

    override x.GetNewNoDefault() =
        let e = new MemoryStreamB(BufferListStream<byte>.BufferSizeDefault, 0uy)
        e.SetDefaults(false)
        e :> BufferListStream<byte>

    override x.ComputeHash(hasher : Security.Cryptography.HashAlgorithm, offset : int64, len : int64) =
        let mutable bDone = false
        let sr = new StreamReader<byte>(x, offset, len)
        sr.ApplyFnToBuffers (fun (buf, pos, cnt) -> hasher.TransformBlock(buf, pos, cnt, null, 0) |> ignore)
        hasher.TransformFinalBlock(emptyBlk, 0, 0) |> ignore
        hasher.Hash

    // Write functions
    // add elements from file - cannot be in generic as file read only supports byte
    override x.WriteFromStream(fh : Stream, count : int64) =
        let mutable bCount = count
        while (bCount > 0L) do
            let (buf, pos, amt) = x.GetWriteBuffer()
            let toRead = int (Math.Min(bCount, int64 amt))
            let writeAmt = fh.Read(buf, int pos, toRead)
            bCount <- bCount - int64 writeAmt
            x.MoveForwardAfterWrite(int64 writeAmt)
            if (writeAmt <> toRead) then
                failwith "Write to memstream from file fails as file is out data"

    member x.AsyncWriteFromStream(fh : Stream, count : int64) : Async<unit> =
        async {
            let mutable bCount = count
            while (bCount > 0L) do
                let (buf, pos, amt) = x.GetWriteBuffer()
                let toRead = int (Math.Min(bCount, int64 amt))
                let! writeAmt = fh.AsyncRead(buf.Buffer, pos, toRead)
                bCount <- bCount - int64 writeAmt
                x.MoveForwardAfterWrite(int64 writeAmt)
                if (writeAmt <> toRead) then
                    failwith "Write to memstream from file fails as file is out data"
        }

    member x.WriteFromStreamAlign(fh : Stream, count : int64, align : int) =
        let mutable bCount = count
        while (bCount > 0L) do
            let (buf, pos, amt) = x.GetWriteBuffer()
            let bufRemAlign = amt / (int64 align) * (int64 align)
            if (bufRemAlign = 0L) then
                x.SealWriteBuffer()
            else
                let toRead = Math.Min(bCount, bufRemAlign)
                let writeAmt = fh.Read(buf, int pos, int toRead)
                bCount <- bCount - int64 writeAmt
                x.MoveForwardAfterWrite(int64 writeAmt)
                if (writeAmt <> int toRead) then
                    failwith "Write to memstream from file fails as file is out data"

    override x.WriteByte(b : byte) =
        x.WriteOne(b)

    override x.Write(buf : byte[], offset : int, count : int) =
        x.WriteArr(buf, offset, count)

    // From something to MemStreamB
    member x.WriteSByte(v : SByte) =
        x.WriteByte(byte(int(v)+128))
    member x.WriteInt16(v : int16) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteInt32(v : int32) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteInt64(v : int64) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteUByte(v : byte) =
        x.WriteByte(v)
    member x.WriteUInt16(v : uint16) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteUInt32(v : uint32) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteUInt64(v : uint64) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteSingle(v : System.Single) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteDouble(v : System.Double) =
        x.WriteArr(BitConverter.GetBytes(v))
    member x.WriteArrWLen<'T>(v : 'T[]) =
        x.WriteArr(BitConverter.GetBytes(v.Length))
        x.WriteArr(v, 0, v.Length)
//    member x.GenericWrite<'T>(v : 'T) =
//        let t = typeof<'T>
//        if (t.IsArray) then
//            let arr = unbox<System.Array>(v)
//            x.WriteInt32(arr.Length)
//            x.WriteArrT(arr, 0, arr.Length)
//        else 
//            Serialize.Serialize<'T> x v

    // Read functions
    override x.ReadByte() =
        let (success, b) = x.ReadOne()
        if (success) then
            int b
        else
            -1

    override x.Read(buf : byte[], offset : int, count : int) =
        x.ReadArr(buf, offset, count)

    // read count from current position & move position forward
    member x.ReadToStream(fh : Stream, count : int64) =
        let mutable bCount = count
        while (bCount > 0L) do
            let (buf, pos, amt) = x.GetReadBuffer()
            let readAmt = Math.Min(bCount, int64 amt)
            fh.Write(buf.Buffer, int pos, int readAmt)
            x.MoveForwardAfterRead(readAmt)
            bCount <- bCount - readAmt

    member x.AsyncReadToStream(fh : Stream, count : int64) =
        async {
            let mutable bCount = count
            while (bCount > 0L) do
                let (buf, pos, amt) = x.GetReadBuffer()
                let readAmt = Math.Min(bCount, int64 amt)
                do! fh.AsyncWrite(buf.Buffer, int pos, int readAmt)
                x.MoveForwardAfterRead(readAmt)
                bCount <- bCount - readAmt
        }

    // read count starting from offset, and don't move position forward
    override x.ReadToStream(s : Stream, offset : int64, count : int64) =
        let sr = new StreamReader<byte>(x, offset, count)
        sr.ApplyFnToBuffers(fun (buf, pos, cnt) -> s.Write(buf, pos, cnt))

    // From MemStreamB to something
    member x.ReadSByte() =
        sbyte (x.ReadByte()-128)
    member x.ReadInt16() =
        x.Read(x.ValBuf, 0, 2) |> ignore
        BitConverter.ToInt16(x.ValBuf, 0)
    member x.ReadInt32() =
        x.Read(x.ValBuf, 0, 4) |> ignore
        BitConverter.ToInt32(x.ValBuf, 0)
    member x.ReadInt64() =
        x.Read(x.ValBuf, 0, 8) |> ignore
        BitConverter.ToInt64(x.ValBuf, 0)
    member x.ReadUByte() =
        byte(x.ReadByte())
    member x.ReadUInt16() =
        x.Read(x.ValBuf, 0, 2) |> ignore
        BitConverter.ToUInt16(x.ValBuf, 0)
    member x.ReadUInt32() =
        x.Read(x.ValBuf, 0, 4) |> ignore
        BitConverter.ToUInt32(x.ValBuf, 0)
    member x.ReadUInt64() =
        x.Read(x.ValBuf, 0, 8) |> ignore
        BitConverter.ToUInt64(x.ValBuf, 0)
    member x.ReadSingle() =
        x.Read(x.ValBuf, 0, sizeof<System.Single>) |> ignore
        BitConverter.ToSingle(x.ValBuf, 0)
    member x.ReadDouble() =
        x.Read(x.ValBuf, 0, sizeof<System.Double>) |> ignore
        BitConverter.ToDouble(x.ValBuf, 0)
    member x.ReadArrWLen<'T>() =
        let len = x.ReadInt32()
        let arr = Array.zeroCreate<'T>(len)
        x.ReadArr<'T>(arr, 0, len) |> ignore
        arr
    member x.ReadArr<'T>(len : int) =
        let arr = Array.zeroCreate<'T>(len)
        x.ReadArr<'T>(arr, 0, len) |> ignore
        arr
//    member x.GenericRead<'T>() =
//        let t = typeof<'T>
//        if (t.IsArray) then
//            let len = x.ReadInt32()
//            let tArr = typeof<'T>
//            let arr = Array.CreateInstance(tArr.GetElementType(), [|len|])
//            x.ReadArrT(arr, 0, len) |> ignore
//            box(arr) :?> 'T
//        else
//            Serialize.Deserialize<'T> x

// ===============================================================================

[<AllowNullLiteral>]
type BufferListStreamWithPool<'T,'TP when 'TP:null and 'TP:(new:unit->'TP) and 'TP :> IRefCounter<string>>() =
    inherit BufferListStream<'T>()

    internal new (pool : SharedPool<string, 'TP>) as x =
        new BufferListStreamWithPool<'T,'TP>()
        then
            if (Utils.IsNotNull pool) then
                x.Pool <- pool
                x.GetNewWriteBuffer <- x.GetNewBuffer

    member val internal Pool : SharedPool<string, 'TP> = null with get, set

    member internal x.GetNewBuffer() =
        let (event, buf) = RBufPart<'T>.GetFromPool(x.GetInfoId()+":RBufPart", x.Pool,
                                                    fun () -> new RBufPart<'T>() :> SafeRefCnt<RefCntBuf<'T>>)
        buf.Elem.UserToken <- box(x)
        buf :?> RBufPart<'T>

    member internal x.TryGetNewBuffer() =
        let (event, buf) = RBufPart<'T>.GetFromPool(x.GetInfoId()+":RBufPart", x.Pool,
                                                    fun () -> new RBufPart<'T>() :> SafeRefCnt<RefCntBuf<'T>>)
        if (Utils.IsNull buf) then
            (event, null)
        else
            buf.Elem.UserToken <- box(x)
            (event, buf :?> RBufPart<'T>)

    member internal x.GetNewBufferWait() =
        let mutable bDone = false
        let mutable buf = null
        while not bDone do
            let (event, bufGet) = RBufPart<'T>.GetFromPool(x.GetInfoId()+":RBufPart", x.Pool,
                                                           fun () -> new RBufPart<'T>() :> SafeRefCnt<RefCntBuf<'T>>)
            if (Utils.IsNotNull bufGet) then
                buf <- bufGet
                bDone <- true
        buf.Elem.UserToken <- box(x)
        buf :?> RBufPart<'T>

// ===============================================================

open Prajna.Tools.Native

type RWHQueue() =
    static let count = ref 0
    static let q = ConcurrentQueue<RegisteredWaitHandle*WaitHandle>()

    static member private Release() =
        let elem = ref Unchecked.defaultof<RegisteredWaitHandle*WaitHandle>
        let mutable bCont = true
        while bCont do
            let ret = q.TryDequeue(elem)
            if (ret) then
                let (rwh, wh) = !elem
                rwh.Unregister(wh) |> ignore
            bCont <- (Interlocked.Decrement(count) > 0)

    static member Add(rwh : RegisteredWaitHandle, wh : WaitHandle) =
        q.Enqueue(rwh, wh)
        if (Interlocked.Increment(count) = 1) then
            PoolTimer.AddTimer(RWHQueue.Release, 1000L)

// This is sort of like .Net FileStream / NetStream, however has following support
// 1. Async I/O using I/O Completion ports
// 2. Support for reading / writing to arbirary 'T instead of just byte
// 3. Prefetch for reading speed improvement
// 4. Option for no file buffering to avoid OS file cache if desired
// 5. Integration with shared memory pool to allow for zero-copy pipeline, e.g. can read directly to another BufferListStream
[<AllowNullLiteral>]
[<AbstractClass>]
type BufferListStreamWithBackingStream<'T,'TP when 'TP:null and 'TP:(new:unit->'TP) and 'TP :> IRefCounter<string>> internal (pool) =
    inherit BufferListStreamWithPool<'T,'TP>(pool)

    let mutable doFetching = true
    let disposer = new DoOnce()

    member val MaxNumValid = 1 with get, set
    member val NumPrefetch = 1 with get, set
    member val Alignment = 1 with get, set

    abstract OpenStream : unit->unit

    member x.SetPrefetch(numPrefetch : int) =
        x.NumPrefetch <- numPrefetch
        x.OpenStream()
        x.TryPrefetch(x.NumPrefetch) |> ignore

    member x.StartPrefetch(numPrefetch : int) =
        x.NumPrefetch <- numPrefetch
        x.OpenStream()
        x.DoBackgroundPrefetch null false

    member x.StopPrefetch() =
        lock (x.IOLock) (fun _ ->
            doFetching <- false
        )

    member x.DoBackgroundPrefetch(state : obj) (bTimeOut : bool) =
        lock (x.IOLock) (fun _ ->
            if doFetching then
                let (bDone, event) = x.TryPrefetch(x.NumPrefetch)
                if not bDone then
                    RWHQueue.Add(ThreadPool.RegisterWaitForSingleObject(event, x.BackgroundPrefetch, null, -1, true), event)
        )
    member private x.BackgroundPrefetch = new WaitOrTimerCallback(x.DoBackgroundPrefetch)

    member val internal FinalFlush = false with get, set
    override x.DisposeInternal(bDisposing : bool) =
        if (not x.FinalFlush) then
            x.WaitForIOFinish()
            x.Flush()
            x.FinalFlush <- true
        base.DisposeInternal(bDisposing)

    override x.Flush() =
        x.WriteBuffers() // flush internal buffers to backing stream

    member internal x.ReplicateFrom(bls : BufferListStreamWithBackingStream<'T,'TP>) =
        bls.WriteBuffers()
        x.Pool <- bls.Pool
        x.GetNewWriteBuffer <- x.GetNewBuffer
        // now copy over data
        for b in bls.BufList do
            let elem = new RBufPart<'T>(b)
            x.BufList.Add(elem)
        x.ReplicateInfoFrom(bls)
        x.ElemPos <- 0
        x.Position <- 0L

    member private x.PrefetchNextRead() =
        if (x.ElemLen > x.ElemPos) then
            let elem = x.BufList.[x.ElemPos] // the next element
            if (elem.Type = RBufPartType.Virtual) then
                x.SplitAndMergeVirtual(x.ElemPos, elem.StreamPos)

    member val WaitForIOFinish : unit->unit = (fun _ -> failwith "WaitForIOFinish not implemented") with get, set
    member val FillWithRead : RBufPart<'T>->unit = (fun _ -> failwith "FillWithRead not implemented") with get, set
    member val FlushToBackingStream : RBufPart<'T>->unit = (fun _ -> failwith "FlushToBackingStream not implemented") with get, set

    // split virtual element "i" to create a (virtual, valid, virtual) or (valid, virtual)
    member private x.SplitVirtual(i : int, pos : int64, elemUse :RBufPart<'T>) =
        // first remove the "i"th element
        let vElem = x.BufList.[i]
        x.BufList.RemoveAt(i)

        let mutable num = 0
        let mutable streamPos = vElem.StreamPos
        let mutable remCount = vElem.Count
        let mutable bFirstVirtual = false

        // first virtual if needed
        if (pos > vElem.StreamPos) then
            let vElemPrev = RBufPart<'T>.GetVirtual()
            vElemPrev.StreamPos <- streamPos
            vElemPrev.Count <- (int64 pos) - vElem.StreamPos
            streamPos <- streamPos + vElemPrev.Count
            remCount <- remCount - vElemPrev.Count
            x.InsertIntoList(vElemPrev, i + num)
            num <- num + 1
            bFirstVirtual <- true

        // the valid element
        let elem = 
            if (Utils.IsNull elemUse) then
                x.GetNewBufferWait()
            else
                elemUse
        // split, take portion of virtual count
        elem.StreamPos <- streamPos
        elem.Count <- Math.Min(int64 elem.Elem.Length, remCount)
        streamPos <- streamPos + elem.Count
        remCount <- remCount - elem.Count
        elem.Type <- RBufPartType.Valid
        x.InsertIntoList(elem, i + num)
        num <- num + 1
        x.FillWithRead(elem)

        // adjust vElem count
        if (remCount > 0L) then
            vElem.StreamPos <- streamPos
            vElem.Count <- remCount
            x.InsertIntoList(vElem, i + num)
            num <- num + 1
        else
            (vElem :> IDisposable).Dispose() // won't do much, but will prevent finalizer from kicking in

        x.ElemLen <- x.ElemLen + num - 1
        if (x.ElemPos-1 > i) then
            x.ElemPos <- x.ElemPos + num - 1
        else if (x.ElemPos-1 = i && bFirstVirtual) then
            x.ElemPos <- x.ElemPos + 1

        num

    // returns new position after merges complete
    member private x.MergeVirtual(i : int) =
        if (x.BufList.[i].Type=RBufPartType.Virtual) then
            // forward
            while (x.BufList.Count > i+1 && x.BufList.[i+1].Type=RBufPartType.Virtual) do
                x.BufList.[i].Count <- x.BufList.[i].Count + x.BufList.[i+1].Count
                x.RemoveFromList(i+1)
                x.ElemLen <- x.ElemLen - 1
                if (x.ElemPos-1 > i) then
                    x.ElemPos <- x.ElemPos - 1
            // backward
            let mutable i = i
            while (i-1 >= 0 && x.BufList.[i-1].Type=RBufPartType.Virtual) do
                x.BufList.[i-1].Count <- x.BufList.[i-1].Count + x.BufList.[i].Count
                x.RemoveFromList(i)
                x.ElemLen <- x.ElemLen - 1
                if (x.ElemPos-1 >= i) then
                    x.ElemPos <- x.ElemPos - 1
                i <- i-1
            i
        else
            i

    member private x.SplitAndMergeVirtual(i : int, pos : int64) =
        let num = x.SplitVirtual(i, pos, null)
        for cnt = 1 to num do
            if (x.BufList.[i+num-cnt].Type = RBufPartType.Virtual) then
                x.MergeVirtual(i+num-cnt) |> ignore

    member private x.TrySplitAndMergeVirtual(i : int, pos : int64) =
        let (event, elem) = x.TryGetNewBuffer()
        if (Utils.IsNotNull elem) then
            let num = x.SplitVirtual(i, pos, elem)
            for cnt = 1 to num do
                if (x.BufList.[i+num-cnt].Type = RBufPartType.Virtual) then
                    x.MergeVirtual(i+num-cnt) |> ignore
        (Utils.IsNotNull elem, event)

    // reset the position
    member private x.ResetPos() : unit =
        x.SetPosToI(x.ElemPos-1)
        let amtToMove = x.Position - x.RBufPart.StreamPos
        x.PositionInternal <- x.RBufPart.StreamPos
        x.MoveForwardAfterRead(amtToMove) 

    member internal x.WriteBuffers() =
        lock (x.IOLock) (fun _ ->
            x.SealWriteBuffer() // seal current write buffer
            for i = 0 to x.BufList.Count-1 do
                let rbuf = x.BufList.[i]
                if (rbuf.Type = RBufPartType.ValidWrite) then
                    // replace with an equivalent element
                    let rbufNew = new RBufPart<'T>(rbuf)
                    rbufNew.Type <- RBufPartType.Valid // don't write again
                    x.BufList.[i] <- rbufNew
                    // now flush if needed
                    let numUser = Interlocked.Decrement(rbuf.NumUser)
                    if (0 = numUser) then
                        x.FlushBuf(rbuf)
        )

    override x.FlushBuf(rbuf : RBufPart<'T>) =
        if (rbuf.Type = RBufPartType.ValidWrite) then
            rbuf.Type <- RBufPartType.Virtual // free upon write
            x.FlushToBackingStream(rbuf)
        else if (rbuf.Type = RBufPartType.Valid) then
            // simply dispose
            (rbuf :> IDisposable).Dispose()

    // for concurrent read/write, this must occur inside ioLock block
    member x.MakeElemVirtual(i : int) =
        let rbuf = x.BufList.[i] // the "i"th element
        if (rbuf.Type <> RBufPartType.Virtual) then
            // replace with an equivalent virtual element
            x.BufList.[i] <- RBufPart<'T>.GetVirtual(rbuf)
            // now flush the buffer if needed
            let numUser = Interlocked.Decrement(rbuf.NumUser)
            // don't check finished here since we close buffer regardless
            if (0 = numUser) then
                x.FlushBuf(rbuf)
            // merge virtual elements in list
            x.MergeVirtual(i)
        else
            i

    // keep at most numKeep up to "i", don't split if it does not exist
    // keep "maxI-numKeep", ..., "maxI-2", "maxI-1" 
    // make virtual: "0", ..., "maxI-(numKeep+2)", "maxI-(numKeep+1)"
    member internal x.VirtualizeOldElem(numKeep : int) =
        let mutable i = 0
        while (i < x.ElemPos-numKeep) do
            i <- x.MakeElemVirtual(i)
            i <- i + 1

    member private x.TryPrefetch(numFetch : int) =
        let mutable i = 0
        let mutable bCont = true
        let mutable event = null
        // len and pos can keep changing
        while bCont && (i < Math.Min(numFetch, x.ElemLen-x.ElemPos)) do
            let rbuf = x.BufList.[x.ElemPos + i]
            if (rbuf.Type = RBufPartType.Virtual) then
                let (bContRet, eventRet) = x.TrySplitAndMergeVirtual(x.ElemPos + i, rbuf.StreamPos)
                bCont <- bContRet
                event <- eventRet
            i <- i + 1
        let ret = (i = Math.Min(numFetch, x.ElemLen-x.ElemPos), event)

        // now remove at end
        i <- x.ElemLen-1
        while (i >= x.ElemPos + numFetch) do
            i <- x.MakeElemVirtual(i)
            i <- i - 1

        ret

    // assumes x.ElemPos is set correctly
    member x.SetCurPos(pos : int64) =
        x.VirtualizeOldElem(x.MaxNumValid)
        let rbuf = x.BufList.[x.ElemPos-1]
        if (rbuf.Type = RBufPartType.Virtual) then
            x.SplitAndMergeVirtual(x.ElemPos-1, pos)
        x.TryPrefetch(x.NumPrefetch) |> ignore
        x.PositionInternal <- pos
        x.ResetPos()
        //x.BufList.[x.ElemPos-1].Elem.ReadIO.Wait()
        x.BufList.[x.ElemPos-1].IOEvent.Wait()

    override x.MoveToNextBuffer(bAllowExtend : bool, rem : int) =
        let ret =
            if (0 = rem) then
                if (bAllowExtend && x.ElemPos=x.ElemLen) then
                    x.VirtualizeOldElem(x.MaxNumValid-1)
                    x.AddNewBuffer() // will increment by 1
                    //x.BufList.[x.ElemLen-1].Elem.ReadIO.Set()
                    x.BufList.[x.ElemLen-1].SetIOEvent()
                if (x.ElemPos < x.ElemLen) then
                    x.ElemPos <- x.ElemPos + 1
                    x.SetCurPos(x.Position)
                    true
                else
                    false
            else
                x.TryPrefetch(x.NumPrefetch) |> ignore
                true
        if (bAllowExtend) then
            x.BufList.[x.ElemPos-1].Type <- ValidWrite
        ret

    member x.CleanToVirtual() =
        x.ElemPos <- x.ElemLen
        x.VirtualizeOldElem(0)
        for e in x.BufList do
            (e :> IDisposable).Dispose()
        x.BufList.Clear()
        let elem = RBufPart<'T>.GetVirtual()
        elem.Count <- x.Length
        elem.StreamPos <- 0L
        x.BufList.Add(elem)
        x.ElemLen <- 1
        x.ElemPos <- 1
        x.ResetPos()

    override x.Seek(offset : int64, origin : SeekOrigin) =
        let mutable offset = offset
        let mutable finalPos =
            match origin with
                | SeekOrigin.Begin -> offset
                | SeekOrigin.Current -> x.Position + offset
                | SeekOrigin.End -> x.Length + offset
                | _ -> Int64.MaxValue
        finalPos <- Math.Min(finalPos, x.Length)
        // binary search
        let mutable left = 0
        let mutable right = x.ElemLen
        let mutable middle = 0
        let mutable bDone = false
        while not bDone do
            middle <- (left + right) >>> 1
            if (finalPos >= x.BufList.[middle].StreamPos) then
                if (x.ElemLen-1 = middle || finalPos < x.BufList.[middle+1].StreamPos) then
                    bDone <- true
                else
                    left <- middle
            else
                right <- middle
        x.ElemPos <- middle+1 // track position middle
        let finalPosAlign = finalPos / (int64 x.Alignment) * (int64 x.Alignment)
        let seekPos = Math.Max(finalPosAlign, x.BufList.[x.ElemPos-1].StreamPos)
        // first use seekPos in splitting
        x.SetCurPos(seekPos)
        // now move forward more if needed
        x.MoveForwardAfterRead(finalPos - finalPosAlign)
        finalPos

    override x.GetMoreBufferPart(elemPos : int byref, pos : int64 byref) : RBufPart<'T> =
        if (pos >= x.Length) then
            null
        else
            if (pos < x.BufList.[elemPos].StreamPos) then
                elemPos <- 0
            while (x.BufList.[elemPos].StreamPos + int64 x.BufList.[elemPos].Count <= pos) do
                elemPos <- elemPos + 1
            x.ElemPos <- elemPos + 1 // track elemPos
            x.SetCurPos(pos)
            let rbuf = x.BufList.[x.ElemPos-1]
            let offset = int(pos - rbuf.StreamPos)
            let cnt = rbuf.Count - int64 offset
            pos <- pos + int64 cnt
            x.MoveForwardAfterRead(cnt)
            new RBufPart<'T>(x.BufList.[x.ElemPos-1], x.BufList.[x.ElemPos-1].Offset + offset, cnt)

    override x.WriteRBufNoCopy(rbuf : RBufPart<'T>) =
        base.WriteRBufNoCopy(rbuf)
        x.BufList.[x.ElemPos-1].SetIOEvent()
        x.SetCurPos(x.Position) // only helps to clear out old 

    member x.Transfer(fillWithReadAndSetCount : RBufPart<'T>->unit) =
        let mutable bCont = true
        while bCont do
            let rbufNew = x.GetNewBufferWait()
            rbufNew.ResetIOEvent()
            fillWithReadAndSetCount(rbufNew)
            rbufNew.IOEvent.Wait() // wait for fill to complete
            if (0L = rbufNew.Count) then
                bCont <- false
            else
                x.WriteRBufNoCopy(rbufNew)
        x.Flush()
        x.FinalFlush <- true

//    member private x.ContinueTransfer(state : obj) (bTimeOut : bool) =
//        let (rbufToAdd, fillWithReadAndSetCount) = state :?> RBufPart<'T>*(RBufPart<'T>->unit)
//        if (Utils.IsNotNull rbufToAdd && 0L = rbufToAdd.Count) then
//            x.Flush() // some sync (wait for callbacks to complete), but only at end
//            x.FinalFlush <- true
//        else
//            if (Utils.IsNotNull rbufToAdd) then
//                x.WriteRBufNoCopy(rbufToAdd) // all async
//                x.VirtualizeOldElem(0)
//            let (event, rbufNew) = x.TryGetNewBuffer()
//            if (Utils.IsNotNull rbufNew) then
//                rbufNew.ResetIOEvent()
//                fillWithReadAndSetCount(rbufNew)
//                RWHQueue.Add(ThreadPool.RegisterWaitForSingleObject(rbuf) // need WaitHandle to use this

// ======================================================================================

type internal DiskIO() =
    static let fileKey = ConcurrentDictionary<char, Object>()

    static member private GetFileKey(fileName : string) =
        let c = fileName.ToLower().[0]
        fileKey.GetOrAdd(c, fun _ -> Object())

    static member val GetLockObj : string->Object = DiskIO.GetFileKey with get, set

    static member OpenFile(fileName : string, fOpt : FileOptions, bufferLess : bool) =
        let file = AsyncStreamIO.OpenFile(fileName, FileAccess.ReadWrite, fOpt, bufferLess)
        file.SetLock(DiskIO.GetLockObj(Path.GetFullPath(fileName)))
        file

type internal DiskIOFn<'T>() =
    static member private DoneIOWrite (ioResult : int) (state : obj) (buffer : 'T[]) (offset : int) (bytesTransferred : int) =
        let (rbuf, file) = state :?> (RBufPart<'T>*AsyncStreamIO)
        if (file.BufferLess()) then
            if (int rbuf.Elem.Length*sizeof<'T> <> bytesTransferred) then
                raise (new Exception("I/O failed "))
        else
            if (int rbuf.Count*sizeof<'T> <> bytesTransferred) then
                raise (new Exception("I/O failed "))
        Console.WriteLine("Finish streampos: {0} {1}", rbuf.StreamPos, rbuf.Type)
        rbuf.IOEvent.Set()
        if (rbuf.Type = Virtual) then
            (rbuf :> IDisposable).Dispose()
    static member val private DoneIOWriteDel = IOCallbackDel<'T>(DiskIOFn<'T>.DoneIOWrite)

    static member WriteBuffer(elem : RBufPart<'T>, file : AsyncStreamIO, ?pos : int64) =
        //elem.Elem.WriteIO.Reset()
        elem.ResetIOEvent()
        let pos = defaultArg pos elem.StreamPos
        Console.WriteLine("Writing element pos: {0} length: {1}", elem.StreamPos, elem.Count)
        if (file.BufferLess()) then
            file.WriteFilePos(elem.Elem.Ptr, elem.Elem.Buffer, elem.Offset, elem.Elem.Length, DiskIOFn<'T>.DoneIOWriteDel, (elem, file), pos) |> ignore
        else
            file.WriteFilePos(elem.Elem.Ptr, elem.Elem.Buffer, elem.Offset, int elem.Count, DiskIOFn<'T>.DoneIOWriteDel, (elem, file), pos) |> ignore

    static member private DoneIORead (ioResult : int) (state : obj) (buffer : 'T[]) (offset : int) (bytesTransferred : int) =
        let rbuf = state :?> RBufPart<'T>
        if (int rbuf.Count*sizeof<'T> <> bytesTransferred) then
            raise (new Exception("I/O failed "))
        rbuf.IOEvent.Set()
        //rbuf.Elem.ReadIO.Set()
    static member val private DoneIOReadDel = IOCallbackDel<'T>(DiskIOFn<'T>.DoneIORead)

    static member ReadBuffer(elem : RBufPart<'T>, file : AsyncStreamIO, ?numRead : int, ?pos : int64) =
        //elem.Elem.ReadIO.Reset()
        elem.ResetIOEvent()
        let pos = defaultArg pos elem.StreamPos
        if (file.BufferLess()) then
            // read full alignment, return should be less
            file.ReadFilePos(elem.Elem.Ptr, elem.Elem.Buffer, elem.Elem.Offset, elem.Elem.Length, DiskIOFn<'T>.DoneIOReadDel, elem, pos) |> ignore
        else
            let numRead = defaultArg numRead (int elem.Count)
            file.ReadFilePos(elem.Elem.Ptr, elem.Elem.Buffer, elem.Elem.Offset, numRead, DiskIOFn<'T>.DoneIOReadDel, elem, pos) |> ignore
  

[<AllowNullLiteral>]
type BufferListStreamWithCache<'T,'TP when 'TP:null and 'TP:(new:unit->'TP) and 'TP :> IRefCounter<string>>
    private (fileName : string, pool : SharedPool<string,'TP>, bPrivate : bool) =

    inherit BufferListStreamWithBackingStream<'T,'TP>(pool) // inherit using default constructor always

    let fileOpenLock = Object()
    let mutable file : Native.AsyncStreamIO = null

    internal new (fileName : string, pool : SharedPool<string,'TP>) as x =
        new BufferListStreamWithCache<'T,'TP>(fileName, pool, false)
        then
            x.Init()

    internal new (fileName : string) as x =
        new BufferListStreamWithCache<'T,'TP>(fileName, null, false)
        then
            x.Init()

    internal new (bls : BufferListStreamWithCache<'T,'TP>) as x =
        new BufferListStreamWithCache<'T,'TP>(bls.FileName, null, false)
        then
            x.ReplicateFrom(bls)
            x.Init()

    member x.Init() =
        x.FillWithRead <- x.ReadBuffer
        x.FlushToBackingStream <- (fun r -> 
            x.OpenFile()
            DiskIOFn<'T>.WriteBuffer(r, file)
        )
        x.WaitForIOFinish <- (fun _ ->
            file.WaitForIOFinish()
        )

    override x.DisposeInternal(bDisposing : bool) =
        if (not x.FinalFlush) then
            x.WaitForIOFinish()
            x.Flush()
            x.FinalFlush <- true
        if (Utils.IsNotNull file) then
            if (file.BufferLess() && file.Length > x.Length) then
                file.SetFileLength<'T>(x.Length)
            file.Dispose()
        base.DisposeInternal(bDisposing)

    member private x.FileName with get() : string = fileName
    // bufList consists of "pre", "numValid", and "post" list (i.e. NumValid + 2 elements at most)
    member private x.File with get() : AsyncStreamIO = file

    member val BufferLess = false with get, set
    member val SequentialRead = true with get, set
    member val SequentialWrite = true with get, set

    override x.Replicate() =
        x.WriteBuffers()
        new BufferListStreamWithCache<'T,'TP>(fileName, x.Pool) :> StreamBase<'T>

    override x.Replicate(pos : int64, cnt : int64) =
        assert(false)
        raise (new Exception("Not supported"))
        x :> StreamBase<'T>

    member private x.ReadBuffer(elem : RBufPart<'T>) =
        // fill in using reader
        x.OpenFile()
        if (file.Length > elem.StreamPos*(int64 sizeof<'T>)) then
            let maxRead = file.Length/(int64 sizeof<'T>) - elem.StreamPos
            let numRead = int (Math.Min(maxRead, elem.Count))
            DiskIOFn.ReadBuffer(elem, file, numRead) // async read, does not block
        else
            //elem.Elem.ReadIO.Set()
            elem.SetIOEvent()

    override x.OpenStream() =
        x.OpenFile()

    member x.Flush(bFlushFileCache : bool) =
        if (bFlushFileCache) then
            x.Flush()
            file.Flush()
        else
            x.Flush()

    member x.OpenFile() =
        if (null = file) then
            lock (fileOpenLock) (fun _ ->
                if (null = file) then
                    let mutable fOpt = FileOptions.Asynchronous
                    if x.SequentialRead then
                        fOpt <- fOpt ||| FileOptions.SequentialScan
                    if x.SequentialWrite then
                        fOpt <- fOpt ||| FileOptions.WriteThrough
                    file <- DiskIO.OpenFile(fileName, fOpt, x.BufferLess)
                    if (x.File.Length > x.Length) then
                        use elem = RBufPart<'T>.GetVirtual()
                        elem.StreamPos <- x.Length
                        elem.Count <- x.File.Length - x.Length
                        x.AddExistingBuffer(elem)
            )
