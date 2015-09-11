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
open System.Threading

open Prajna.Tools
open Prajna.Tools.Queue
open Prajna.Tools.FSharp

// Helper classes for ref counted objects & shared memory pool
// A basic refcounter interface
type [<AllowNullLiteral>] IRefCounter<'K> =
    interface   
        abstract Key : 'K with get
        abstract Info : 'K with get, set
        abstract Release : (IRefCounter<'K>->unit) with get, set
        abstract SetRef : int64->unit
        abstract GetRef : int64 with get
        abstract AddRef : unit->unit
        abstract DecRef : unit->unit
    end

// A shared pool of RefCounters
//type [<AllowNullLiteral>] internal SharedPool<'K,'T when 'T :> IRefCounter and 'T:(new:unit->'T)> private () =
type [<AllowNullLiteral>] internal SharedPool<'K,'T when 'T :> IRefCounter<'K> and 'T:(new:unit->'T)>() =
    let mutable stack : SharedStack<'T> = null

    let debugLeak = true
    let usedList = 
#if DEBUG
        if (debugLeak) then
            new ConcurrentDictionary<'K,'T>()
        else
#endif
            null

    member private x.Stack with get() = stack and set(v) = stack <- v
    member x.GetStack with get() = stack
    member x.StackSize with get() = stack.Count()

    abstract InitStack : int*int*'K -> unit
    default x.InitStack(initSize : int, maxSize : int, info : 'K) =
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
        stack.ReleaseElem(elem :?> 'T)
#if DEBUG
        if (debugLeak) then
            usedList.TryRemove(elem.Key) |> ignore
#endif

    abstract GetElem : 'K->ManualResetEvent*'T
    default x.GetElem(info : 'K) =
        let (event, elem) = stack.GetElem()
#if DEBUG
        if (debugLeak) then
            elem.Info <- info
            usedList.[info] <- elem
#endif
        (event, elem)

    abstract GetElem : 'K*'T ref->ManualResetEvent
    default x.GetElem (info : 'K, elem : 'T ref) =
        let event = stack.GetElem(elem)
#if DEBUG
        if (debugLeak) then
            (!elem).Info <- info
            usedList.[info] <- !elem
#endif
        event

[<AllowNullLiteral>]
type SafeRefCnt<'T when 'T:null and 'T:(new:unit->'T) and 'T :> IRefCounter<string>> (infoStr : string, bAlloc : bool)=
    static let g_id = ref -1L
    let id = Interlocked.Increment(g_id)
    let bRelease = ref 0
    let mutable elem : 'T = 
        if (bAlloc) then
            new 'T()
        else
            null

    new(infoStr : string) as x =
        new SafeRefCnt<'T>(infoStr, true)
        then
            let r : IRefCounter<string> = x.RC
            x.InitElem()
            x.RC.SetRef(1L)
            x.RC.Info <- infoStr + ":" + id.ToString()

    new(infoStr : string, e : SafeRefCnt<'T>) as x =
        new SafeRefCnt<'T>(infoStr, false)
        then
            x.Element <- e.Elem // check for released element prior to setting
            x.RC.AddRef()
            x.RC.Info <- infoStr + ":" + id.ToString()
            //let e : 'T = x.Element
            //Logger.LogF(LogLevel.WildVerbose, fun _ -> sprintf "Also using %s for id %d - refcount %d" x.Element.Key x.Id x.Element.GetRef)
    
    static member internal GetFromPool<'TP when 'TP:null and 'TP:(new:unit->'TP) and 'TP :> IRefCounter<string>>
                           (infoStr : string, pool : SharedPool<string,'TP>, createNew : unit->SafeRefCnt<'T>) : ManualResetEvent*SafeRefCnt<'T> =
        let getInfo = infoStr + ":" + Interlocked.Increment(g_id).ToString()
        let (event, poolElem) = pool.GetElem(getInfo)
        if (Utils.IsNotNull poolElem) then
            let x = createNew()
            x.Element <- poolElem :> IRefCounter<string> :?> 'T
            x.InitElem()
            x.RC.SetRef(1L)
            //Logger.LogF(LogLevel.WildVerbose, fun _ -> sprintf "Using element %s for id %d - refcount %d" x.Element.Key x.Id x.Element.GetRef)
            (event, x)
        else
            (event, null)

    abstract InitElem : unit->unit
    default x.InitElem() =
        ()

    abstract ReleaseElem : Option<bool>->unit
    default x.ReleaseElem(bFinalize : Option<bool>) =
        if (Interlocked.CompareExchange(bRelease, 1, 0) = 0) then
            if (Utils.IsNotNull elem) then
                let bFinalize = defaultArg bFinalize false
                //Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Releasing %s with id %d elemId %s finalize %b - refcount %d" infoStr id x.RC.Key bFinalize x.Element.GetRef)
                x.RC.DecRef()

    member x.Release(?bFinalize : bool) =
        x.ReleaseElem(bFinalize)

    override x.Finalize() =
        x.Release(true)

    interface IDisposable with
        member x.Dispose() =
            x.Release(true)
            GC.SuppressFinalize(x)

    member private x.Element with get() = elem and set(v) = elem <- v
    member private x.RC with get() = (elem :> IRefCounter<string>)

    // use to access the element from outside
    /// Obtain element contained wit
    member x.Elem
        with get() = 
            if (!bRelease = 1) then
                failwith (sprintf "Already Released %s %d" infoStr id)
            else
                elem

    member x.ElemNoCheck
        with get() =
            elem

    member x.Id with get() = id

type [<AbstractClass>] [<AllowNullLiteral>] RefCountBase() =
    let mutable key = ""

    member val RefCount = ref 0L with get
    member x.RC with get() = (x :> IRefCounter<string>)
    member x.SetKey(k : string) =
        key <- k

    interface IRefCounter<string> with
        override x.Key with get() = key
        override val Info : string = "" with get, set
        override val Release : IRefCounter<string>->unit = (fun _ -> ()) with get, set
        override x.SetRef(v) =
            x.RefCount := v
        override x.GetRef with get() = !x.RefCount
        override x.AddRef() =
            Interlocked.Increment(x.RefCount) |> ignore
        override x.DecRef() =
            let newCount = Interlocked.Decrement(x.RefCount)
            if (0L = newCount) then
                x.RC.Release(x :> IRefCounter<string>)
            else if (newCount < 0L) then
                failwith (sprintf "RefCount object %s has Illegal ref count of %d" key !x.RefCount)

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

    let bRelease = ref 0
    let mutable buffer : 'T[] = null

    new(size : int) as x =
        new RefCntBuf<'T>()
        then
            x.SetBuffer(Array.zeroCreate<'T>(size))

    new(buf : 'T[]) as x =
        new RefCntBuf<'T>()
        then 
            x.SetBuffer(buf)

    member x.Reset() =
        x.RC.SetRef(0L)
        bRelease := 0

    abstract Alloc : int->unit
    default x.Alloc (size : int) =
        x.SetBuffer(Array.zeroCreate<'T>(size))

    static member internal AllocBuffer (size : int) (releaseFn : IRefCounter<string>->unit) (x : RefCntBuf<'T>) =
        x.SetBuffer(Array.zeroCreate<'T>(size))
        x.RC.Release <- releaseFn

    member internal x.Id with get() = id
    member internal x.SetBuffer(v : 'T[]) =
        buffer <- v
    member internal x.GetBuffer() =
        buffer

    member x.Buffer 
        with get() =
            if (!bRelease = 1) then
                failwith (sprintf "Buffer %d already released" id)
                null
            else
                buffer
    member val UserToken : obj = null with get, set

type [<AllowNullLiteral>] RBufPart<'T> =
    inherit SafeRefCnt<RefCntBuf<'T>>

    [<DefaultValue>] val mutable Offset : int
    [<DefaultValue>] val mutable Count : int
    // the beginning element's position in the stream 
    [<DefaultValue>] val mutable StreamPos : int64

    // following is used when getting element from pool
    new(bAlloc : bool) as x =
        { inherit SafeRefCnt<RefCntBuf<'T>>("RBufPart", bAlloc) }
        then
            x.Init()

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

    new(e : RBufPart<'T>, offset : int, count : int) as x =
        { inherit SafeRefCnt<RefCntBuf<'T>>("RBufPart", e) }
        then
            x.Init()
            x.Offset <- offset
            x.Count <- count

    new (buf : 'T[], offset : int, count : int) as x =
        { inherit SafeRefCnt<RefCntBuf<'T>>("RBufPart", false) }
        then
            x.Init()
            let rcb = new RefCntBuf<'T>(buf)
            x.Offset <- offset
            x.Count <- count
            x.Elem.RC.SetRef(1L)

    member x.Init() =
        ()

    override x.InitElem() =
        base.InitElem()
        x.Offset <- 0
        x.Count <- 0
        x.StreamPos <- 0L

    override x.ReleaseElem(b) =
        base.ReleaseElem(b)

    override x.Finalize() =
        x.Release(true)

    interface IDisposable with
        member x.Dispose() = 
            x.Release(true)
            GC.SuppressFinalize(x)

type [<AllowNullLiteral>] internal SharedMemoryPool<'T,'TBase when 'T :> RefCntBuf<'TBase> and 'T: (new : unit -> 'T)> =
    inherit SharedPool<string,'T>

    [<DefaultValue>] val mutable InitFunc : 'T->unit
    [<DefaultValue>] val mutable BufSize : int

    new (initSize : int, maxSize : int, bufSize : int, initFn : 'T -> unit, infoStr : string) as x =
        { inherit SharedPool<string, 'T>() }
        then
            x.InitFunc <- initFn
            x.BufSize <- bufSize
            x.InitStack(initSize, maxSize, infoStr)

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

type IdCounter() =
    static let id = ref -1
    static member GetNext() =
        Interlocked.Increment(id)

// put counter in separate class as classes without primary constructor do not allow let bindings
// and static val fields cannot be easily initialized, also makes it independent of type 'T
type StreamBaseCounter() =
    inherit IdCounter()
type [<AllowNullLiteral>] [<AbstractClass>] StreamBase<'T> =
    inherit MemoryStream

    // sufficient for upto GUID
    [<DefaultValue>] val mutable ValBuf : byte[]
    [<DefaultValue>] val mutable Writable : bool
    [<DefaultValue>] val mutable Visible : bool
    [<DefaultValue>] val mutable Id : int
    [<DefaultValue>] val mutable private info : string

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

    abstract member ComputeHash : Security.Cryptography.HashAlgorithm*int64*int64 -> byte[]
    default x.ComputeHash(hasher : Security.Cryptography.HashAlgorithm, offset : int64, len : int64) =
        hasher.ComputeHash(x.GetBuffer(), int offset, int len)

    abstract member AddRef : unit -> unit
    default x.AddRef() =
        ()
    abstract member DecRef : unit -> unit
    default x.DecRef() =
        ()

    abstract member GetNew : unit -> StreamBase<'T>
    abstract member GetNew : int -> StreamBase<'T>
    abstract member GetNew : 'T[]*int*int*bool*bool -> StreamBase<'T>

    member internal x.GetInfoId() =
        sprintf "%s:%d" x.Info x.Id

    member private x.GetNewMs() =
        x.GetNew() :> MemoryStream

    member private x.GetNewMsBuf(buf,pos,len,a,b) =
        x.GetNew(buf,pos,len,a,b) :> MemoryStream

    member private x.GetNewMsByteBuf(buf : byte[], pos, len, a, b) =
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

    member private x.Init() =
        x.Id <- StreamBaseCounter.GetNext()
        x.ValBuf <- Array.zeroCreate<byte>(32)
        x.Writable <- true
        x.Visible <- true
        x.info <- ""

    member x.ComputeSHA512(offset : int64, len : int64) =
        use sha512 = new Security.Cryptography.SHA512Managed() // has dispose
        x.ComputeHash(sha512, offset, len)

    member x.ComputeSHA256(offset : int64, len : int64) =
        use sha256managed = new Security.Cryptography.SHA256Managed()
        x.ComputeHash(sha256managed, offset, len)

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
        x.Append(xbuf, int64 xpos, int64 xlen)
        x

    // Read a MemStream out of the current MemStream
    member x.ReadMemStream() = 
        let xlen = x.ReadInt32()
        let xpos = x.Position
        x.Seek(int64 xlen, SeekOrigin.Current) |> ignore
        let ms = x.GetNew()
        ms.Append(x, xpos, int64 xlen)
        ms

    member internal x.BinaryFormatterSerializeFromTypeName(obj, fullname) =
        CustomizedSerialization.BinaryFormatterSerializeFromTypeName(x, x.GetNewMs, x.GetNewMsByteBuf, obj, fullname)

    member internal x.BinaryFormatterDeserializeToTypeName(fullname) =
        CustomizedSerialization.BinaryFormatterDeserializeToTypeName(x, x.GetNewMs, x.GetNewMsByteBuf, fullname)

    /// Serialize an object to bytestream with BinaryFormatter, support serialization of null. 
    member internal x.Serialize( obj )=
        x.BinaryFormatterSerializeFromTypeName(obj, obj.GetType().FullName)
//        let fmt = new Runtime.Serialization.Formatters.Binary.BinaryFormatter()
//        if Utils.IsNull obj then 
//            fmt.Serialize( x, NullObjectForSerialization() )
//        else
//            fmt.Serialize( x, obj )

    /// Deserialize an object from bytestream with BinaryFormatter, support deserialization of null. 
    member internal x.Deserialize() =
//        let fmt = new Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        let fmt = CustomizedSerialization.GetBinaryFormatter(x.GetNewMs, x.GetNewMsByteBuf)
        let o = fmt.Deserialize( x )
        match o with 
        | :? NullObjectForSerialization -> 
            null
        | _ -> 
            o

[<AllowNullLiteral>] 
type internal StreamReader<'T>(_bls : StreamBase<'T>, _bufPos : int64, _maxLen : int64) =
    let bls = _bls
    let bReleased = ref 0
    let mutable elemPos = 0
    let mutable bufPos = _bufPos
    let mutable maxLen = _maxLen
    do
        bls.AddRef()

    new (bls, bufPos) =
        new StreamReader<'T>(bls, bufPos, Int64.MaxValue)

    member x.Release() =
        if (Interlocked.CompareExchange(bReleased, 1, 0)=0) then
            bls.DecRef()
    override x.Finalize() =
        x.Release()
    interface IDisposable with
        member x.Dispose() = 
            x.Release()
            GC.SuppressFinalize(x)

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
            new RBufPart<'T>(rbuf, rbuf.Offset, int retCnt)
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
        new RBufPart<byte>(buf, pos, cnt)

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

    override x.AddRef() = ()
    override x.DecRef() = ()

// essentially a generic list of buffers of type 'T
// use AddRef/Release to acquire / release resource
[<AllowNullLiteral>] 
type internal BufferListStream<'T>(defaultBufSize : int, doNotUseDefault : bool) =
    inherit StreamBase<'T>()

#if DEBUG
    static let streamsInUse = new ConcurrentDictionary<string, BufferListStream<'T>>()
#endif

    static let mutable memStack : SharedMemoryPool<RefCntBuf<'T>,'T> = null
    static let memStackInit = ref 0

    let refCount = ref 1 // constructor automatically increments ref count
    let bReleased = ref 0

    let mutable stackTrace = ""

    let mutable defaultBufferSize =
        if (defaultBufSize > 0) then
            defaultBufSize
        else
            64000
    let mutable getNewWriteBuffer : unit->RBufPart<'T> = (fun _ ->
        let buf = Array.zeroCreate<'T>(defaultBufferSize)
        new RBufPart<'T>(buf, 0, 0)
    )
    let bufList = new List<RBufPart<'T>>(8)
    let mutable rbufPart : RBufPart<'T> = null
    let mutable rbuf : RefCntBuf<'T> = null
    let mutable bufBeginPos = 0 // beginning position in current buffer
    let mutable bufPos = 0 // position into current buffer
    let mutable bufRem = 0 // remaining number of elements in current buffer
    let mutable bufRemWrite = 0 // remaining elements that can be written
    let mutable elemPos = 0 // position in list
    let mutable elemLen = 0 // total length of list
    let mutable finalWriteElem = 0 // last element into which we have written
    let mutable length = 0L // total length of stream
    let mutable position = 0L // current position in stream
    let mutable bSimpleBuffer = false // simple buffer read (one input buffer)
    let mutable capacity = 0L

    new(size : int) as x =
        new BufferListStream<'T>(size, false)
        then
            x.SetDefaults()

    new() =
        new BufferListStream<'T>(64000)

    // use an existing buffer to initialize
    new(buf : 'T[], index : int, count : int) as x =
        new BufferListStream<'T>()
        then
            x.SimpleBuffer <- true
            x.AddExistingBuffer(new RBufPart<'T>(buf, index, count))

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

    member private x.SetDefaults() =
        getNewWriteBuffer <- x.GetStackElem
        ()

    override x.Replicate() =
        let e = new BufferListStream<'T>()
        for b in x.BufList do
            e.WriteRBufNoCopy(b)
        e.Seek(x.Position, SeekOrigin.Begin) |> ignore
        e :> StreamBase<'T>        

    override x.GetNew() =
        new BufferListStream<'T>() :> StreamBase<'T>

    override x.GetNew(size) =
        new BufferListStream<'T>(size) :> StreamBase<'T>

    override x.GetNew(buf : 'T[], offset : int, count : int, a : bool, b : bool) =
        new BufferListStream<'T>(buf, offset, count) :> StreamBase<'T>

    override x.Info
        with get() =
            base.Info
        and set(v) =
            if not (base.Info.Equals("")) then
                streamsInUse.TryRemove(base.Info) |> ignore
            // for extra debugging info, can include stack trace of memstream info
            //base.Info <- v + ":" + x.Id.ToString() + Environment.StackTrace
            base.Info <- v + ":" + x.Id.ToString()
            streamsInUse.[base.Info] <- x

    static member DumpStreamsInUse() =
        Logger.LogF (LogLevel.WildVerbose, fun _ ->
            let sb = new System.Text.StringBuilder()
            sb.AppendLine(sprintf "Num streams in use: %d" streamsInUse.Count) |> ignore
            for s in streamsInUse do
                let (key, value) = (s.Key, s.Value)
                sb.AppendLine(sprintf "%s : %s" s.Key s.Value.Info) |> ignore
            sb.ToString()
        )

    // static memory pool
    static member InitFunc (e : RefCntBuf<'T>) =
        // override the release function
        e.RC.Release <- BufferListStream<'T>.ReleaseStackElem

    static member internal MemStack with get() = memStack
    static member internal InitMemStack(numBufs : int, bufSize : int) =
        if (Interlocked.CompareExchange(memStackInit, 1, 0)=0) then
            memStack <- new SharedMemoryPool<RefCntBuf<'T>,'T>(numBufs, -1, bufSize, BufferListStream<'T>.InitFunc, "Memory Stream")
            // start monitor timer
            PoolTimer.AddTimer(BufferListStream<'T>.DumpStreamsInUse, 10000L, 10000L)

    member internal x.GetStackElem() =
        let (event, buf) = RBufPart<'T>.GetFromPool(x.GetInfoId()+":RBufPart", BufferListStream<'T>.MemStack,
                                                    fun () -> new RBufPart<'T>(false) :> SafeRefCnt<RefCntBuf<'T>>)
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
        (src : 'T1, srcOffset : int byref, srcLen : int byref,
         dst : 'T2, dstOffset : int byref, dstLen : int byref) =
        let toCopy = Math.Min(srcLen*sizeof<'T1Elem>, dstLen*sizeof<'T2Elem>) // in units of bytes
        let numSrc = toCopy / sizeof<'T1Elem>
        let numDst = toCopy / sizeof<'T2Elem>
        if (toCopy > 0) then
            Buffer.BlockCopy(src, srcOffset*sizeof<'T1Elem>, dst, dstOffset*sizeof<'T2Elem>, toCopy)
            srcOffset <- srcOffset + numSrc
            srcLen <- srcLen - numSrc
            dstOffset <- dstOffset + numDst
            dstLen <- dstLen - numDst
        (numSrc, numDst)

    member private x.Release(bFromFinalize : bool) =
        if (Interlocked.CompareExchange(bReleased, 1, 0)=0) then
            if (bFromFinalize) then
                Logger.LogF(LogLevel.ExtremeVerbose, fun _ -> sprintf "List release for %s with id %d %A finalize: %b remain: %d" x.Info x.Id (Array.init bufList.Count (fun index -> bufList.[index].ElemNoCheck.Id)) bFromFinalize streamsInUse.Count)
            else
                Logger.LogF(LogLevel.ExtremeVerbose, fun _ -> sprintf "List release for %s with id %d %A finalize: %b remain: %d" x.Info x.Id (Array.init bufList.Count (fun index -> bufList.[index].Elem.Id)) bFromFinalize streamsInUse.Count)
            for l in bufList do
                l.Release()
            if not (base.Info.Equals("")) then
                streamsInUse.TryRemove(base.Info) |> ignore

    override x.AddRef() =
        Interlocked.Increment(refCount) |> ignore

    override x.DecRef() =
        Interlocked.Decrement(refCount) |> ignore
        if (!refCount <= 0) then
            if (!refCount < 0) then
                Logger.LogF(LogLevel.ExtremeVerbose, fun _ -> sprintf "Memory stream is decrefed after refCount is zero")    
            x.Release(false)

   // backup release in destructor
    override x.Finalize() =
        x.Release(true)

    member x.GetNewWriteBuffer with set(v) = getNewWriteBuffer <- v

    member internal x.SimpleBuffer with set(v) = bSimpleBuffer <- v

    member x.DefaultBufferSize with get() = defaultBufferSize and set(v) = defaultBufferSize <- v

    member x.NumBuf with get() = elemLen

    member private x.BufList with get() = bufList

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
                let len = Math.Min(l.Count*sizeof<'T>, (int32) length - off2)
                Buffer.BlockCopy(l.Elem.Buffer, off1, arr,off2, len)
                offset <- offset + l.Count
            arr

    member x.GetBuffer(arr : byte[], offset : int64, count : int64) =
        use sr = new StreamReader<'T>(x, offset, count)
        let count = Math.Min(count, length-offset)
        let offset = ref 0
        let rem = ref arr.Length
        let copyBuffer (buf : 'T[], soffset : int, scount : int) =
            let mutable scount = scount
            let mutable soffset = soffset
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
            let cnt = bufList.[elemPos].Count - offset
            pos <- pos + int64 cnt
            let totalOffset = bufList.[elemPos].Offset + offset
            new RBufPart<'T>(bufList.[elemPos], totalOffset, cnt)

    override x.GetMoreBuffer(elemPos : int byref, pos : int64 byref) : 'T[]*int*int =
        if (pos >= length) then
            (null, 0, 0)
        else
            use part = x.GetMoreBufferPart(&elemPos, &pos)
            (part.Elem.Buffer, part.Offset, part.Count)

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

    member x.Capacity64
        with get() =
            capacity
        and set(v) =
            while (capacity < v) do
                x.AddNewBuffer()

    override x.Capacity with get() = int x.Capacity64 and set(v) = x.Capacity64 <- int64 v

    // add new buffer to pool
    member internal x.AddNewBuffer() =
        let rbufPartNew = getNewWriteBuffer()
        rbufPartNew.StreamPos <- position
        bufList.Add(rbufPartNew)
        capacity <- capacity + int64 rbufPartNew.Elem.Buffer.Length
        elemLen <- elemLen + 1

    // add existing buffer to pool
    member internal x.AddExistingBuffer(rbuf : RBufPart<'T>) =
        // if use linked list instead of list, then theoretically could insert if bufRemWrite = 0 also
        // then condition would be if (position <> length && bufRemWrite <> 0) then
        if (position <> length) then
            failwith "Splicing RBuf in middle is not supported"
        else
            let rbufPartNew = new RBufPart<'T>(rbuf) // make a copy
            rbufPartNew.StreamPos <- position
            bufList.Add(rbufPartNew)
            length <- length + int64 rbuf.Count
            capacity <- capacity + int64 rbuf.Count
            elemLen <- elemLen + 1
            finalWriteElem <- elemLen
            for i = elemPos+1 to elemLen-1 do
                bufList.[i].StreamPos <- bufList.[i-1].StreamPos + int64 bufList.[i-1].Count

    // move to beginning of buffer i
    member private x.MoveToBufferI(bAllowExtend : bool, i : int) =
        if (bAllowExtend && i >= elemLen) then
            let mutable j = elemLen
            while (j <= i) do
                // extend by grabbing another buffer and appending to list
                x.AddNewBuffer()
                j <- j + 1
        if (i < elemLen) then
            rbufPart <- bufList.[i]
            elemPos <- i + 1
            finalWriteElem <- Math.Max(finalWriteElem, elemPos)
            rbuf <- rbufPart.Elem
            bufBeginPos <- rbufPart.Offset
            bufPos <- rbufPart.Offset
            bufRem <- rbufPart.Count
            if (i=0) then
                rbufPart.StreamPos <- 0L
            else
                rbufPart.StreamPos <- bufList.[i-1].StreamPos + int64 bufList.[i-1].Count
            // allow writing at end of last buffer
            if (rbufPart.StreamPos + int64 rbufPart.Count >= length) then
                bufRemWrite <- rbufPart.Elem.Buffer.Length - rbufPart.Offset
            else
                // if already written buffer, then don't extend count
                bufRemWrite <- rbufPart.Count
            true
        else
            false

    member private x.MoveToNextBuffer(bAllowExtend : bool, rem : int) =
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
                bufPos <- rbufPart.Offset + rbufPart.Count
                bufRem <- 0
                bufRemWrite <- bufRemWrite - rbufPart.Count
            ret
        else
            true

    member internal x.MoveForwardAfterWrite(amt : int) =
        bufPos <- bufPos + amt
        bufRemWrite <- bufRemWrite - amt
        bufRem <- Math.Max(0, bufRem - amt)
        rbufPart.Count <- Math.Max(rbufPart.Count, bufPos - bufBeginPos)
        position <- position + int64 amt
        length <- Math.Max(length, position)

    member internal x.MoveForwardAfterRead(amt : int) =
        bufPos <- bufPos + amt
        bufRem <- bufRem - amt
        bufRemWrite <- bufRemWrite - amt
        position <- position + int64 amt

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
                    x.MoveToBufferI(false, finalWriteElem-1) |> ignore
                    bufPos <- bufPos + rbufPart.Count
                    bufRem <- bufRem - rbufPart.Count
                    bufRemWrite <- bufRemWrite - rbufPart.Count
                    position <- length
                | _ -> ()
        if (finalPos > position) then
            let mutable diff = finalPos - position
            while (diff > 0L) do
                if (x.MoveToNextBuffer(false, bufRem)) then
                    let amtSeek = int32(Math.Min(diff, int64 bufRem))
                    x.MoveForwardAfterRead(amtSeek)
                    diff <- diff - int64 amtSeek
                else
                    diff <- 0L
        else if (finalPos < position) then
            let mutable diff = position - finalPos
            while (diff > 0L) do
                if (x.MoveToPreviousBuffer()) then
                    let amtSeek = int32(Math.Min(diff, int64 (bufPos - bufBeginPos)))
                    x.MoveForwardAfterRead(-amtSeek)
                    diff <- diff - int64 amtSeek
                else
                    diff <- 0L
        position

    // write functions
    member private x.WriteRBufNoCopy(rbuf : RBufPart<'T>) =
        x.AddExistingBuffer(rbuf)
        position <- position + int64 rbuf.Count
        elemPos <- elemPos + 1
        length <- Math.Max(length, position)
        finalWriteElem <- Math.Max(finalWriteElem, elemPos)

    member internal x.GetWriteBuffer() =
        x.MoveToNextBuffer(true, bufRemWrite) |> ignore
        (rbuf, bufPos, bufRemWrite)

    member x.WriteOne(b : 'T) =
        x.MoveToNextBuffer(true, bufRemWrite) |> ignore
        rbuf.Buffer.[bufPos] <- b
        x.MoveForwardAfterWrite(1)

    member x.WriteArr<'TS>(buf : 'TS[], offset : int, count : int) =
        let mutable bOffset = offset
        let mutable bCount = count
        while (bCount > 0) do
            x.MoveToNextBuffer(true, bufRemWrite) |> ignore
            let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<'TS[],'T[],'TS,'T>(buf, &bOffset, &bCount, rbuf.Buffer, &bufPos, &bufRemWrite)
            bufRem <- Math.Max(0, bufRem - dstCopy)
            rbufPart.Count <- Math.Max(rbufPart.Count, bufPos - bufBeginPos)
            position <- position + int64 dstCopy
            length <- Math.Max(length, position)

    member x.WriteArr<'TS>(buf : 'TS[]) =
        x.WriteArr<'TS>(buf, 0, buf.Length)

    override x.Write(buf, offset, count) =
        x.WriteArr<byte>(buf, offset, count)

    // add elements from another type of array - perhaps not needed with previous one being generic
    member x.WriteArrT(buf : System.Array, offset : int, count : int) =
        let elemSize = Runtime.InteropServices.Marshal.SizeOf(buf.GetType().GetElementType())
        let mutable bOffset = offset*elemSize
        let mutable bCount = count*elemSize
        while (bCount > 0) do
            x.MoveToNextBuffer(true, bufRemWrite) |> ignore
            let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<Array,'T[],byte,'T>(buf, &bOffset, &bCount, rbuf.Buffer, &bufPos, &bufRemWrite)
            bufRem <- Math.Max(0, bufRem - dstCopy)
            rbufPart.Count <- Math.Max(rbufPart.Count, bufPos - bufBeginPos)
            position <- position + int64 dstCopy
            length <- Math.Max(length, position)

    // Read functions
    member internal x.GetReadBuffer() =
        if (x.MoveToNextBuffer(false, bufRem)) then
            (rbuf, bufPos, bufRem)
        else
            (null, 0, 0)

    member x.ReadOne() =
        if (x.MoveToNextBuffer(false, bufRem)) then
            let b = rbuf.Buffer.[bufPos]
            x.MoveForwardAfterRead(1)
            (true, b)
        else
            (false, Unchecked.defaultof<'T>)

    member x.ReadArr<'TD>(buf : 'TD[], offset : int, count : int) =
        let mutable bOffset = offset
        let mutable bCount = count
        let mutable readAmt = 0
        while (bCount > 0) do
            if (x.MoveToNextBuffer(false, bufRem)) then
                let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<'T[],'TD[],'T,'TD>(rbuf.Buffer, &bufPos, &bufRem, buf, &bOffset, &bCount)
                readAmt <- readAmt + dstCopy
                bufRemWrite <- bufRemWrite - srcCopy
                position <- position + int64 srcCopy
            else
                bCount <- 0 // quit, no more data
        readAmt

    member x.ReadArr<'TD>(buf : 'TD[]) =
        x.ReadArr<'TD>(buf, 0, buf.Length)

    override x.Read(buf, offset, count) =
        x.ReadArr<byte>(buf, offset, count)

    // add elements from another type of array - perhaps not needed with previous one being generic
    member x.ReadArrT(buf : System.Array, offset : int, count : int) =
        let elemSize = Runtime.InteropServices.Marshal.SizeOf(buf.GetType().GetElementType())
        let mutable bOffset = offset*elemSize
        let mutable bCount = count*elemSize
        let mutable readAmt = 0
        while (bCount > 0) do
            if (x.MoveToNextBuffer(false, bufRem)) then
                let (srcCopy, dstCopy) = BufferListStream<'T>.SrcDstBlkCopy<'T[],Array,'T,byte>(rbuf.Buffer, &bufPos, &bufRem, buf, &bOffset, &bCount)
                readAmt <- readAmt + dstCopy
                bufRemWrite <- bufRemWrite - srcCopy
                position <- position + int64 srcCopy
            else
                bCount <- 0
        readAmt

    // append another memstream onto this one
    override x.Append(strmList : StreamBase<'TS>, offset : int64, count : int64) =
        use sr = new StreamReader<'TS>(strmList, offset, count)
        let mutable bDone = false
        while (not bDone) do
            let (buf, pos, cnt) = sr.GetMoreBuffer()
            if (Utils.IsNotNull buf) then
                x.WriteArr<'TS>(buf, pos, cnt)
            else
                bDone <- true

    override x.AppendNoCopy(strmList : StreamBase<'T>, offset : int64, count : int64) =
        use sr = new StreamReader<'T>(strmList, offset, count)
        let mutable bDone = false
        while (not bDone) do
            use rbuf = sr.GetMoreBufferPart()
            if (Utils.IsNotNull rbuf) then
                x.WriteRBufNoCopy(rbuf)
            else
                bDone <- true

    override x.AppendNoCopy(rbuf : RBufPart<'T>, offset : int64, count : int64) =
        let rbufAdd = new RBufPart<'T>(rbuf, int offset, int count)
        x.WriteRBufNoCopy(rbufAdd)

// MemoryStream which is essentially a collection of RefCntBuf
// Not GetBuffer is not supported by this as it is not useful
[<AllowNullLiteral>]
type internal MemoryStreamB(defaultBufSize : int, toAvoidConfusion : byte) =
    inherit BufferListStream<byte>(defaultBufSize)

    let emptyBlk = [||]

    new(size : int) as x =
        new MemoryStreamB(size, 0uy)
        then
            while (x.Capacity64 < int64 size) do
                x.AddNewBuffer() // add until size reached

    new() =
        new MemoryStreamB(64000, 0uy)

    new(buf : byte[], index : int, count : int) as x =
        new MemoryStreamB()
        then
            x.SimpleBuffer <- true
            x.AddExistingBuffer(new RBufPart<byte>(buf, index, count))

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

    override x.ComputeHash(hasher : Security.Cryptography.HashAlgorithm, offset : int64, len : int64) =
        let mutable bDone = false
        use sr = new StreamReader<byte>(x, offset, len)
        sr.ApplyFnToBuffers (fun (buf, pos, cnt) -> hasher.TransformBlock(buf, pos, cnt, null, 0) |> ignore)
        hasher.TransformFinalBlock(emptyBlk, 0, 0) |> ignore
        hasher.Hash

    // Write functions
    // add elements from file - cannot be in generic as file read only supports byte
    member x.WriteFromStream(fh : Stream, count : int) =
        let mutable bCount = count
        while (bCount > 0) do
            let (buf, pos, amt) = x.GetWriteBuffer()
            let toRead = Math.Min(bCount, amt)
            let writeAmt = fh.Read(buf.Buffer, pos, toRead)
            bCount <- bCount - writeAmt
            x.MoveForwardAfterWrite(writeAmt)
            if (writeAmt <> toRead) then
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
    member x.GenericWrite<'T>(v : 'T) =
        let t = typeof<'T>
        if (t.IsArray) then
            let arr = unbox<System.Array>(v)
            x.WriteInt32(arr.Length)
            x.WriteArrT(arr, 0, arr.Length)
        else 
            Serialize.Serialize<'T> x v

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
            fh.Write(buf.Buffer, pos, int readAmt)
            x.MoveForwardAfterRead(int readAmt)

    // read count starting from offset, and don't move position forward
    override x.ReadToStream(s : Stream, offset : int64, count : int64) =
        use sr = new StreamReader<byte>(x, offset, count)
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
    member x.GenericRead<'T>() =
        let t = typeof<'T>
        if (t.IsArray) then
            let len = x.ReadInt32()
            let tArr = typeof<'T>
            let arr = Array.CreateInstance(tArr.GetElementType(), [|len|])
            x.ReadArrT(arr, 0, len) |> ignore
            box(arr) :?> 'T
        else
            Serialize.Deserialize<'T> x

