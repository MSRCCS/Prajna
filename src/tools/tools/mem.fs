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
		mem.fs
  
	Description: 
		Memory tools

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Apr. 2013
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Reflection
open System.Runtime.Serialization
open System.Threading

open Prajna.Tools
open Prajna.Tools.Queue
open Prajna.Tools.FSharp

module internal NodeConfig =
    [<Measure>] 
    type MB

    [<Measure>] 
    type GB

    [<Literal>]
    let BytePerMB = 1048576<MB^-1>

    [<Literal>]
    let MBPerGB = 1024<MB/GB>

    let BytePerGB = BytePerMB * MBPerGB

    let ByteToMB ( x : int ) = x / BytePerMB

    let ByteToGB ( x : int ) = x / BytePerGB

    let GBToMB ( x : int<GB> ) = x * MBPerGB

    let MBToByte ( x : int<MB> ) = x * BytePerMB

    let GBToByte ( x : int<GB> ) = x * BytePerGB

/// ExpandableBuffer wraps around a bytearray object that can be extended in both direction, front & back
/// The valid bytes in the Buffer is between Head & Tail. 
/// The current implementation of expandableBuffer is not threadsafe, and should not be used in multiple thread environment. 
type ExpandableBuffer internal ( initialBuffer:byte[], posAtZero:int ) = 
    member val internal Buffer = initialBuffer with get, set
    member val internal PosAtZero = posAtZero with get, set
    /// Head position
    member val Head = posAtZero with get, set
    /// Tail position
    member val Tail = posAtZero with get, set
    static member val internal DefaultPosAtZero = 1024 with get, set
    static member val internal DefaultBufferLength = 1024 with get, set
    /// Buffer Length extension: 
    /// It should be a strict monotonically increasing function so that x < y and f(x) < f(y), and x < f(x)
    member val internal ExtensionFunc = ( fun (x:int) -> x*4) with get, set
    /// <summary> Create an expandable buffer of default size </summary> 
    new() = ExpandableBuffer( Array.zeroCreate<byte> (ExpandableBuffer.DefaultBufferLength + ExpandableBuffer.DefaultPosAtZero ), ExpandableBuffer.DefaultPosAtZero )
    /// <summary> Create an expandable buffer of a certain size, with a default position at zero </summary> 
    new( size ) = ExpandableBuffer( Array.zeroCreate<byte> ( size + ExpandableBuffer.DefaultPosAtZero ), ExpandableBuffer.DefaultPosAtZero )
    /// <summary> Create an expandable buffer of a certain size, and a position at zero. The buffer can be extended both at head and at tail </summary> 
    new( size, posAtZero:int ) = ExpandableBuffer( Array.zeroCreate<byte> ( size + posAtZero ), posAtZero )
    /// <summary>
    /// Get Buffer with position and length
    /// Return:
    /// buf: byte[]
    /// start: start position
    /// len: length of the valid buffer
    /// </summary>
    member x.GetBufferTuple() = 
        x.Buffer, x.Head, x.Tail - x.Head
    /// Get Buffer at tail 
    member x.GetBufferAfterTail() = 
        x.Buffer, x.Tail, x.Buffer.Length - x.Tail
    /// Return a buffer (as a new Object)
    member x.GetTruncatedBuffer() = 
        let buf, start, len = x.GetBufferTuple()
        let newBuf = Array.zeroCreate<byte> len
        Buffer.BlockCopy( buf, start, newBuf, 0, len )
        newBuf
    /// Length of the buffer
    member x.Length with get() = let (_, _, len) = x.GetBufferTuple()
                                 len
    /// Try extend the buffer at tail by len byte
    /// do not move the tail pointer. 
    member x.TryExtendTail( len ) = 
        let tail = x.Tail + len
        if tail > x.Buffer.Length then 
            let mutable newLen = x.PosAtZero + x.ExtensionFunc( x.Buffer.Length - x.PosAtZero ) 
            // Extend the buffer until there is enough head room. 
            while tail > newLen do 
                newLen <- x.PosAtZero + x.ExtensionFunc( newLen - x.PosAtZero )
            if newLen > 0 then 
                let newBuf = Array.zeroCreate<byte> newLen
                Buffer.BlockCopy( x.Buffer, 0, newBuf, 0, x.Buffer.Length )
                // no change of x.PosAtZero, x.Head & x.Tail
                x.Buffer <- newBuf
            else
                failwith (sprintf "TryExtendTail(%d) has a wrong newLen, Tail: %d, PosAtZero %d, Buffer.Length %d, tail:%d newLen:%d"
                                    len x.Tail x.PosAtZero x.Buffer.Length tail newLen
                         )           
    /// Extend the buffer at tail by len byte, move the Tail pointer. 
    member x.ExtendTail( len ) = 
        x.Tail <- x.Tail + len
        if x.Tail > x.Buffer.Length then 
            // Trigger Buffer Extention
            x.TryExtendTail( 0 ) 
    /// Extend the buffer at head by len byte
    member x.TryExtendHead( len ) = 
        let head = x.Head - len 
        if head < 0 then 
            let mutable newPosAtZero = x.ExtensionFunc( x.PosAtZero )
            while head + newPosAtZero - x.PosAtZero < 0 do 
                newPosAtZero <- x.ExtensionFunc( newPosAtZero )
            let newBuf = Array.zeroCreate<_> ( newPosAtZero - x.PosAtZero + x.Buffer.Length )
            Buffer.BlockCopy( x.Buffer, 0, newBuf, newPosAtZero - x.PosAtZero, x.Buffer.Length )
            // adjust x.PosAtZero, x.Head & x.Tail
            x.Head <- x.Head + newPosAtZero - x.PosAtZero
            x.Tail <- x.Tail + newPosAtZero - x.PosAtZero
            x.PosAtZero <- newPosAtZero
            x.Buffer <- newBuf
    /// Extend the buffer at head by len byte, move the Head pointer. 
    member x.ExtendHead( len ) = 
        x.Head <- x.Head - len
        if x.Head < 0 then 
            // Trigger Buffer Extention
            x.TryExtendHead( 0 ) 
    /// If using Expandable buffer to read stream, the chunk size used to readin stream data 
    static member val DefaultReadStreamBlockLength = 10240 with get, set 
    /// Read All stream into an expandable buffer
    member x.ReadStream( readStream: Stream, blocklen ) = 
        let useblocklen = if blocklen <= 0 then ExpandableBuffer.DefaultReadStreamBlockLength else blocklen
        let mutable nRead = useblocklen
        while nRead > 0 do 
            x.TryExtendTail( nRead ) 
            let buf, tail, count = x.GetBufferAfterTail()
            if count <= 0 then 
                nRead <- 0
            else
                nRead <- readStream.Read( buf, tail, count ) 
                if nRead > 0 then 
                    x.ExtendTail( nRead ) 
    /// Read stream into an expandable buffer
    static member ReadStreamToBuffer( readStream ) = 
        let x = ExpandableBuffer( ExpandableBuffer.DefaultReadStreamBlockLength, 0 )
        x.ReadStream( readStream, ExpandableBuffer.DefaultReadStreamBlockLength)
        x.GetBufferTuple()

type MStream = MemoryStreamB
//type MStream = StreamBaseByte

/// <summary>
/// MemStream is similar to MemoryStream and provide a number of function to ease the read and write object
/// This class is used if you need to implement customized serializer and deserializer. 
/// </summary>
type [<AllowNullLiteral>] MemStream = 
    inherit MStream
    /// Initializes a new instance of the MemStream class with an expandable capacity initialized to zero.
    new () = { inherit MStream() }
    /// Initializes a new instance of the MemStream class with an expandable capacity initialized as specified.
    new (size:int) = { inherit MStream(size) }
    /// Initializes a new non-resizable instance of the MemoryStream class that is not writable, and can use GetBuffer to access the underlyingbyte array.
    new (buf:byte[]) = { inherit MStream(buf, 0, buf.Length, false, true) }
    /// <summary>
    /// Initializes a new non-resizable instance of the MemStream class based on the specified byte array with the CanWrite property set as specified.
    /// </summary>
    /// <param name="buf"> byte array used </param>
    /// <param name="writable"> CanWrite property </param>
    new (buf, writable : bool) = { inherit MStream( buf, writable)  }
    /// <summary>
    /// Initializes a new non-resizable instance of the MemStream class based on the specified region (index) of a byte array.
    /// </summary> 
    /// <param name="buffer"> byte array used </param>
    /// <param name="index"> The index into buffer at which the steram begins. </param>
    /// <param name="count"> The length of the stream in bytes. </param>
    new (buffer, index, count :int) = { inherit MStream( buffer, index, count) }
    /// <summary>
    /// Initializes a new non-resizable instance of the MemStream class based on the specified region of a byte array, with the CanWrite property set as specified.
    /// </summary>
    /// <param name="buffer"> byte array used </param>
    /// <param name="index"> The index into buffer at which the steram begins. </param>
    /// <param name="count"> The length of the stream in bytes. </param>
    /// <param name="writable"> CanWrite property </param>
    new (buffer, index, count, writable ) = { inherit MStream( buffer, index, count, writable)  }
    /// <summary>
    /// Initializes a new instance of the MemStream class based on the specified region of a byte array, with the CanWrite property set as specified, and the ability to call GetBuffer set as specified.
    /// </summary> 
    /// <param name="buffer"> byte array used </param>
    /// <param name="index"> The index into buffer at which the steram begins. </param>
    /// <param name="count"> The length of the stream in bytes. </param>
    /// <param name="writable"> CanWrite property </param>
    /// <param name="publiclyVisible"> true to enable GetBuffer, which returns the unsigned byte array from which the stream was created; otherwise, false.  </param>
    new (buffer, index, count, writable, publiclyVisible ) = { inherit MStream( buffer, index, count, writable, publiclyVisible)  }
    /// <summary> 
    /// Initializes a new instance of the MemStream class based on a prior instance of MemStream. The resultant MemStream is not writable. 
    /// </summary>
    new (ms:MemStream) = 
        //{ inherit MStream( ms.GetBuffer(), int ms.Position, ms.GetBuffer().Length - (int ms.Position), false, true ) } 
        { inherit MStream(ms) }
    /// <summary> 
    /// Initializes a new instance of the MemStream class based on a prior instance of MemStream, with initial position to be set at offset. The resultant MemStream is not writable. 
    /// </summary>
    new (ms:MemStream, offset) = 
        //{ inherit MStream( ms.GetBuffer(), 0, ms.GetBuffer().Length, false, true ) } 
        { inherit MStream(ms, offset) }

    override x.GetNew() =
        new MemStream() :> StreamBase<byte>

    override x.GetNew(size) =
        new MemStream(size) :> StreamBase<byte>

    override x.GetNew(buffer, index, count, writable, publiclyVisible) =
        new MemStream(buffer, index, count, writable, publiclyVisible) :> StreamBase<byte>

    member internal  x.GetValidBuffer() =
        Array.sub (x.GetBuffer()) 0 (int x.Length)

type Strm =
    /// <summary> 
    /// Serialize a particular object to bytestream, allow use of customizable serializer if installed. 
    /// If obj is null, it is serialized to a specific reserved NullObjectGuid for null. 
    /// If obj is not null, and no customized serailizer is installed, the bytestream is written as DefaultSerializerGuid + BinaryFormatter() serialized bytestream. 
    /// If obj is not null, and a customized serailizer is installed, the bytestream is written as GUID_SERIALIZER + content. 
    /// </summary>
    /// <param name="obj"> Object to be serialized </param> 
    /// <param name="fullname"> TypeName of the Object to be used to lookup for installed customizable serializer </param>
    static member CustomizableSerializeFromTypeName( x : StreamBase<byte>, obj: Object, fullname:string )=
        let mutable bCustomized = false
        if Utils.IsNull obj then 
            x.WriteBytes( CustomizedSerialization.NullObjectGuid.ToByteArray() ) 
            bCustomized <- true
        else
            let bExist, typeGuid = CustomizedSerialization.EncoderCollectionByName.TryGetValue( fullname ) 
            if bExist then 
                let bE, encodeFunc = CustomizedSerialization.EncoderCollectionByGuid.TryGetValue( typeGuid ) 
                if bE then 
                    x.WriteBytes( typeGuid.ToByteArray() )
                    encodeFunc( obj, x ) 
                    bCustomized <- true
        if not bCustomized then 
            match (obj) with
//                | :? ((System.Byte)[][]) as arr ->
//                    x.WriteBytes( CustomizedSerialization.ArrSerializerGuid.ToByteArray() )
//                    x.WriteInt32(arr.Length)
//                    for i=0 to arr.Length-1 do
//                        x.WriteInt32(arr.[i].Length)
//                        x.WriteBytes(arr.[i])
//                | :? ((StreamBase<byte>)[]) as arr ->
//                    x.WriteBytes( CustomizedSerialization.MStreamSerializerGuid.ToByteArray() )
//                    x.WriteInt32(arr.Length)
//                    for i=0 to arr.Length-1 do
//                        x.WriteInt32(int32 arr.[i].Length)
//                    for i=0 to arr.Length-1 do
//                        x.AppendNoCopy(arr.[i], 0L, arr.[i].Length)
//                        arr.[i].DecRef()
                | _ ->
                    x.WriteBytes( CustomizedSerialization.DefaultSerializerGuid.ToByteArray() )
                    x.BinaryFormatterSerializeFromTypeName( obj, fullname )

    //static member ArrToReadTo = Array.zeroCreate<byte>(50000)

    /// <summary> 
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if installed. 
    /// A Guid is first read from bytestream, if it is a specific reserved NullObjectGuid, return object is null. 
    /// If the GUID is DefaultSerializerGuid, BinaryFormatter is used to deserialize the object. 
    /// For other GUID, installed customized deserailizer is used to deserialize the object. 
    /// </summary>
    /// <param name="fullname"> Should always be null </param>
    static member CustomizableDeserializeToTypeName( x : StreamBase<byte>, fullname:string ) =
        let buf = Array.zeroCreate<_> 16 
        let pos = x.Position
        x.ReadBytes( buf ) |> ignore
        let typeGuid = Guid( buf ) 
        if typeGuid = CustomizedSerialization.NullObjectGuid then 
            null 
//        elif typeGuid = CustomizedSerialization.ArrSerializerGuid then
//            let arr = Array.zeroCreate<System.Byte[]>(x.ReadInt32())
//            for i=0 to arr.Length-1 do
//                arr.[i] <- Array.zeroCreate<System.Byte>(x.ReadInt32())
//                x.ReadBytes(arr.[i]) |> ignore
////                let len = x.ReadInt32()
////                x.Seek(int64 len, SeekOrigin.Current) |> ignore
////                arr.[i] <- Strm.ArrToReadTo
//            box(arr)
//        elif typeGuid = CustomizedSerialization.MStreamSerializerGuid then
//            let len = x.ReadInt32()
//            let arr = Array.zeroCreate<MemoryStreamB>(len)
//            let arrLen = Array.zeroCreate<int32>(arr.Length)
//            for i=0 to arr.Length-1 do
//                arrLen.[i] <- x.ReadInt32()
//            for i=0 to arr.Length-1 do
//                arr.[i] <- new MemoryStreamB()
//                //arr.[i].WriteFromStream(x, arrLen.[i])
//                arr.[i].AppendNoCopy(x, x.Position, int64 arrLen.[i])
//                x.Seek(int64 arrLen.[i], SeekOrigin.Current) |> ignore
//            //x.DecRef()
//            box(arr)
        elif typeGuid <> CustomizedSerialization.DefaultSerializerGuid then 
            let bE, decodeFunc = CustomizedSerialization.DecoderCollectionByGuid.TryGetValue( typeGuid ) 
            if bE then 
                decodeFunc( x ) 
            else
                // This move back is added for compatible reason, old Serializer don't add a Guid, so if we can't figure out the guid in the beginning of
                // bytestream, we just move back 
                x.Seek( pos, SeekOrigin.Begin ) |> ignore
                // Can't parse the guid, using the default serializer
                x.BinaryFormatterDeserializeToTypeName( fullname ) 
                // failwith (sprintf "Customized deserializable for typeGuid %A has not been installed" typeGuid ) 
        else
            x.BinaryFormatterDeserializeToTypeName( fullname ) 
    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of typeof<'U>.FullName, even the object passed in is of a derivative type. 
    static member SerializeFromWithTypeName( x, obj: 'U )=
        Strm.CustomizableSerializeFromTypeName( x, obj, typeof<'U>.FullName )
    /// <summary>
    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of name obj.GetType().FullName. 
    /// </summary> 
    static member SerializeObjectWithTypeName( x, obj: Object )=
        let objName = if Utils.IsNull obj then null else obj.GetType().FullName
        Strm.CustomizableSerializeFromTypeName( x, obj, objName )
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if one is installed.  
    static member DeserializeObjectWithTypeName( x ) =
        Strm.CustomizableDeserializeToTypeName( x, null ) 
    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of typeof<'U>.FullName, even the object passed in is of a derivative type. 
    static member SerializeFrom( x, obj: 'U )=
        /// x.BinaryFormatterSerializeFromTypeName( obj, typeof<'U>.FullName )
        Strm.CustomizableSerializeFromTypeName( x, obj, typeof<'U>.FullName ) 
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if one is installed.  
    static member DeserializeTo<'U>(x) =
        // x.BinaryFormatterDeserializeToTypeName( typeof<'U>.FullName ) :?> 'U
        let obj = Strm.CustomizableDeserializeToTypeName( x, null ) 
        if Utils.IsNull obj then
            Unchecked.defaultof<_>
        else
            obj :?> 'U

//  JinL: 05/02/2014 MemoryStream is purely mnanaged code, add Finalize() just degrades performance. 
//    /// Finalize
//    override x.Finalize() = 
//        x.Dispose( true )  
        
/// MemBitStream Will potentially read & write bits
//type internal MemBitStream = 
//    inherit MemStream
//    /// 0..7 current bits to be written 
//    val mutable BitPnt : int 
//    [<DefaultValue>]
//    val mutable CurBytePos : int64 
//    new () = { inherit MemStream(); BitPnt = 8 }
//    new (size:int) = { inherit MemStream(size); BitPnt = 8 }
//    new (buf:byte[]) = { inherit MemStream(buf); BitPnt = 8 }
//    new (buf:byte[], b) = { inherit MemStream( buf, b); BitPnt = 8  }
//    new (buf, p1, p2 ) = { inherit MemStream( buf, p1, p2); BitPnt = 8 }
//    new (buf, p1, p2, b1 ) = { inherit MemStream( buf, p1, p2, b1); BitPnt = 8 }
//    new (buf, p1, p2, b1, b2 ) = { inherit MemStream( buf, p1, p2, b1, b2); BitPnt = 8 }
//    new (ms:MemBitStream ) = { inherit MemStream( ms ); BitPnt = 8 }
//    new (ms:MemBitStream, offset ) = { inherit MemStream( ms, offset ); BitPnt = 8 }
//    member x.WriteBits( v, nbit ) = 
//        if x.BitPnt<0 || x.BitPnt>=8 then 
//            x.BitPnt <- 0 
//            x.CurBytePos <- x.Position
//            x.WriteByte(0uy)
//    member x.ReadBits( nbit ) = 
//        ()
                
[<AllowNullLiteral>]
type internal CircularBuffer<'a> (size: int) =
    member val buffer = Array.zeroCreate<'a> size with get, set
    member val bufferSize = size with get,set
    member val readPtr = 0 with get,set
    member val writePtr = 0 with get,set
    member x.length = (x.writePtr + x.bufferSize - x.readPtr) % x.bufferSize
    member x.freeSize = (x.readPtr-1-x.writePtr + x.bufferSize)% x.bufferSize
    member x.readPtrToEnd = x.bufferSize - x.readPtr
    member x.writePtrToEnd = x.bufferSize - x.writePtr

    member x.MoveReadPtr (len:int) =
        if len > x.length then
            failwith "Cannot move readptr in Circular buffer"
        x.readPtr <- (x.readPtr + len)
        if x.readPtr >= x.bufferSize then
            x.readPtr <- x.readPtr - x.bufferSize
    member x.MoveWritePtr (len:int) =
        if len > x.freeSize then
            failwith "Cannot move writeptr in Circular buffer"
        x.writePtr <- (x.writePtr + len)
        if x.writePtr >= x.bufferSize then
            x.writePtr <- x.writePtr - x.bufferSize

    member x.Write(src:'a) =
        if x.freeSize < 1 then
            failwith "Cannot write to Circular buffer, buffer is full; need to check the free size before write"
        x.buffer.[x.writePtr] <- src
        x.MoveWritePtr(1)
        
    member x.Write(writePtr:int, src:'a) =
        if x.freeSize < 1 then
            failwith "Cannot write to Circular buffer, buffer is full; need to check the free size before write"
        x.buffer.[writePtr] <- src


    member x.Write(src:byte[],posi:int, len:int) =
        if len > x.freeSize then
            failwith "Cannot write to Circular buffer, buffer is full; need to check the free size before write"
        if len <= x.writePtrToEnd then
            Buffer.BlockCopy( src, posi, x.buffer, x.writePtr, len )
        else 
            Buffer.BlockCopy( src, posi, x.buffer, x.writePtr, x.writePtrToEnd )
            Buffer.BlockCopy( src, posi, x.buffer, 0, len - x.writePtrToEnd )
        x.MoveWritePtr(len)

    member x.Write(writePtr:int,src:byte[],posi:int, len:int) =
        if len > x.freeSize then
            failwith "Cannot write to Circular buffer, buffer is full; need to check the free size before write"
        if len <= x.RelativePtrToEnd(writePtr) then
            Buffer.BlockCopy( src, posi, x.buffer, writePtr, len )
        else 
            Buffer.BlockCopy( src, posi, x.buffer, writePtr, x.RelativePtrToEnd(writePtr) )
            Buffer.BlockCopy( src, posi, x.buffer, 0, len - x.RelativePtrToEnd(writePtr) )


    member x.RelativeLength (readPosition:int) =   
        (x.writePtr + x.bufferSize - readPosition) % x.bufferSize

    member x.RelativeReadPtrToEnd (readPosition:int) = x.bufferSize - readPosition
    member x.RelativePtrToEnd (posi:int) = x.bufferSize - posi

    member x.MovePtr (ptr:int, len:int) =
        let mutable tptr = (ptr + len)
        while tptr >= x.bufferSize do
            tptr <- tptr - x.bufferSize
        tptr
