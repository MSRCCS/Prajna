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
open System.Runtime.Serialization
open System.Collections.Concurrent
open System.Reflection
open System.Runtime.Serialization
open System.Runtime.CompilerServices
open System.Threading

open Prajna.Tools
open Prajna.Tools.Utils

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

// currently use StreamBaseByte as opposed to MemoryStreamB when default MemStream is used
type MemStream = MemoryStreamB
//type MemStream = StreamBaseByte
  
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
                
