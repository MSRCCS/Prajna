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
		extension.fs
  
	Description: 
		Extension of System.IO.Stream class with specific serialization function

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Aug. 2015
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.IO
open System.Runtime.CompilerServices
open System.Runtime.Serialization.Json

[<StructuralEquality; StructuralComparison>]
type internal UInt128 = 
    struct 
        val High : uint64
        val Low : uint64
        new ( high: uint64, low: uint64 ) = { High = high; Low = low }
        new ( value:byte[] ) = { High = BitConverter.ToUInt64( value, 0 ); Low = BitConverter.ToUInt64( value, 8) }
        override x.ToString() = x.High.ToString("X16")+x.Low.ToString("X16")
    end
    
/// Extension Methods for System.IO.Stream
[<Extension>]
type StreamExtension =  
    /// Writes a bytearray to the current stream.
    [<Extension>]
    static member WriteBytes( x: Stream, buf ) = 
        x.Write( buf, 0, buf.Length ) |> ignore
    /// <summary>
    /// Writes a bytearray with offset and count to the current stream.
    /// </summary>
    [<Extension>]
    static member WriteBytesWithOffset( x: Stream, buf, offset, count ) = 
        x.Write( buf, offset, count ) |> ignore    
    /// <summary>
    /// Read a bytearray from the current stream. The length of bytes to be read is determined by the size of the bytearray. 
    /// </summary>
    /// <param name="buf"> byte array to be read </param>
    /// <returns> The total number of bytes written into the buffer. This can be less than the number of bytes requested if that number of bytes are not currently available, or zero if the end of the stream is reached before any bytes are read.
    /// </returns>
    [<Extension>]
    static member ReadBytes( x:Stream, buf ) = 
        x.Read( buf, 0, buf.Length ) 
    /// <summary>
    /// Read a bytearray of len bytes to the current stream. The function always return a bytearray of size len even if the number of bytes that is currently available is less (or even zero if the end of the stream is reached before any bytes are read). 
    /// In such a case, the remainder of the bytearray is filled with zero. 
    /// </summary>
    /// <returns> A bytearray of size len. If the number of bytes that is currently available is less (or if the end of the stream is reached before any bytes are read), 
    /// the remainder of the bytearray is filled with zero. 
    /// </returns>
    [<Extension>]
    static member ReadBytes( x:Stream, len : int) =
        let buf = Array.zeroCreate<byte>(len)
        StreamExtension.ReadBytes(x, buf) |> ignore
        buf
    /// Write a Guid to bytestream. 
    [<Extension>]
    static member WriteGuid( x:Stream, id: Guid ) = 
        StreamExtension.WriteBytes( x, id.ToByteArray() )
    /// Read a Guid from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any bytes are read), the remainder of Guid will be
    /// fillled with zero.  
    [<Extension>]
    static member ReadGuid( x:Stream ) = 
        let buf = Array.zeroCreate<_> 16
        StreamExtension.ReadBytes( x, buf ) |> ignore
        Guid( buf )
    /// Attempt to read the remainder of the bytestream as a single bytearray. 
    [<Extension>]
    static member ReadBytesToEnd( x:Stream ) = 
        let buf = Array.zeroCreate<byte> (int ( x.Length - x.Position ))
        if buf.Length>0 then 
            x.Read( buf, 0, buf.Length ) |> ignore
        buf
    /// Write a uint16 to bytestream.  
    [<Extension>]
    static member WriteUInt16( x:Stream, b:uint16 ) = 
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a uint16 from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any bytes are read), the high order bytes of uint16 will be
    /// fillled with zero.  
    [<Extension>]
    static member ReadUInt16( x: Stream ) = 
        let buf = Array.zeroCreate<byte> 2
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToUInt16( buf, 0 )
    /// Write a int16 to bytestream. 
    [<Extension>]
    static member WriteInt16( x: Stream, b:int16 ) = 
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a int16 from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the high order bytes of int16 will be fillled with zero.  
    [<Extension>]
    static member ReadInt16( x: Stream ) = 
        let buf = Array.zeroCreate<byte> 2
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToInt16( buf, 0 )
    /// Write a uint32 to bytestream. 
    [<Extension>]
    static member WriteUInt32( x:Stream, b:uint32 ) = 
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a uint32 from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the high order bytes of uint32 will be fillled with zero. 
    [<Extension>]
    static member ReadUInt32( x: Stream ) = 
        let buf = Array.zeroCreate<byte> 4
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToUInt32( buf, 0 )
    /// Write a int32 to bytestream. 
    [<Extension>]
    static member WriteInt32( x: Stream, b:int32 ) = 
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a int32 from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the high order bytes of uint32 will be fillled with zero.
    [<Extension>]
    static member ReadInt32( x: Stream ) = 
        let buf = Array.zeroCreate<byte> 4
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToInt32( buf, 0 )
    /// Write a int32 to bytestream with variable length coding (VLC). 
    /// For value between -127..+127 -> one byte is used to encode the int32
    /// For other value              -> 0x80 + 4Byte (5 bytes are used to encode the int32)
    [<Extension>]
    static member WriteVInt32( x:Stream, b:int32 ) =
        match b with 
        | v when -127<=v && v<=127 -> 
            x.WriteByte( byte v ) 
        | _ ->
            x.WriteByte( 0x80uy )
            StreamExtension.WriteInt32( x, b )
    /// Read a int32 from bytestream with variable length coding (VLC). 
    /// For value between -127..+127 -> one byte is used to encode the int32
    /// For other value              -> 0x80 + 4Byte (5 bytes are used to encode the int32)
    /// If the end of the stream has been reached at the moment of read, ReadVInt32() will return 0. 
    /// If at least one byte is read, but the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the high order bytes of int32 will be fillled with zero.
    [<Extension>]
    static member ReadVInt32( x: Stream ) =
        let b = x.ReadByte()
        match b with 
        | v when v<>128 -> 
            if v <= -1 then 0 else if v<128 then v else int( sbyte v)
        | _ ->
            StreamExtension.ReadInt32( x )
    /// Write a boolean into bytestream, with 1uy represents true and 0uy represents false
    [<Extension>]
    static member WriteBoolean( x: Stream, b:bool ) = 
        x.WriteByte( if b then 1uy else 0uy )
    /// Read a boolean from bytestream. If the end of the stream is reached, the function will return true.
    [<Extension>]
    static member ReadBoolean( x: Stream ) = 
        x.ReadByte() <> 0
    /// Write a uint64 to bytestream.     
    [<Extension>]
    static member WriteUInt64( x:Stream, b:uint64 ) = 
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a uint64 from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the high order bytes of uint64 will be fillled with zero.
    [<Extension>]
    static member ReadUInt64( x: Stream ) = 
        let buf = Array.zeroCreate<byte> 8
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToUInt64( buf, 0 )
    /// Write a int64 to bytestream.     
    [<Extension>]
    static member WriteInt64( x: Stream, b:int64 ) = 
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a int64 from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the high order bytes of int64 will be fillled with zero.
    [<Extension>]
    static member ReadInt64( x: Stream ) = 
        let buf = Array.zeroCreate<byte> 8
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToInt64( buf, 0 )
    /// Write a double float value to bytestream.     
    [<Extension>]
    static member WriteDouble( x:Stream, b: float ) =
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a double float value from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the late read bytes of double will be fillled with zero.
    [<Extension>]
    static member ReadDouble( x:Stream ) = 
        let buf = Array.zeroCreate<byte> 8
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToDouble( buf, 0 )
    /// Write a single float value to bytestream.     
    [<Extension>]
    static member WriteSingle( x:Stream, b: float32 ) =
        StreamExtension.WriteBytes( x, BitConverter.GetBytes( b ) )
    /// Read a single float value from bytestream. If the number of bytes that is currently available is less (or if the end of the stream is reached before any 
    /// bytes are read), the late read bytes of single will be fillled with zero.
    [<Extension>]
    static member ReadSingle( x:Stream ) = 
        let buf = Array.zeroCreate<byte> 4
        StreamExtension.ReadBytes( x, buf ) |> ignore
        BitConverter.ToSingle( buf, 0 )
    /// Write a bytearray to bytestream and prefix with length of the bytearray. 
    [<Extension>]
    static member WriteBytesWLen( x:Stream, buf:byte[] ) = 
        if Utils.IsNull buf then
            StreamExtension.WriteInt32( x, 0 )
        else
            StreamExtension.WriteInt32( x, buf.Length )
            StreamExtension.WriteBytes( x, buf )
    /// Read a bytearray from bytestream that is prefixed with length of the bytearray. If the bytestream is truncated prematurely, the returned bytearray will be a bytearray of 
    /// remainder of the bytestram. 
    [<Extension>]
    static member ReadBytesWLen( x:Stream ) = 
        let len = StreamExtension.ReadInt32( x )
        if len <= 0 then 
            Array.zeroCreate<_> 0 
        else
            let remaining = x.Length - x.Position
            if remaining >= int64 len then 
                let buf = Array.zeroCreate<byte> len
                StreamExtension.ReadBytes( x, buf ) |> ignore
                buf
            else
                let buf = Array.zeroCreate<byte> (int remaining) 
                if remaining > 0L then 
                    StreamExtension.ReadBytes( x, buf ) |> ignore
                buf
    /// Write a bytearray to bytestream and prefix with a length (VLC coded) of the bytearray. 
    /// If the bytearray is null, the bytestream is written to indicate that is a bytearray of size 0. 
    [<Extension>]
    static member WriteBytesWVLen( x:Stream, buf:byte[] ) = 
        if Utils.IsNull buf then
            StreamExtension.WriteVInt32( x, 0 )
        else
            StreamExtension.WriteVInt32( x, buf.Length )
            StreamExtension.WriteBytes( x, buf )
    /// Read a bytearray from bytestream that is prefixed with length of the bytearray. If the bytestream is truncated prematurely, the returned bytearray will be a bytearray of 
    /// remainder of the bytestram. 
    [<Extension>]
    static member ReadBytesWVLen( x: Stream ) = 
        let len = StreamExtension.ReadVInt32( x )
        if len <= 0 then 
            Array.zeroCreate<_> 0 
        else
            let remaining = x.Length - x.Position
            if remaining >= int64 len then 
                let buf = Array.zeroCreate<byte> len
                StreamExtension.ReadBytes( x, buf ) |> ignore
                buf    
            else
                let buf = Array.zeroCreate<byte> (int remaining) 
                StreamExtension.ReadBytes( x, buf ) |> ignore
                buf
    /// Write a string (UTF8Encoded) to bytestream and prefix with a length (VLC coded) of the bytearray. 
    /// If the string is null, the bytestream is written to indicate that is a string of length 0. 
    [<Extension>]
    static member WriteStringV( x: Stream, s: string ) =
        let enc = System.Text.UTF8Encoding()
        if Utils.IsNotNull s then 
            StreamExtension.WriteBytesWVLen( x, enc.GetBytes( s ) )
        else
            StreamExtension.WriteBytesWVLen( x, null )
    /// Read a string (UTF8Encoded) from bytestream with prefix (VLC coded) of the bytearray. 
    /// If the bytestream is truncated prematurely, the returned string will be ""  
    [<Extension>]
    static member ReadStringV( x: Stream ) = 
        let buf = StreamExtension.ReadBytesWVLen( x )
        let enc = System.Text.UTF8Encoding()
        enc.GetString( buf )
    /// Write a string (UTF8Encoded) to bytestream and prefix with a length of the bytearray. 
    /// If the string is null, the bytestream is written to indicate that is a string of length 0. 
    [<Extension>]
    static member WriteString( x: Stream, s: string ) =
        let enc = System.Text.UTF8Encoding()
        if Utils.IsNotNull s then 
            StreamExtension.WriteBytesWLen( x, enc.GetBytes( s ) )
        else
            StreamExtension.WriteBytesWLen( x, enc.GetBytes( "" ) )
    /// Read a string (UTF8Encoded) from bytestream with prefix of the bytearray. 
    /// If the bytestream is truncated prematurely, the returned string will be ""  
    [<Extension>]
    static member ReadString( x: Stream ) = 
        let buf = StreamExtension.ReadBytesWLen( x )
        let enc = System.Text.UTF8Encoding()
        enc.GetString( buf )

    /// Write IPEndPoint to bytestream 
    [<Extension>]
    static member WriteIPEndPoint( x: Stream, addr: Net.IPEndPoint ) =
//        x.WriteVInt32( int addr.AddressFamily )       
        StreamExtension.WriteBytesWLen( x, addr.Address.GetAddressBytes() )
        StreamExtension.WriteInt32( x, addr.Port )

/// Read IPEndPoint from bytestream, if the bytestream is truncated prematurely, the later IPAddress and port information will be 0. 
    [<Extension>]
    static member ReadIPEndPoint( x: Stream ) = 
//        let addrFamily = x.ReadVInt32()
        let buf = StreamExtension.ReadBytesWLen( x )
        let port = StreamExtension.ReadInt32( x ) 
        Net.IPEndPoint( Net.IPAddress( buf ), port )

    [<Extension>]
    static member internal WriteUInt128( x: Stream, data: UInt128 ) = 
        StreamExtension.WriteUInt64( x, data.Low )
        StreamExtension.WriteUInt64( x, data.High ) 

    [<Extension>]
    static member internal ReadUInt128( x: Stream ) = 
        let low = StreamExtension.ReadUInt64( x )
        let high = StreamExtension.ReadUInt64( x )
        UInt128( high, low )

    /// Write a json object to bytestream
    [<Extension>]
    static member WriteJson( x: Stream, data: 'T ) = 
        let json = DataContractJsonSerializer(typedefof<'T>)
        json.WriteObject(x, data)

    /// Read a json object from bytestream
    [<Extension>]
    static member ReadJson( x: Stream ): 'T = 
        let json = DataContractJsonSerializer(typedefof<'T>)
        json.ReadObject(x) :?> 'T

/// Extension Methods for System.String
[<Extension>]
type StringExtension =  
    // Serialize data to json string
    [<Extension>]
    static member SerializeToJson( data: 'T ): String = 
        use ms = new MemoryStream()
        ms.WriteJson(data)
        ms.Position <- 0L
        use sr = new StreamReader(ms)
        sr.ReadToEnd()

    // Deserialize a json string to data
    [<Extension>]
    static member DeserializeFromJson( s: String ): 'T = 
        use ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(s))
        ms.ReadJson()
    /// Helper function to pack a remote exception 
    /// See: http://blogs.msdn.com/b/brada/archive/2005/03/27/402801.aspx
    [<Extension>]
    static member WriteException( x: Stream, ex: Exception ) = 
        let fmt = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        fmt.Serialize( x, ex )
    /// Helper function to pack a remote exception 
    /// See: http://blogs.msdn.com/b/brada/archive/2005/03/27/402801.aspx
    [<Extension>]
    static member ReadException( x: Stream ) = 
        let fmt = System.Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        fmt.Deserialize( x ) :?> System.Exception



