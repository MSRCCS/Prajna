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

open Prajna.Tools


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



[<Serializable>]
type internal NullObjectForSerialization() = 
    class
    end

[<StructuralEquality; StructuralComparison>]
type internal UInt128 = 
    struct 
        val High : uint64
        val Low : uint64
        new ( high: uint64, low: uint64 ) = { High = high; Low = low }
        new ( value:byte[] ) = { High = BitConverter.ToUInt64( value, 0 ); Low = BitConverter.ToUInt64( value, 8) }
        override x.ToString() = x.High.ToString("X16")+x.Low.ToString("X16")
    end
    
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

// For C# lambda, C# compiler generates a display class that is not marked as "Serializable"
// To be able to serialize C# lambdas that is supplied to Prajna functions, use serialization surrogate

// The serialization surrogate for C# compiler generated display class. It implements ISerializationSurrogate
type private CSharpDisplayClassSerializationSurrogate () =
    let getInstanceFields (obj : obj) =
        obj.GetType().GetFields(BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)

    interface ISerializationSurrogate with
        member x.GetObjectData(obj: obj, info: SerializationInfo, context: StreamingContext): unit = 
            getInstanceFields obj |> Array.iter (fun f -> info.AddValue(f.Name, f.GetValue(obj)))
        
        member x.SetObjectData(obj: obj, info: SerializationInfo, context: StreamingContext, selector: ISurrogateSelector): obj = 
            getInstanceFields obj |> Array.iter (fun f -> f.SetValue(obj, info.GetValue(f.Name, f.FieldType)))
            obj

          

/// <summary>
/// Prajna allows user to implement customized memory manager. The corresponding class 'Type implement a 
///     allocFunc: () -> 'Type to grab an object of 'Type (from a pool of preallocated object). 
///     resetFunc: int -> () to return an object to the pool. 
/// Jin Li: This class is to be internalized, with a corresponding external implementation at JobDependencies. 
/// </summary> 
type internal CustomizedMemoryManager() = 
    static member val internal MemoryManagerCollectionByName = ConcurrentDictionary<string, _>() with get
    /// <summary>
    /// Install a customized Memory Manager, in raw format of storage and no checking
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="fullname"> Type name of the object. </param>
    /// <param name="wrappedEncodeFunc"> Customized Serialization function that encodes an Object to a bytestream.  </param>
    static member InstallMemoryManager( fullname, allocFunc: unit -> Object, preallocFunc: int -> unit ) = 
        CustomizedMemoryManager.MemoryManagerCollectionByName.Item( fullname ) <- ( allocFunc, preallocFunc )
    static member GetAllocFunc<'Type when 'Type :> IDisposable >() = 
        let bExist, tuple = CustomizedMemoryManager.MemoryManagerCollectionByName.TryGetValue( typeof<'Type>.FullName )
        if bExist then 
            let allocFunc, _ = tuple 
            let wrappedAllocFunc() =
                allocFunc() :?> 'Type
            wrappedAllocFunc
        else 
            failwith (sprintf "Can't find customized memory manager for type %s" typeof<'Type>.FullName )
    static member GetPreallocFunc<'Type when 'Type :> IDisposable >() =
        let bExist, tuple = CustomizedMemoryManager.MemoryManagerCollectionByName.TryGetValue( typeof<'Type>.FullName )
        if bExist then 
            let _, preallocFunc = tuple 
            preallocFunc
        else 
            failwith (sprintf "Can't find customized memory manager for type %s" typeof<'Type>.FullName )


/// Programmer will implementation CustomizedSerializerAction of Action<Object*Stream> to customarily serialize an object 
type CustomizedSerializerAction = Action<Object*Stream>
/// Programmer will implementation CustomizedSerializerAction of Func<MemStream, Object> to customarily deserialize an object 
and CustomizedDeserializerFunction = Func<Stream, Object>

/// <summary>
/// In Prajna, except closure serialization, user may install a customized encoder/decoder to serialize data, to make serialization/deserialization more efficient
/// Jin Li: This class is internal, with a corresponding external interface to be accessed at JobDependencies. 
/// </summary> 
and internal CustomizedSerialization() = 
    /// For null boject
    static member val internal NullObjectGuid = Guid( "45BD7C41-0595-4854-A375-0A1895B10AAD" ) with get
    /// Use default system serializer
    static member val internal DefaultSerializerGuid = Guid( "4721F23B-65D3-499D-9750-2D6FE6A6AE54" ) with get
    /// Collection of Customized Serializer by name
    static member val internal EncoderCollectionByName = ConcurrentDictionary<string, Guid>(StringComparer.Ordinal) with get
    /// Collection of Customized Serializer by Guid
    static member val internal EncoderCollectionByGuid = ConcurrentDictionary<Guid, (Object*Stream)->unit>() with get
    /// Collection of Customized Deserializer by name
    static member val internal DecoderCollectionByName = ConcurrentDictionary<string, Guid>(StringComparer.Ordinal) with get
    /// Collection of Customized Serializer by Guid
    static member val internal DecoderCollectionByGuid = ConcurrentDictionary<Guid, Stream->Object>() with get
    /// <summary>
    /// Install a customized serializer, with a unique GUID that identified the use of the serializer in the bytestream. 
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="encodeFunc"> Customized Serialization function that encodes the 'Type to a bytestream.  </param>
    static member InstallSerializer<'Type >( id: Guid, encodeFunc: 'Type*Stream->unit ) = 
        if id = Guid.Empty || id = CustomizedSerialization.NullObjectGuid then 
            failwith ( sprintf "Guid %A has been reserved, please generate a different guid" id )
        else
            let fullname = typeof<'Type>.FullName
            let wrappedEncodeFunc (o:Object, ms ) = 
                encodeFunc ( o :?> 'Type, ms )    
            CustomizedSerialization.EncoderCollectionByName.Item( fullname ) <- id 
            CustomizedSerialization.EncoderCollectionByGuid.Item( id ) <- wrappedEncodeFunc
    /// <summary>
    /// Install a customized serializer, in raw format of storage and no checking
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="fullname"> Type name of the object. </param>
    /// <param name="wrappedEncodeFunc"> Customized Serialization function that encodes an Object to a bytestream.  </param>
    static member InstallSerializer( id: Guid, fullname, wrappedEncodeFunc ) = 
        CustomizedSerialization.EncoderCollectionByName.Item( fullname ) <- id 
        CustomizedSerialization.EncoderCollectionByGuid.Item( id ) <- wrappedEncodeFunc
    /// <summary>
    /// Install a customized deserializer, with a unique GUID that identified the use of the deserializer in the bytestream. 
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the deserializer in the bytestream. </param>
    /// <param name="decodeFunc"> Customized Deserialization function that decodes bytestream to 'Type.  </param>
    static member InstallDeserializer<'Type>( id: Guid, decodeFunc: Stream -> 'Type ) = 
        if id = Guid.Empty || id = CustomizedSerialization.NullObjectGuid then 
            failwith ( sprintf "Guid %A has been reserved, please generate a different guid" id )
        else
            let fullname = typeof<'Type>.FullName
            let wrappedDecodeFunc (ms) = 
                decodeFunc ( ms ) :> Object
            CustomizedSerialization.DecoderCollectionByName.Item( fullname ) <- id 
            CustomizedSerialization.DecoderCollectionByGuid.Item( id ) <- wrappedDecodeFunc
    /// <summary>
    /// Install a customized serializer, in raw format of storage and no checking
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="fullname"> Type name of the object. </param>
    /// <param name="wrappedDecodeFunc"> Customized Deserialization function that decodes the bytestream to object. </param>
    static member InstallDeserializer( id: Guid, fullname, wrappedDecodeFunc ) = 
        CustomizedSerialization.DecoderCollectionByName.Item( fullname ) <- id 
        CustomizedSerialization.DecoderCollectionByGuid.Item( id ) <- wrappedDecodeFunc

    /// <summary> 
    /// InstallSerializerDelegate allows language other than F# to install its own type serialization implementation. 
    /// </summary> 
    /// <param name="id"> Guid, that uniquely identifies the serializer/deserializer installed. </param>
    /// <param name="fulltypename"> Type.FullName that captures object that will trigger the serializer. 
    ///         please note that the customized serializer/deserializer will not be triggered on the derivative type. You may need to install additional 
    ///         serializer if multiple derivative type share the same customzied serializer/deserializer. </param>
    /// <param name="del"> An action delegate that perform the serialization function. </param>
    static member InstallSerializerDelegate( id: Guid, fulltypename, del: CustomizedSerializerAction ) = 
        if id = Guid.Empty || id = CustomizedSerialization.NullObjectGuid then 
            failwith ( sprintf "Guid %A has been reserved, please generate a different guid" id )
        else
            let wrappedEncodeFunc( o:Object, ms:Stream ) = 
                del.Invoke( o, ms ) 
            CustomizedSerialization.EncoderCollectionByName.Item( fulltypename ) <- id
            CustomizedSerialization.EncoderCollectionByGuid.Item( id ) <- wrappedEncodeFunc
    /// <summary> 
    /// InstallDeserializerDelegate allows language other than F# to install its own type deserialization implementation. 
    /// </summary> 
    /// <param name = "id"> Guid, that uniquely identifies the serializer/deserializer installed. </param>
    /// <param name = "fulltypename"> Type.FullName that captures object that will trigger the serializer. 
    ///         please note that the customized serializer/deserializer will not be triggered on the derivative type. You may need to install additional 
    ///         serializer if multiple derivative type share the same customzied serializer/deserializer </param>
    /// <param name = "del"> A function delegate that perform the deserialization function. </param>
    static member InstallDeserializerDelegate( id: Guid, fulltypename, del: CustomizedDeserializerFunction ) = 
        if id = Guid.Empty || id = CustomizedSerialization.NullObjectGuid then 
            failwith ( sprintf "Guid %A has been reserved, please generate a different guid" id )
        else
            let wrappedDecodeFunc( ms:Stream ) = 
                del.Invoke( ms ) 
            CustomizedSerialization.DecoderCollectionByName.Item( fulltypename ) <- id
            CustomizedSerialization.DecoderCollectionByGuid.Item( id ) <- wrappedDecodeFunc

and private CustomizedSerializationSurrogate(nameObj, encoder, decoder ) = 
    let getInstanceFields (obj : obj) =
        obj.GetType().GetFields(BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic)
    member val ObjName : string = nameObj with get
    member val Encoder: (Object*Stream)->unit = encoder with get
    member val Decoder: Stream->Object = decoder with get
    interface ISerializationSurrogate with
        member x.GetObjectData(obj: obj, info: SerializationInfo, context: StreamingContext): unit = 
            if Utils.IsNull( x.Encoder ) then 
                getInstanceFields obj |> Array.iter (fun f -> info.AddValue(f.Name, f.GetValue(obj)))
            else
                use ms = new MemStream()
                x.Encoder( obj, ms :> Stream )
                let bytes = ms.GetBuffer()
                info.AddValue( nameObj, bytes )       
        member x.SetObjectData(obj: obj, info: SerializationInfo, context: StreamingContext, selector: ISurrogateSelector): obj = 
            if Utils.IsNull( x.Decoder ) then 
                getInstanceFields obj |> Array.iter (fun f -> f.SetValue(obj, info.GetValue(f.Name, f.FieldType)))
                obj
            else
                let bytes = info.GetValue(nameObj, typeof<byte[]>) :?> byte[]
                use ms = new MemStream( bytes, 0, bytes.Length, false, true )
                let o = x.Decoder( ms :> Stream )
                o        

// The serialization surrogate selector that selects CSharpDisplayClassSerializationSurrogate for C# compiler generated display class. 
// It implements ISurrogateSelector
and private CustomizedSerializationSurrogateSelector () =
    let mutable nextSelector = Unchecked.defaultof<ISurrogateSelector> 

    interface ISurrogateSelector with
        member x.ChainSelector(selector: ISurrogateSelector): unit = 
            nextSelector <- selector
        
        member x.GetNextSelector(): ISurrogateSelector = 
            nextSelector
        
        member x.GetSurrogate(ty: Type, context: StreamingContext, selector: byref<ISurrogateSelector>): ISerializationSurrogate = 
            let fullname = ty.FullName
            let bExistEncoder, guidEncoder = CustomizedSerialization.EncoderCollectionByName.TryGetValue( fullname ) 
            let bExistDecoder, guidDecoder = CustomizedSerialization.DecoderCollectionByName.TryGetValue( fullname ) 
            if bExistEncoder || bExistDecoder then 
                // Customized encoder/decoder exist 
                let encoder = ref Unchecked.defaultof<_>
                let decoder = ref Unchecked.defaultof<_>
                if bExistEncoder then 
                    CustomizedSerialization.EncoderCollectionByGuid.TryGetValue( guidEncoder, encoder ) |> ignore 
                if bExistDecoder then 
                    CustomizedSerialization.DecoderCollectionByGuid.TryGetValue( guidDecoder, decoder ) |> ignore 
                if Utils.IsNull( !encoder ) && Utils.IsNull( !decoder ) then 
                    null 
                else
                    CustomizedSerializationSurrogate( fullname, !encoder, !decoder) :> ISerializationSurrogate
            else      
                // Use CSharpDisplayClassSerializationSurrogate if we see a type:
                // * is not serializable
                // * is compiler generated
                // * The name contains "DisplayClass"                     
                if (not ty.IsSerializable) && 
                   ty.IsDefined(typeof<System.Runtime.CompilerServices.CompilerGeneratedAttribute>, false) &&
                   ty.Name.Contains("<>c__DisplayClass") then
                    CSharpDisplayClassSerializationSurrogate() :> ISerializationSurrogate
                else
                    null

/// <summary>
/// MemStream is similar to MemoryStream and provide a number of function to ease the read and write object
/// This class is used if you need to implement customized serializer and deserializer. 
/// </summary>
and [<AllowNullLiteral>]
    MemStream = 
    inherit MemoryStream
    /// Initializes a new instance of the MemStream class with an expandable capacity initialized to zero.
    new () = { inherit MemoryStream() }
    /// Initializes a new instance of the MemStream class with an expandable capacity initialized as specified.
    new (size:int) = { inherit MemoryStream(size) }
    /// Initializes a new non-resizable instance of the MemoryStream class that is not writable, and can use GetBuffer to access the underlyingbyte array.
    new (buf:byte[]) = { inherit MemoryStream(buf, 0, buf.Length, false, true) }
    /// <summary>
    /// Initializes a new non-resizable instance of the MemStream class based on the specified byte array with the CanWrite property set as specified.
    /// </summary>
    /// <param name="buf"> byte array used </param>
    /// <param name="writable"> CanWrite property </param>
    new (buf, writable) = { inherit MemoryStream( buf, writable)  }
    /// <summary>
    /// Initializes a new non-resizable instance of the MemStream class based on the specified region (index) of a byte array.
    /// </summary> 
    /// <param name="buffer"> byte array used </param>
    /// <param name="index"> The index into buffer at which the steram begins. </param>
    /// <param name="count"> The length of the stream in bytes. </param>
    new (buffer, index, count ) = { inherit MemoryStream( buffer, index, count) }
    /// <summary>
    /// Initializes a new non-resizable instance of the MemStream class based on the specified region of a byte array, with the CanWrite property set as specified.
    /// </summary>
    /// <param name="buffer"> byte array used </param>
    /// <param name="index"> The index into buffer at which the steram begins. </param>
    /// <param name="count"> The length of the stream in bytes. </param>
    /// <param name="writable"> CanWrite property </param>
    new (buffer, index, count, writable ) = { inherit MemoryStream( buffer, index, count, writable)  }
    /// <summary>
    /// Initializes a new instance of the MemStream class based on the specified region of a byte array, with the CanWrite property set as specified, and the ability to call GetBuffer set as specified.
    /// </summary> 
    /// <param name="buffer"> byte array used </param>
    /// <param name="index"> The index into buffer at which the steram begins. </param>
    /// <param name="count"> The length of the stream in bytes. </param>
    /// <param name="writable"> CanWrite property </param>
    /// <param name="publiclyVisible"> true to enable GetBuffer, which returns the unsigned byte array from which the stream was created; otherwise, false.  </param>
    new (buffer, index, count, writable, publiclyVisible ) = { inherit MemoryStream( buffer, index, count, writable, publiclyVisible)  }
    /// <summary> 
    /// Initializes a new instance of the MemStream class based on a prior instance of MemStream. The resultant MemStream is not writable. 
    /// </summary>
    new (ms:MemStream) = 
        { inherit MemoryStream( ms.GetBuffer(), int ms.Position, ms.GetBuffer().Length - (int ms.Position), false, true ) } 
    /// <summary> 
    /// Initializes a new instance of the MemStream class based on a prior instance of MemStream, with initial position to be set at offset. The resultant MemStream is not writable. 
    /// </summary>
    new (ms:MemStream, offset) as x = 
        { inherit MemoryStream( ms.GetBuffer(), 0, ms.GetBuffer().Length, false, true ) } 
        then 
            x.Seek( offset, SeekOrigin.Begin ) |> ignore 
    member internal  x.GetValidBuffer() =
        Array.sub (x.GetBuffer()) 0 (int x.Length)
    member private x.GetBinaryFormatter() =
//      Uncomment to use new BinarySerializer
//        let fmt = new BinarySerializer()
        let fmt = Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        fmt.SurrogateSelector <- CustomizedSerializationSurrogateSelector()
        fmt
    /// Serialize an object to bytestream with BinaryFormatter, support serialization of null. 
    member internal x.Serialize( obj )=
        let fmt = x.GetBinaryFormatter()
        if Utils.IsNull obj then 
            fmt.Serialize( x, NullObjectForSerialization() )
        else
            fmt.Serialize( x, obj )
    /// Deserialize an object from bytestream with BinaryFormatter, support deserialization of null. 
    member internal x.Deserialize() =
        let fmt = x.GetBinaryFormatter()
        let o = fmt.Deserialize( x )
        match o with 
        | :? NullObjectForSerialization -> 
            null
        | _ -> 
            o
    /// <summary> 
    /// Serialize a particular object to bytestream using BinaryFormatter, support serialization of null.  
    /// </summary>
    member private x.BinaryFormatterSerializeFromTypeName( obj: 'U, fullname:string )=
            let fmt = x.GetBinaryFormatter()
            if Utils.IsNull obj then 
                fmt.Serialize( x, NullObjectForSerialization() )
            else
#if DEBUG
                if obj.GetType().FullName<>fullname then 
                    System.Diagnostics.Trace.WriteLine ( sprintf "!!! Warning !!! MemStream.SerializeFromTypeName, expect type of %s but get %s"
                                                                    fullname
                                                                    (obj.GetType().FullName) )     
#endif
                fmt.Serialize( x, obj )
    /// <summary> 
    /// Deserialize a particular object from bytestream using BinaryFormatter, support serialization of null.
    /// </summary>
    member private x.BinaryFormatterDeserializeToTypeName(fullname:string) =
            let fmt = x.GetBinaryFormatter()
            let o = fmt.Deserialize( x )
            match o with 
            | :? NullObjectForSerialization -> 
                Unchecked.defaultof<_>
            | _ -> 
#if DEBUG
                if Utils.IsNotNull fullname && o.GetType().FullName<>fullname then 
                    System.Diagnostics.Trace.WriteLine ( sprintf "!!! Warning !!! MemStream.DeserializeToTypeName, expect type of %s but get %s"
                                                                    fullname
                                                                    (o.GetType().FullName) )     
#endif
                o 
    /// <summary> 
    /// Serialize a particular object to bytestream, allow use of customizable serializer if installed. 
    /// If obj is null, it is serialized to a specific reserved NullObjectGuid for null. 
    /// If obj is not null, and no customized serailizer is installed, the bytestream is written as DefaultSerializerGuid + BinaryFormatter() serialized bytestream. 
    /// If obj is not null, and a customized serailizer is installed, the bytestream is written as GUID_SERIALIZER + content. 
    /// </summary>
    /// <param name="obj"> Object to be serialized </param> 
    /// <param name="fullname"> TypeName of the Object to be used to lookup for installed customizable serializer </param>
    member x.CustomizableSerializeFromTypeName( obj: Object, fullname:string )=
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
            x.WriteBytes( CustomizedSerialization.DefaultSerializerGuid.ToByteArray() )
            x.BinaryFormatterSerializeFromTypeName( obj, fullname )
    /// <summary> 
    /// Peak next 16B (GUID), and check if the result is null.
    /// </summary>
    /// <returns> true if null has been serialized </returns>
    member x.PeekIfNull() = 
        let pos = x.Position
        let buf = Array.zeroCreate<_> 16 
        x.ReadBytes( buf ) |> ignore
        let bRet = Guid( buf ) = CustomizedSerialization.NullObjectGuid
        x.Seek( pos, SeekOrigin.Begin ) |> ignore
        bRet
    /// <summary> 
    /// Write a Null Guid to bytestream
    /// </summary>
    member x.WriteNull() = 
        x.WriteBytes( CustomizedSerialization.NullObjectGuid.ToByteArray() ) 
    /// <summary> 
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if installed. 
    /// A Guid is first read from bytestream, if it is a specific reserved NullObjectGuid, return object is null. 
    /// If the GUID is DefaultSerializerGuid, BinaryFormatter is used to deserialize the object. 
    /// For other GUID, installed customized deserailizer is used to deserialize the object. 
    /// </summary>
    /// <param name="fullname"> Should always be null </param>
    member x.CustomizableDeserializeToTypeName(fullname:string) =
        let buf = Array.zeroCreate<_> 16 
        let pos = x.Position
        x.ReadBytes( buf ) |> ignore
        let typeGuid = Guid( buf ) 
        if typeGuid = CustomizedSerialization.NullObjectGuid then 
            null 
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
    member x.SerializeFromWithTypeName( obj: 'U )=
        x.CustomizableSerializeFromTypeName( obj, typeof<'U>.FullName )
    /// <summary>
    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of name obj.GetType().FullName. 
    /// </summary> 
    member x.SerializeObjectWithTypeName( obj: Object )=
        let objName = if Utils.IsNull obj then null else obj.GetType().FullName
        x.CustomizableSerializeFromTypeName( obj, objName )
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if one is installed.  
    member x.DeserializeObjectWithTypeName() =
        x.CustomizableDeserializeToTypeName( null ) 
    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of typeof<'U>.FullName, even the object passed in is of a derivative type. 
    member x.SerializeFrom( obj: 'U )=
        /// x.BinaryFormatterSerializeFromTypeName( obj, typeof<'U>.FullName )
        x.CustomizableSerializeFromTypeName( obj, typeof<'U>.FullName ) 
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if one is installed.  
    member x.DeserializeTo<'U>() =
        // x.BinaryFormatterDeserializeToTypeName( typeof<'U>.FullName ) :?> 'U
        let obj = x.CustomizableDeserializeToTypeName( null ) 
        if Utils.IsNull obj then
            Unchecked.defaultof<_>
        else
            obj :?> 'U
    /// Write IPEndPoint to bytestream 
    member x.WriteIPEndPoint( addr: Net.IPEndPoint ) =
//        x.WriteVInt32( int addr.AddressFamily )       
        x.WriteBytesWLen( addr.Address.GetAddressBytes() )
        x.WriteInt32( addr.Port )
    /// Read IPEndPoint from bytestream, if the bytestream is truncated prematurely, the later IPAddress and port information will be 0. 
    member x.ReadIPEndPoint( ) = 
//        let addrFamily = x.ReadVInt32()
        let buf = x.ReadBytesWLen()
        let port = x.ReadInt32() 
        Net.IPEndPoint( Net.IPAddress( buf ), port )
    // ToDo: Reimplement MemStream to allow this operation to be done without a memory copy operation. 
    /// Insert a bytearray (buf) before the current MemStream, and return the resultant MemStream
    member x.InsertBytesBefore( buf:byte[] ) = 
        let xbuf = x.GetBuffer()
        let xpos, xlen = if x.Position = x.Length then 0, int x.Length else int x.Position, int ( x.Length - x.Position )
        let y = new MemStream( buf.Length + xlen )
        y.WriteBytes( buf ) 
        y.Write( xbuf, xpos, xlen ) 
        y
    // ToDo: Reimplement MemStream to allow this operation to be done without a memory copy operation. 
    /// Insert a second MemStream before the current MemStream, and return the resultant MemStream
    member x.InsertBefore( mem2:MemStream ) = 
        let xbuf = x.GetBuffer()
        let xpos, xlen = if x.Position = x.Length then 0, int x.Length else int x.Position, int ( x.Length - x.Position )
        if mem2.Position < mem2.Length then mem2.Seek( 0L, SeekOrigin.End ) |> ignore
        mem2.Write( xbuf, xpos, xlen )
        mem2
    /// <summary>
    /// Return the buffer, position, count as a tuple that captures the state of the current MemStream. 
    /// buffer: bytearray of the underlying bytestream. 
    /// position: the current position if need to write out the bytestream. 
    /// count: number of bytes if need to write out the bytestream. 
    /// </summary>
    member x.GetBufferPosLength() = 
        let xbuf = x.GetBuffer()
        let xpos, xlen = if x.Position = x.Length then 0, int x.Length else int x.Position, int ( x.Length - x.Position )
        xbuf, xpos, xlen        
    // Write a MemStream at the end of the current MemStream, return the current MemStream after the write
    member x.WriteMemStream( mem2: MemStream ) = 
        let xbuf, xpos, xlen = mem2.GetBufferPosLength()
        x.WriteInt32( xlen )
        x.Write( xbuf, xpos, xlen)
        x
    // Read a MemStream out of the current MemStream
    member x.ReadMemStream() = 
        let xbuf = x.GetBuffer()
        let xlen = x.ReadInt32()
        let xpos = x.Seek( 0L, SeekOrigin.Current )
        x.Seek( int64 xlen, SeekOrigin.Current ) |> ignore 
        new MemStream( xbuf, int xpos, xlen, false, true )
    //    override x.Dispose( bFinalizing ) = 
    /// For a writable MemStream, convert bytes from 0..Positoin to a bytearray
    member internal x.ForWriteConvertToByteArray() = 
        let xbuf = x.GetBuffer()
        let xpos, xlen = if x.Position = x.Length then 0, int x.Length else int x.Position, int ( x.Length - x.Position )
        let bytearray = Array.zeroCreate<byte> xlen
        Buffer.BlockCopy( xbuf, xpos, bytearray, 0, xlen )
        bytearray      
    member internal x.WriteUInt128( data: UInt128 ) = 
        x.WriteUInt64( data.Low )
        x.WriteUInt64( data.High ) 
    member internal x.ReadUInt128() = 
        let low = x.ReadUInt64()
        let high = x.ReadUInt64()
        UInt128( high, low )


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
