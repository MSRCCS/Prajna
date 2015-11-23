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
		customserialize.fs
  
	Description: 
		Custom Serialization supporting serialization of null

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
        Sanjeev Mehrotra, Principal Software Architect

    Date:
        Aug. 2015	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.IO
open System.Runtime.CompilerServices
open System.Reflection
open System.Runtime.Serialization
open System.Collections.Concurrent
open Prajna.Tools.FSharp

[<Serializable>]
type internal NullObjectForSerialization() = 
    class
    end

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
  
module GenericSerialization = 
    
    let private raiseAlreadyPresent =
        Func<Guid, (ISurrogateSelector -> IFormatter), (ISurrogateSelector -> IFormatter)>(fun _ _ -> 
            raise <| ArgumentException("Formatter with same Guid already present."))

    /// Default generic serializer (standard .Net BinaryFormatter)
    let BinaryFormatterGuid = Guid( "4721F23B-65D3-499D-9750-2D6FE6A6AE54" )
    
    /// New generic serializer (our BinarySerializer)
    let PrajnaFormatterGuid = Guid("99CB89AA-A823-41D5-B817-8E582DE8E086")

    let mutable DefaultFormatterGuid = PrajnaFormatterGuid

    let internal FormatterMap : ConcurrentDictionary<Guid, ISurrogateSelector -> IFormatter> = 
        let stdFormatter surrogateSelector = Formatters.Binary.BinaryFormatter(SurrogateSelector = surrogateSelector) :> IFormatter
        let prajnaFormatter surrogateSelector = 
            let ser = BinarySerializer() :> IFormatter
            ser.SurrogateSelector <- surrogateSelector
            ser
        let fmtMap = ConcurrentDictionary<_,_>()
        fmtMap.AddOrUpdate( BinaryFormatterGuid, stdFormatter, raiseAlreadyPresent ) |> ignore
        fmtMap.AddOrUpdate( PrajnaFormatterGuid, prajnaFormatter, raiseAlreadyPresent) |> ignore
        fmtMap

    let GetFormatter (guid: Guid, surrogateSelector: ISurrogateSelector) = FormatterMap.[guid] surrogateSelector

    let AddFormatter(guid: Guid, fmt: ISurrogateSelector -> IFormatter) = 
        FormatterMap.AddOrUpdate(guid, Func<Guid, (ISurrogateSelector -> IFormatter)>(fun _ -> fmt),  raiseAlreadyPresent )

    let GetDefaultFormatter(surrogateSelector: ISurrogateSelector) = GetFormatter(DefaultFormatterGuid, surrogateSelector)


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
    /// Memstream[] serializer
    static member val internal MStreamSerializerGuid = Guid("4D6BD747-AC4C-43A9-BDDA-2EB007B4C601") with get
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
    /// <param name="bInstallAsDefault"> If true, the installed serializer will be used for default serialization.  </param>
    static member InstallSerializer<'Type >( id: Guid, encodeFunc: 'Type*Stream->unit, bInstallAsDefault ) = 
        if id = Guid.Empty || id = CustomizedSerialization.NullObjectGuid then 
            failwith ( sprintf "Guid %A has been reserved, please generate a different guid" id )
        else
            let fullname = typeof<'Type>.FullName
            let wrappedEncodeFunc (o:Object, ms ) = 
                encodeFunc ( o :?> 'Type, ms )    
            if bInstallAsDefault then 
                CustomizedSerialization.EncoderCollectionByName.Item( fullname ) <- id 
            CustomizedSerialization.EncoderCollectionByGuid.Item( id ) <- wrappedEncodeFunc
    /// <summary>
    /// Install a customized serializer, in raw format of storage and no checking
    /// </summary>
    /// <param name="id"> Guid that uniquely identified the use of the serializer in the bytestream. The Guid is used by the deserializer to identify the need to 
    /// run a customized deserializer function to deserialize the object. </param>
    /// <param name="fullname"> Type name of the object. </param>
    /// <param name="wrappedEncodeFunc"> Customized Serialization function that encodes an Object to a bytestream.  </param>
    /// <param name="bInstallAsDefault"> If true, the installed serializer will be used for default serialization.  </param>
    static member InstallSerializer( id: Guid, fullname, wrappedEncodeFunc, bInstallAsDefault  ) = 
        if bInstallAsDefault then 
            CustomizedSerialization.EncoderCollectionByName.Item( fullname ) <- id 
        CustomizedSerialization.EncoderCollectionByGuid.Item( id ) <- wrappedEncodeFunc
    /// A schema has been requested, the deserializer of the particular schema hasn't been installed, 
    /// However, the developer has claimed that an alternative deserializer (of a different schema) will be able to handle the deserialization of the object. 
    static member AlternateDeserializerID (id: Guid ) = 
        // Currently, we haven't install alternate serializer/deserializer 
        id
    /// A schema has been requested, the deserializer of the particular schema hasn't been installed, 
    /// However, the developer has claimed that an alternative deserializer (of a different schema) will be able to handle the deserialization of the object. 
    static member AlternateSerializerID (id: Guid ) = 
        // Currently, we haven't install alternate serializer/deserializer 
        id


    /// Get the Installed Serializer SchemaID of a certain type 
    static member GetInstalledSchemaID( fullname ) = 
        let bExist, id = CustomizedSerialization.EncoderCollectionByName.TryGetValue( fullname )
        if bExist then 
            id
        else
            Guid.Empty
    /// Get the Installed Serializer SchemaID of a certain type 
    static member GetInstalledSchemaID<'Type>( ) = 
        CustomizedSerialization.GetInstalledSchemaID( typeof<'Type>.FullName )


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
    static member InstallSerializerDelegate( id: Guid, fulltypename, del: CustomizedSerializerAction, bInstallAsDefault ) = 
        if id = Guid.Empty || id = CustomizedSerialization.NullObjectGuid then 
            failwith ( sprintf "Guid %A has been reserved, please generate a different guid" id )
        else
            let wrappedEncodeFunc( o:Object, ms:Stream ) = 
                del.Invoke( o, ms ) 
            if bInstallAsDefault then 
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

    static member internal GetBinaryFormatter(getNewMs, getNewMsBuf) =
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(getNewMs, getNewMsBuf)) 

    /// <summary> 
    /// Serialize a particular object to bytestream using BinaryFormatter, support serialization of null.  
    /// </summary>
    static member internal BinaryFormatterSerializeFromTypeName( x, getNewMs, getNewMsBuf, obj: 'U, fullname:string )=
            let fmt = CustomizedSerialization.GetBinaryFormatter(getNewMs, getNewMsBuf)
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
    static member internal BinaryFormatterDeserializeToTypeName( x, getNewMs, getNewMsBuf, fullname:string ) =
            let fmt = CustomizedSerialization.GetBinaryFormatter(getNewMs, getNewMsBuf)
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
    /// Peak next 16B (GUID), and check if the result is null.
    /// </summary>
    /// <returns> true if null has been serialized </returns>
    static member PeekIfNull(x : Stream) = 
        let pos = x.Position
        let buf = Array.zeroCreate<_> 16
        x.ReadBytes( buf ) |> ignore
        let bRet = Guid( buf ) = CustomizedSerialization.NullObjectGuid
        x.Seek( pos, SeekOrigin.Begin ) |> ignore
        bRet
    /// <summary> 
    /// Write a Null Guid to bytestream
    /// </summary>
    static member WriteNull(x : Stream) = 
        x.WriteBytes( CustomizedSerialization.NullObjectGuid.ToByteArray() ) 

and private CustomizedSerializationSurrogate(nameObj, encoder, decoder, getNewMs : unit->MemoryStream, getNewMsWithBuf : byte[]*int*int*bool*bool->MemoryStream) = 
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
                use ms = getNewMs()
                x.Encoder( obj, ms :> Stream )
                let bytes = ms.GetBuffer()
                info.AddValue( nameObj, bytes )       
        member x.SetObjectData(obj: obj, info: SerializationInfo, context: StreamingContext, selector: ISurrogateSelector): obj = 
            if Utils.IsNull( x.Decoder ) then 
                getInstanceFields obj |> Array.iter (fun f -> f.SetValue(obj, info.GetValue(f.Name, f.FieldType)))
                obj
            else
                let bytes = info.GetValue(nameObj, typeof<byte[]>) :?> byte[]
                use ms = getNewMsWithBuf( bytes, 0, bytes.Length, false, true )
                let o = x.Decoder( ms :> Stream )
                o

// The serialization surrogate selector that selects CSharpDisplayClassSerializationSurrogate for C# compiler generated display class. 
// It implements ISurrogateSelector
and private CustomizedSerializationSurrogateSelector(getNewMs : unit->MemoryStream, getNewMsWithBuf : byte[]*int*int*bool*bool->MemoryStream) =
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
                if Utils.IsNull( !encoder ) || Utils.IsNull( !decoder ) then 
                    null 
                else
                    Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Install a customized surrogate for type %s" fullname )
                    // JinL: if either encoder or decoder is null, the CustomizedSerializationSurrogate will lead to a StackOverFlow exception, which is 
                    // difficult to debug/trace
                    CustomizedSerializationSurrogate(fullname, !encoder, !decoder, getNewMs, getNewMsWithBuf) :> ISerializationSurrogate
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

///// <summary> 
///// Adds serialization related methods to MemStream, which allows us to break dependency cycles between
///// MemStream and CustomizedSerialization stuff.
///// Usage only looks nice from F#, which is ok since MemStream will be internal.
///// If/when we MemStream goes public, we'll need the to use [<Extension>] methods for C# clients.
///// </summary> 
//type MemStream with 

/// Extensions to StreamBase<byte> to allow for custom serialization
[<Extension>]
type StreamBaseExtension =
    [<Extension>]
    static member internal BinaryFormatterSerializeFromTypeName(x : StreamBase<'T>, obj, fullname) =
        CustomizedSerialization.BinaryFormatterSerializeFromTypeName(x, x.GetNewMs, x.GetNewMsByteBuf, obj, fullname)

    [<Extension>]
    static member internal BinaryFormatterDeserializeToTypeName(x : StreamBase<'T>, fullname) =
        CustomizedSerialization.BinaryFormatterDeserializeToTypeName(x, x.GetNewMs, x.GetNewMsByteBuf, fullname)

    /// Serialize an object to bytestream with BinaryFormatter, support serialization of null. 
    [<Extension>]
    static member internal Serialize( x : StreamBase<'T>, obj )=
        StreamBaseExtension.BinaryFormatterSerializeFromTypeName(x, obj, obj.GetType().FullName)
//        let fmt = Runtime.Serialization.Formatters.Binary.BinaryFormatter()
//        if Utils.IsNull obj then 
//            fmt.Serialize( x, NullObjectForSerialization() )
//        else
//            fmt.Serialize( x, obj )

    /// Deserialize an object from bytestream with BinaryFormatter, support deserialization of null. 
    [<Extension>]
    static member internal Deserialize(x : StreamBase<'T>) =
//        let fmt = Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        let fmt = CustomizedSerialization.GetBinaryFormatter(x.GetNewMs, x.GetNewMsByteBuf)
        let o = fmt.Deserialize( x )
        match o with 
        | :? NullObjectForSerialization -> 
            null
        | _ -> 
            o

    /// <summary> 
    /// Serialize a particular object to bytestream using BinaryFormatter, support serialization of null.  
    /// </summary>
    [<Extension>]
    static member internal FormatterSerializeFromTypeName( x : StreamBase<'T>, obj: 'U, fullname:string, fmt: IFormatter )=
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
    static member internal FormatterDeserializeToTypeName(x : StreamBase<'T>, fullname:string, fmt: IFormatter) =
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
    [<Extension>]
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
                | :? ((StreamBase<byte>)[]) as arr ->
                    x.WriteBytes( CustomizedSerialization.MStreamSerializerGuid.ToByteArray() )
                    x.WriteInt32(arr.Length)
                    for i=0 to arr.Length-1 do
                        x.WriteInt32(int32 arr.[i].Length)
                    for i=0 to arr.Length-1 do
                        x.AppendNoCopy(arr.[i], 0L, arr.[i].Length)
                        (arr.[i] :> IDisposable).Dispose()
                | _ ->
                    x.WriteBytes( GenericSerialization.DefaultFormatterGuid.ToByteArray() )
                    let fmt = 
                        let getNew = fun () -> new MemStream() :> MemoryStream
                        let getNewBuf = fun (a,b,c,d,e) -> new MemStream(a,b,c,d,e) :> MemoryStream
                        GenericSerialization.GetDefaultFormatter( CustomizedSerializationSurrogateSelector(getNew, getNewBuf) )
                    StreamBaseExtension.FormatterSerializeFromTypeName( x, obj, fullname, fmt )

    /// <summary> 
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if installed. 
    /// A Guid is first read from bytestream, if it is a specific reserved NullObjectGuid, return object is null. 
    /// If the GUID is DefaultSerializerGuid, BinaryFormatter is used to deserialize the object. 
    /// For other GUID, installed customized deserailizer is used to deserialize the object. 
    /// </summary>
    /// <param name="fullname"> Should always be null </param>
    [<Extension>]
    static member CustomizableDeserializeToTypeName( x : StreamBase<byte>, fullname:string ) =
        let buf = Array.zeroCreate<_> 16 
        let pos = x.Position
        x.ReadBytes( buf ) |> ignore
        let markerGuid = Guid( buf ) 
        if markerGuid = CustomizedSerialization.NullObjectGuid then 
            null 
        elif markerGuid = CustomizedSerialization.MStreamSerializerGuid then
            let len = x.ReadInt32()
            let arr = Array.zeroCreate<StreamBase<byte>>(len)
            let arrLen = Array.zeroCreate<int32>(arr.Length)
            for i=0 to arr.Length-1 do
                arrLen.[i] <- x.ReadInt32()
            for i=0 to arr.Length-1 do
                arr.[i] <- x.GetNew()
                arr.[i].AppendNoCopy(x, x.Position, int64 arrLen.[i])
                x.Seek(int64 arrLen.[i], SeekOrigin.Current) |> ignore
            box(arr)
        else
            match CustomizedSerialization.DecoderCollectionByGuid.TryGetValue( markerGuid ) with
            | true, decodeFunc -> decodeFunc x
            | _ -> 
                let selector = 
                    let getNew = fun () -> new MemStream() :> MemoryStream
                    let getNewBuf = fun (a,b,c,d,e) -> new MemStream(a,b,c,d,e) :> MemoryStream
                    CustomizedSerializationSurrogateSelector(getNew, getNewBuf)
                match GenericSerialization.FormatterMap.TryGetValue( markerGuid ) with
                | true, fmt -> 
                    StreamBaseExtension.FormatterDeserializeToTypeName( x, fullname, fmt selector ) 
                | _ -> 
                    // This move back is added for compatible reason, old Serializer don't add a Guid, so if we can't figure out the guid in the beginning of
                    // bytestream, we just move back 
                    x.Seek( pos, SeekOrigin.Begin ) |> ignore
                    // Can't parse the guid, using the default serializer
                    StreamBaseExtension.FormatterDeserializeToTypeName( x, fullname, GenericSerialization.GetFormatter(GenericSerialization.BinaryFormatterGuid, selector)) 

    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of typeof<'U>.FullName, even the object passed in is of a derivative type. 
    [<Extension>]
    static member SerializeFromWithTypeName( x : StreamBase<byte>, obj: 'U )=
        StreamBaseExtension.CustomizableSerializeFromTypeName( x, obj, typeof<'U>.FullName )

    /// <summary>
    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of name obj.GetType().FullName. 
    /// </summary> 
    [<Extension>]
    static member SerializeObjectWithTypeName( x : StreamBase<byte>, obj: Object )=
        let objName = if Utils.IsNull obj then null else obj.GetType().FullName
        StreamBaseExtension.CustomizableSerializeFromTypeName( x, obj, objName )

    /// Deserialize a particular object from bytestream, allow use of customizable serializer if one is installed.  
    [<Extension>]
    static member DeserializeObjectWithTypeName( x : StreamBase<byte> ) =
        StreamBaseExtension.CustomizableDeserializeToTypeName( x, null ) 

    /// Serialize a particular object to bytestream, allow use of customizable serializer if one is installed. 
    /// The customized serializer is of typeof<'U>.FullName, even the object passed in is of a derivative type. 
    [<Extension>]
    static member SerializeFrom( x : StreamBase<byte>, obj: 'U )=
        /// x.BinaryFormatterSerializeFromTypeName( obj, typeof<'U>.FullName )
        StreamBaseExtension.CustomizableSerializeFromTypeName( x, obj, typeof<'U>.FullName ) 
 
    /// Deserialize a particular object from bytestream, allow use of customizable serializer if one is installed.  
    [<Extension>]
    static member DeserializeTo<'U>(x : StreamBase<byte>) =
        // x.BinaryFormatterDeserializeToTypeName( typeof<'U>.FullName ) :?> 'U
        let obj = StreamBaseExtension.CustomizableDeserializeToTypeName( x, null ) 
        if Utils.IsNull obj then
            Unchecked.defaultof<_>
        else
            obj :?> 'U
