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
    /// Byte[] serializer
    static member val internal ArrSerializerGuid = Guid("0C57DE71-CAB7-46DE-9A51-133E003862D1") with get
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
                if Utils.IsNull( !encoder ) && Utils.IsNull( !decoder ) then 
                    null 
                else
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

