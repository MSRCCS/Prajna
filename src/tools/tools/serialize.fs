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

	Author: Sanjeev Mehrotra
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools
open System
open System.Runtime.Serialization
open System.Runtime.InteropServices
open System.Reflection
open System.Collections.Generic
open System.IO
open System.Runtime.Serialization.Formatters.Binary

type ReferenceType =
    | Null = 0uy
    | InlineObject = 1uy
    | ObjectPosition = 2uy
    | InlineType = 3uy
    | TypePosition = 4uy

module internal Serialize =

    // generic convert of primitive native types ============
    // can easily add support for other types

    let SupportedConvert<'T>() =
        (typeof<'T> = typeof<System.SByte> ||
         typeof<'T> = typeof<System.Int16> ||
         typeof<'T> = typeof<System.Int32> ||
         typeof<'T> = typeof<System.Int64> ||
         typeof<'T> = typeof<System.Byte> ||
         typeof<'T> = typeof<System.UInt16> ||
         typeof<'T> = typeof<System.UInt32> ||
         typeof<'T> = typeof<System.UInt64> ||
         typeof<'T> = typeof<System.Single> ||
         typeof<'T> = typeof<System.Double> ||
         typeof<'T> = typeof<System.Boolean>
         )

    // byte[] to 'T
    let ConvertTo<'T> (buffer : byte[]) =
        if (typeof<'T> = typeof<System.SByte>) then
            box(sbyte ((int buffer.[0])-128)) :?> 'T
        else if (typeof<'T> = typeof<System.Int16>) then
            box(BitConverter.ToInt16(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Int32>) then
            box(BitConverter.ToInt32(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Int64>) then
            box(BitConverter.ToInt64(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Byte>) then
            box(buffer.[0]) :?> 'T
        else if (typeof<'T> = typeof<System.UInt16>) then
            box(BitConverter.ToUInt16(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt32>) then
            box(BitConverter.ToUInt32(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt64>) then
            box(BitConverter.ToUInt64(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Single>) then
            box(BitConverter.ToSingle(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Double>) then
            box(BitConverter.ToDouble(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Boolean>) then
            box(BitConverter.ToBoolean(buffer, 0)) :?> 'T
        else
            // deserialize
            let ms = new MemoryStream(buffer)
            let fmt = BinaryFormatter()
            fmt.Deserialize(ms) :?> 'T

    // 'T -> byte[]
    let ConvertFrom<'T> (x : 'T) =
        if (typeof<'T> = typeof<System.SByte>) then
            [|byte ((int (unbox<sbyte>(x)))+128)|]
        else if (typeof<'T> = typeof<System.Int16>) then
            BitConverter.GetBytes(unbox<int16>(x))
        else if (typeof<'T> = typeof<System.Int32>) then
            BitConverter.GetBytes(unbox<int32>(x))
        else if (typeof<'T> = typeof<System.Int64>) then
            BitConverter.GetBytes(unbox<int64>(x))
        else if (typeof<'T> = typeof<System.Byte>) then
            [|unbox<byte>(x)|]
        else if (typeof<'T> = typeof<System.UInt16>) then
            BitConverter.GetBytes(unbox<uint16>(x))
        else if (typeof<'T> = typeof<System.UInt32>) then
            BitConverter.GetBytes(unbox<uint32>(x))
        else if (typeof<'T> = typeof<System.UInt64>) then
            BitConverter.GetBytes(unbox<uint64>(x))
        else if (typeof<'T> = typeof<System.Boolean>) then
            BitConverter.GetBytes(unbox<bool>(x))
        else if (typeof<'T> = typeof<System.Single>) then
            BitConverter.GetBytes(unbox<System.Single>(x))
        else if (typeof<'T> = typeof<System.Double>) then
            BitConverter.GetBytes(unbox<System.Double>(x))
        else
            // serialize
            let ms = new MemoryStream()
            let fmt = BinaryFormatter()
            fmt.Serialize(ms, x)
            ms.GetBuffer()

    // ===================================================

    // compiler should hopefully optimize since typeof<'V> resolves at compile-time
    let Deserialize<'V> (ms : MemoryStream) =
        if (SupportedConvert<'V>()) then
            let buf = Array.zeroCreate<byte> sizeof<'V>
            ms.Read(buf, 0, sizeof<'V>) |> ignore
            ConvertTo<'V> buf
        else
            let fmt = BinaryFormatter()
            fmt.Deserialize(ms) :?> 'V

    // could make non-generic (without 'V) by using x.GetType() instead of typeof<'V>
    // but typeof<'V> resolves at compile time and is probably more performant
    let Serialize<'V> (ms : MemoryStream) (x : 'V) =
        if (SupportedConvert<'V>()) then
            ms.Write(ConvertFrom x, 0, sizeof<'V>)
        else
            let fmt = BinaryFormatter()
            fmt.Serialize(ms, x)

    // =======================================================

    // string to 'T
    let ConvertStringTo<'T> (str : string) =
        if (typeof<'T> = typeof<System.String>) then
            box(str) :?> 'T
        else if (typeof<'T> = typeof<System.SByte>) then
            box(Convert.ToSByte(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Int16>) then
            box(Convert.ToInt16(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Int32>) then
            box(Convert.ToInt32(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Int64>) then
            box(Convert.ToInt64(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Byte>) then
            box(Convert.ToByte(str)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt16>) then
            box(Convert.ToUInt16(str)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt32>) then
            box(Convert.ToUInt32(str)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt64>) then
            box(Convert.ToUInt64(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Single>) then
            box(Convert.ToSingle(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Double>) then
            box(Convert.ToDouble(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Boolean>) then
            box(Convert.ToBoolean(str)) :?> 'T
        else
            assert(false)
            Unchecked.defaultof<'T>

    let inline memo (f: 'a -> 'b) =
        let cache = new Dictionary<'a, 'b>()
        fun x -> 
            match cache.TryGetValue(x) with
                | true, y -> y
                | _ ->
                    let y = f x
                    cache.[x] <- y
                    y

    let AllInstance = BindingFlags.Instance ||| BindingFlags.FlattenHierarchy ||| BindingFlags.Public ||| BindingFlags.NonPublic

    let rec isBlittable (t: Type) = 
//        let rec fieldsAreBlittable = memo <| fun (t: Type) -> t.GetFields(AllInstanceFields) |> Seq.forall (fun field -> isBlittable field.FieldType)
        t.IsPrimitive //|| (t.IsValueType && fieldsAreBlittable t)

    let theConverter = FormatterConverter()
    let theContext = StreamingContext(StreamingContextStates.Remoting)

type SerializationCallback = obj -> StreamingContext -> unit

type internal SerTypeInfo(objType: Type) =

    let getSerializationCallback (attributeType: Type) : SerializationCallback option = 
        objType.GetMethods Serialize.AllInstance  
        |> Array.tryFind (fun mi -> CustomAttributeExtensions.IsDefined(mi, attributeType, true))
        |> Option.map (fun mi obj streamContext -> mi.Invoke(obj, [|streamContext|]) |> ignore)

    let onSerializing, onSerialized, onDeserializing, onDeserialized = 
        if objType.IsPrimitive || objType.IsArray then
            None, None, None, None
        else
            getSerializationCallback typeof<OnSerializingAttribute>, 
            getSerializationCallback typeof<OnSerializedAttribute>,
            getSerializationCallback typeof<OnDeserializingAttribute>, 
            getSerializationCallback typeof<OnDeserializedAttribute>

    let serializedFields = lazy (objType.GetFields(Serialize.AllInstance) |> Array.filter (fun fi -> not fi.IsNotSerialized))

    member inline this.OnSerializing(obj, sc) = 
        match onSerializing with
        | Some func -> func obj sc
        | _ -> ()
    
    member inline this.OnSerialized(obj, sc) = 
        match onSerialized with
        | Some func -> func obj sc
        | _ -> ()
    
    member inline this.OnDeserializing(obj, sc) = 
        match onDeserializing with
        | Some func -> func obj sc
        | _ -> ()
    
    member inline this.OnDeserialized(obj, sc) = 
        match onSerializing with
        | Some func -> func obj sc
        | _ -> ()

    member val SerializedPosition = -1 with get, set
    
    member inline this.Type = objType

    member inline this.IsPrimitive = objType.IsPrimitive

    member inline this.IsValueType = objType.IsValueType

    member inline this.IsSerializable = objType.IsSerializable

    member val HaveSerializationCallbacks = 
        Option.isSome onSerializing || Option.isSome onSerialized || Option.isSome onDeserializing || Option.isSome onDeserialized

    member inline this.SerializedFields : FieldInfo[] = serializedFields.Value

//    member val FieldTypes : SerTypeInfo[]


type internal TypeSerializer() =

    let mutable serializedCount = 0
    
    member val GetSerTypeInfo = Serialize.memo SerTypeInfo

    member this.Serialize (objType: SerTypeInfo, stream: BinaryWriter) = 
        match objType.SerializedPosition with
        | -1 ->
            stream.Write(byte ReferenceType.InlineType)
            stream.Write objType.Type.AssemblyQualifiedName
            objType.SerializedPosition <- serializedCount
            serializedCount <- serializedCount + 1
        | position ->
            stream.Write(byte ReferenceType.TypePosition)
            stream.Write objType.SerializedPosition

type internal Serializer(stream: BinaryWriter, marked: Dictionary<obj, int>, typeSerializer: TypeSerializer) as self = 

    let writePrimitive (obj: obj) = 
        // List of primitive types obtained from FieldInfo.IsPrimitive documentation
        match Type.GetTypeCode(obj.GetType()) with
        | TypeCode.Boolean -> stream.Write (obj :?> Boolean)
        | TypeCode.Byte -> stream.Write (obj :?> Byte)
        | TypeCode.SByte -> stream.Write (obj :?> SByte)
        | TypeCode.Int16 -> stream.Write (obj :?> Int16)
        | TypeCode.UInt16 -> stream.Write (obj :?> UInt16)
        | TypeCode.Int32 -> stream.Write (obj :?> Int32)
        | TypeCode.UInt32 -> stream.Write (obj :?> UInt32)
        | TypeCode.Int64 -> stream.Write (obj :?> Int64)
        | TypeCode.UInt64 -> stream.Write (obj :?> UInt64)
        | TypeCode.Char -> stream.Write (obj :?> Char)
        | TypeCode.Double -> stream.Write (obj :?> Double)
        | TypeCode.Single -> stream.Write (obj :?> Single)
        | _ -> failwith <| sprintf "Unknown primitive type %A" (obj.GetType())

    let writeMemoryBlittableArray (elType: Type, arrObj: Array, memStream: MemoryStream) =
        let sizeInBytes = arrObj.Length * Marshal.SizeOf(elType)
        let curPos = int memStream.Position
        while memStream.Capacity < curPos + sizeInBytes do
            memStream.Capacity <- memStream.Capacity * 2
        let buffer = memStream.GetBuffer()
        Buffer.BlockCopy(arrObj, 0, buffer, curPos, sizeInBytes)
        memStream.Position <- int64 (curPos + sizeInBytes)

    let writePrimitiveArray : Type * Array -> unit = 
        let writePrimitiveArrayOneByOne(elType: Type, arrObj: Array) = 
            match Type.GetTypeCode(elType) with
            | TypeCode.Boolean -> let arr = arrObj :?> Boolean[] in arr |> Array.iter stream.Write
            | TypeCode.Byte -> let arr = arrObj :?> Byte[] in arr |> Array.iter stream.Write
            | TypeCode.SByte -> let arr = arrObj :?> SByte[] in arr |> Array.iter stream.Write
            | TypeCode.Int16 -> let arr = arrObj :?> Int16[] in arr |> Array.iter stream.Write
            | TypeCode.UInt16 -> let arr = arrObj :?> UInt16[] in arr |> Array.iter stream.Write
            | TypeCode.Int32 -> let arr = arrObj :?> Int32[] in arr |> Array.iter stream.Write
            | TypeCode.UInt32 -> let arr = arrObj :?> UInt32[] in arr |> Array.iter stream.Write
            | TypeCode.Int64 -> let arr = arrObj :?> Int64[] in arr |> Array.iter stream.Write
            | TypeCode.UInt64 -> let arr = arrObj :?> UInt64[] in arr |> Array.iter stream.Write
            | TypeCode.Char -> let arr = arrObj :?> Char[] in arr |> Array.iter stream.Write
            | TypeCode.Double -> let arr = arrObj :?> Double[] in arr |> Array.iter stream.Write
            | TypeCode.Single -> let arr = arrObj :?> Single[] in arr |> Array.iter stream.Write
            | TypeCode.Decimal | TypeCode.DateTime | TypeCode.DBNull | TypeCode.String | TypeCode.Object | TypeCode.Empty | _ -> 
                failwith <| sprintf "Unknown primitive type %A" elType
        match stream.BaseStream with
        | :? MemoryStream as memStream -> 
            fun (elType: Type, arrObj: Array) -> writeMemoryBlittableArray (elType, arrObj, memStream)
        | _ -> writePrimitiveArrayOneByOne

    let writeValueArray : Type * Array -> unit = 
        let writeOtherValueTypeArray(elType: Type, arrObj: Array) = 
            let len = arrObj.Length
            let elTypeInfo = typeSerializer.GetSerTypeInfo elType
            for i = 0 to len - 1 do
                self.WriteContents(elTypeInfo, arrObj.GetValue(i))
        match stream.BaseStream with
        | :? MemoryStream as memStream ->
            fun (elType: Type, arrObj: Array) ->
                if Serialize.isBlittable elType then
                    writeMemoryBlittableArray(elType, arrObj, memStream)
                else
                    writeOtherValueTypeArray(elType, arrObj)
        | _ -> writeOtherValueTypeArray

    let writeObjectArray(arrObj: Array) = 
        let len = arrObj.Length
        for i = 0 to len - 1 do
            self.WriteObject (arrObj.GetValue(i))

    member private this.WriteArray (arrayType: Type, arrObj: Array) =
        // TODO: Support higher rank and non-zero-based arrays
        let len = arrObj.Length
        stream.Write len
        let elType = arrayType.GetElementType()
        if elType.IsPrimitive then
            writePrimitiveArray(elType, arrObj)
        elif elType.IsValueType then 
            writeValueArray(elType, arrObj)
        else 
            writeObjectArray(arrObj)

    member private this.WriteContents (objType: SerTypeInfo, obj: obj) = 
        for field in objType.SerializedFields (*objType.GetFields(Serialize.AllInstance)*) do 
            let fieldType = field.FieldType
            let fieldValue = field.GetValue(obj)
            if fieldType.IsPrimitive then
                writePrimitive fieldValue
            elif fieldType.IsValueType then
                this.WriteContents(typeSerializer.GetSerTypeInfo fieldType, fieldValue)
            else
                this.WriteObject fieldValue

    member this.WriteObject (obj: obj) =
        if obj = null then
            stream.Write(byte ReferenceType.Null)
        else
            let objType = obj.GetType()
            if objType.IsSerializable then
                match marked.TryGetValue(obj) with
                | true, position -> 
                    stream.Write(byte ReferenceType.ObjectPosition)
                    stream.Write position
                | _ ->
                    stream.Write(byte ReferenceType.InlineObject)
                    let serTypeInfo = typeSerializer.GetSerTypeInfo objType
                    typeSerializer.Serialize(serTypeInfo, stream)
                    marked.Add(obj, marked.Count)
                    match obj with
                    | :? Type as typeObj -> stream.Write typeObj.AssemblyQualifiedName
                    | :? Array as arrObj -> this.WriteArray(objType, arrObj)
                    | :? string as strObj ->  stream.Write strObj  
                    | _ -> this.WriteContents(serTypeInfo, obj)

type Deserializer(reader: BinaryReader, marked: List<obj>, types: List<Type>) as self =

    let readPrimitive (objType: Type) : obj = 
        match Type.GetTypeCode(objType) with
        | TypeCode.Boolean -> upcast reader.ReadBoolean()
        | TypeCode.Byte -> upcast reader.ReadByte()
        | TypeCode.SByte -> upcast reader.ReadSByte()
        | TypeCode.Int16 -> upcast reader.ReadInt16()
        | TypeCode.UInt16 -> upcast reader.ReadUInt16()
        | TypeCode.Int32 -> upcast reader.ReadInt32()
        | TypeCode.UInt32 -> upcast reader.ReadUInt32()
        | TypeCode.Int64 -> upcast reader.ReadInt64()
        | TypeCode.UInt64 -> upcast reader.ReadUInt64()
        | TypeCode.Char -> upcast reader.ReadChar()
        | TypeCode.Double -> upcast reader.ReadDouble()
        | TypeCode.Single -> upcast reader.ReadSingle()
        | _ -> failwith <| sprintf "Unknown primitive type %A" objType

    let readMemoryBlittableArray (elType: Type, arrObj: Array, memStream: MemoryStream) =
        let buffer = memStream.GetBuffer()
        let sizeInBytes = arrObj.Length * Marshal.SizeOf(elType)
        Buffer.BlockCopy(buffer, int memStream.Position, arrObj, 0, sizeInBytes)
        memStream.Position <- memStream.Position + int64 sizeInBytes

    let readPrimitiveArray : Type -> Array -> unit = 
        let inline readPrimitiveArrayOneByOne (elType: Type) (arrObj: Array) = 
            match Type.GetTypeCode(elType) with
            | TypeCode.Boolean -> let arr = arrObj :?> Boolean[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadBoolean())
            | TypeCode.Byte -> let arr = arrObj :?> Byte[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadByte())
            | TypeCode.SByte -> let arr = arrObj :?> SByte[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadSByte())
            | TypeCode.Int16 -> let arr = arrObj :?> Int16[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadInt16())
            | TypeCode.UInt16 -> let arr = arrObj :?> UInt16[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadUInt16())
            | TypeCode.Int32 -> let arr = arrObj :?> Int32[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadInt32())
            | TypeCode.UInt32 -> let arr = arrObj :?> UInt32[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadUInt32())
            | TypeCode.Int64 -> let arr = arrObj :?> Int64[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadInt64())
            | TypeCode.UInt64 -> let arr = arrObj :?> UInt64[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadUInt64())
            | TypeCode.Char -> let arr = arrObj :?> Char[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadChar())
            | TypeCode.Double -> let arr = arrObj :?> Double[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadDouble())
            | TypeCode.Single -> let arr = arrObj :?> Single[] in arr |> Array.iteri (fun i _ -> arr.[i] <- reader.ReadSingle())
            | TypeCode.Decimal | TypeCode.DateTime | TypeCode.DBNull | TypeCode.String | TypeCode.Object | TypeCode.Empty | _ -> 
                failwith <| sprintf "Unknown primitive type %A" elType
        match reader.BaseStream with
        | :? MemoryStream as memStream -> fun (elType: Type) (arrObj: Array) -> readMemoryBlittableArray (elType, arrObj, memStream)
        | _ -> readPrimitiveArrayOneByOne

    let readValueArray : Type -> Array -> unit = 
        let inline readOtherValueTypeArray (elType: Type) (arrObj: Array) = 
            let mutable value = FormatterServices.GetUninitializedObject(elType)
            for i = 0 to arrObj.Length - 1 do
                do self.ReadContents(elType, &value)
                arrObj.SetValue(value, i)
        match reader.BaseStream with
        | :? MemoryStream as memStream ->
            fun (elType: Type) (arrObj: Array) ->
                if Serialize.isBlittable elType then
                    readMemoryBlittableArray(elType, arrObj, memStream)
                else
                    readOtherValueTypeArray elType arrObj
        | _ -> readOtherValueTypeArray

    let readObjectArray(arrObj: Array) =
        for i = 0 to arrObj.Length - 1 do
            let obj = self.ReadObject(reader, marked)
            arrObj.SetValue(obj, i)

    let DeserConstructorArgTypes = [| typeof<SerializationInfo>; typeof<StreamingContext> |]

    member private this.ReadArray (elType: Type, arrObj: Array) =
        if elType.IsPrimitive then
            readPrimitiveArray elType arrObj
        elif elType.IsValueType then 
            readValueArray elType arrObj
        else 
            readObjectArray arrObj

    member private this.ReadContents (objType: Type, obj: byref<obj>) : unit = 

        for field in objType.GetFields(Serialize.AllInstance) do 
            if not field.IsNotSerialized then
                let fieldType = field.FieldType
                if fieldType.IsPrimitive then
                    let value = readPrimitive fieldType
                    field.SetValue(obj, value)
                elif fieldType.IsValueType then
                    let mutable newValue = FormatterServices.GetUninitializedObject(fieldType) 
                    do this.ReadContents(fieldType, &newValue)
                    field.SetValue(obj, newValue)
                else
                    let newValue = this.ReadObject(reader, marked)
                    field.SetValue(obj, newValue)

//    member private this.ReadCustomSerializedObject(objType: Type) : obj =
//        let numFields = reader.ReadInt32()
//        let deserInfo = new SerializationInfo(objType, Serialize.theConverter)
//        for _ in 1..numFields do
//            let name = this.ReadObject() :?> string
////            let value = this
//            
//        
//        
//        let deserConstructor = objType.GetConstructor(DeserConstructorArgTypes)
//        deserConstructor.Invoke( [|   ;Serialize.theContext |]  )
//        let mutable newObj = FormatterServices.GetUninitializedObject(objType) 
//        marked.Add newObj
//        do this.ReadContents(objType, &newObj)
//        newObj
            

    member private this.ReadObject (reader: BinaryReader, marked: List<obj>) : obj =
        let tag = LanguagePrimitives.EnumOfValue<byte, ReferenceType>(reader.ReadByte()) 
        match tag with
        | ReferenceType.Null -> null
        | ReferenceType.ObjectPosition -> marked.[reader.ReadInt32()]
        | ReferenceType.InlineObject ->
            let typeTag = LanguagePrimitives.EnumOfValue<byte, ReferenceType>(reader.ReadByte()) 
            let objType =
                match typeTag with
                | ReferenceType.InlineType -> 
                    let newTypeName = reader.ReadString()
                    let newType = Type.GetType(newTypeName)
                    if newType = null then
                        failwith <| sprintf "Could not load type %A." newTypeName
                    types.Add newType
                    newType
                | ReferenceType.TypePosition -> 
                    let typePos = reader.ReadInt32()
                    types.[typePos]
                | _ -> failwith <| sprintf "Unexpected tag: %A" tag
            match objType with
            | strType when strType = typeof<string> -> 
                let str = reader.ReadString()
                marked.Add str
                upcast str
            | arrType when arrType.IsArray ->
                let size = reader.ReadInt32()
                let elType = arrType.GetElementType()
                let newArr = Array.CreateInstance(elType, size)
                marked.Add newArr
                do this.ReadArray(elType, newArr)
                upcast newArr
//            | serType when typeof<ISerializable>.IsAssignableFrom(serType) ->
//                this.ReadCustomSerializedObject(objType)
            | typeType when typeof<Type>.IsAssignableFrom(typeType) -> 
                let typeName = reader.ReadString()
                let ``type`` = Type.GetType(typeName)
                marked.Add ``type``
                upcast ``type``
            | _ -> 
                let mutable newObj = FormatterServices.GetUninitializedObject(objType) 
                marked.Add newObj
                do this.ReadContents(objType, &newObj)
                newObj
        | _ -> failwith <| sprintf "Unexpected tag: %A" tag

    member this.ReadObject() = this.ReadObject(reader, marked)

type internal ReferenceComparer() =
    interface IEqualityComparer<obj> with
        member __.Equals(x: obj, y: obj): bool = 
            Object.ReferenceEquals(x, y)
        member x.GetHashCode(obj: obj): int = 
            System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj)

type BinarySerializer() =

    member this.Serialize(stream: Stream, graph: obj): unit = 
        let writer = new BinaryWriter(stream, Text.UTF8Encoding.UTF8)
        let marked = new Dictionary<obj, int>(ReferenceComparer())
        let ser = new Serializer(writer, marked, new TypeSerializer())
        do ser.WriteObject(graph)
    
    member this.Deserialize (stream: Stream) : obj = 
        let reader = new BinaryReader(stream, Text.UTF8Encoding.UTF8)
        let marked = new List<obj>()
        let types = new List<Type>()
        let deser = new Deserializer(reader, marked, types)
        let ret = deser.ReadObject()
        ret
    
//        member this.Deserialize(stream: Stream): obj = 
//            let reader = new BinaryReader(stream)
//            let marked = new List<obj>()
//            let ret = readObject reader marked
//            do marked.Clear()
//            ret


