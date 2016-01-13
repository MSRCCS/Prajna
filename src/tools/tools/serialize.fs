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
open System.Reflection
open System.Runtime.InteropServices
open System.Runtime.Serialization
open System.Collections.Generic
open System.IO
open System.Runtime.Serialization.Formatters.Binary

// Describes the type of reference of an upcomming object or type
type internal ReferenceType =
    // The null object
    | Null = 0uy 
    // An inline object. This is used the first time an object appears in the graph,
    // in depth-first traversal order. If it's a reference type, this is followed by the
    // the object's actual type. Value types implicitly are of same type as the field. 
    // In either case, this is followed by the object's (or struct's) fields.
    | InlineObject = 1uy
    // An object that's already been serialized, represented by its index in the depth-first traversal
    | ObjectPosition = 2uy
    // Similar to InlineObject, used the first time an object of a given runtime type appears.
    // The type is represented by it's full AssemblyQualifiedName.
    // This is used by the deserializer only. Objects of type System.Type are not special, and will
    // be an InlineObject the first type they appear, as usual.
    | InlineType = 3uy
    // Similar to ObjectPosition, but reference to a type that appeared previously.
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
            use ms = new MemoryStream(buffer)
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
            use ms = new MemoryStream()
            let fmt = BinaryFormatter()
            fmt.Serialize(ms, x)
            ms.GetBuffer()

    // ===================================================

    // compiler should hopefully optimize since typeof<'V> resolves at compile-time
    let Deserialize<'V> (s : Stream) =
        if (SupportedConvert<'V>()) then
            let buf = Array.zeroCreate<byte> sizeof<'V>
            s.Read(buf, 0, sizeof<'V>) |> ignore
            ConvertTo<'V> buf
        else
            let fmt = BinaryFormatter()
            fmt.Deserialize(s) :?> 'V

    // could make non-generic (without 'V) by using x.GetType() instead of typeof<'V>
    // but typeof<'V> resolves at compile time and is probably more performant
    let Serialize<'V> (s : Stream) (x : 'V) =
        if (SupportedConvert<'V>()) then
            s.Write(ConvertFrom x, 0, sizeof<'V>)
        else
            let fmt = BinaryFormatter()
            fmt.Serialize(s, x)

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

    let memo (f: 'a -> 'b) =
        let cache = Dictionary<'a, 'b>()
        fun x -> 
            match cache.TryGetValue(x) with
                | true, y -> y
                | _ ->
                    let y = f x
                    cache.[x] <- y
                    y

    let AllInstance = BindingFlags.Instance ||| BindingFlags.FlattenHierarchy ||| BindingFlags.Public ||| BindingFlags.NonPublic

    let CSharpFuncFields = BindingFlags.Instance ||| BindingFlags.Public ||| BindingFlags.NonPublic

    let rec isBlittable (t: Type) = 
//        let rec fieldsAreBlittable = memo <| fun (t: Type) -> t.GetFields(AllInstanceFields) |> Seq.forall (fun field -> isBlittable field.FieldType)
        t.IsPrimitive //|| (t.IsValueType && fieldsAreBlittable t)

    let theConverter = FormatterConverter()
    let theContext = StreamingContext(StreamingContextStates.Remoting)

    // Increments a vector of indices to access the next element of a 
    // possibly multidimensional array with possibly non-zero lower bounds
    let incrementIndex (lowerBounds: int[]) (lengths: int[]) : int[] -> bool =
        fun (curIndices: int[]) ->
            let rec go curDim =
                if curDim < 0 then
                    true
                else
                    let nextVal = curIndices.[curDim] + 1
                    let lo = lowerBounds.[curDim]
                    if nextVal < lo + lengths.[curDim] then
                        curIndices.[curDim] <- nextVal
                        false
                    else
                        curIndices.[curDim] <- lo
                        go (curDim - 1)
            let rank = lowerBounds.Length
            go (rank - 1)


type internal SerializationCallback = obj -> StreamingContext -> unit

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

    let serializedFields = 
        lazy 
            let bindingFlags = 
                if objType.IsDefined(typeof<System.Runtime.CompilerServices.CompilerGeneratedAttribute>, false) && objType.Name.Contains("<>c__DisplayClass") then
                    Serialize.CSharpFuncFields
                else
                    Serialize.AllInstance
            objType.GetFields(bindingFlags) |> Array.filter (fun fi -> not fi.IsNotSerialized)

    member this.OnSerializing(obj, sc) = 
        match onSerializing with
        | Some func -> func obj sc
        | _ -> ()
    
    member this.OnSerialized(obj, sc) = 
        match onSerialized with
        | Some func -> func obj sc
        | _ -> ()
    
    member this.OnDeserializing(obj, sc) = 
        match onDeserializing with
        | Some func -> func obj sc
        | _ -> ()
    
    member this.OnDeserialized(obj, sc) = 
        match onSerializing with
        | Some func -> func obj sc
        | _ -> ()

    member val SerializedPosition = -1 with get, set
    
    member inline this.Type = objType

    member inline this.IsPrimitive = objType.IsPrimitive

    member inline this.IsValueType = objType.IsValueType

    member val HaveSerializationCallbacks = 
        Option.isSome onSerializing || Option.isSome onSerialized 

    member val HaveDeserializationCallbacks = 
        Option.isSome onDeserializing || Option.isSome onDeserialized

    member val NoArgConstructor = 
        let mi = objType.GetConstructor(Type.EmptyTypes)
        if mi <> null then
            (fun (obj: obj) -> mi.Invoke(obj, null) |> ignore)
        else
            (fun obj -> ())

    static member private SizeOf<'T>() = sizeof<'T>

    // We use two levels of caching: 
    // The reflection call below is made at most once per type per AppDomain.
    // The hashtable lookup (implied by Serialize.memo) is done at most once per call to Serialize.
    // Within a single call to Serialize, ElementSize caches the element sizes with a simple bool check.
    // A hashtable lookup per object is still used to obtain the SerTypeInfo, and the ElementSize
    // below avoid a second one (except for the first time a type is used as an array element in 
    // the current call to Serialize)
    static member val private GetSize : Type -> int = 
        Serialize.memo (fun t -> 
            typeof<SerTypeInfo>
                .GetMethod("SizeOf", BindingFlags.Static ||| BindingFlags.NonPublic)
                .GetGenericMethodDefinition()
                .MakeGenericMethod(t)
                .Invoke(null, null) :?> int)

    member val private ElementSize = lazy SerTypeInfo.GetSize(objType.GetElementType())

    member this.GetElementSize() = this.ElementSize.Value

    member inline this.SerializedFields : FieldInfo[] = serializedFields.Value


type internal TypeSerializer() =

    let mutable serializedCount = 0
    
    let deserializedTypes = List<SerTypeInfo>()

    member val GetSerTypeInfo = Serialize.memo (fun t -> SerTypeInfo t)

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

    member this.Deserialize (reader: BinaryReader) : SerTypeInfo =
        let typeTag = LanguagePrimitives.EnumOfValue<byte, ReferenceType>(reader.ReadByte()) 
        match typeTag with
        | ReferenceType.InlineType -> 
            let newTypeName = reader.ReadString()
            let newType = Type.GetType(newTypeName)
            if newType = null then
                failwith <| sprintf "Could not load type %A." newTypeName
            let newSerType = this.GetSerTypeInfo newType
            deserializedTypes.Add newSerType
            newSerType
        | ReferenceType.TypePosition -> 
            let typePos = reader.ReadInt32()
            deserializedTypes.[typePos]
        | _ -> failwith <| sprintf "Unexpected tag: %A" typeTag


type internal Serializer(stream: BinaryWriter, marked: Dictionary<obj, int>, typeSerializer: TypeSerializer, surrogateSelector: ISurrogateSelector) as self = 

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

    let writeMemoryBlittableArray (arrObj: Array, elemSize: int, memStream: MemoryStream) =
        match memStream with
            | :? BufferListStream<byte> as ms ->
                ms.WriteArrT(arrObj, 0, arrObj.Length, elemSize)
            | _ as memStream ->
                let sizeInBytes = arrObj.Length * elemSize
                let curPos = int memStream.Position
                let newLen = int64 (curPos + sizeInBytes)
                memStream.SetLength newLen
                let buffer = memStream.GetBuffer()
                Buffer.BlockCopy(arrObj, 0, buffer, curPos, sizeInBytes)
                memStream.Position <- newLen

    let writePrimitiveArray : SerTypeInfo * int[] * int[] * Array -> unit = 
        let writePrimitiveArrayOneByOne(arrayTypeInfo: SerTypeInfo, lowerBounds: int[], lengths: int[], arrObj: Array) = 
            match Type.GetTypeCode(arrayTypeInfo.Type.GetElementType()) with
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
                failwith <| sprintf "Unknown primitive type %A" (arrayTypeInfo.Type.GetElementType())
        match stream.BaseStream with
        | :? MemoryStream as memStream -> 
            fun (arrayTypeInfo: SerTypeInfo, lowerBounds: int[], lengths: int[], arrObj: Array) -> 
                writeMemoryBlittableArray (arrObj, arrayTypeInfo.GetElementSize(), memStream)
        | _ -> writePrimitiveArrayOneByOne

    let writeValueArray : Type * int[] * int[] * Array -> unit = 
        let writeOtherValueTypeArray(elType: Type, lowerBounds: int[], lengths: int[], arrObj: Array) = 
            let inc = Serialize.incrementIndex lowerBounds lengths
            let indices = Array.copy lowerBounds
            let elTypeInfo = typeSerializer.GetSerTypeInfo elType
            if lengths |> Array.forall (fun dimLen -> dimLen > 0) then
                self.WriteContents(elTypeInfo, arrObj.GetValue(indices))
                while not <| inc indices do
                    self.WriteContents(elTypeInfo, arrObj.GetValue(indices))
        match stream.BaseStream with
//        | :? MemoryStream as memStream ->
//            fun (elType: Type, arrObj: Array) ->
//                if Serialize.isBlittable elType then
//                    writeMemoryBlittableArray(elType, arrObj, memStream)
//                else
//                    writeOtherValueTypeArray(elType, arrObj)
        | _ -> writeOtherValueTypeArray

    let writeObjectArray(arrObj: Array, lowerBounds: int[], lengths: int[]) = 
        if lowerBounds.Length = 1 && lowerBounds.[0] = 0 then
            (arrObj :?> obj[]) |> Array.iter self.WriteObject
        else
            let inc = Serialize.incrementIndex lowerBounds lengths
            let indices = Array.copy lowerBounds
            if lengths |> Array.forall (fun dimLen -> dimLen > 0) then
                self.WriteObject(arrObj.GetValue(indices))
                while not <| inc indices do
                    self.WriteObject (arrObj.GetValue(indices))

    let intArrayTypeInfo = typeSerializer.GetSerTypeInfo(typeof<int[]>)
    let zeroLowerBound : int[] = Array.zeroCreate 1

    member private this.WriteArray (arrayTypeInfo: SerTypeInfo, arrObj: Array) =
        let rank = arrObj.Rank
        stream.Write rank
        let elType = arrayTypeInfo.Type.GetElementType()
        if rank = 1 && arrObj.GetLowerBound(0) = 0 then
            stream.Write 0
            stream.Write arrObj.Length
            if elType.IsPrimitive then
                writePrimitiveArray(arrayTypeInfo, null, null, arrObj)
            else
                let lengths : int[] = Array.zeroCreate 1 
                lengths.[0] <- arrObj.Length
                if elType.IsValueType then 
                    writeValueArray(elType, zeroLowerBound, lengths, arrObj)
                else 
                    writeObjectArray(arrObj, zeroLowerBound, lengths)
        else
            let lowerBounds : int[] = Array.init arrObj.Rank arrObj.GetLowerBound
            let lengths : int[] = Array.init arrObj.Rank arrObj.GetLength
            writePrimitiveArray(intArrayTypeInfo, null, null, lowerBounds)
            writePrimitiveArray(intArrayTypeInfo, null, null, lengths)
            
            if elType.IsPrimitive then
                writePrimitiveArray(arrayTypeInfo, lowerBounds, lengths, arrObj)
            elif elType.IsValueType then 
                writeValueArray(elType, lowerBounds, lengths, arrObj)
            else 
                writeObjectArray(arrObj, lowerBounds, lengths)

    member private this.WriteContents (objType: SerTypeInfo, obj: obj) = 
        for field in objType.SerializedFields do 
            let fieldType = field.FieldType
            let fieldValue = field.GetValue(obj)
            if fieldType.IsPrimitive then
                writePrimitive fieldValue
            elif fieldType.IsValueType then
                this.WriteContents(typeSerializer.GetSerTypeInfo fieldType, fieldValue)
            else
                this.WriteObject fieldValue

    member private this.WriteMembers(serInfo: SerializationInfo) = 
        stream.Write serInfo.MemberCount
        for entry in serInfo do
            this.WriteObject(entry.Name)
            this.WriteObject(entry.Value)

    member private this.WriteCustomSerializedObject(objType: Type, obj: ISerializable) : unit =
        let serInfo = SerializationInfo(objType, Serialize.theConverter)
        obj.GetObjectData(serInfo, Serialize.theContext)
        this.WriteObject serInfo.ObjectType
        marked.Add(obj, marked.Count)
        this.WriteMembers serInfo

    member this.WriteSurrogateSerializedObject(surrogate: ISerializationSurrogate, objType: Type, obj: obj) =
        let serInfo = SerializationInfo(objType, Serialize.theConverter)
        surrogate.GetObjectData(obj, serInfo, Serialize.theContext)
        this.WriteMembers serInfo

    member val private stringTypeInfo = typeSerializer.GetSerTypeInfo typeof<string> with get

    member this.WriteObject (obj: obj) =
        if obj = null then
            stream.Write(byte ReferenceType.Null)
        else
            match marked.TryGetValue(obj) with
            | true, position -> 
                stream.Write(byte ReferenceType.ObjectPosition)
                stream.Write position
            | _ ->
                let objType = obj.GetType()
                let surrogate = 
                    if surrogateSelector = null then null 
                    else surrogateSelector.GetSurrogate(objType, Serialize.theContext, ref (Unchecked.defaultof<ISurrogateSelector>))
                stream.Write(byte ReferenceType.InlineObject)
                let serTypeInfo = typeSerializer.GetSerTypeInfo objType
                typeSerializer.Serialize(serTypeInfo, stream)
                if surrogate <> null then
                    marked.Add(obj, marked.Count)
                    this.WriteSurrogateSerializedObject(surrogate, serTypeInfo.Type, obj)
                else
                    match obj with
                    | :? Type as typeObj -> 
                        marked.Add(obj, marked.Count)
                        stream.Write typeObj.AssemblyQualifiedName
                    | :? Array as arrObj -> 
                        marked.Add(obj, marked.Count)
                        this.WriteArray(serTypeInfo, arrObj)
                    | :? string as strObj ->  
                        marked.Add(obj, marked.Count)
                        stream.Write strObj  
                    | :? ISerializable as customSerObj -> 
                        // omitting marked.Add because custom serialization requires writing 
                        // the SerInfo type before the actual object, so WriteCustomSerializedObject will do this
                        this.WriteCustomSerializedObject(objType, customSerObj)
                    | _ -> 
                        marked.Add(obj, marked.Count)
                        if serTypeInfo.HaveSerializationCallbacks then
                            serTypeInfo.OnSerializing(obj, Serialize.theContext)
                            this.WriteContents(serTypeInfo, obj)
                            serTypeInfo.OnSerialized(obj, Serialize.theContext)
                        else
                            this.WriteContents(serTypeInfo, obj)

type internal Deserializer(reader: BinaryReader, marked: List<obj>, typeSerializer: TypeSerializer, surrogateSelector: ISurrogateSelector) as self =

    let onDeserializationList = List<IDeserializationCallback>()
    let delayedSetList = List<IObjectReference * (obj -> unit)>()

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

    let readMemoryBlittableArray (arrObj: Array, elemSize: int, memStream: MemoryStream) =
        match memStream with
            | :? BufferListStream<byte> as ms ->
                ms.ReadArrT(arrObj, 0, arrObj.Length, elemSize) |> ignore
            | _ as memStream ->
                let buffer = memStream.GetBuffer()
                let sizeInBytes = arrObj.Length * elemSize
                Buffer.BlockCopy(buffer, int memStream.Position, arrObj, 0, sizeInBytes)
                memStream.Position <- memStream.Position + int64 sizeInBytes

    let readPrimitiveArray : SerTypeInfo -> Array -> unit = 
        let readPrimitiveArrayOneByOne (arrayTypeInfo: SerTypeInfo) (arrObj: Array) = 
            match Type.GetTypeCode(arrayTypeInfo.Type.GetElementType()) with
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
                failwith <| sprintf "Unknown primitive type %A" (arrayTypeInfo.Type.GetElementType())
        match reader.BaseStream with
        | :? MemoryStream as memStream -> 
            fun (arrayTypeInfo: SerTypeInfo) (arrObj: Array) -> 
                readMemoryBlittableArray (arrObj, arrayTypeInfo.GetElementSize(), memStream)
        | _ -> readPrimitiveArrayOneByOne

    let readValueArray : SerTypeInfo -> int[] -> int[] -> Array -> unit = 
        let readOtherValueTypeArray (elType: SerTypeInfo) (lowerBounds: int[]) (lengths: int[]) (arrObj: Array) = 
            let mutable value = FormatterServices.GetUninitializedObject(elType.Type)
            let inc = Serialize.incrementIndex lowerBounds lengths
            let indices = Array.copy lowerBounds
            if lengths |> Array.forall (fun dimLen -> dimLen > 0) then
                self.ReadContents(elType, &value)
                arrObj.SetValue(value, indices)
                while not <| inc indices do
                    do self.ReadContents(elType, &value)
                    arrObj.SetValue(value, indices)
        match reader.BaseStream with
//        | :? MemoryStream as memStream ->
//            fun (elType: SerTypeInfo) (arrObj: Array) ->
//                if Serialize.isBlittable elType.Type then
//                    readMemoryBlittableArray(elType.Type, arrObj, memStream)
//                else
//                    readOtherValueTypeArray elType arrObj
        | _ -> readOtherValueTypeArray

    let readObjectArray (lowerBounds: int[]) (lengths: int[]) (arrObj: Array) =
        let inc = Serialize.incrementIndex lowerBounds lengths
        let indices = Array.copy lowerBounds
        if lengths |> Array.forall (fun dimLen -> dimLen > 0) then
            let setValue() =
                // We might be called just once, with the actual value, which will happen most often
                // or twice, once with null and once with the actual value, which will happen rarely.
                // We never get called with null twice because the the second call is always 
                // with the result of an IObjectReference.GetRealObject(), and it doesn't make
                // sense to return an IObjectReference to null.
                // If we are called with null, we need to copy the indices array, since it's value
                // will continue changing for other elements.
                // If we are called with a non-null and we were never called with null before
                // (we know it from indicesCopy.IsValueCreated), then we just use the original indices.
                // If we are called with a non-null and we were called before, then we use the copy.
                let indicesCopy = lazy Array.copy indices
                fun obj ->
                    match obj, indicesCopy.IsValueCreated with
                    | null, false -> indicesCopy.Force() |> ignore
                    | null, true -> failwith "We are only supposed to be called with null once." // arrObj.SetValue(obj, indicesCopy.Value)
                    | obj, false -> arrObj.SetValue(obj, indices)
                    | obj, true -> arrObj.SetValue(obj, indicesCopy.Value) 
            self.ReadObject(reader, marked, setValue())
            while not <| inc indices do
                self.ReadObject(reader, marked, setValue())

    let DeserConstructorArgTypes = [| typeof<SerializationInfo>; typeof<StreamingContext> |]

    let intArrayTypeInfo = typeSerializer.GetSerTypeInfo(typeof<int[]>)

    member private this.ReadArray (arrayTypeInfo: SerTypeInfo, lowerBounds: int[], lengths: int[], arrObj: Array) =
        let elType = arrayTypeInfo.Type.GetElementType()
        if elType.IsPrimitive then
            readPrimitiveArray arrayTypeInfo arrObj
        elif elType.IsValueType then 
            readValueArray (typeSerializer.GetSerTypeInfo elType) lowerBounds lengths arrObj
        else 
            readObjectArray lowerBounds lengths arrObj

    member private this.ReadContents (objType: SerTypeInfo, obj: byref<obj>) : unit = 
        for field in objType.SerializedFields do 
            let fieldType = field.FieldType
            if fieldType.IsPrimitive then
                let value = readPrimitive fieldType
                field.SetValue(obj, value)
            elif fieldType.IsValueType then
                let mutable newValue = FormatterServices.GetUninitializedObject(fieldType) 
                do this.ReadContents(typeSerializer.GetSerTypeInfo fieldType, &newValue)
                field.SetValue(obj, newValue)
            else
                let myObj = obj
                this.ReadObject(reader, marked, fun newValue -> field.SetValue(myObj, newValue))

    member private this.ReadSerializationInfo(deserType: Type) : SerializationInfo  =
        let deserInfo = SerializationInfo(deserType, Serialize.theConverter)
        let numFields = reader.ReadInt32()
        for _ in 1..numFields do
            this.ReadObject(reader, marked, function 
                | :? string as name -> 
                    this.ReadObject(reader, marked, 
                        fun value -> deserInfo.AddValue(name, value, if value = null then typeof<obj> else value.GetType()))
                | _ -> failwith "Expecting field name to be a string" )
        deserInfo

    member this.ReadSurrogateSerializedObject(surrogate: ISerializationSurrogate, objType: Type, obj: obj) : unit =
        let serInfo = this.ReadSerializationInfo objType
        surrogate.SetObjectData(obj, serInfo, Serialize.theContext, null) |> ignore

    member private this.ReadCustomSerializedObject(k : obj -> unit) : unit =
        this.ReadObject(reader, marked, fun ty ->
            let deserType = ty :?> Type
            let mutable newObj = FormatterServices.GetUninitializedObject(deserType)
            if newObj = null then
                failwith <| sprintf "Failed to create unintialized instance of %A." deserType
            let refPosition = marked.Count
            marked.Add newObj
            let deserInfo = this.ReadSerializationInfo deserType
            let deserConstructor = deserType.GetConstructor(Serialize.AllInstance, null, DeserConstructorArgTypes, null)
            deserConstructor.Invoke( newObj, [| deserInfo; Serialize.theContext |]) |> ignore
            match newObj with
            | :? IDeserializationCallback as cb -> onDeserializationList.Add(cb)
            | _ -> ()
            match newObj with
            | :? IObjectReference as objRef -> 
                let realObject = objRef.GetRealObject(Serialize.theContext)
                match realObject with
                | :? IDeserializationCallback as cb -> onDeserializationList.Add(cb)
                | _ -> () 
                marked.[refPosition] <- realObject
                k realObject
            | _ -> 
                marked.[refPosition] <- newObj
                k newObj
        )

    // Due to circular IObjectReferences generated by C# Func custom serialization, 
    // and the fact that we want to keep using a simple recursive strategy, we need to use
    // Continuation Passing Style (CPS) to deserialize (either that or change the deserialization
    // code structure completely, storing first-class FieldInfo and array positions explicity,
    // which would be more complex and slower).
    // This means that instead of returning the deserialized object, we take a continuation k
    // as the last parameter, and call it instead of returning an object.
    // Normally the continuation is called only once with the "return" value. But when the "return"
    // is an IObjectReference (i.e.: the object is not fully deserialized), it is called twice:
    // once with null, and second time at the end, after all objects have been deserialized, 
    // with objectReference.GetRealObject(). The null call is needed in case the receiver
    // needs to store some state for the second call. In our case, arrays deserializers need to
    // store a copy of the indices array so they can insert the real object in the right position later.
    member private this.ReadObject (reader: BinaryReader, marked: List<obj>, k: obj -> unit) : unit =
        let tag = LanguagePrimitives.EnumOfValue<byte, ReferenceType>(reader.ReadByte()) 
        match tag with
        | ReferenceType.Null -> k null
        | ReferenceType.ObjectPosition -> 
            let pos = reader.ReadInt32()
            let obj = marked.[pos]
            match obj with
            | :? IObjectReference as objRef -> 
                delayedSetList.Add(objRef, k)
                k null
            | _ -> k obj
        | ReferenceType.InlineObject ->
            let serType = typeSerializer.Deserialize(reader)
            let surrogate = if surrogateSelector = null then null else surrogateSelector.GetSurrogate(serType.Type, Serialize.theContext, ref (Unchecked.defaultof<ISurrogateSelector>))
            if surrogate <> null then
                let mutable newObj = FormatterServices.GetUninitializedObject(serType.Type)
                if newObj = null then
                    failwith <| sprintf "Failed to create unintialized instance of %A." serType.Type
                marked.Add newObj
                this.ReadSurrogateSerializedObject(surrogate, serType.Type, newObj)
                k newObj
            else
                match serType.Type with
                | strType when strType = typeof<string> -> 
                    let str = reader.ReadString()
                    marked.Add str
                    k str
                | arrType when arrType.IsArray ->
                    let rank = reader.ReadInt32()
                    let lowerBounds : int[] = Array.zeroCreate rank
                    let lengths : int[] = Array.zeroCreate rank
                    do readPrimitiveArray intArrayTypeInfo lowerBounds
                    do readPrimitiveArray intArrayTypeInfo lengths
                    let elType = arrType.GetElementType()
                    let newArr = Array.CreateInstance(elType, lengths, lowerBounds)
                    marked.Add newArr
                    do this.ReadArray(serType, lowerBounds, lengths, newArr)
                    k newArr
                | typeType when typeof<Type>.IsAssignableFrom(typeType) -> 
                    let typeName = reader.ReadString()
                    let ``type`` = Type.GetType(typeName)
                    marked.Add ``type``
                    k ``type``
                | ty when typeof<ISerializable>.IsAssignableFrom(ty) ->
                    this.ReadCustomSerializedObject(k)
                | _ -> 
                    let mutable newObj = FormatterServices.GetUninitializedObject(serType.Type)
                    marked.Add newObj
                    if serType.HaveDeserializationCallbacks then
                        serType.OnDeserializing(newObj, Serialize.theContext)
                        if not serType.IsValueType then
                            serType.NoArgConstructor newObj
                        this.ReadContents(serType, &newObj)
                        serType.OnDeserialized(newObj, Serialize.theContext)
                    else
                        if not serType.IsValueType then
                            serType.NoArgConstructor newObj
                        this.ReadContents(serType, &newObj)
                    k newObj
        | _ -> failwith <| sprintf "Unexpected tag: %A" tag

    member this.ReadObject() = 
        let root : obj ref = ref null
        this.ReadObject(reader, marked, fun obj -> root := obj)
        for i = 0 to delayedSetList.Count - 1 do
            let objRef, k = delayedSetList.[i]
            k <| objRef.GetRealObject(Serialize.theContext)
        for cb in onDeserializationList do
            cb.OnDeserialization(null)
        !root

type internal ReferenceComparer() =
    interface IEqualityComparer<obj> with
        member __.Equals(x: obj, y: obj): bool = 
            Object.ReferenceEquals(x, y)
        member x.GetHashCode(obj: obj): int = 
            System.Runtime.CompilerServices.RuntimeHelpers.GetHashCode(obj)

type internal BinarySerializer() =

    interface IFormatter with
        
        member val SurrogateSelector: ISurrogateSelector = null with get, set

        member x.Binder
            with get (): SerializationBinder = 
                failwith "Not implemented yet"
            and set (v: SerializationBinder): unit = 
                failwith "Not implemented yet"
        
        member x.Context
            with get (): StreamingContext = 
                failwith "Not implemented yet"
            and set (v: StreamingContext): unit = 
                failwith "Not implemented yet"
        
        member x.Deserialize (stream: Stream) : obj = 
    //        assert (this.SurrogateSelector <> null)
            use reader = new BinaryReader(stream, Text.UTF8Encoding.UTF8, true)
            let marked = List<obj>()
            let deser = Deserializer(reader, marked, new TypeSerializer(), (x :> IFormatter).SurrogateSelector)
            let ret = deser.ReadObject()
            ret
        
        member x.Serialize(stream: Stream, graph: obj): unit = 
    //        assert (this.SurrogateSelector <> null)
            use writer = new BinaryWriter(stream, Text.UTF8Encoding.UTF8, true)
            let marked = Dictionary<obj, int>(ReferenceComparer())
            let ser = Serializer(writer, marked, new TypeSerializer(), (x :> IFormatter).SurrogateSelector)
            do ser.WriteObject(graph)
