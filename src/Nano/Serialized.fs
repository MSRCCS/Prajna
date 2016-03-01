namespace Prajna.Nano

open System
open System.IO
open Prajna.Tools


module SerializerModule = 

    MemoryStreamB.InitSharedPool()

    let memStreamBGuid = new Guid("D325A888-32E5-4758-A9B3-1A607CBD3542")
    let copyTo ((memStream,outStream) : MemoryStreamB * Stream) = 
        let count = memStream.Length - memStream.Position
        outStream.WriteInt64 count
        match outStream with 
        | :? MemoryStreamB as outStreamB -> 
            outStreamB.AppendNoCopy(memStream, memStream.Position, count)
        | _ -> 
            use clone = new MemoryStreamB(memStream)
            clone.Seek(0L, SeekOrigin.Begin) |> ignore
            clone.ReadToStream(outStream, count)
    let copyFrom (inStream: Stream) = 
        let retStream = new MemoryStreamB()
        let count = inStream.ReadInt64()
        match inStream with
        | :? MemoryStreamB as inStreamB -> 
            retStream.AppendNoCopy(inStreamB, inStreamB.Position, count)
        | _ -> 
            retStream.WriteFromStream(inStream, count)
        retStream.Seek(0L, SeekOrigin.Begin) |> ignore
        retStream
    CustomizedSerialization.InstallSerializer<MemoryStreamB>(memStreamBGuid, copyTo, true)
    CustomizedSerialization.InstallDeserializer<MemoryStreamB>(memStreamBGuid, copyFrom)



type Serialized internal (bytes: MemoryStreamB) = 

    member this.Bytes = bytes


type Serializer private () =

    member val serializer = 
        let memStreamBConstructors = (fun () -> new MemoryStreamB() :> MemoryStream), (fun (a,b,c,d,e) -> new MemoryStreamB(a,b,c,d,e) :> MemoryStream)
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(memStreamBConstructors))

    static member val instance = new Serializer()

    static member Serialize<'T>(obj: 'T) =
        let bytes = new MemoryStreamB()
        Serializer.instance.serializer.Serialize(bytes, obj)
        bytes.Seek(0L, SeekOrigin.Begin) |> ignore
        new Serialized<'T>(bytes)

    static member Serialize<'T>(obj: 'T, stream: MemoryStreamB) =
        Serializer.instance.serializer.Serialize(stream, obj)

    static member internal Deserialize(bytes: MemoryStreamB) =
        Serializer.instance.serializer.Deserialize(bytes)

and Serialized<'T> internal (bytes: MemoryStreamB) =
    inherit Serialized(bytes)

    interface IDisposable with
        member this.Dispose() = bytes.Dispose()        

