namespace Prajna.Nano

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent

open Prajna.Tools
open Prajna.Tools.FSharp

open BaseADTs

type ServerRequestHandler(readQueue: BlockingCollection<byte[]>, writeQueue: BlockingCollection<byte[]>, objects: List<obj>) =
    
    let serializer = 
        let memStreamBConstructors = (fun () -> new MemoryStreamB() :> MemoryStream), (fun (a,b,c,d,e) -> new MemoryStreamB(a,b,c,d,e) :> MemoryStream)
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(memStreamBConstructors))

    let handleRequest(request: Request) : Response =
        match request with
        | RunDelegate(pos,func) ->
            let argument : obj[] = if pos = -1 then null else (Array.init 1 (fun _ -> objects.[pos]))
            let ret = func.DynamicInvoke(argument)
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "Ran method")
            if func.Method.ReturnType <> typeof<Void> then
                let retPos = lock objects (fun _ -> objects.Add ret; objects.Count - 1)
                RunDelegateResponse(retPos)
            else
                RunDelegateResponse(-1)
        | GetValue(pos) -> 
            GetValueResponse(objects.[pos])

    // Eventually deserialization/serialization can be done in parallel for various requests.
    // We just need to tag requests and responses so we can match them up
    // For now, correctness and simplicity are more important
    let processRequests() = 
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting to consume request bytes")
            for bytes in readQueue.GetConsumingEnumerable() do
                async {
                    let (Numbered(number,request)) : Numbered<Request> = downcast serializer.Deserialize(new MemoryStream(bytes)) 
                    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Deserialized request: %d bytes." bytes.Length)
                    let numberedResponse = Numbered(number, handleRequest request)
                    let responseStream = new MemoryStream()
                    serializer.Serialize(responseStream, numberedResponse)
                    writeQueue.Add (responseStream.GetBuffer().[0..(int responseStream.Length)-1])
                }
                |> Async.Start
            writeQueue.CompleteAdding()
        }

    member this.Start() =
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting request handler")
        processRequests() |> Async.Start

type ServerNode(port: int) =

    static let network = new ConcreteNetwork()
    let objects = new List<obj>()
    
    let onConnect readQueue writeQueue =
        let handler = ServerRequestHandler(readQueue, writeQueue, objects)
        handler.Start()

    do
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting server node")
        network.Listen<BufferStreamConnection>(port, onConnect)

    interface IDisposable with
        
        member __.Dispose() = network.StopListen()
        



