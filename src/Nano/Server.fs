namespace Prajna.Nano

open System
open System.Net
open System.Net.NetworkInformation
open System.Net.Sockets
open System.IO
open System.Collections.Generic

open Prajna.Tools
open Prajna.Tools.FSharp

open BaseADTs

type IRequestHandler =
    abstract member HandleRequest : Request -> Async<Response>
    abstract member ServerId : Async<Guid>
    abstract member Address : IPAddress
    abstract member Port : int

type ServerBufferHandler(readQueue: BufferQueue, writeQueue: BufferQueue, handler: IRequestHandler) =
    
    let serializer = 
        let memStreamBConstructors = (fun () -> new MemoryStreamB() :> MemoryStream), (fun (a,b,c,d,e) -> new MemoryStreamB(a,b,c,d,e) :> MemoryStream)
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(memStreamBConstructors))

    let onNewBuffer (requestBytes: MemoryStreamB) =
        async {
            let (Numbered(number,request)) : Numbered<Request> = downcast serializer.Deserialize(requestBytes) 
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Deserialized request: %d bytes." requestBytes.Length)
            requestBytes.Dispose()
            let! response = handler.HandleRequest request
            let numberedResponse = Numbered(number, response)
            let responseStream = new MemoryStreamB()
            serializer.Serialize(responseStream, numberedResponse)
            responseStream.Seek(0L, SeekOrigin.Begin) |> ignore
            writeQueue.Add responseStream
        }
        |> Async.Start

    let processRequests() = 
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting to consume request bytes")
            QueueMultiplexer<MemoryStreamB>.AddQueue(readQueue, onNewBuffer, fun _ -> writeQueue.CompleteAdding())
        }

    member this.Start() =
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting request handler")
        processRequests()  |> Async.Start

    member this.Shutdown() =
        readQueue.CompleteAdding()

type ServerNode(port: int) as self =

    let network = new ConcreteNetwork()
    let objects = new List<obj>()
    let handlers = new List<ServerBufferHandler>()
    let serverId = Guid.NewGuid()
    let address = 
        let firstIP =
            Dns.GetHostAddresses("")
            |> Seq.tryFind (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
        match firstIP with
        | Some ip -> ip
        | None -> failwith "Could not find Internet IP"

    let handleRequest(request: Request) : Response =
        match request with
        | RunDelegate(pos,func) ->
            let argument : obj[] = if pos = -1 then null else (Array.init 1 (fun _ -> objects.[pos]))
            let ret = func.DynamicInvoke(argument)
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Ran method")
            if func.Method.ReturnType <> typeof<Void> then
                let retPos = lock objects (fun _ -> objects.Add ret; objects.Count - 1)
                RunDelegateResponse(retPos)
            else
                RunDelegateResponse(-1)
        | GetValue(pos) -> 
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Returning GetValue response")
            GetValueResponse(objects.[pos])
        | GetServerId -> 
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Returning Server id")
            GetServerIdResponse(serverId)

    let onConnect readQueue writeQueue =
        let handler = ServerBufferHandler(readQueue, writeQueue, self)
        lock handlers (fun _ ->
            handlers.Add(handler)
            handler.Start()
        )

    static let instances = new Dictionary<Guid, ServerNode>()

    do
        lock instances (fun _ -> instances.Add(serverId, self))
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting server node")
        network.Listen<BufferStreamConnection>(port, (*address.ToString(),*) onConnect)

    static member TryGetServer(guid: Guid) = 
        match instances.TryGetValue guid with
        | true, server -> Some (server :> IRequestHandler)
        | _ -> None

    static member TryGetAny() = 
        if instances.Count > 0 then
            (instances.Values |> Seq.nth 0) :> IRequestHandler |> Option.Some
        else
            None

    interface IRequestHandler with
        member __.Address = address
        member x.Port = port
        member val ServerId = async.Return serverId with get 
        
        member __.HandleRequest(req: Request) = 
            async { return handleRequest req  }

    interface IDisposable with
        
        member __.Dispose() = 
            network.StopListen()
            for handler in handlers do
                handler.Shutdown()
