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
//    abstract member ServerId : Async<Guid>
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

    let address = ServerNode.GetDefaultIP()

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


    let onConnect readQueue writeQueue =
        let handler = ServerBufferHandler(readQueue, writeQueue, self)
        lock handlers (fun _ ->
            handlers.Add(handler)
            handler.Start()
        )

    static let instances = new Dictionary<IPAddress * int, ServerNode>()

    do
        lock instances (fun _ -> instances.Add( (address,port)  , self))
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting server node")
        //BUGBUG: the server key is "address,port", with address being the first IP address returned by Dns.GetHostAddresses("")
        // But there's no guarantee that this is what we'll be listening at.
        network.Listen<BufferStreamConnection>(port, (*address.ToString(),*) onConnect)

    static member GetDefaultIP() =
        let firstIP =
            Dns.GetHostAddresses("")
            |> Seq.tryFind (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
        match firstIP with
        | Some ip -> ip
        | None -> failwith "Could not find Internet IP"

    static member TryGetServer(ip: IPAddress, port: int) = 
        match instances.TryGetValue ((ip,port)) with
        | true, server -> Some (server :> IRequestHandler)
        | _ -> None

    interface IRequestHandler with
        member __.Address = address
        member x.Port = port
        
        member __.HandleRequest(req: Request) = 
            async { return handleRequest req  }

    interface IDisposable with
        
        member __.Dispose() = 
            instances.Remove((address,port)) |> ignore
            network.StopListen()
            for handler in handlers do
                handler.Shutdown()
