namespace Prajna.Nano

open System
open System.Runtime.Serialization
open System.Net
open System.Collections.Generic
open System.Threading
open System.Collections.Concurrent
open System.IO

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

open BaseADTs

type ServerInfo = {Address: IPAddress; Port: int}

type ClientNode(address: IPAddress, port: int) =

    static let network = new ConcreteNetwork()

    let mutable writeQueue : BufferQueue = null
    let callbacks = new ConcurrentDictionary<int64, Response -> unit>()

    let serializer = 
        let memStreamBConstructors = (fun () -> new MemoryStreamB() :> MemoryStream), (fun (a,b,c,d,e) -> new MemoryStreamB(a,b,c,d,e) :> MemoryStream)
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(memStreamBConstructors))

    let onNewBuffer (responseBytes: MemoryStreamB) =
        async { 
            let (Numbered(number,response)) = serializer.Deserialize(responseBytes) :?> Numbered<Response>
            responseBytes.Dispose()
            callbacks.[number] response
        }
        |>  Async.Start 
    
    let onConnect readQueue wq =
        writeQueue <- wq
        QueueMultiplexer<MemoryStreamB>.AddQueue(readQueue, onNewBuffer)

    do
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting client node")
        network.Connect<BufferStreamConnection>(address, port, onConnect) |> ignore

    interface IRequestHandler with
        member this.HandleRequest(request: Request) : Async<Response> =
            let memStream = new MemoryStreamB()
            let numberedRequest = newNumbered request
            serializer.Serialize(memStream, numberedRequest)
            memStream.Seek(0L, SeekOrigin.Begin) |> ignore
            let responseHolder : Response option ref = ref None
            let semaphore = new SemaphoreSlim(0) 
            let callback (response: Response) = 
                responseHolder := Some(response)
                lock responseHolder (fun _ ->
                    semaphore.Release() |> ignore
                )
            callbacks.AddOrUpdate(numberedRequest.N, callback, Func<_,_,_>(fun _ _ -> raise <| Exception("Unexpected pre-existing request number."))) |> ignore
            lock responseHolder (fun _ ->
                writeQueue.Add memStream
                async {
                    do! Async.AwaitIAsyncResult(semaphore.WaitAsync(), 10) |> Async.Ignore
                    while !responseHolder = None do
                        do! Async.Sleep 100
                    return responseHolder.Value.Value
                }
            )
        member this.Address = address
        member this.Port = port


    member this.NewRemote(func: Func<'T>) : Async<Remote<'T>> =
        async {
            let handler = defaultArg (ServerNode.TryGetServer((address,port))) (this :> IRequestHandler)
            let! response = handler.HandleRequest(RunDelegate(-1, func))
            match response with
            | RunDelegateResponse(pos) -> return new Remote<'T>(pos, this)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    interface IDisposable with

        member __.Dispose() = 
            writeQueue.CompleteAdding()

and Remote<'T> =
    
    [<NonSerialized>]
    val mutable handler: IRequestHandler 
    val pos: int
    val mutable serverInfo : ServerInfo

    internal new(pos: int, handler: IRequestHandler) = 
        {handler = (*handler*) Unchecked.defaultof<IRequestHandler>; pos = pos; serverInfo = {Address = handler.Address; Port = handler.Port }}

//    [<System.Runtime.Serialization.OnSerializing>]
//    member internal this.OnSerializing() = 
//        this.serverInfo <- Some {ServerId = this.handler.ServerId; Address = this.handler.Address; Port = this.handler.Port }

//    [<System.Runtime.Serialization.OnDeserialized>]
//    member internal this.OnDeserialized() = 
//        match ServerNode.TryGetServer(this.serverInfo.Value.ServerId) with
//        | Some(server) -> this.handler <- server
//        | None -> this.handler <- ClientNode(this.serverInfo.Value.Address, this.serverInfo.Value.Port) :> IRequestHandler

    member private this.ReinitHandler() = 
        match ServerNode.TryGetServer((this.serverInfo.Address, this.serverInfo.Port)) with
            | Some(server) -> this.handler <- server
            | None -> 
                if Object.ReferenceEquals(this.handler, null) then
                    this.handler <- ClientNode(this.serverInfo.Address, this.serverInfo.Port) :> IRequestHandler
            
    member this.Run(func: Func<'T, 'U>) =
        this.ReinitHandler()
        async {
            let! response = this.handler.HandleRequest( RunDelegate(this.pos, func) )
            match response with
            | RunDelegateResponse(pos) -> return new Remote<'U>(pos, this.handler)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    member this.GetValue() =
        this.ReinitHandler()
        async {
            let! response = this.handler.HandleRequest( GetValue(this.pos) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'T
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }
