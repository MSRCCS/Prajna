namespace Prajna.Nano

open System
open System.Threading.Tasks
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

type ClientNode(address: IPAddress, port: int) as self =

    static let network = new ConcreteNetwork()

    let mutable writeQueue : BufferQueue = null
    let callbacks = new ConcurrentDictionary<int64, Response -> unit>()

    let onNewBuffer (responseBytes: MemoryStreamB) =
        async { 
            let (Numbered(number,response)) = Serializer.Deserialize(responseBytes) :?> Numbered<Response>
            responseBytes.Dispose()
            callbacks.[number] response
            match callbacks.TryRemove(number) with
            | true, _ -> ()
            | false, _ -> raise <| Exception("Cound not remove callback.")
            responseBytes.Dispose()
        }
        |>  Async.Start 
    
    let onConnect readQueue wq =
        writeQueue <- wq
        QueueMultiplexer<MemoryStreamB>.AddQueue(readQueue, onNewBuffer)

    let getRemote (response: Response) =
        match response with
        | RunDelegateResponse(pos) -> Remote<'T>(pos, self)
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

    do
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting client node")
        network.Connect<BufferStreamConnection>(address, port, onConnect) |> ignore

    interface IRequestHandler with
        member this.AsyncHandleRequest(request: Request) : Async<Response> =
            let memStream = new MemoryStreamB()
            let (Numbered(number,_)) as numberedRequest = newNumbered request
            let memStream = Serializer.Serialize(numberedRequest).Bytes

            let responseHolder : Response option ref = ref None
            let semaphore = new SemaphoreSlim(0) 
            let callback (response: Response) = 
                responseHolder := Some(response)
                lock responseHolder (fun _ ->
                    semaphore.Release() |> ignore
                )
            callbacks.AddOrUpdate(number, callback, Func<_,_,_>(fun _ _ -> raise <| Exception("Unexpected pre-existing request number."))) |> ignore
            lock responseHolder (fun _ ->
                writeQueue.Add memStream
                async {
                    do! Async.AwaitIAsyncResult(semaphore.WaitAsync(), 10) |> Async.Ignore
                    while !responseHolder = None do
                        do! Async.Sleep 10
                    return responseHolder.Value.Value
                }
            )
        member this.HandleRequest(request: Request) = 
            (this :> IRequestHandler).AsyncHandleRequest(request)
            |> Async.RunSynchronously

        member this.Address = address
        member this.Port = port


    member this.AsyncNewRemote(func: Func<'T>) : Async<Remote<'T>> =
        async {
            let handler = (* defaultArg (ServerNode.TryGetServer((address,port))) *) (this :> IRequestHandler)
            let! response = handler.AsyncHandleRequest(RunDelegate(-1, func))
            match response with
            | RunDelegateResponse(pos) -> return new Remote<'T>(pos, this)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    member this.NewRemote(func: Func<'T>) : Remote<'T> =
        let handler = (* defaultArg (ServerNode.TryGetServer((address,port))) *) (this :> IRequestHandler)
        let response = handler.HandleRequest(RunDelegate(-1, func))
        getRemote response

    member this.NewRemoteAsync(func: Func<'T>) = this.AsyncNewRemote(func) |> Async.StartAsTask

    member this.AsyncNewRemote(func: Serialized<Func<'T>>) : Async<Remote<'T>> =
        async {
            let handler = (* defaultArg (ServerNode.TryGetServer((address,port))) *) (this :> IRequestHandler)
            let! response = handler.AsyncHandleRequest(RunDelegateSerialized(-1, func.Bytes))
            return getRemote response
        }

    member this.NewRemoteAsync(func: Serialized<Func<'T>>) = this.AsyncNewRemote(func) |> Async.StartAsTask

    interface IDisposable with
        member __.Dispose() = 
            writeQueue.CompleteAdding()

and Remote<'T> =
    
    [<NonSerialized>]
    val mutable handler: IRequestHandler 
    val pos: int
    val mutable serverInfo : ServerInfo

    internal new(pos: int, handler: IRequestHandler) = 
        {handler = handler (*Unchecked.defaultof<IRequestHandler>*); pos = pos; serverInfo = {Address = handler.Address; Port = handler.Port }}

    member private this.ReinitHandler() = 
        if Object.ReferenceEquals(this.handler, null) then
            match ServerNode.TryGetServer((this.serverInfo.Address, this.serverInfo.Port)) with
            | Some(server) -> this.handler <- server
            | None -> 
                this.handler <- ClientNode(this.serverInfo.Address, this.serverInfo.Port) :> IRequestHandler
            
    /// Apply<Func>
    member this.AsyncApply(func: Func<'T, 'U>) =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegate(this.pos, func) )
            match response with
            | RunDelegateResponse(pos) -> return new Remote<'U>(pos, this.handler)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }
    member this.ApplyAsync(func: Func<'T, 'U>) = this.AsyncApply(func) |> Async.StartAsTask
    member this.Apply(func: Func<'T, 'U>) =
        this.ReinitHandler()
        let response = this.handler.HandleRequest( RunDelegate(this.pos, func) )
        match response with
        | RunDelegateResponse(pos) -> new Remote<'U>(pos, this.handler)
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

    /// Apply<Func>
    member this.AsyncApplyAndGetValue(func: Func<'T, 'U>) : Async<'U> =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegateAndGetValue(this.pos, func) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'U
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }


    /// Apply<SerializedFunc>
    member this.ApplyAsync(func: Serialized<Func<'T, 'U>>) = this.AsyncApply(func) |> Async.StartAsTask
    member this.AsyncApply(func: Serialized<Func<'T, 'U>>) =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( RunDelegateSerialized(this.pos, func.Bytes) )
            match response with
            | RunDelegateResponse(pos) -> return new Remote<'U>(pos, this.handler)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }
    member this.Apply(func: Serialized<Func<'T, 'U>>) =
        this.ReinitHandler()
        let response = this.handler.HandleRequest( RunDelegateSerialized(this.pos, func.Bytes) )
        match response with
        | RunDelegateResponse(pos) -> new Remote<'U>(pos, this.handler)
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

    // GetValue
    member this.AsyncGetValue() =
        this.ReinitHandler()
        async {
            let! response = this.handler.AsyncHandleRequest( GetValue(this.pos) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'T
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }
    member this.GetValueAsync() = this.AsyncGetValue() |> Async.StartAsTask
    member this.GetValue() =
        this.ReinitHandler()
        let response = this.handler.HandleRequest( GetValue(this.pos) )
        match response with
        | GetValueResponse(obj) -> obj :?> 'T
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

//    static member Flip(asyncRemote: Async<Remote<'T>>) : Remote<Async<'T>> =
        
