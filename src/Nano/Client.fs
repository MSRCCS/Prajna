namespace Prajna.Nano

open System
open System.Collections.Generic
open System.Threading
open System.Collections.Concurrent
open System.IO

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

open BaseADTs

type RequestId = RequestId of IP: int * Port : int * Id: int

type ClientNode(addr: string, port: int) =

    let mutable writeQueue : BufferQueue = null

    let callbacks = new ConcurrentDictionary<int64, Response -> unit>()

    let serializer = 
        let memStreamBConstructors = (fun () -> new MemoryStreamB() :> MemoryStream), (fun (a,b,c,d,e) -> new MemoryStreamB(a,b,c,d,e) :> MemoryStream)
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(memStreamBConstructors))

    let onNewBuffer (responseBytes: byte[]) =
        async { 
            let (Numbered(number,response)) = serializer.Deserialize(new MemoryStream(responseBytes)) :?> Numbered<Response>
            callbacks.[number] response
        }
        |>  Async.Start 
    
    let onConnect readQueue wq =
        writeQueue <- wq
        QueueMultiplexer<byte[]>.AddQueue(readQueue, onNewBuffer)

    do
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting client node")
        QueueMultiplexer<byte[]>.Connect<BufferStreamConnection>(addr, port, onConnect) |> ignore

    member internal this.Run(request: Request) : Async<Response> =
        let memStream = new MemoryStream()
        let numberedRequest = newNumbered request
        serializer.Serialize(memStream, numberedRequest)
        let responseHolder : Response option ref = ref None
        let semaphore = new SemaphoreSlim(0) 
        let callback (response: Response) = 
            responseHolder := Some(response)
            lock responseHolder (fun _ ->
                semaphore.Release() |> ignore
            )
        callbacks.AddOrUpdate(numberedRequest.N, callback, Func<_,_,_>(fun _ _ -> raise <| Exception("Unexpected pre-existing request number."))) |> ignore
        lock responseHolder (fun _ ->
            writeQueue.Add (memStream.GetBuffer().[0..(int memStream.Length)-1])
            async {
                do! Async.AwaitIAsyncResult(semaphore.WaitAsync(), 10) |> Async.Ignore
                while !responseHolder = None do
                    do! Async.Sleep 10
                return responseHolder.Value.Value
            }
        )

    member this.NewRemote(func: Func<'T>) : Async<Remote<'T>> =
        async {
            let! response = this.Run(RunDelegate(-1, func))
            match response with
            | RunDelegateResponse(pos) -> return new Remote<'T>(pos, this)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    interface IDisposable with
        
        member __.Dispose() = 
            writeQueue.CompleteAdding()

and Remote<'T> internal (pos: int, node: ClientNode) =
    
    member __.Run(func: Func<'T, 'U>) =
        async {
            let! response = node.Run( RunDelegate(pos, func) )
            match response with
            | RunDelegateResponse(pos) -> return new Remote<'U>(pos, node)
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }

    member __.GetValue() =
        async {
            let! response = node.Run( GetValue(pos) )
            match response with
            | GetValueResponse(obj) -> return obj :?> 'T
            | _ -> return (raise <| Exception("Unexpected response to RunDelegate request."))
        }



