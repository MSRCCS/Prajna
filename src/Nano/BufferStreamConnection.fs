namespace Prajna.Nano

open System
open System.Collections.Generic
open System.Collections.Concurrent

open System.Net.Sockets

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network


type BufferQueue = BlockingCollection<byte[]>

type ConcreteNetwork() = 
    inherit Network()

type BufferStreamConnection() =

    // Eventually these should change to using MemoryStreamB's, so we get better buffer management
    // For now, correctness and simplicity are more important
    let readQueue  = new BufferQueue(50)
    let writeQueue = new BufferQueue(50)

    let receiveRequests(socket: Socket) =
        let reader = new NetworkStream(socket)
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "BufferStreamConnection(%A): starting to read" socket.LocalEndPoint)
            while true do
                let! countBytes = reader.AsyncRead 4
                let count = BitConverter.ToInt32(countBytes, 0)
                Logger.LogF(LogLevel.Info, fun _ -> sprintf "Read count: %d." count)
                let! bytes = reader.AsyncRead count 
                readQueue.Add bytes
        }

    let sendResponses(socket: Socket) = 
        let writer = new NetworkStream(socket)
        async {
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "BufferStreamConnection(%A): starting to write" socket.LocalEndPoint)
            for response in writeQueue.GetConsumingEnumerable() do
                Logger.LogF(LogLevel.Info, fun _ -> sprintf "Responding with %d bytes." response.Length)
                let countBytes = BitConverter.GetBytes(response.Length)
                do! writer.AsyncWrite(countBytes)
                do! writer.AsyncWrite(response)
                Logger.LogF(LogLevel.Info, fun _ -> sprintf "%d bytes written." response.Length)
        }

    interface IConn with 

        member val Socket = null with get, set

        member this.Init(socket: Socket, state: obj) = 
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "New connection created (%A)." socket.LocalEndPoint)
            let onConnect : BufferQueue -> BufferQueue -> unit = downcast state
            onConnect readQueue writeQueue
            receiveRequests socket |> Async.Start
            sendResponses socket |> Async.Start

        member this.Close() = ()  

