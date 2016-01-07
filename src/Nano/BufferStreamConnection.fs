namespace Prajna.Nano

open System
open System.IO
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

    let matchOrThrow (choice: Choice<'T,exn>) =
        match choice with 
        | Choice1Of2(t) -> t
        | Choice2Of2(exc) -> raise exc

    let receiveBuffers(socket: Socket) =
        let reader = new NetworkStream(socket)
        async {
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "BufferStreamConnection: starting to read")
            try
                while true do
                    let! countBytesOrExc = Async.Catch <| reader.AsyncRead 4
                    let countBytes = matchOrThrow countBytesOrExc
                    let count = BitConverter.ToInt32(countBytes, 0)
                    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Read count: %d." count)
                    let! receivedBufferOrExc = Async.Catch <| reader.AsyncRead count
                    let receivedBuffer = matchOrThrow receivedBufferOrExc
                    readQueue.Add receivedBuffer
            with
                | :? IOException -> readQueue.CompleteAdding()
        }

    let sendBuffers(socket: Socket) = 
        let writer = new NetworkStream(socket)
        async {
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "BufferStreamConnection: starting to write")
            try
                for bufferToSend in writeQueue.GetConsumingEnumerable() do
                    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Responding with %d bytes." bufferToSend.Length)
                    let countBytes = BitConverter.GetBytes(bufferToSend.Length)
                    let! possibleExc = Async.Catch <| writer.AsyncWrite countBytes
                    do matchOrThrow possibleExc
                    let! possibleExc2 = Async.Catch <| writer.AsyncWrite bufferToSend
                    do matchOrThrow possibleExc2
                    Logger.LogF(LogLevel.Info, fun _ -> sprintf "%d bytes written." bufferToSend.Length)
            with
                | :? IOException -> ()
        }

    interface IConn with 

        member val Socket = null with get, set

        member this.Init(socket: Socket, state: obj) = 
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "New connection created (%A)." socket.LocalEndPoint)
            (this :> IConn).Socket <- socket
            let onConnect : BufferQueue -> BufferQueue -> unit = downcast state
            onConnect readQueue writeQueue
            receiveBuffers socket |> Async.Start
            sendBuffers socket |> Async.Start

        member this.Close() = ()  

