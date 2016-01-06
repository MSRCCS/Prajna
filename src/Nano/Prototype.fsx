#I __SOURCE_DIRECTORY__
#r @"..\tools\tools\bin\Debugx64\Prajna.Tools.dll"

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.IO
open System.Net.Sockets

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

//do Logger.ParseArgs([|"-con"|])
do BufferListStream<byte>.InitSharedPool()

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


type Request = 
    | RunDelegate of int * Delegate
    | GetValue of int

type Response =
    | RunDelegateResponse of int
    | GetValueResponse of obj

type ServerRequestHandler(readQueue: BlockingCollection<byte[]>, writeQueue: BlockingCollection<byte[]>, objects: List<obj>) =
    
    let serializer = 
        let memStreamBConstructors = (fun () -> new MemoryStreamB() :> MemoryStream), (fun (a,b,c,d,e) -> new MemoryStreamB(a,b,c,d,e) :> MemoryStream)
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(memStreamBConstructors))

    let handleRequest(request: Request) : Response =
        match request with
        | RunDelegate(pos,func) ->
            let argument : obj[] = if pos = -1 then null else (Array.singleton objects.[pos])
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
                let request : Request = downcast serializer.Deserialize(new MemoryStream(bytes)) 
                Logger.LogF(LogLevel.Info, fun _ -> sprintf "Deserialized request: %d bytes." bytes.Length)
                let response = handleRequest request
                let responseStream = new MemoryStream()
                serializer.Serialize(responseStream, response)
                writeQueue.Add (responseStream.GetBuffer().[0..(int responseStream.Length)-1])
        }

    member this.Start() =
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting request handler")
        processRequests() |> Async.Start

type ServerNode(port: int) =

    let network = new ConcreteNetwork()
    let objects = new List<obj>()
    
    let onConnect readQueue writeQueue =
        let handler = ServerRequestHandler(readQueue, writeQueue, objects)
        handler.Start()

    do
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting server node")
        network.Listen<BufferStreamConnection>(port, onConnect)


type ClientNode(addr: string, port: int) =

    let mutable readQueue : BufferQueue = null
    let mutable writeQueue : BufferQueue = null
    let network = new ConcreteNetwork()

    let serializer = 
        let memStreamBConstructors = (fun () -> new MemoryStreamB() :> MemoryStream), (fun (a,b,c,d,e) -> new MemoryStreamB(a,b,c,d,e) :> MemoryStream)
        GenericSerialization.GetDefaultFormatter(CustomizedSerializationSurrogateSelector(memStreamBConstructors))

    let onConnect rq wq =
        readQueue <- rq
        writeQueue <- wq

    do
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Starting client node")
        network.Connect<BufferStreamConnection>(addr, port, onConnect) |> ignore

    member internal this.Run(request: Request) : Response =
        let memStream = new MemoryStream()
        serializer.Serialize(memStream, request)
        writeQueue.Add (memStream.GetBuffer().[0..(int memStream.Length)-1])

        let responseBytes = readQueue.Take()
        serializer.Deserialize(new MemoryStream(responseBytes)) :?> Response

    member this.NewRemote(func: Func<'T>) : Remote<'T> =
        let response = this.Run(RunDelegate(-1, func))
        match response with
        | RunDelegateResponse(pos) -> new Remote<'T>(pos, this)
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

and Remote<'T> internal (pos: int, node: ClientNode) =
    
    member __.Run(func: Func<'T, 'U>) =
        let response = node.Run( RunDelegate(pos, func) )
        match response with
        | RunDelegateResponse(pos) -> new Remote<'U>(pos, node)
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

    member __.GetValue() =
        let response = node.Run( GetValue(pos) )
        match response with
        | GetValueResponse(obj) -> obj :?> 'T
        | _ -> raise <| Exception("Unexpected response to RunDelegate request.")

        
let serverNode = new ServerNode(1500)

let cn = new ClientNode("127.0.0.1", 1500)
let cn2 = new ClientNode("127.0.0.1", 1500)

let r1 = cn.NewRemote(fun _ -> "Test2")
let r2 = r1.Run(fun str -> str.Length)

let r3 = cn2.NewRemote(fun _ -> "Test33")
let r4 = r3.Run(fun str -> str.Length)

r1.GetValue()
r2.GetValue()
r3.GetValue()
r4.GetValue()

