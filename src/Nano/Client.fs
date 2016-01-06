namespace Prajna.Nano

open System
open System.IO

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

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



