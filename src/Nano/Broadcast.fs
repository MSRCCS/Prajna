namespace Prajna.Nano

open System
open System.Threading.Tasks
open System.Diagnostics
open System.Collections.Concurrent
open System.IO

open Prajna.Tools
open Prajna.Tools.FSharp

type Distributed<'T>(remotes: Remote<'T>[]) =

    member this.Remotes = remotes    

    member this.Apply(func: Func<'T, 'U>) : Async<Distributed<'U>> = 
        async {
            let! us = 
                remotes
                |> Array.map (fun r -> r.AsyncApply(func))
                |> Async.Parallel
            return Distributed<'U>(us)
        }

    member this.GetValues() : Async<'T[]> =
        remotes 
        |> Array.map (fun r -> r.AsyncGetValue())
        |> Async.Parallel


type internal RemoteStack = Remote<ConcurrentStack<int*MemoryStreamB>>

type Broadcaster(clients: ClientNode[]) =

    [<Literal>]
    let PaddingToLeaveForSerialization = 4096

    let remoteNextClients : Remote<ClientNode> list =
        clients
        |> Seq.skip 1
        |> Seq.map (fun c -> (c :> IRequestHandler).Address, (c :> IRequestHandler).Port, c)
        |> Seq.map (fun (address, port, c) -> c.AsyncNewRemote(Func<_>(fun _ -> new ClientNode(address, port))))
        |> Seq.toArray
        |> Async.Parallel
        |> Async.RunSynchronously
        |> Array.toList

    let newRemoteStacks() =
        async {
            let newStack = Serializer.Serialize(Func<_>(fun _ -> new ConcurrentStack<int*MemoryStreamB>()))
            let! ret =
                clients 
                |> Array.map (fun c -> c.AsyncNewRemote newStack)
                |> Async.Parallel 
            newStack.Bytes.Dispose()
            return ret
        }

    let newForwarder2 (stacks: RemoteStack[])  =
        let forwardOne (rs: RemoteStack) (nextFuncOption: Option<Async<Remote<int * MemoryStreamB -> Async<unit> >>>) : Option<Async<Remote<int * MemoryStreamB -> Async<unit>>>> =
            match nextFuncOption with 
            | None -> Some(rs.AsyncApply(Func<_,_>(fun (stack: ConcurrentStack<_>) -> (fun bytes -> async { return stack.Push bytes }))))
            | Some(asyncNextFunc) -> 
                Some(async {
                        let! nextFunc = asyncNextFunc
                        return! rs.AsyncApply(
                                    Func<_,_>(fun (stack: ConcurrentStack<_>) -> 
                                        fun posBytesPair ->
                                            stack.Push posBytesPair
                                            nextFunc.AsyncApplyAndAsyncGetValue(fun f -> f posBytesPair)
                                    )
                                )
                        })
        Array.foldBack forwardOne stacks None |> Option.get

    let getChunks (stream: MemoryStreamB) : MemoryStreamB[] =
        let chunkSize = (stream.Length / 100L) + 1L
        let ret = 
            [|while stream.Position < stream.Length do
                let curChunkSize = min (int64 chunkSize) (stream.Length - stream.Position)
                let ret = new MemoryStreamB(stream, stream.Position, curChunkSize)
                ret.Seek(0L, SeekOrigin.Begin) |> ignore
                stream.Position <- stream.Position + curChunkSize
                yield ret|]
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Broadcast: chunk size: %d. NumChunks: %d." chunkSize ret.Length)
        ret

    let transpose (arr: 'T[][]) : 'T[][] =
        let nRows = arr.Length
        let nCols = arr.[0].Length
        Array.init nCols (fun j -> Array.init nRows (fun i -> arr.[i].[j]))

    member internal this.BroadcastChained2<'T>(serFunc: MemoryStreamB) : Async<Distributed<'T>> =
        async {
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: starting")
            let chunks = getChunks serFunc
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got chunks")
            let! remoteStacks = newRemoteStacks()
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got remote stacks")
            let! forwarder = newForwarder2 remoteStacks
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: got forwarder")
            do!
                chunks
                |> Array.mapi (fun i chunk -> forwarder.AsyncApplyAndAsyncGetValue(fun addToStackAndForward -> addToStackAndForward (i,chunk)))
                |> Async.Parallel
                |> Async.Ignore
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: all chunks sent and received")
            let! remotes = 
                remoteStacks
                |> Array.map (fun remoteStack ->
                    remoteStack.AsyncApply(fun stack ->
                        let newStream = new MemoryStreamB()
                        let orderedChunks = 
                            seq {while stack.Count > 0 do match stack.TryPop() with | true,v -> yield v | _ -> failwith "Failed to pop"}
                            |> Seq.sortBy fst
                            |> Seq.map snd
                            |> Seq.toArray
                        for chunk in orderedChunks do
                            newStream.AppendNoCopy(chunk, 0L, chunk.Length)
                            //chunk.Dispose()
                        newStream.Seek(0L, SeekOrigin.Begin) |> ignore
                        let f = Serializer.Deserialize(newStream) :?> Func<'T>
                        let ret = f.Invoke()
                        newStream.Dispose()
                        ret)
                    )
                |> Async.Parallel
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: chunks assembled on remotes")
            chunks |> Array.iter (fun c -> c.Dispose())
            Logger.LogF(LogLevel.Info, fun _ -> "Broadcast: chunks disposed")
            return Distributed(remotes)
        }

    member this.BroadcastChained(func: Serialized<Func<'T>>) : Async<Distributed<'T>> =
        this.BroadcastChained2<'T>(func.Bytes)

    member this.BroadcastChained(func: Func<'T>) : Async<Distributed<'T>> =
        let serFunc = (Serializer.Serialize func).Bytes
        async {
            let! dist = this.BroadcastChained2<'T>(serFunc)
            serFunc.Dispose()
            return dist
        }

    member this.BroadcastParallel(func: Func<'T>) = 
        async {
            let! rs = 
                clients
                |> Array.map (fun c -> c.AsyncNewRemote func)
                |> Async.Parallel
            return new Distributed<'T>(rs)
        }
