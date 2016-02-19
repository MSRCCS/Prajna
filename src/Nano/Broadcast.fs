namespace Prajna.Nano

open System
open System.IO

open Prajna.Tools

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

////  Keeping this around as it seems to require less memory on the client
//
//    let distributedForwardRawOld (newRemote: ClientNode -> Async<Remote<'T>>) :  Async<Remote<'T>[]> =
//            let rec broadcastTo (remotes: Remote<ClientNode> list) (building: Async<Remote<'T> list>) : Async<Remote<'T> list> =
//                match remotes with 
//                | [] -> building 
//                | r::rs -> 
//                    async {
//                        let! syncBuilding = building
//                        let prev = syncBuilding.Head
//                        let! ret1 = r.AsyncApplyAndGetValue(fun next -> 
//                            async {
//                                let! myFoo = next.AsyncNewRemote(fun _ -> prev.GetValue())
//                                return broadcastTo rs (async { return (myFoo :: syncBuilding) })
//                            }
//                            |> Async.RunSynchronously)
//                        return! ret1
//                    }
//            async {
//                let! first = newRemote clients.[0] 
//                let! list = broadcastTo remoteNextClients (async { return [first] })
//                return list
//                    |> List.rev
//                    |> List.toArray
//            }

    let distributedForward (newRemote: ClientNode -> Async<Remote<'T>>) :  Async<Remote<'T>[]> =
            let rec broadcastTo (remotes: Remote<ClientNode> list) (prev: Remote<'T>) : Async<Remote<'T> list> =
                match remotes with 
                | [] -> async { return [] }
                | r::rs -> 
                    r.AsyncApplyAndGetValue(fun cur -> 
                        async {
                            let! myRemote = cur.AsyncNewRemote(fun _ -> prev.GetValue())
                            let! tail = broadcastTo rs myRemote 
                            return myRemote :: tail
                        }
                        |> Async.RunSynchronously)
            async {
                let! head = newRemote clients.[0] 
                let! tail = broadcastTo remoteNextClients head
                return (head::tail) |> List.toArray
            }

    let transpose (arr: 'T[][]) : 'T[][] =
        let nRows = arr.Length
        let nCols = arr.[0].Length
        Array.init nCols (fun j -> Array.init nRows (fun i -> arr.[i].[j]))

    member private this.ForwardChained(func: Func<'T>) : Async<Remote<'T>[]> = 
        distributedForward (fun c -> c.AsyncNewRemote func)

    member internal this.BroadcastChained<'T>(serFunc: MemoryStreamB) : Async<Distributed<'T>> =
        let chunkSize = 
            if BufferListStream<byte>.BufferSizeDefault <= PaddingToLeaveForSerialization then
                BufferListStream<byte>.BufferSizeDefault
            else
                BufferListStream<byte>.BufferSizeDefault - PaddingToLeaveForSerialization
        let chunks =
            [|while serFunc.Position < serFunc.Length do
                let curChunkSize = min (int64 chunkSize) (serFunc.Length - serFunc.Position)
                let ret = new MemoryStreamB(serFunc, serFunc.Position, curChunkSize)
                ret.Seek(0L, SeekOrigin.Begin) |> ignore
                serFunc.Position <- serFunc.Position + curChunkSize
                yield ret|]
        async {
            try
                let! distChunks = 
                    chunks 
                    |> Array.map (fun chunk ->  this.ForwardChained(fun _ -> chunk)) 
                    |> Async.Parallel
                let chunkColumns = transpose distChunks
                let! assembledChunkRemotes = 
                    chunkColumns |> Array.map (fun chunkColumn -> 
                        async {
                            let firstChunk = chunkColumn.[0]
                            let! ret = 
                                firstChunk.AsyncApply(fun _ ->
                                    async {
                                        use newStream = new MemoryStreamB()
                                        for localRemoteChunk in chunkColumn do
                                            let! chunkBytes = localRemoteChunk.AsyncGetValue()
                                            newStream.AppendNoCopy( chunkBytes, 0L, chunkBytes.Length)
                                            //chunkBytes.Dispose()
                                        newStream.Seek(0L, SeekOrigin.Begin) |> ignore
                                        let f = Serializer.Deserialize(newStream) :?> Func<'T>
                                        return f.Invoke()
                                    }
                                    |> Async.RunSynchronously)
                            return ret
                        })
                    |> Async.Parallel
                return new Distributed<'T>(assembledChunkRemotes)
            finally
                chunks |> Array.iter (fun c -> c.Dispose())
        }

    member this.BroadcastChained(func: Serialized<Func<'T>>) : Async<Distributed<'T>> =
        this.BroadcastChained(func.Bytes)

    member this.BroadcastChained(func: Func<'T>) : Async<Distributed<'T>> =
        use serFunc = (Serializer.Serialize func).Bytes
        this.BroadcastChained(serFunc)

    member this.BroadcastParallel(func: Func<'T>) = 
        async {
            let! rs = 
                clients
                |> Array.map (fun c -> c.AsyncNewRemote func)
                |> Async.Parallel
            return new Distributed<'T>(rs)
        }
