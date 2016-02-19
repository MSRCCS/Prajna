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

    let broadcastSync (value: 'T) =
        let rec broadcastTo (remotes: Remote<ClientNode> list) (building: Remote<'T> list) : Remote<'T> list  =
            match remotes with 
            | [] -> building 
            | r::rs -> 
                let myRemote = r.Apply(fun next -> 
                    let myRemote = next.NewRemote(fun _ -> building.Head.GetValue())
                    broadcastTo rs (myRemote :: building))
                myRemote.GetValue()
        let first = clients.[0].NewRemote(fun _ -> value)
        broadcastTo remoteNextClients [first]
        |> List.rev
        |> List.toArray

//    let distributedForwardRaw2 (newRemote: ClientNode -> Async<Remote<'T>>) :  Async<Remote<Async<'T>>[]> =
//            let rec broadcastTo (remotes: Remote<ClientNode> list) (building: Async<Remote<Async<'T>> list>) : Async<Remote<Async<'T>> list> =
//                match remotes with 
//                | [] -> building 
//                | r::rs -> 
//                    async {
//                        let! syncBuilding = building
//                        let prev = syncBuilding.Head
//                        let! ret1 = r.AsyncApplyAndGetValue(fun next -> 
//                            async {
//                                let! myFoo = next.AsyncNewRemote(fun _ -> prev.AsyncApplyAndGetValue(fun t -> t |> Async.RunSynchronously))
//                                return broadcastTo rs (async { return (myFoo :: syncBuilding) })
//                            })
//                        let! ret2 = ret1
//                        return! ret2
//                    }
//            async {
//                let! first = newRemote clients.[0] 
//                let! firstAsync = first.AsyncApply(fun f -> async {return f})
//                return
//                    broadcastTo remoteNextClients [firstAsync]
//                    |> List.rev
//                    |> List.toArray
//            }

    let asyncBroadcast (value: 'T) =
        let rec asyncBroadcastTo (remotes: Remote<ClientNode> list) (buildingAsync: Async<Remote<'T> list>) : Async<Remote<'T> list>  =
            match remotes with 
            | [] -> buildingAsync
            | r::rs -> 
                async {
                    let! building = buildingAsync
                    let! myRemote = r.AsyncApply(fun next -> 
                        async {
                            let! myRemote = next.AsyncNewRemote(fun _ -> building.Head.GetValue())
                            return! asyncBroadcastTo rs (async {return (myRemote :: building)})
                        }
                        |> Async.RunSynchronously)
                    return! myRemote.AsyncGetValue()
                }
        async {
            let! first = clients.[0].AsyncNewRemote(fun _ -> value)
            let! remotes = asyncBroadcastTo remoteNextClients (async {return [first]})
            return 
                remotes
                |> List.rev
                |> List.toArray
        }

    let distributedForwardRaw3 (newRemote: ClientNode -> Async<Remote<'T>>) :  Async<Remote<'T>[]> =
            let rec broadcastTo (remotes: Remote<ClientNode> list) (building: Async<Remote<'T> list>) : Async<Remote<'T> list> =
                match remotes with 
                | [] -> building 
                | r::rs -> 
                    async {
                        let! syncBuilding = building
                        let prev = syncBuilding.Head
                        let! ret1 = r.AsyncApplyAndGetValue(fun next -> 
                            async {
                                let! myFoo = next.AsyncNewRemote(fun _ -> prev.GetValue())
                                return broadcastTo rs (async { return (myFoo :: syncBuilding) })
                            }
                            |> Async.RunSynchronously)
                        return! ret1
                    }
            async {
                let! first = newRemote clients.[0] 
                let! list = broadcastTo remoteNextClients (async { return [first] })
                return list
                    |> List.rev
                    |> List.toArray
            }

    let distributedForwardRaw (newRemote: ClientNode -> Async<Remote<'T>>) :  Async<Remote<'T>[]> =
        let rec asyncBroadcastTo (remotes: Remote<ClientNode> list) (buildingAsync: Remote<'T> list) : Remote<'T> list  =
            match remotes with 
            | [] -> buildingAsync 
            | r::rs -> 
                async {
                    let prev = buildingAsync.Head
                    let! myRemote = r.AsyncApply(fun next -> 
                            async {
                                let! thisRemote = next.AsyncNewRemote(fun _ -> prev.GetValue())
                                return asyncBroadcastTo rs (thisRemote :: buildingAsync)
                            }
                            |> Async.RunSynchronously
                            )
                    return! myRemote.AsyncGetValue()
                }
                |> Async.RunSynchronously
        async {
            let! first = newRemote clients.[0]
            let remotes = asyncBroadcastTo remoteNextClients [first]
            return (remotes |> List.rev |> List.toArray)
        }

    let distributedForward (newRemote: ClientNode -> Async<Remote<'T>>) =
        let rec asyncBroadcastTo (remotes: Remote<ClientNode> list) (buildingAsync: Async<Remote<'T> list>) : Async<Remote<'T> list>  =
            match remotes with 
            | [] -> buildingAsync
            | r::rs -> 
                async {
                    let! building = buildingAsync
                    let! myRemote = r.AsyncApply(fun next -> 
                        async {
                            let! myRemote = next.AsyncNewRemote(fun _ -> building.Head.GetValue())
                            return! asyncBroadcastTo rs (async {return (myRemote :: building)})
                        }
                        |> Async.RunSynchronously
                        )
                    return! myRemote.AsyncGetValue()
                }
        async {
            let! first = newRemote clients.[0]
            let! remotes = asyncBroadcastTo remoteNextClients (async {return [first]})
            return Distributed<_>(remotes |> List.rev |> List.toArray)
        }

    let forward (newRemote: ClientNode -> Async<Remote<'T>>) = 
        fun (rs: Async<Remote<'T> option>) (c: ClientNode) (* : Async<Remote<'T> option> *) ->
            async {
                let! curOption = rs
                match curOption with
                | None -> 
                    let! first = newRemote c
                    return Some first
                | Some previous -> 
                    let! next = c.AsyncNewRemote(fun _ -> previous.AsyncGetValue() |> Async.RunSynchronously)
                    return Some next
            }

    let broadcastChained (newRemote: ClientNode -> Async<Remote<'T>>)  =
        async {
            let! remotes = 
                clients 
                |> Array.scan (forward newRemote) (async {return None})
                |> Async.Parallel
            return //Distributed<'T>(
                remotes |> Seq.skip 1 |> Seq.map Option.get |> Seq.toArray
//                )
        }

    let broadcastChainedRaw (newRemote: ClientNode -> Async<Remote<'T>>)  =
        clients 
        |> Seq.scan (forward newRemote) (async {return None})
        |> Seq.skip 1
        |> Seq.toArray

    let transpose (arr: 'T[][]) : 'T[][] =
        let nRows = arr.Length
        let nCols = arr.[0].Length
        Array.init nCols (fun j -> Array.init nRows (fun i -> arr.[i].[j]))

    member private this.BroadcastChainedFuncRaw(func: Func<'T>) : Async<Remote<'T> option>[] = 
        broadcastChainedRaw (fun c -> c.AsyncNewRemote func)

    member private this.DistributedBroadcastChainedFuncRaw(func: Func<'T>) = //: Async<Remote<'T>>[] = 
        distributedForwardRaw3 (fun c -> c.AsyncNewRemote func)
        // broadcastChained (fun c -> c.AsyncNewRemote func)
        //async { return broadcastSync (func.Invoke()) }

    member private this.BroadcastChainedFunc(func: Func<'T>) = 
        distributedForward (fun c -> c.AsyncNewRemote func)

//    member private this.BroadcastChainedFunc(func: Func<'T>) = broadcastChained (fun c -> c.AsyncNewRemote func)
//    member this.BroadcastChained(func: Serialized<Func<'T>>) = broadcastChained (fun c -> c.NewRemote func)

    member this.BroadcastChainedOld(func: Func<'T>) : Async<Distributed<'T>> =
        use serFunc = (Serializer.Serialize func).Bytes
        let chunkSize = 
            if BufferListStream<byte>.BufferSizeDefault <= PaddingToLeaveForSerialization then
                BufferListStream<byte>.BufferSizeDefault
            else
                (BufferListStream<byte>.BufferSizeDefault - PaddingToLeaveForSerialization) / 1
        let chunks =
            [|while serFunc.Position < serFunc.Length do
                let curChunkSize = min (int64 chunkSize) (serFunc.Length - serFunc.Position)
                let ret = new MemoryStreamB(serFunc, serFunc.Position, curChunkSize)
                ret.Seek(0L, SeekOrigin.Begin) |> ignore
                serFunc.Position <- serFunc.Position + curChunkSize
                yield ret|]
        let numChunks = chunks.Length
        async {
            try
                let! distChunks = 
                    chunks
                    |> Array.map (fun chunk ->  this.BroadcastChainedFunc(fun _ -> chunk))
                    |> Async.Parallel
                chunks |> Array.iter (fun c -> c.Dispose())
                let chunksRemotes : Remote<MemoryStreamB>[][] = distChunks |> Array.map (fun dc -> dc.Remotes)
                let chunkColumns = clients |> Array.mapi (fun i _ -> Array.init numChunks (fun j -> chunksRemotes.[j].[i]))
            
                let! assembledChunkRemotes = 
                    clients |> Array.mapi (fun i _ -> 
                        let chunkColumn = chunkColumns.[i] //Array.init numChunks (fun j -> chunksRemotes.[j].[i])
                        chunkColumn.[0].AsyncApply(fun _ ->
                            use newStream = new MemoryStreamB()
                            for localRemoteChunk in chunkColumn do
                                let chunk = localRemoteChunk.AsyncGetValue() |> Async.RunSynchronously
                                newStream.AppendNoCopy( chunk, 0L, chunk.Length)
                                //chunk.Dispose()
                            newStream.Seek(0L, SeekOrigin.Begin) |> ignore
                            let f = Serializer.Deserialize(newStream) :?> Func<'T>
                            f.Invoke()))
                    |> Async.Parallel
                return new Distributed<'T>(assembledChunkRemotes)
            finally
                chunks |> Array.iter (fun c -> c.Dispose())
        }

    member this.BroadcastChained(func: Func<'T>) : Async<Distributed<'T>> =
        use serFunc = (Serializer.Serialize func).Bytes
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
                let chunkColumns = 
                    chunks 
                    |> Array.map (fun chunk ->  this.BroadcastChainedFuncRaw(fun _ -> chunk)) 
                    |> transpose
                let! assembledChunkRemotes = 
                    clients |> Array.mapi (fun i _ -> 
                        async {
                            let! chunkColumn = chunkColumns.[i] |> Async.Parallel //Array.init numChunks (fun j -> chunksRemotes.[j].[i])
                            let firstChunk = chunkColumn.[0].Value
                            let! ret = 
                                firstChunk.AsyncApply(fun _ ->
                                    use newStream = new MemoryStreamB()
                                    async {
                                        for localRemoteChunk in chunkColumn |> Array.map Option.get do
                                            let! chunkBytes = localRemoteChunk.AsyncGetValue() 
                                            newStream.AppendNoCopy( chunkBytes, 0L, chunkBytes.Length)
                                            chunkBytes.Dispose()
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

    member this.BroadcastChainedNew(func: Func<'T>) : Async<Distributed<'T>> =
        use serFunc = (Serializer.Serialize func).Bytes
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
                    |> Array.map (fun chunk ->  this.DistributedBroadcastChainedFuncRaw(fun _ -> chunk)) 
                    |> Async.Parallel
//                chunks |> Array.iter (fun c -> c.Dispose())
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

    member this.BroadcastParallel(func: Func<'T>) = 
        async {
            let! rs = 
                clients
                |> Array.map (fun c -> c.AsyncNewRemote func)
                |> Async.Parallel
            return new Distributed<'T>(rs)
        }
