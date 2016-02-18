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
                |> Array.map (fun r -> r.Apply(func))
                |> Async.Parallel
            return Distributed<'U>(us)
        }

    member this.GetValues() : Async<'T[]> =
        remotes 
        |> Array.map (fun r -> r.GetValue())
        |> Async.Parallel


type Broadcaster(clients: ClientNode[]) =

    [<Literal>]
    let PaddingToLeaveForControlData = 4096

    let forward (newRemote: ClientNode -> Async<Remote<'T>>) = 
        fun (rs: Async<Remote<'T> option>) (c: ClientNode) (* : Async<Remote<'T> option> *) ->
            async {
                let! curOption = rs
                match curOption with
                | None -> 
                    let! first = newRemote c
                    return Some first
                | Some previous -> 
                    let! next = c.NewRemote(fun _ -> previous.GetValue() |> Async.RunSynchronously)
                    return Some next
            }

    let broadcastChained (newRemote: ClientNode -> Async<Remote<'T>>)  =
        async {
            let! remotes = 
                clients 
                |> Array.scan (forward newRemote) (async {return None})
                |> Async.Parallel
            return Distributed<'T>(remotes |> Seq.skip 1 |> Seq.map Option.get |> Seq.toArray)
        }

    member private this.BroadcastChainedFunc(func: Func<'T>) = broadcastChained (fun c -> c.NewRemote func)

//    member this.BroadcastChained(func: Serialized<Func<'T>>) = broadcastChained (fun c -> c.NewRemote func)

    member this.BroadcastChained(func: Func<'T>) : Async<Distributed<'T>> =
        use serFunc = (Serializer.Serialize func).Bytes
        let chunkSize = 
            if BufferListStream<byte>.BufferSizeDefault <= PaddingToLeaveForControlData then
                BufferListStream<byte>.BufferSizeDefault
            else
                BufferListStream<byte>.BufferSizeDefault - PaddingToLeaveForControlData
        let chunks =
            [|while serFunc.Position < serFunc.Length do
                let curChunkSize = min (int64 chunkSize) (serFunc.Length - serFunc.Position)
                let ret = new MemoryStreamB(serFunc, serFunc.Position, curChunkSize)
                ret.Seek(0L, SeekOrigin.Begin) |> ignore
                serFunc.Position <- serFunc.Position + curChunkSize
                yield ret|]
        let numChunks = chunks.Length
        async {
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
                    chunkColumn.[0].Apply(fun _ ->
                        use newStream = new MemoryStreamB()
                        for localRemoteChunk in chunkColumn do
                            let chunk = localRemoteChunk.GetValue() |> Async.RunSynchronously
                            newStream.AppendNoCopy( chunk, 0L, chunk.Length)
                            chunk.Dispose()
                        newStream.Seek(0L, SeekOrigin.Begin) |> ignore
                        let f = Serializer.Deserialize(newStream) :?> Func<'T>
                        f.Invoke()))
                |> Async.Parallel
            return new Distributed<'T>(assembledChunkRemotes)
        }

    member this.BroadcastParallel(func: Func<'T>) = 
        async {
            let! rs = 
                clients
                |> Array.map (fun c -> c.NewRemote func)
                |> Async.Parallel
            return new Distributed<'T>(rs)
        }
