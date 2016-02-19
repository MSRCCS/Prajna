namespace Nano.Tests

open System
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Collections.Concurrent
open System.Diagnostics

open NUnit.Framework

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Nano

[<TestFixture(Description = "Tests for Nano project")>]
module NanoTests =

    do Prajna.Tools.Logger.ParseArgs([|"-verbose"; "wild"|])

    let disposeAll xs = xs |> Seq.iter(fun x -> (x :> IDisposable).Dispose())


    [<TearDown>]
    let TearDown() =
        QueueMultiplexer<byte[]>.Shutdown()

    [<SetUp>]
    let SetUp() = 
        do BufferListStream<byte>.BufferSizeDefault <- 1 <<< 20

    [<Test>]
    let NanoStartLocalServer() = 
        use __ = new ServerNode(1500)
        ()

    [<Test>]
    let NanoConnectClient() = 
        use sn = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500) 
        ()

    [<Test>]
    let NanoNewRemote() = 
        use __ = new ServerNode(1500) 
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        async {
            let! r = cn.AsyncNewRemote(fun _ -> 5)
            return ()
        }
        |> Async.RunSynchronously

    [<Test>]
    let NanoGetValue() = 
        use sn = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let value = 
            async {
                let! r = cn.AsyncNewRemote(fun _ -> "Test")
                let! ret =  r.AsyncGetValue()
                return ret
            }
            |> Async.RunSynchronously
        Assert.AreEqual(value, "Test")

    [<Test>]
    let NanoGetValueSequential() = 
        use sn = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let numIters = 20
        let sw = Stopwatch.StartNew()
        for i = 1 to numIters do
            async {
                let! r = cn.AsyncNewRemote(fun _ -> i)
                let! r2 = r.AsyncApply(fun x -> x * x)
                let! ret =  r.AsyncGetValue()
                return ret
            }
            |> Async.RunSynchronously
            |> ignore
        sw.Stop()
        printfn "%s" <| sprintf "%d round trips took: %A" (numIters * 3) sw.Elapsed

    [<Test>]
    let NanoGetValueSequentialNoSerialization() = 
        use sn = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let numIters = 20
        let sw = new Stopwatch()
        async {
            let mutable s = 0
            sw.Start()
            for i = 1 to numIters do
                let! r = cn.AsyncNewRemote(fun _ -> 2)
                let! r2 = r.AsyncApply(fun x -> x * x)
                let! x =  r.AsyncGetValue()
                s <- s + x
            sw.Stop()
            return s
        }
        |> Async.RunSynchronously
        |> ignore
        printfn "%s" <| sprintf "%d round trips took: %A" (numIters * 3) sw.Elapsed
//        Assert.AreEqual(value, "Test")

    let baseNumFloatsForPerf = 75000000 // 0.3GB

    [<Test>]
    let NanoBigArrayRoundTrip() =
        use sn = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let r = Random()
        let bigMatrix = Array.init<float32> baseNumFloatsForPerf (fun _ -> r.NextDouble() |> float32) 
        let sw = Stopwatch.StartNew()
        let value = 
            async {
                let! r = cn.AsyncNewRemote(fun _ -> bigMatrix)
                let! ret =  r.AsyncGetValue()
                return ret
            }
            |> Async.RunSynchronously
        printf "%s" <| sprintf "Big matrix round-trip took: %A" sw.Elapsed

    [<Test>]
    let NanoBigArrayRawSocket() = 
        let numFloats = baseNumFloatsForPerf * 2
        let numBytes = numFloats * sizeof<float32>
        let r = Random()
        let bigMatrix = Array.init<float32> numFloats (fun _ -> r.NextDouble() |> float32) 
        let server = new TcpListener(ServerNode.GetDefaultIP(), 1500)
        server.Start()
        let swt = new Stopwatch()
        let sw = new Stopwatch()

        let clientThread = 
            new Thread(new ThreadStart(fun _ ->
                let client = new Socket(SocketType.Stream, ProtocolType.IP)
                client.Connect(ServerNode.GetDefaultIP(), 1500)
                let bytes = Array.zeroCreate numBytes
                //let sw = Stopwatch.StartNew()
                sw.Start()
                Buffer.BlockCopy(bigMatrix, 0, bytes, 0, bytes.Length)
                printfn "%s" <| sprintf "Copy only: %A" sw.Elapsed
                let mutable count = 0
                swt.Start()
                while count < numBytes do            
                    count <- count + client.Send(bytes, count, numBytes - count, SocketFlags.None)
                client.Shutdown(SocketShutdown.Both)))
        clientThread.Start()

        let mutable result = 0.0f
        let serverThread = 
            new Thread(new ThreadStart(fun _ -> 
                let socket = server.AcceptSocket()
                let bytes = Array.zeroCreate<byte> numBytes
                let mutable count = 0
                while count < numBytes do
                    count <- count + socket.Receive(bytes, count, numBytes - count, SocketFlags.None)
                swt.Stop()
                sw.Stop()
                printfn "%s" <| sprintf "Data transfer only: %A" swt.Elapsed
                printfn "%s" <| sprintf "Copy and transfer: %A" sw.Elapsed
                let bigMatrixCopy = Array.zeroCreate<float32> numFloats
                Buffer.BlockCopy(bytes, 0, bigMatrixCopy, 0, bytes.Length)
                result <- bigMatrixCopy.[bigMatrixCopy.Length - 1]))
        serverThread.Start()

        clientThread.Join()
        serverThread.Join()
        server.Stop()
        Assert.AreEqual(bigMatrix.[bigMatrix.Length-1], result)

    [<Test>]
    let NanoRunRemote() = 
        use __ = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let value = 
            async {
                let! r = cn.AsyncNewRemote(fun _ -> "Test")
                let! r2 = r.AsyncApply(fun str -> str.Length)
                return! r2.AsyncGetValue()
            }
            |> Async.RunSynchronously
        Assert.AreEqual(value, 4)

    [<Test>]
    let NanoApplyAndGetValue() = 
        use __ = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let value = 
            async {
                let! r = cn.AsyncNewRemote(fun _ -> "Test")
                return! r.AsyncApplyAndGetValue(fun str -> str.Length)
            }
            |> Async.RunSynchronously
        Assert.AreEqual(value, 4)

    let inAnyOrder (asyncs: Async<'T>[]) : Async<'T seq> =
        async {
            let col = new BlockingCollection<'T>()
            let mutable count = asyncs.Length
            for a in asyncs do
                async { 
                    let! res = a
                    col.Add res
                    if Interlocked.Decrement(&count) = 0 then
                        col.CompleteAdding()
                }
                |> Async.Start
            return col.GetConsumingEnumerable()
        }

    let makeSquares (cns: ClientNode[]) (rndClient: Random) (numAsyncs: int) (maxWait: int) =
        let sqr x = x * x
        [|for i in 1..numAsyncs ->
            let clientNum = rndClient.Next(cns.Length)
            let cn = cns.[clientNum]
            Logger.LogF(LogLevel.MediumVerbose, fun _ -> sprintf "Chose client %d for number %d" clientNum i)
            async {
                let! r1 = cn.AsyncNewRemote(fun _ -> i)
                let! r2 = r1.AsyncApply(fun x ->
                    if maxWait > 0 then
                        let rnd = new Random(i)
                        let wait = rnd.Next(maxWait) |> int64
                        let sw = Stopwatch.StartNew()
                        let mutable breakNow = false
                        while not breakNow do
                            if sw.ElapsedMilliseconds > wait then 
                                breakNow <- true
                    sqr x)
                return! r2.AsyncGetValue()
            }|]

    let nanoParallelWild (numAsyncs: int) (maxWait: int) (numClients: int) (numServers: int) = 
        let baseServerPort = 1500
        let servers = Array.init numServers (fun i -> new ServerNode(baseServerPort + i))
        let rnd = new Random()
        let clients = Array.init numClients (fun _ -> new ClientNode(ServerNode.GetDefaultIP(), baseServerPort + rnd.Next(numServers)))
        try
            let sw = Stopwatch.StartNew()
            let sqr x = x * x
            // Have to use weird printf <| sprintf form so VSTest doesn't insert newlines where we don't want
            printfn "%s" <| sprintf "Running %d asyncs (%d round-trips) in parallel." numAsyncs (numAsyncs * 3)
            let rets =
                makeSquares clients rnd numAsyncs maxWait
                |> inAnyOrder
                |> Async.RunSynchronously
            for x in rets do
                //printf "%s" <| sprintf "%d, " x 
                Assert.IsTrue(let sqrt = Math.Sqrt(float x) in sqrt = Math.Round(sqrt))
            printfn "%s" <| sprintf "Took: %A." sw.Elapsed
        finally
            disposeAll clients
            disposeAll servers

    [<Test>]
    let NanoParallel() = 
        nanoParallelWild 36 1000 1 1

    [<Test>]
    let NanoParallelNoWait() = 
        do Logger.ParseArgs([|"-verbose"; "error"|])
        nanoParallelWild 33 0 1 1

    [<Test>]
    let NanoParallelManyToMany() = 
        do Logger.ParseArgs([|"-verbose"; "info"|])
        nanoParallelWild 20 300 20 10

    [<Test>]
    let NanoParallelNoWaitManyToMany() = 
        do Logger.ParseArgs([|"-verbose"; "info"|])
        nanoParallelWild 20 0 20 10

    [<Test>]
    let NanoParallelForkJoin() =
        use __ = new ServerNode(1500)
        let cns = Array.init 1 (fun _ -> new ClientNode(ServerNode.GetDefaultIP(), 1500))
        try
            let sw = Stopwatch.StartNew()
            let sqr x = x * x
            let numSquares = 50
            let rets =
                makeSquares cns (Random()) numSquares 0
                |> Async.Parallel
                |> Async.RunSynchronously
            printfn "%s" <| sprintf "%d asyncs (%d round-trips) in parallel took: %A." numSquares (numSquares * 3) sw.Elapsed
            Assert.AreEqual(rets, [|1..numSquares|] |> Array.map sqr)
        finally
            cns |> Array.iter (fun cn -> (cn :> IDisposable).Dispose())

    [<Test>]
    let NanoTwoClients() =
        use __ = new ServerNode(1500)
        use cn1 = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        use cn2 = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let sw = Stopwatch.StartNew()
        async {
            let! r1 = cn1.AsyncNewRemote(fun _ -> 2)
            let! r2 = cn2.AsyncNewRemote(fun _ -> 3)
            let! r1Squared = r1.AsyncApply(fun x -> x * x)
            let! r2Squared = r2.AsyncApply(fun x -> x * x)
            return ()
        }
        |> Async.RunSynchronously


    [<Test>]
    let NanoRemoteRef() = 
        use __ = new ServerNode(1500)
        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
        let value = 
            async {
                let! r = cn.AsyncNewRemote(fun _ -> "Test")
                let! r2 = cn.AsyncNewRemote(fun _ -> r.AsyncGetValue() |> Async.RunSynchronously |> fun str -> str.Length)
                return! r2.AsyncGetValue()
            }
            |> Async.RunSynchronously
        Assert.AreEqual(value, 4)

//    [<Test>]
//    let NanoPreSerialized() = 
//        use __ = new ServerNode(1500)
//        use cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
//        let ints = [|1..1000000|]
//        let sw = Stopwatch.StartNew()
//        use createInts = Serializer.Serialize <|  Func<int[]>(fun _ -> ints) 
//        use addOne = Serializer.Serialize <|  Func<int[], int[]>(Array.map (fun x -> x + 1) )
//        printfn "%s" <| sprintf "Serialization took: %A" sw.Elapsed
//        sw.Restart()
//        let remotes = 
//            Array.init 30 (
//                fun _ -> async { 
//                    let! r = cn.AsyncNewRemote( createInts )
//                    return! r.Apply(addOne)
//                    } ) 
//            |> Async.Parallel |> Async.RunSynchronously 
//        printfn "%s" <| sprintf "Remote creation took: %A" sw.Elapsed
//        sw.Restart()
//        let arrays =
//            remotes |> Array.map (fun r -> r.AsyncGetValue()) 
//            |> Async.Parallel |> Async.RunSynchronously 
//        printfn "%s" <| sprintf "Bringing back took: %A" sw.Elapsed
//        printfn "%s" "Done"

    let broadcast (numClients: int) (obj: 'T) : ServerNode * ClientNode[] * Distributed<'T> =
        let server = new ServerNode(1500)
        let clients = Array.init numClients (fun _ -> new ClientNode(ServerNode.GetDefaultIP(), 1500))
        let broadcaster = new Broadcaster(clients)
        server, clients, (broadcaster.BroadcastChainedNew(fun _ -> obj) |> Async.RunSynchronously)
    
    let printTime (message: string) (sw: Stopwatch) =
        let elapsed = sw.Elapsed
        printfn "%s" <| sprintf "%s: %A" message sw.Elapsed
        sw.Restart()

    let nanoBroadcastArray (numInts: int) =
        let sw = Stopwatch.StartNew()
        let numClients = 10
        let server, clients, dist = broadcast numClients [|1..numInts|]
        printTime "Broadcasting took:" sw
        try
            let vals =
                async {
                    sw.Restart()
                    let! multiples = dist.Apply(fun arr -> arr |> Array.map (fun x -> x * 10))
                    printTime "Applying map took:" sw
                    let! vals = multiples.GetValues()
                    printTime "Fetching results took:" sw
                    return vals
                }
                |> Async.RunSynchronously
            Assert.IsTrue( (vals = Array.init numClients (fun _ -> [|10..10..(numInts * 10)|])) )
        finally
            (server :> IDisposable).Dispose()
            disposeAll clients
    
    [<Test>]
    let NanoBroadcastTiny() =
        nanoBroadcastArray 10

    [<Test>]
    let NanoBroadcastSmall() =
        nanoBroadcastArray 10000

    [<Test>]
    let NanoBroadcastMedium() =
        nanoBroadcastArray 1000000

    [<Test>]
    let NanoBroadcastLarge() =
        do BufferListStream<byte>.BufferSizeDefault <- 1 <<< 21
        nanoBroadcastArray 10000000

