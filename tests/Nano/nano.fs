namespace Nano.Tests

open System
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


    [<TearDown>]
    let TearDown() =
        QueueMultiplexer<byte[]>.Shutdown()

    [<Test>]
    let NanoStartLocalServer() = 
        use __ = new ServerNode(1500)
        ()

    [<Test>]
    let NanoConnectClient() = 
        use sn = new ServerNode(1500)
        use cn = new ClientNode("127.0.0.1", 1500) 
        ()

    [<Test>]
    let NanoNewRemote() = 
        use __ = new ServerNode(1500) 
        use cn = new ClientNode("127.0.0.1", 1500)
        async {
            let! r = cn.NewRemote(fun _ -> 5)
            return ()
        }
        |> Async.RunSynchronously

    [<Test>]
    let NanoGetValue() = 
        use sn = new ServerNode(1501)
        use cn = new ClientNode("127.0.0.1", 1501)
        let value = 
            async {
                let! r = cn.NewRemote(fun _ -> "Test")
                let! ret =  r.GetValue()
                return ret
            }
            |> Async.RunSynchronously
        Assert.AreEqual(value, "Test")

    [<Test>]
    let NanoRunRemote() = 
        use __ = new ServerNode(1500)
        use cn = new ClientNode("127.0.0.1", 1500)
        let value = 
            async {
                let! r = cn.NewRemote(fun _ -> "Test")
                let! r2 = r.Run(fun str -> str.Length)
                return! r2.GetValue()
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
                let! r1 = cn.NewRemote(fun _ -> i)
                let! r2 = r1.Run(fun x ->
                    if maxWait > 0 then
                        let rnd = new Random(i)
                        let wait = rnd.Next(maxWait) |> int64
                        let sw = Stopwatch.StartNew()
                        let mutable breakNow = false
                        while not breakNow do
                            if sw.ElapsedMilliseconds > wait then 
                                breakNow <- true
                    sqr x)
                return! r2.GetValue()
            }|]

    let disposeAll xs = xs |> Seq.iter(fun x -> (x :> IDisposable).Dispose())

    let nanoParallelWild (numAsyncs: int) (maxWait: int) (numClients: int) (numServers: int) = 
        let baseServerPort = 1500
        let servers = Array.init numServers (fun i -> new ServerNode(baseServerPort + i))
        let rnd = new Random()
        let clients = Array.init numClients (fun _ -> new ClientNode("127.0.0.1", baseServerPort + rnd.Next(numServers)))
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
                printf "%s" <| sprintf "%d, " x 
                Assert.IsTrue(let sqrt = Math.Sqrt(float x) in sqrt = Math.Round(sqrt))
            printfn "%s" <| sprintf "Took: %A." sw.Elapsed
        finally
            disposeAll clients
            disposeAll servers

    [<Test>]
    let NanoParallelAnyOrder() = 
        nanoParallelWild 36 1000 1 1

    [<Test>]
    let NanoParallelAnyOrderNoWait() = 
        do Logger.ParseArgs([|"-verbose"; "info"|])
        nanoParallelWild 100 0 1 1

    [<Test>]
    let NanoParallelAnyOrderManyClients() = 
        do Logger.ParseArgs([|"-verbose"; "info"|])
        nanoParallelWild 100 300 100 10

    [<Test>]
    let NanoParallelAnyOrderNoWaitManyClients() = 
        do Logger.ParseArgs([|"-verbose"; "info"|])
        nanoParallelWild 100 0 100 10

    [<Test>]
    let NanoParallelForkJoin() =
        use __ = new ServerNode(1500)
        let cns = Array.init 1 (fun _ -> new ClientNode("127.0.0.1", 1500))
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
        use cn1 = new ClientNode("127.0.0.1", 1500)
        use cn2 = new ClientNode("127.0.0.1", 1500)
        let sw = Stopwatch.StartNew()
        async {
            let! r1 = cn1.NewRemote(fun _ -> 2)
            let! r2 = cn2.NewRemote(fun _ -> 3)
            let! r1Squared = r1.Run(fun x -> x * x)
            let! r2Squared = r2.Run(fun x -> x * x)
            return ()
        }
        |> Async.RunSynchronously


