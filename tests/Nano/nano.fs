namespace Nano.Tests

open System
open System.Threading
open System.Collections.Concurrent
open System.Diagnostics

open NUnit.Framework

open Prajna.Nano

[<TestFixture(Description = "Tests for Nano project")>]
module NanoTests =

    [<Test>]
    let NanoStartLocalServer() = 
        use __ = new ServerNode(1500)
        ()

    [<Test>]
    let NanoConnectClient() = 
        use __ = new ServerNode(1500)
        use ___ = new ClientNode("127.0.0.1", 1500) 
        ()

    [<Test>]
    let NanoNewRemote() = 
        use __ = new ServerNode(1500) 
        use cn = new ClientNode("127.0.0.1", 1500)
        cn.NewRemote(fun _ -> 5) |> ignore

    [<Test>]
    let NanoGetValue() = 
        use __ = new ServerNode(1500)
        use cn = new ClientNode("127.0.0.1", 1500)
        let value = 
            async {
                let! r = cn.NewRemote(fun _ -> "Test")
                return! r.GetValue()
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

    let makeSquares (cn: ClientNode) (numAsyncs: int) (maxWait: int) =
        let sqr x = x * x
        [|for i in 1..numAsyncs ->
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

    let nanoParallelWild (numAsyncs: int) (maxWait: int) = 
        use __ = new ServerNode(1500)
        let cn = new ClientNode("127.0.0.1", 1500)
        let sw = Stopwatch.StartNew()
        let sqr x = x * x
        // Have to use weird printf <| sprintf form so VSTest doesn't insert newlines where we don't want
        printfn "%s" <| sprintf "Running %d asyncs (%d round-trips) in parallel." numAsyncs (numAsyncs * 3)
        let rets =
            makeSquares cn numAsyncs maxWait
            |> inAnyOrder
            |> Async.RunSynchronously
        for x in rets do
            printf "%s" <| sprintf "%d, " x 
            Assert.IsTrue(let sqrt = Math.Sqrt(float x) in sqrt = Math.Round(sqrt))
        printfn "%s" <| sprintf "Took: %A." sw.Elapsed

    [<Test>]
    let NanoParallelAnyOrder() = nanoParallelWild 10 1000

    [<Test>]
    let NanoParallelAnyOrderNoWait() = nanoParallelWild 20 0

    [<Test>]
    let NanoParallelForkJoin() =
        use __ = new ServerNode(1500)
        let cn = new ClientNode("127.0.0.1", 1500)
        let sw = Stopwatch.StartNew()
        let sqr x = x * x
        let numSquares = 20
        let rets =
            makeSquares cn numSquares 0
            |> Async.Parallel
            |> Async.RunSynchronously
        printfn "%s" <| sprintf "%d asyncs (%d round-trips) in parallel took: %A." numSquares (numSquares * 3) sw.Elapsed
        Assert.AreEqual(rets, [|1..numSquares|] |> Array.map sqr)


