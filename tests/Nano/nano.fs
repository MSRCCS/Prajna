namespace Nano.Tests

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
        let cn = new ClientNode("127.0.0.1", 1500)
        cn.NewRemote(fun _ -> 5) |> ignore

    [<Test>]
    let NanoGetValue() = 
        use __ = new ServerNode(1500)
        let cn = new ClientNode("127.0.0.1", 1500)
        let r = cn.NewRemote(fun _ -> "Test")
        Assert.AreEqual(r.GetValue(), "Test")

    [<Test>]
    let NanoRunRemote() = 
        use __ = new ServerNode(1500)
        let cn = new ClientNode("127.0.0.1", 1500)
        let r = cn.NewRemote(fun _ -> "Test")
        let r2 = r.Run(fun str -> str.Length)
        Assert.AreEqual(r2.GetValue(), 4)

