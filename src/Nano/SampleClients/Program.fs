// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open System.Net
open System.Diagnostics
open Prajna.Nano

let hello() =
    use client = new ClientNode(ServerNode.GetDefaultIP(), 1500)
    client.NewRemote(fun _ -> printfn "Hello") |> ignore

let helloParallel() =
    let clients = [1500..1503] |> List.map (fun p -> new ClientNode(ServerNode.GetDefaultIP(), p)) |> List.toArray
    try
        clients 
        |> Array.mapi (fun i c -> c.AsyncNewRemote(fun _ -> printfn "Hello at Machine----: %d!" i; i))
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
        ()
    finally
        clients |> Array.iter (fun c -> (c :> IDisposable).Dispose())

let broadcast() =
    let clients = [1500..1503] |> List.map (fun p -> new ClientNode(ServerNode.GetDefaultIP(), p)) |> List.toArray
    try
        async {
            let broadcaster = new Broadcaster(clients)
            let data = [|1L..80000000L|]
            let sw = Stopwatch.StartNew()
            let! distributed = broadcaster.BroadcastParallel(fun _ -> 
                let pid = Process.GetCurrentProcess().Id
                printfn "PID: %d" pid
                data.[0] <- int64 pid
                data)
            printfn "Broadcasting took: %A" sw.Elapsed
            do! distributed.Apply(fun data -> printfn "First element is: %d" data.[0]) |> Async.Ignore

        }
        |> Async.RunSynchronously
    finally
        clients |> Array.iter (fun c -> (c :> IDisposable).Dispose())

let getOneNetClusterIPs(machines: int list) =
    machines 
    |> List.map(fun m -> 
        Dns.GetHostAddresses("OneNet" + m.ToString()) 
        |> Seq.filter(fun ad -> ad.AddressFamily.ToString() = System.Net.Sockets.ProtocolFamily.InterNetwork.ToString())
        |> Seq.nth 0)
    |> Seq.toList

let startTiming, time =
    let sw = Stopwatch()
    (fun () -> sw.Restart()), (fun (msg: string) -> printfn "%s: %A" msg sw.Elapsed; sw.Restart())

[<EntryPoint>]
let main argv = 

    Prajna.Tools.BufferListStream<byte>.BufferSizeDefault <- 1 <<< 23

    startTiming()
    let ips = getOneNetClusterIPs [20..35]
    time "Getting IPs"

    let clients = ips |> List.map(fun ip -> new ClientNode(ip, 1500))
    time "Connecting"

    let broadcaster = Broadcaster(clients |> List.toArray)
    let d = 
        broadcaster.BroadcastParallel(fun _ ->
            let m = Environment.MachineName
            printfn "Hello from %s!" m
            m)
        |> Async.RunSynchronously
    time "Broadcasting machine name fetch"

    printfn "Machine names: %A" (d.Remotes |> Array.map (fun r -> r.GetValue()))

    let longs = [|1..100000000|] 

    startTiming()
    let mbs = (float(longs.Length * 8) / 1000000.0)
    printfn "Broadcasting %2.2fMB" mbs
    let arrs = broadcaster.BroadcastParallel(fun _ -> printfn "Received longs"; longs) |> Async.RunSynchronously
    time (sprintf "Broadcasted %2.2fMB" mbs)


//    Prajna.Tools.BufferListStream<byte>.BufferSizeDefault <- 1 <<< 24
//
//    broadcast()

    0 // return an integer exit code
