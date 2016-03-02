// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open System.Threading
open System.Net
open System.Net.Sockets
open System.Diagnostics
open Prajna.Nano

let hello() =
    use client = new ClientNode(ServerNode.GetDefaultIP(), 1500)
    client.AsyncNewRemote(fun _ -> printfn "Hello") |> Async.RunSynchronously |> ignore

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

let resetTiming, time =
    let sw = Stopwatch()
    (fun () -> sw.Restart()), (fun (msg: string) -> printfn "%s: %A" msg sw.Elapsed; sw.Restart())


let broadcastCluster() =
    Prajna.Tools.BufferListStream<byte>.BufferSizeDefault <- 1 <<< 20

    resetTiming()
    let ips = getOneNetClusterIPs [21..35]
    time "Getting IPs"

    let clients = 
        ips 
        |> List.toArray 
        |> Array.map(fun ip -> async{ return new ClientNode(ip, 1500) })
        |> Async.Parallel
        |> Async.RunSynchronously
    time "Connecting"

    let broadcaster = Broadcaster(clients)
    time "Starting broadcaster"

    let d = 
        broadcaster.BroadcastParallel(fun _ ->
            let m = Environment.MachineName
            printfn "Hello from %s!" m
            m)
        |> Async.RunSynchronously
    time "Broadcasting machine name fetch"

    printfn "Machine names: %A" (d.Remotes |> Array.map (fun r -> r.AsyncGetValue()) |> Async.Parallel |> Async.RunSynchronously )

    resetTiming()
    let longs = Array.init 5 (fun _ -> 
                    Array.init 100000000 (fun i -> i)
                ) 
    time "Initializing arrays"

    resetTiming()
    let mbs = [for arr in longs -> 
                (float(arr.Length * 8) / 1000000.0 
              )] |> List.sum
    printfn "Broadcasting %2.2fMB" mbs
    let arrs = broadcaster.BroadcastChained(fun _ -> printfn "Received longs"; longs) |> Async.RunSynchronously
    time (sprintf "Broadcast %2.2fMB" mbs)

let latency() =

    printfn "Starting"
    let client = new ClientNode( getOneNetClusterIPs [21] |> Seq.nth 0, 1500 )
    let r = client.AsyncNewRemote(fun _ -> 1) |> Async.RunSynchronously
    time "Connected and created"

    do r.AsyncGetValue() |> Async.RunSynchronously |> ignore
    time "First get"

    let numTrips = 200
    resetTiming()
    let vals = Array.init numTrips (fun _ -> r.AsyncGetValue() |> Async.RunSynchronously)
    time (sprintf "%d round trips" numTrips)


[<EntryPoint>]
let main argv = 

    do Prajna.Tools.Logger.ParseArgs([|"-verbose"; "info"; "-con"|])

    broadcastCluster()

    0 // return an integer exit code
