#r "System.Management.Automation"
#r "Microsoft.Management.Infrastructure"

open System
open System.Collections.Generic
open System.Management.Automation
open Microsoft.Management.Infrastructure

type PowerShell with 
    member this.InvokeAndClear() =
        let ret = this.Invoke()
        this.Commands.Clear()
        ret

type RemoteHost(hostName: string) =
    let ps = PowerShell.Create()
    member this.PS = ps
    member this.HostName = hostName


let testConnection (host: RemoteHost) : bool =
    host.PS.AddCommand("Test-WSMan")
        .AddParameter("ComputerName", host.HostName)
        .InvokeAndClear()
        .Count > 0

let testAllConnections (hosts: RemoteHost[]) : bool =
    hosts
    |> Array.map(fun h -> async {return testConnection h})
    |> Async.Parallel
    |> Async.RunSynchronously
    |> Array.forall (fun x -> x)

type CreateProcessResult = Pid of uint32 | CannotConnect | CannotCreateProcess

let createRemoteProcess (commandLine: string) (host: RemoteHost) =
    if testConnection host then
        let ret = 
            host.PS.AddCommand("Invoke-CimMethod")
                .AddParameter("ClassName", "Win32_Process")
                .AddParameter("MethodName", "Create")
                .AddParameter("Arguments", Dictionary(dict ["CommandLine", commandLine]))
                .AddParameter("ComputerName", host.HostName)
                .InvokeAndClear()
        if (ret.[0].Properties.["ReturnValue"].Value :?> uint32) = 0u then
            Pid(ret.[0].Properties.["ProcessId"].Value :?> uint32)
        else
            CannotCreateProcess
    else
        CannotConnect

type KillResult = Success | ProcessNotFound | ErrorKillingProcess 

let killProcessCore (proc: PSObject) (host: RemoteHost) =
    let killInstanceResult = 
        host.PS
            .AddCommand("Invoke-CimMethod")
            .AddParameter("InputObject", proc)
            .AddParameter("MethodName", "Terminate")
            .InvokeAndClear()
    if killInstanceResult.Count > 0 
        && (killInstanceResult.[0].Properties.["ReturnValue"].Value :?> uint32) = 0u then
            Success
    else
        ErrorKillingProcess

let killRemoteProcess (pid: uint32) (host: RemoteHost) : KillResult =
    let procInstanceResult = 
        host.PS.AddCommand("Get-CimInstance")
            .AddParameter("ClassName", "Win32_Process")
            .AddParameter("Filter", sprintf "ProcessId = %d" pid)
            .AddParameter("ComputerName", host.HostName)
            .InvokeAndClear()
    if procInstanceResult.Count > 0 then
        killProcessCore procInstanceResult.[0] host
    else
        ProcessNotFound

let killRemoteProcessByName (imageBase: string) (host: RemoteHost) : KillResult =
    let procInstanceResults = 
        host.PS.AddCommand("Get-CimInstance")
            .AddParameter("ClassName", "Win32_Process")
            .AddParameter("ComputerName", host.HostName)
            .InvokeAndClear()
    let matchingProcs = 
        let imageBaseLowered = imageBase.ToLower()
        procInstanceResults
        |> Seq.filter (fun proc -> 
            let procImageBase = proc.Properties.["ExecutablePath"].Value :?> string
            procImageBase <> null && procImageBase.ToLower() = imageBaseLowered)
        |> Seq.toList
    let results = matchingProcs |> List.map (fun p -> killProcessCore p host)
    if matchingProcs.Length > 0 then
        if results |> List.forall(fun r -> r = Success) then
            Success
        else
            ErrorKillingProcess
    else
        ProcessNotFound

let machines =
    [|20..35|]
    |> Array.map(fun n -> async {return RemoteHost(sprintf "OneNet%02d" n)})
    |> Async.Parallel
    |> Async.RunSynchronously

let testResults = 
    machines 
    |> Array.map (fun h -> async {return testConnection h})
    |> Async.Parallel
    |> Async.RunSynchronously

testAllConnections machines

let pids =
    machines
    |> Array.map (fun host -> async{ return createRemoteProcess @"c:\Nano\NanoServer.exe 1500" host})
    |> Async.Parallel
    |> Async.RunSynchronously

let allSuccessful = pids |> Seq.forall (function | Pid(_) -> true | _ -> false)

//let killResults =
//    pids 
//    |> Array.map (fun (Pid(pid)) -> pid)
//    |> Array.zip machines
//    |> Array.map (fun (host,pid) -> async {return killRemoteProcess pid host})
//    |> Async.Parallel
//    |> Async.RunSynchronously

let kill() = 
    let killByNameResults =
        machines
        |> Array.map (fun host -> async {return killRemoteProcessByName @"c:\Nano\NanoServer.exe" host})
        |> Async.Parallel
        |> Async.RunSynchronously
    killByNameResults |> Array.forall (function | Success | ProcessNotFound -> true | _ -> false)

let restart() = 
    do
        machines
        |> Array.map (fun host -> async {return killRemoteProcessByName @"c:\Nano\NanoServer.exe" host})
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore
    let pids =
        machines
        |> Array.map (fun host -> async{ return createRemoteProcess @"c:\Nano\NanoServer.exe 1500" host})
        |> Async.Parallel
        |> Async.RunSynchronously
    pids |> Seq.forall (function | Pid(_) -> true | _ -> false)

restart()
            


