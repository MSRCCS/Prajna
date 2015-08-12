// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.
open System
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

open Prajna.Core
open System.Text.RegularExpressions

[<EntryPoint>]
let main argv = 
    let parse = ArgumentParser(argv)
    let outputClusterFile = parse.ParseString( "-outcluster", "" )
    let inputClusterFiles = parse.ParseStrings( "-incluster", [||] )
    let inputStartNode = parse.ParseInts( "-instart", [||] )
    let inputEndNode = parse.ParseInts( "-inend", [||] )
    let dumpClusterFile = parse.ParseBoolean( "-dump", false )
    let machinePort = parse.ParseInt( "-port", -1 )
    let machineExt = parse.ParseString( "-nameext", "" )
    let internalIP = parse.ParseString( "-intip", "" )
    let externalIP = parse.ParseString( "-extip", "" )
    let bWriteIP = parse.ParseBoolean( "-writeip", false )

    if (dumpClusterFile = true) then
        // dump inputcluster as text to screen
        for i=0 to inputClusterFiles.Length-1 do
            let info = ClusterInfo.Read(inputClusterFiles.[i])
            match info with
                | None -> printfn "File %A not found" inputClusterFiles.[i]
                | Some(x) ->
                    printfn "File %A" inputClusterFiles.[i]
                    printfn "%s" (x.DumpString())
    else if (inputClusterFiles.Length = 0) then
        // save computer cluster info to .lst file
        let config = new DetailedConfig()
        // last two params don't matter for this
        let clientStatus = ClientStatus.Create(config, "", "")
        clientStatus.Name <- clientStatus.Name + machineExt
        // populate missing fields by calling ToString which populates missing fields
        let configStr = clientStatus.ToString()
        let mutable clientInfo = new ClientInfo(clientStatus)
        // overwrite default listening port of 1082 if desired
        if (machinePort >= 0) then
            clientInfo.MachinePort <- machinePort
        if (bWriteIP) then
            clientInfo.MachineName <- NetworkSocket.GetIpv4Addr(clientInfo.MachineName).ToString()
        if (externalIP <> "") then
            clientInfo.ExternalIPAddress <- Array.create 1 ((Net.IPAddress.Parse( externalIP )).GetAddressBytes())
        if (internalIP <> "") then
            clientInfo.InternalIPAddress <- Array.create 1 ((Net.IPAddress.Parse( internalIP )).GetAddressBytes())
        let clusterInfo = new ClusterInfo()
        clusterInfo.Version <- (DateTime.UtcNow)
        clusterInfo.Name <- outputClusterFile.Substring(0, outputClusterFile.LastIndexOf(".inf"))
        clusterInfo.ListOfClients <- Array.zeroCreate<ClientInfo>(1)
        clusterInfo.ListOfClients.[0] <- clientInfo
        clusterInfo.Save(outputClusterFile)
    else
        // read multiple cluster and save file
        let clusterInfo = new ClusterInfo()
        clusterInfo.Version <- (DateTime.UtcNow)
        clusterInfo.Name <- outputClusterFile.Substring(0, outputClusterFile.LastIndexOf(".inf"))
        let m = Regex.Match(clusterInfo.Name, """(.*)(\\|\/)(.*)""")
        if (m.Success) then
            clusterInfo.Name <- m.Groups.[3].Value
        let inputClusterFilesI = inputClusterFiles |> Seq.mapi (fun i x-> (i,x))
        clusterInfo.ListOfClients <-
            inputClusterFilesI
            |> Seq.choose (fun (index, inputClusterFile) -> 
                            match ClusterInfo.Read inputClusterFile with
                            | None -> printfn "File %A not found and ignored" inputClusterFile; None
                            | info -> 
                                let start = if (index < inputStartNode.Length) then inputStartNode.[index] else 0
                                let stop = if (index < inputEndNode.Length) then inputEndNode.[index] else info.Value.ListOfClients.Length-1
                                Some(Array.sub info.Value.ListOfClients start (stop-start+1)))
            |> Seq.concat
            |> Seq.toArray
        clusterInfo.Save(outputClusterFile)

    0 // return an integer exit code

(*
[<EntryPoint>]
let main2 argv =
    let name = "VER"
    let machineName = "ver"
    let pattern = "^"+machineName+"""(\.|$)"""
    printfn "PATTERN: %s" pattern
    if Regex.Match(name, pattern, RegexOptions.IgnoreCase).Success then
        printfn "MATCH"
    else
        printfn "NO MATCH"
    0
*)
