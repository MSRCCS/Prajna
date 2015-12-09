// Learn more about F# at http://fsharp.net
// See the 'F# Tutorial' project for more help.
open System
open System.IO
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

open Prajna.Core
open System.Text.RegularExpressions

[<EntryPoint>]
let main argv = 
    MemoryStreamB.InitSharedPool()

    let parse = ArgumentParser(argv)
    let outputClusterFile = parse.ParseString( "-outcluster", "" )
    let inputClusterFiles = parse.ParseStrings( "-incluster", [||] )
    let inputStartNode = parse.ParseInts( "-instart", [||] )
    let inputEndNode = parse.ParseInts( "-inend", [||] )
    let dumpClusterFile = parse.ParseBoolean( "-dump", false )
    let machinePort = parse.ParseInt( "-port", -1 )
    let machineExt = parse.ParseString( "-nameext", "" )
    let internalIP = parse.ParseStrings( "-intip", [||] )
    let externalIP = parse.ParseStrings( "-extip", [||] )
    let useIP = parse.ParseString("-useip", "") // use particular IP address for machine
    let bWriteIP = parse.ParseBoolean( "-writeip", false )
    let machineID = parse.ParseBoolean( "-id", false )
    let machineName = parse.ParseString( "-name", "" )

    if (dumpClusterFile = true) then
        // dump inputcluster as text to screen
        for i=0 to inputClusterFiles.Length-1 do
            let info, _ = ClusterInfo.Read(inputClusterFiles.[i])
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
        if not (useIP.Equals("")) then
            clientInfo.MachineName <- useIP
            clientInfo.InternalIPAddress <- Array.create 1 ((Net.IPAddress.Parse(useIP)).GetAddressBytes())
        else if (bWriteIP) then
            clientInfo.MachineName <- NetworkSocket.GetIpv4Addr(clientInfo.MachineName).ToString()
        if (externalIP.Length > 0) then
            //clientInfo.ExternalIPAddress <- Array.create 1 ((Net.IPAddress.Parse( externalIP )).GetAddressBytes())
            clientInfo.ExternalIPAddress <- externalIP |> Array.map (fun a -> Net.IPAddress.Parse(a).GetAddressBytes())
        if (internalIP.Length > 0) then
            //clientInfo.InternalIPAddress <- Array.create 1 ((Net.IPAddress.Parse( internalIP )).GetAddressBytes())
            clientInfo.InternalIPAddress <- internalIP |> Array.map (fun a -> Net.IPAddress.Parse(a).GetAddressBytes())
        if not (machineName.Equals("")) then
            clientInfo.MachineName <- machineName
        if (machineID) then
            let rnd = new System.Random()
            let b = Array.zeroCreate<byte>(sizeof<uint64>)
            rnd.NextBytes(b)
            clientInfo.MachineID <- BitConverter.ToUInt64(b, 0)
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
                            let clusterInfo, _ = ClusterInfo.Read inputClusterFile
                            match clusterInfo with
                            | None -> printfn "File %A not found and ignored" inputClusterFile; None
                            | Some info -> 
                                let start = if (index < inputStartNode.Length) then inputStartNode.[index] else 0
                                let stop = if (index < inputEndNode.Length) then inputEndNode.[index] else info.ListOfClients.Length-1
                                Some(Array.sub info.ListOfClients start (stop-start+1)))
            |> Seq.concat
            |> Seq.toArray
        if (File.Exists outputClusterFile) then
            Console.Write("File {0} exists, overwrite:", outputClusterFile)
            let resp = Console.ReadLine()
            if (resp.ToLower().[0] = 'y') then
                File.Delete(outputClusterFile)
                clusterInfo.Save(outputClusterFile)
            else
                Console.WriteLine("File exists, not saving")
        else
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
