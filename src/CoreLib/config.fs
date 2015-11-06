(*---------------------------------------------------------------------------
    Copyright 2013 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.                                                      

    File: 
        config.fs
  
    Description: 
        Getting the configuration of the machine

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core



open System
open System.Net
open System.Collections.Generic
open System.IO
open System.Diagnostics
open System.Security.Cryptography
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.FileTools


/// Machine configuration
type internal Config() =
    static let mutable curNetworkMTU = 
        let nics = NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()
        let mutable MTU = 1000
        for adapter in nics do
            if adapter.Supports( NetworkInformation.NetworkInterfaceComponent.IPv4 ) then 
                let p = adapter.GetIPProperties().GetIPv4Properties()
                MTU <- Math.Max( p.Mtu, MTU )
        MTU

    static let mutable curNetworkSpeed = 
        let nics = NetworkInformation.NetworkInterface.GetAllNetworkInterfaces()
        let mutable speed = 0L
        for adapter in nics do
            if adapter.Speed > speed then 
                speed <- adapter.Speed
        speed

    static let currentMachineName = Environment.MachineName 

    member val MachineName = currentMachineName with get

    member x.NetworkSpeed() = 
        curNetworkSpeed

    member x.NetworkMTU() = 
        curNetworkMTU

    static member CurrentNetworkSpeed with get() = curNetworkSpeed
                                       and set( v ) = curNetworkSpeed <- v

    static member CurrentNetworkMTU with get() = curNetworkMTU
                                     and set( v ) = curNetworkMTU <- v

    static member NetworkSpeedToString( speed ) = 
        match speed with 
        | b when b <= 500000L -> "<Mbps"
        | b when b <= 5000000L -> "1Mbps"
        | b when b <= 50000000L -> "10Mbps"
        | b when b <= 500000000L -> "100Mbps"
        | b when b <= 5000000000L -> "1Gbps"
        | b when b <= 20000000000L -> "10GE"
        | b when b <= 50000000000L -> "40GE"
        | _ -> ">40GE"




    


                                    
/// Read in the master configuration file of Prajna client
/// First line of the master configuration file should be:
/// Version\tMasterName\tPort
/// Each additional line has one executable
type internal ClientMasterConfig internal ()= 
    member val VersionInfo = "" with get, set
    member val MasterName = "" with get, set
    // This is the port for home in
    member val MasterPort = 0 with get, set
    // This is the port for query cluster infor
    member val ControlPort = 0 with get, set
    // Reporting Server List
    member val ReportingServers = List<_>() with get, set
    // Configuration file, no much contention, OK to use Dictionary.
    member val Executables = Dictionary<string, string>() with get
    member val RootDeployPath = "" with get, set
    member val CurPath = Path.GetDirectoryName(Reflection.Assembly.GetExecutingAssembly().Location) with get

    static member Parse( name:string ) =
        let x = ClientMasterConfig()
        if Utils.IsNotNull name && name.Length > 0 then 
            let path = x.CurPath
            let fname = BuildFName path name
            let masterConfigContent = ReadFromFile( fname )
            let sp = masterConfigContent.Split ("\r\n".ToCharArray(), StringSplitOptions.RemoveEmptyEntries )
            let mutable firstLine = true
            let sepchars = "\t ".ToCharArray()
            for line in sp do
                match line with 
                | Prefix @"//" _ -> ()
                | "" -> ()
                | Prefixi @"ReportTo" info ->
                    let arr = info.Split(sepchars,  StringSplitOptions.RemoveEmptyEntries )
                    let mastername = if arr.Length>1 then arr.[0] else null
                    let port = if arr.Length>2 then Int32.Parse(arr.[1]) else 1080
                    x.ReportingServers.Add( (mastername, port ) )
                | s -> 
                    if firstLine then 
                        firstLine <- false
                        let arr = line.Split(sepchars,  StringSplitOptions.RemoveEmptyEntries )
                        if arr.Length>0 then x.VersionInfo <- arr.[0]
                        if arr.Length>1 then x.MasterName <- arr.[1]
                        x.MasterPort <- if arr.Length>2 then Int32.Parse( arr.[2]) else 1080
                        x.ControlPort <- if arr.Length>3 then Int32.Parse( arr.[3]) else 1081
                        x.RootDeployPath <- if arr.Length>4 then arr.[4] else @"\\research\root\Share\jinl\Prajna"
                        x.ReportingServers.Add( (x.MasterName, x.MasterPort ) )
                    else
                        let pos = line.IndexOfAny(sepchars)
                        if pos<0 then 
                            x.Executables.Add( line, "" )
                        else
                            x.Executables.Add( line.Substring(0, pos), line.Substring( pos+1, line.Length-pos-1).TrimStart() )
        x
    static member internal ToParse( parse: ArgumentParser) = 
        let x = ClientMasterConfig()
        let mutable bFurtherReport = true
        let mutable mport = DeploymentSettings.MasterPort
        let mutable cport = DeploymentSettings.ControlPort
        while bFurtherReport do
            mport <- parse.ParseInt( "-mport", mport )
            cport <- parse.ParseInt( "-cport", cport )
            let server = parse.ParseString( "-homein", "" )
            if Utils.IsNotNull server && server.Length>0 then 
                x.MasterName <- server
                x.VersionInfo <- DeploymentSettings.LibVersion
                x.MasterPort <- mport
                x.ControlPort <- cport
                x.RootDeployPath <- ""
                x.ReportingServers.Add( x.MasterName, x.MasterPort ) 
            else
                bFurtherReport <- false
        x

/// Threads 
type internal ThreadsInformation() = 
    static member ThreadCollectionInfo() = 
        let proc = Process.GetCurrentProcess()
        let threads = proc.Threads
        let infos = [|
                for thread in threads do
                    if Utils.IsNotNull thread then
                        yield sprintf "%d(%A)" (thread.Id) (thread.ThreadState)
            |]
        ( String.concat ", " infos )

type internal MemoryStatus() = 
    static let mutable maxMemory = System.Diagnostics.Process.GetCurrentProcess().WorkingSet64>>>20
    static member CurMemoryInMB with get() = System.Diagnostics.Process.GetCurrentProcess().WorkingSet64>>>20
    static member CheckMemoryPressure() =
        let curMemory = MemoryStatus.CurMemoryInMB
        if curMemory > maxMemory then 
            maxMemory <- curMemory
            true
        else
            false
    static member MaxUsedMemoryInMB with get()=maxMemory
