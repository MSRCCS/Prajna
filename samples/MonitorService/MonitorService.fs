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
        MonitorService.fs
  
    Description: 
        Monitor the performance of gateway/service. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        May. 2015
    
 ---------------------------------------------------------------------------*)
open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Diagnostics
open System.IO
open System.Net
open System.Net.Sockets
open System.Net.Http
open System.Runtime.Serialization
open System.Threading.Tasks
open System.Runtime.InteropServices
open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Core
open Prajna.Tools.Network
open Prajna.Service.CoreServices
open Prajna.Service.FSharp


let Usage = "
    Usage: Monitor the performance of gateways.  \n\
    Command line arguments:\n\
    -start      start monitoring service \n\
    -stop       stop monitoring service \n\
    -gateway    gateways to be monitored  \n\
    -port       port to be monitored  \n\
    -cluster    Clusters used for monitoring service \n\
    -URL        Web page to be montiroed \n\
    "

// Define your library scripting code here
[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let gateways = parse.ParseStrings( "-gateway", [|"imhubr.trafficmanager.net"|] )
    let port = parse.ParseInt( "-port", 80 )
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let monitorURL = parse.ParseString( "-URL", "web/info" )
    let monitorTable = parse.ParseString( "-table", "..\..\MonitorService\monitor_port.txt" )
    let bStart = parse.ParseBoolean( "-start", false )
    let bStop = parse.ParseBoolean( "-stop", false )
    let mutable bExecute = false

    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))

    let bAllParsed = parse.AllParsed Usage

    if bAllParsed then 
        let cluster = 
            if not (StringTools.IsNullOrEmpty PrajnaClusterFile) then 
                Cluster.Start( null, PrajnaClusterFile )
                Cluster.GetCurrent()
            else
                null
        let param = MonitorWebServiceParam( monitorURL, 20 )
        for gateway in gateways do 
            param.ServerInfo.AddServerBehindTrafficManager( gateway, port)
        if not (StringTools.IsNullOrEmpty monitorTable) then 
            let tableFile = 
                if Path.IsPathRooted( monitorTable ) then 
                    monitorTable
                else
                    let curPath = Path.GetDirectoryName( Process.GetCurrentProcess().MainModule.FileName )
                    Path.Combine( curPath, monitorTable )                        
            if File.Exists( tableFile ) then 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Load table %s" tableFile ) )
                param.LoadServerTable( tableFile )   
        if bStart then 
            if Utils.IsNull cluster then 
                RemoteInstance.StartLocal( MonitorWebServiceParam.ServiceName, param, fun _ -> MonitorInstance() )
                while RemoteInstance.IsRunningLocal(MonitorWebServiceParam.ServiceName) do 
                    if Console.KeyAvailable then 
                        let cki = Console.ReadKey( true ) 
                        if cki.Key = ConsoleKey.Enter then 
                            RemoteInstance.StopLocal(MonitorWebServiceParam.ServiceName)
                        else
                            Thread.Sleep(10)
                bExecute <- true 
            else
                RemoteInstance.Start( MonitorWebServiceParam.ServiceName, param, ( fun _ -> MonitorInstance() ) )
                bExecute <- true 
            ()
        elif bStop then 
            RemoteInstance.Stop( MonitorWebServiceParam.ServiceName )
            bExecute <- true 
        Cluster.Stop()
    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
