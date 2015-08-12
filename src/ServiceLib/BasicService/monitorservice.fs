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
        monitorservice.fs
  
    Description: 
        Implementing a generic service to monitor certain web services/action. 
    The service/action will be repeatedly executed, with the performance information 
    printing in a monitor service. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        May. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.CoreServices

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Net
open System.Net.Sockets
open System.Net.Http
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Network
open Prajna.Core
open Prajna.Service
open Prajna.Service.CoreServices.Data

open Prajna.Service.FSharp

/// <summary>
/// This class contains the parameter used to start the monitor service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// </summary> 
[<AllowNullLiteral; Serializable>]
type MonitorServiceParam() =
    inherit WorkerRoleInstanceStartParam()
    /// Monitor Interval value in milliseconds, all gateways/servers are monitored within the interval
    member val MonitorInternvalInMilliSecond = 5000 with get, set
    /// A table of servers to be monitored and port opened (we can monitor the port and check if the server is alive )
    member val ServerTable = ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase) with get, set
    /// A set of servers to be monitored 
    member val ServerInfo = ContractServersInfo() with get
    /// A function to be called to monitor a specific endpoint. 
    /// The input parameter is a encoded IPEndPoint that can be decoded via  LocalDNS.Int64ToIPEndPoint( sig64 )
    /// The output tuple is: a RTT of the endpoint, perf of service, and service status
    member val MonitorFunc : Func<int64, int*int*string> = null with get, set
    /// Load a table of servers to be monitored and port opened (we can monitor the port and check if the server is alive )
    member x.LoadServerTable( filename) = 
        let str = FileTools.ReadFromFile( filename )
        for line in str.Split( Environment.NewLine.ToCharArray() ) do 
            if not (line.StartsWith( @"//" )) then 
                let info = line.Split()
                if info.Length >= 2 then 
                    let server = info.[0]
                    let bParse, port = Int32.TryParse( info.[1] )
                    if bParse && port > 0 then 
                        x.ServerTable.GetOrAdd( server, port ) |> ignore 
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Server %s to monitor port %d" server port ))
    /// A function that get the name of the log file 
    member val GetMonitorLogName = Func<int64, string>( MonitorServiceParam.DefaultGetMonitorLogName ) with get, set 
    static member DefaultGetMonitorLogName( ticks ) = 
        let folder = Path.Combine( RemoteExecutionEnvironment.GetLogFolder(), "MonitorService " )
        FileTools.DirectoryInfoCreateIfNotExists folder |> ignore 
        Path.Combine( folder, sprintf "MonitorService_%s.log" (DateTime(ticks).ToString("yyMMdd")) )

/// <summary>
/// This class contains the parameter used to start the monitor service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// Parameter url gives a url to be monitored under the gateway. 
/// Parameter minlength gives a mininal size replied expected from the gateway (if the returned information is smaller, the service is considered failed). 
/// </summary> 
type MonitorWebServiceParam(url, minlength) as x = 
    inherit MonitorServiceParam()
    do
        x.MonitorFunc <- Func<_,_>( MonitorWebServiceParam.MonitorURL x )
    /// The URL to be monitored by the service 
    member val internal MonitorURLString = url with get
    /// The expected size of the return from the gateway 
    member val ExpectedURLLength = minlength with get
    /// Default Name for montioring the service 
    static member val ServiceName = "MonitorWebService" with get
    /// Default contract name for retreving a set of monitored service
    static member val GetServicePerformanceContractName = "GetMonitoredServicePerformance" with get
    /// Monitor function to be executed
    static member internal MonitorURL (x) ( sig64 ) = 
        let ep = LocalDNS.Int64ToIPEndPoint( sig64 )
        let hostname = LocalDNS.GetHostInfoInt64( sig64 )
        let bExistPort, portMonitor = x.ServerTable.TryGetValue( hostname )
        let mutable rtt = 
            if bExistPort then 
                let ep1 = new IPEndPoint( ep.Address, portMonitor )
                let ticks0 = (PerfADateTime.UtcNowTicks())
                try 
                    use soc = new Socket( AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp );
                    soc.Connect( ep1 )
                    let ticks1 = (PerfADateTime.UtcNowTicks())
                    soc.Close()
                    int( (( ticks1 - ticks0 ) / TimeSpan.TicksPerMillisecond) )
                with
                | e -> 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Failed to connect to %s:%d with %A" hostname portMonitor e  ))
                    -2
                    
            else
                // sprintf "No ping", failed to find a port to ping 
                -1
        let ticks2 = (PerfADateTime.UtcNowTicks())
        let verifyUrl = StringTools.BuildWebName ( "http://" + ep.ToString() ) (x.MonitorURLString) 
        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Monitor %s" verifyUrl ))
        try 
            use httpClient = new HttpClient()
            let response = httpClient.GetStringAsync(verifyUrl )
            let res = response.Result 
            let ticks3 = (PerfADateTime.UtcNowTicks())
            let elapse = int( (( ticks3 - ticks2 ) / TimeSpan.TicksPerMillisecond) )
            if rtt < 0 then rtt <- elapse
            if res.Length > x.ExpectedURLLength then 
                rtt, elapse, sprintf "%dB (OK)" res.Length 
            else
                rtt, elapse, sprintf "%dB (Incomplete)" res.Length
        with 
        | e -> 
            let ticks3 = (PerfADateTime.UtcNowTicks())
            let elapse = int( (( ticks3 - ticks2 ) / TimeSpan.TicksPerMillisecond) )
            rtt, elapse, "(Failed)"

/// <summary>
/// Network Performance statistics of Request
/// </summary>
[<AllowNullLiteral>]
type internal OneServerPerformance(sig64, hostname) = 
    let mutable ticksLast = (PerfADateTime.UtcNowTicks())
    /// IP Endpoint signature of the monitored host
    member val EndPointSignature = sig64 with get
    /// IP Host name of the monitored host
    member val Hostname = hostname with get
    /// Total ticks that has been monitored 
    member val TicksTotal = 0L with get, set
    /// Total ticks that the service is observed working 
    member val TicksServiceAlive = 0L with get, set
    /// Total ticks that the gateway server is observed working 
    member val TicksGatewayAlive = 0L with get, set
    /// <summary> Number of Rtt samples to statistics </summary>
    static member val RTTSamples = 32 with get, set
    member val internal RttArray = Array.zeroCreate<_> OneServerPerformance.RTTSamples with get
    member val internal PerfArray = Array.zeroCreate<_> OneServerPerformance.RTTSamples with get
    /// Filter out 1st RTT value, as it contains initialization cost, which is not part of network RTT
    member val internal RttCount = ref -1L with get
    /// Last RTT from the remote node. 
    member val LastRtt = 0 with get, set
    /// Last Gateway performance
    member val LastPerf = 0 with get, set
    member internal x.Monitor( monFunc: int64 -> int*int*string ) = 
        let rtt, perfgateway, result = monFunc(sig64)
        let ticksCur = (PerfADateTime.UtcNowTicks())
        let elapseTicks = ticksCur - ticksLast
        ticksLast <- ticksCur
        x.TicksTotal <- x.TicksTotal + elapseTicks
        let nOn = result.IndexOf("(OK)", StringComparison.OrdinalIgnoreCase)
        if nOn >=0 then 
            let idx = int (Interlocked.Increment( x.RttCount ) % int64 OneServerPerformance.RTTSamples )
            x.RttArray.[idx ] <- Math.Max( rtt, 0 )
            x.PerfArray.[idx] <- perfgateway
            x.TicksServiceAlive <- x.TicksServiceAlive + elapseTicks
            x.TicksGatewayAlive <- x.TicksGatewayAlive + elapseTicks
        elif rtt >= 0 then 
            x.TicksGatewayAlive <- x.TicksGatewayAlive + elapseTicks       
        x.LastRtt <- rtt
        x.LastPerf <- perfgateway
        sprintf "Monitor Server %s is %s at %s, rtt=%dms, perf=%dms" hostname result (VersionToString(DateTime(ticksCur))) rtt perfgateway
    member internal x.GetPerf() = 
        let mutable cnt = Volatile.Read( x.RttCount )
        if cnt < 0L then 
            1000, 1000
        else
            let div = int (Math.Min( cnt + 1L, int64 OneServerPerformance.RTTSamples ) )
            let mutable rttSum = 0
            let mutable perfSum = 0 
            for idx = 0 to div - 1 do 
                rttSum <- rttSum + x.RttArray.[idx]
                perfSum <- perfSum + x.PerfArray.[idx]
            rttSum / div, perfSum / div

/// <summary>
/// This class represent a instance to monitor the performance and status of other gateways or servers. The developer may extend MonitorInstance class, to implement the missing functions.
/// </summary> 
[<AllowNullLiteral>]
type MonitorInstance< 'StartParamType 
                    when 'StartParamType :> MonitorServiceParam >() =
    inherit WorkerRoleInstance<'StartParamType>()
    let mutable writer : StreamWriter = null
    let mutable writer_date = DateTime.MinValue
    /// Monitor Interval value in milliseconds, all gateways/servers are monitored within the interval
    member val internal MonitorInternvalInMilliSecond = 5000 with get, set
    /// A table of servers to be monitored and port opened (we can monitor the port and check if the server is alive )
    member val internal ServerTable = ConcurrentDictionary<string, int>(StringComparer.OrdinalIgnoreCase) with get, set
    /// A set of servers to be monitored 
    member val internal ServerInfo = null with get, set
    /// Monitored collection
    member val internal MonitoredServers = ConcurrentDictionary<int64, OneServerPerformance>() with get
    /// A function to be called to monitor 
    member val internal MonitorFunc : Func<int64, int*int*string> = null with get, set
    /// A function that get the name of the log file 
    member val internal GetMonitorLogName = null with get, set 
    /// Contract interface to get monitored ServicePerformance 
    member internal x.GetMonitoredServicePerformance() = 
        x.MonitoredServers |> Seq.map ( fun kv -> let perf = kv.Value 
                                                  let ep = LocalDNS.Int64ToIPEndPoint( kv.Key )
                                                  let rtt, pf = perf.GetPerf()
                                                  OneServerMonitoredStatus( HostName = perf.Hostname, 
                                                                            Address = ep.Address.GetAddressBytes(), 
                                                                            Port = ep.Port, 
                                                                            PeriodMonitored = perf.TicksTotal / TimeSpan.TicksPerSecond, 
                                                                            PeriodAlive = perf.TicksServiceAlive / TimeSpan.TicksPerSecond, 
                                                                            GatewayAlive = perf.TicksGatewayAlive / TimeSpan.TicksPerSecond, 
                                                                            RttInMs = rtt, 
                                                                            PerfInMs = pf

                                                                           )
                                                  )

    /// OnStart is run once during the start of the  MonitorInstance
    /// It is generally a good idea in OnStart to copy the specific startup parameter to the local instance. 
    override x.OnStart param =
        // Setup distribution policy
        let mutable bSuccess = true 
        x.ServerInfo <- ContractServerInfoLocal.Parse( param.ServerInfo )
        x.MonitorInternvalInMilliSecond <- param.MonitorInternvalInMilliSecond
        x.MonitorFunc <- param.MonitorFunc
        x.ServerTable <- param.ServerTable
        x.GetMonitorLogName <- param.GetMonitorLogName
        if bSuccess then 
            x.EvTerminated.Reset() |> ignore 
        else
            x.EvTerminated.Set() |> ignore
        ContractStore.ExportSeqFunction( MonitorWebServiceParam.GetServicePerformanceContractName, x.GetMonitoredServicePerformance, -1, true )
        bSuccess 
    override x.Run() = 
        let mutable en = x.MonitoredServers.GetEnumerator()
        let mutable ticksCycleStart = (PerfADateTime.UtcNowTicks())
        let mutable cntCycle = x.MonitoredServers.Count
        let mutable cntCur = 0 
        while not (x.EvTerminated.WaitOne(0)) do 
            // Resolve for server, if there is any 
            x.ServerInfo.DNSResolveOnce()
            let col = x.ServerInfo.GetServerCollection()
            for pair in col do
                x.MonitoredServers.GetOrAdd( pair.Key, fun _ -> OneServerPerformance(pair.Key, pair.Value) ) |> ignore 
            let pairCur = 
                if en.MoveNext() then 
                    cntCur <- cntCur + 1
                    en.Current             
                else
                    en <- x.MonitoredServers.GetEnumerator()
                    ticksCycleStart <- (PerfADateTime.UtcNowTicks())
                    cntCycle <- x.MonitoredServers.Count
                    cntCur <- 0 
                    if en.MoveNext() then 
                        cntCur <- cntCur + 1
                        en.Current
                    else
                        KeyValuePair<_,_>(0L, null)
            if not (x.EvTerminated.WaitOne(0)) && pairCur.Key<>0L then 
                let cur = (PerfDateTime.UtcNow())
                let cur_date = DateTime( cur.Year, cur.Month, cur.Day )
                if cur_date.Ticks > writer_date.Ticks then 
                    if Utils.IsNotNull writer then 
                        writer.Flush()
                        writer.Close()
                        writer <- null
                    writer_date <- cur_date
                    writer <- new StreamWriter( x.GetMonitorLogName.Invoke(writer_date.Ticks), true, System.Text.Encoding.UTF8 )
                let monResult = pairCur.Value.Monitor( x.MonitorFunc.Invoke )
                writer.WriteLine( monResult )
//              Logger.Log( LogLevel.Info, ( monResult  ))
            let ticksCur = (PerfADateTime.UtcNowTicks())
            let elapseMS = int ( ( ticksCur - ticksCycleStart ) / TimeSpan.TicksPerMillisecond )
            let timeWait = if cntCur > cntCycle then 0 else if cntCycle > 0 then x.MonitorInternvalInMilliSecond * cntCur / cntCycle else x.MonitorInternvalInMilliSecond
            if elapseMS < timeWait then 
                x.EvTerminated.WaitOne(timeWait - elapseMS ) |> ignore 
            else
                ()
        if Utils.IsNotNull writer then 
            writer.Flush()
            writer.Close()
            writer <- null
    override x.OnStop() = 
        // Cancel all pending jobs. 
        x.EvTerminated.Set() |> ignore 
    override x.IsRunning() = 
        not (x.EvTerminated.WaitOne(0))



