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
		monitornetwork.fs
  
	Description: 
		Implementing a generic service to monitor network interface. The service will 
    send a deterministic amount of traffic trhough each network interface, and record the 
    ammount of network traffic send/received from each. 

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
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Network
open Prajna.Core
open Prajna.Service.CoreServices.Data

open Prajna.Service.FSharp

/// <summary>
/// This class contains the parameter used to start the monitor network service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// </summary> 
[<AllowNullLiteral; Serializable>]
type MonitorNetworkParam() =
    inherit WorkerRoleInstanceStartParam()
    static member val MonitorNetworkServiceName = "NetworkService" with get
    /// Monitor Interval value in milliseconds, all gateways/servers are monitored within the interval
    member val MonitorInternvalInMilliSecond = 5000 with get, set
    /// Object to be used in MonitorFunc
    member val MonitorFuncObject : Object = null with get, set
    /// A function to be called to decite if a specific network interface is to be monitored.  
    /// MonitorFunc takes three parameter: 
    /// epRemote: remote IP EndPoint
    /// epLocal: local IP EndPoint
    /// obj: Object, 
    /// and evaluate if a remote connection to addr:port needed to be inserted with testing traffic and monitored. 
    /// The function returns a int64 value trafficValue. If trafficValue is less than 0, the port will not be monitored. If trafficValue is larger than 0, 
    /// a network flow of trafficValue bit per second is put on the network interface. If trafficValue is equal to zero, the port is monitored, but no 
    /// additional load traffic is put on the network. 
    member val MonitorFunc : Func<IPEndPoint, IPEndPoint, Object, int64> = null with get, set
    /// Writing echo stream length
    member val EchoStreamLength = 102400 with get, set
    /// A function that get the name of the log file 
    member val GetMonitorLogName = Func<int64, string>( MonitorNetworkParam.DefaultGetMonitorLogName ) with get, set 
    /// Maximum bytes pending in network queue, when this amount is reached, we don't send more traffic 
    member val MaxBytesInNetworkQueue = 10240000L with get, set
    static member DefaultGetMonitorLogName( ticks ) = 
        let folder = Path.Combine( RemoteExecutionEnvironment.GetLogFolder(), "MonitorNetwork " )
        FileTools.DirectoryInfoCreateIfNotExists folder |> ignore 
        Path.Combine( folder, sprintf "MonitorNetwork_%s.log" (DateTime(ticks).ToString("yyMMdd")) )

/// <summary>
/// This class represent a instance to monitor the network interface. The developer may extend MonitorInstance class, to implement the missing functions.
/// </summary> 
[<AllowNullLiteral>]
type MonitorNetworkInstance< 'StartParamType 
                    when 'StartParamType :> MonitorNetworkParam >() =
    inherit WorkerRoleInstance<'StartParamType>()
    let mutable writer : StreamWriter = null
    let mutable writer_date = DateTime.MinValue
    let trafficMatrix = ConcurrentDictionary<int64, int64 ref>()
    let monitorMatrix = ConcurrentDictionary<int64, int64 ref>()
    let monitorTrafficMatrix = ConcurrentDictionary<int64, (int64*int64)>()
    /// Monitor Interval value in milliseconds, all gateways/servers are monitored within the interval
    member val internal MonitorInternvalInMilliSecond = 5000 with get, set
    /// Object to be used in MonitorFunc
    member val internal MonitorFuncObject : Object = null with get, set
    /// A function to be called to decite if a specific network interface is to be monitored.  
    /// MonitorFunc takes three parameter: 
    /// addr: IPAddress, 
    /// port: int,
    /// obj: Object, 
    /// and evaluate if a remote connection to addr:port needed to be inserted with testing traffic and monitored. 
    /// The function returns a int64 value trafficValue. If trafficValue is less than 0, the port will not be monitored. If trafficValue is larger than 0, 
    /// a network flow of trafficValue bit per second is put on the network interface. If trafficValue is equal to zero, the port is monitored, but no 
    /// additional load traffic is put on the network. 
    member val internal MonitorFunc : Func<IPEndPoint, IPEndPoint, Object, int64> = null with get, set
    /// Writing echo stream length
    member val internal EchoStreamLength = 102400 with get, set
    /// A function that get the name of the log file 
    member val internal GetMonitorLogName = null with get, set 
    /// Maximum bytes pending in network queue, when this amount is reached, we don't send more traffic 
    member val internal MaxBytesInNetworkQueue = 10240000L with get, set
    /// OnStart is run once during the start of the  MonitorInstance
    /// It is generally a good idea in OnStart to copy the specific startup parameter to the local instance. 
    override x.OnStart param =
        // Setup distribution policy
        let mutable bSuccess = true 
        trafficMatrix.Clear() 
        x.MonitorInternvalInMilliSecond <- param.MonitorInternvalInMilliSecond
        x.MonitorFuncObject <- param.MonitorFuncObject
        x.MonitorFunc <- param.MonitorFunc
        x.EchoStreamLength <- param.EchoStreamLength
        x.GetMonitorLogName <- param.GetMonitorLogName
        x.MaxBytesInNetworkQueue <- param.MaxBytesInNetworkQueue
        if bSuccess then 
            x.EvTerminated.Reset() |> ignore 
        else
            x.EvTerminated.Set() |> ignore
        bSuccess 
    override x.Run() = 
        let mutable ticksCycleStart = (PerfADateTime.UtcNowTicks())
        let mutable cntCur = 0 
        /// Generate a 102400B stream that to be written out. 
        let msStream = new MemStream( x.EchoStreamLength )
        let rnd = System.Random(0)
        let byt = Array.zeroCreate( x.EchoStreamLength )
        rnd.NextBytes( byt )
        msStream.Write( byt, 0, x.EchoStreamLength )


        while not (x.EvTerminated.WaitOne(0)) do 
            let ticksCycleStart = (PerfDateTime.UtcNowTicks())
            // Resolve for server, if there is any 
            for queue in NetworkConnections.Current.GetAllChannels() do
                let epRemote = queue.RemoteEndPoint :?> IPEndPoint
                let epLocal = queue.LocalEndPoint :?> IPEndPoint
                let sig64 = queue.RemoteEndPointSignature
                let monitorValue = 
                    if Utils.IsNull x.MonitorFunc then 
                        40000000000L // Insert a 40 Gbps traffic
                    else
                        let traffic = x.MonitorFunc.Invoke( epRemote, epLocal, x.MonitorFuncObject )
                        Math.Min( traffic, 40000000000L )
                if monitorValue >= 0L && not (x.EvTerminated.WaitOne(0)) then 
                    // Only monitor if the monitor value is larger than or equal to 0L
                    // Determine how many traffic to send at this moment. 
                    if monitorValue > 0L then 
                        let refTicks = trafficMatrix.GetOrAdd( sig64, ref ((PerfADateTime.UtcNowTicks()) - TimeSpan.TicksPerMinute) )
                        let oldValue = !refTicks
                        let curValue = (PerfADateTime.UtcNowTicks())
                        // Avoid value overflow in int64 math
                        let trafficToSend = Convert.ToInt64( float monitorValue * float ( curValue - oldValue ) / float TimeSpan.TicksPerSecond )
                        if trafficToSend >= int64 x.EchoStreamLength && 
                            Interlocked.CompareExchange( refTicks, curValue, oldValue ) = oldValue then 
                            // Only insert filler packet when monitor Value is larger than 0L
                            let mutable sendLen = 0L

                            //while queue.CanSend && queue.UnProcessedCmdInBytes < x.MaxBytesInNetworkQueue && sendLen <= trafficToSend do
                            while queue.CanSend && sendLen <= trafficToSend do
                                queue.ToSend(new ControllerCommand(ControllerVerb.Unknown,ControllerNoun.Unknown), msStream )
                                sendLen <- sendLen + int64 x.EchoStreamLength
                    
                    // Monitoring. 
                    if not (x.EvTerminated.WaitOne(0)) then 
                        let cur = (PerfDateTime.UtcNow())
                        let cur_date = DateTime( cur.Year, cur.Month, cur.Day )
                        if cur_date.Ticks > writer_date.Ticks then 
                            lock( x ) ( fun _ -> 
                                if Utils.IsNotNull writer then 
                                    writer.Flush()
                                    writer.Close()
                                    writer <- null
                                writer_date <- cur_date
                                writer <- new StreamWriter( x.GetMonitorLogName.Invoke(writer_date.Ticks), true, System.Text.Encoding.UTF8 )
                                )
                        let refTicks = monitorMatrix.GetOrAdd( sig64, ref DateTime.MinValue.Ticks )
                        let curValue = (PerfADateTime.UtcNowTicks())
                        let oldValue = !refTicks
                        let elapseInMs = (curValue - oldValue )/ TimeSpan.TicksPerMillisecond
                        if elapseInMs > int64 x.MonitorInternvalInMilliSecond && 
                            Interlocked.CompareExchange( refTicks, curValue, oldValue )=oldValue then 
                            let curSend = queue.TotalBytesSent
                            let curRcvd = queue.TotalBytesRcvd
                            if oldValue = DateTime.MinValue.Ticks then 
                                // First time, don't monitor
                                monitorTrafficMatrix.Item(sig64) <- (curSend, curRcvd)
                            else
                                let tuple = monitorTrafficMatrix.GetOrAdd( sig64, (curSend, curRcvd) )
                                let oldSend, oldRcvd = tuple
                                monitorTrafficMatrix.Item(sig64) <- (curSend, curRcvd)
                                let sentBytes = curSend - oldSend
                                let rcvdBytes = curRcvd - oldRcvd
                                let sendThroughput = float sentBytes / 128. /1024. * 1000. / float elapseInMs // Mbps
                                let rcvdThroughput = float sentBytes / 128. /1024. * 1000. / float elapseInMs // Mbps
                                let monResult = sprintf "Network queue from %s to %s, traffic is %.2f Mbps (send) %.2f Mbps (rcvd)" 
                                                        (epLocal.ToString()) (LocalDNS.GetShowInfo(epRemote)) sendThroughput rcvdThroughput
                                writer.WriteLine( monResult )
                //              Logger.Log(LogLevel.Info, ( monResult  ))
                    let t2 = (PerfDateTime.UtcNowTicks())
                    let elapseMS = int ( ( t2 - ticksCycleStart ) / TimeSpan.TicksPerMillisecond )
                    let timeWait = x.MonitorInternvalInMilliSecond
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



