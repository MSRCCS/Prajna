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
        backend.fs
  
    Description: 
        Back end of the distributed query service. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jan. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.ServiceEndpoint

open System
open System.IO
open System.Net
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Prajna.Core
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

open Prajna.Service.FSharp

/// <summary>
/// Query execution mode, what are the possible execution 
/// In all execution, a request is uniquely identified by a Guid, a timebudget (in millisecond), and a request. 
/// </summary>
type QueryExecutionMode<'Request, 'Reply> = 
    // Query through a synchronous executed function, 
    // the function returns a reply for a certain request. 
    | QueryExecutionByFunc of ( Guid*int*'Request -> 'Reply )
    // Query through a callback function. 
    // When the reply is ready, the callback function (1st func) is called to present the reply to the QueryService
    // If there is error in processing, the error function (2nd func) is called to present an error string. 
    | QueryByCallback of ( Guid*int*'Request*('Reply -> unit) *(string->unit)-> unit )
    // Query execution by a Task
    | QueryByTask of ( Guid*int*'Request -> Task<'Reply> )
    | UndefinedQueryExecution

/// <summary> 
/// Front end collection
/// </summary>
[<Serializable>]
type FrontEndServer = 
    /// <summary> represent Front end server as a Cluster cluster file (we don't use Cluster here for Serializability) </summary>
    | Cluster of string * int
    /// <summary> represent Front end server by servername: port </summary>
    | Server of string * int
    /// <summary> Front end server is servername: port, which may be resolved to a collection of servers. Repeated DNS requests are called to resolve the server </summary> 
    | TrafficManager of string * int

/// <summary>
/// Non Generic form of service instance. 
/// </summary>
[<AbstractClass; AllowNullLiteral>]
type ServiceInstanceBasic() = 
    /// A Guid that uniquely identifies the domain of the service
    member val ServiceID = Guid.Empty with get, set
    /// Input Schema that determines the coding of the input of data
    member val InputSchemaID = Guid.Empty with get, set
    /// Output Schema that determines the coding of the output of data
    member val OutputSchemaID = Guid.Empty with get, set
    /// <summary>
    /// Maximum number of items that can be serviced by this instance, if 1, the class is being locked when it service one existing item. 
    /// If this is a large value, e.g., Int32.MaxValue, then there is no limit on number of service that can be executed on the service instance. 
    /// </summary>
    member val MaxConcurrentItems = 1 with get, set 
    /// <summary> 
    /// Current number of items in service, a reference value
    /// </summary>
    member val CurrentItems = ref 0 with get
    /// <summary> 
    /// Grab one instance to process request, should call Release after processing. 
    /// </summary> 
    member x.GrabOneInstance() = 
        if (!x.CurrentItems >= x.MaxConcurrentItems ) then 
            false
        else
            let nitems = Interlocked.Increment( x.CurrentItems ) 
            let bSuccess = nitems <= x.MaxConcurrentItems
            if not bSuccess then 
                Interlocked.Decrement( x.CurrentItems )  |> ignore 
            bSuccess
    /// <summary> 
    /// Release one instance, reverse of GrabOneInstance()
    /// </summary> 
    member x.ReleaseOneInstance() =
        Interlocked.Decrement( x.CurrentItems ) |> ignore 
    /// <summary> 
    /// Process a request 
    /// </summary>
    abstract ProcessRequest: Guid * Object * int * ( Object -> unit ) * (string -> unit ) * (unit -> unit ) -> unit
    override x.ToString() = 
        sprintf "service %A (%d of %d)" x.ServiceID (!x.CurrentItems) x.MaxConcurrentItems

/// <summary> 
/// Service capacity of a node
/// </summary> 
type ServiceCapacity = 
    struct
        /// Total capacity
        val Capacity : int
        /// Number of items under service 
        val CurrentItems : int ref
        /// Total item serviced 
        val TotalItems : int64 ref
        internal new ( capacity ) = { Capacity = capacity; CurrentItems = ref 0; TotalItems = ref 0L }
        override x.ToString() = sprintf "total:%d %d/%d" (!x.TotalItems) (!x.CurrentItems) (x.Capacity)
    end

    


/// <summary>
/// Service instance define one instance class that serves request and reply. 
/// </summary>
[<AllowNullLiteral>]
type ServiceInstance<'Request, 'Reply>() = 
    inherit ServiceInstanceBasic()
    /// <summary> 
    /// Process a request, proper function will be executed to get the reply, 
    /// If function completes, replyFunc is called. 
    ///    function fails, errorFunc is called with a message. 
    /// always at end, finishFunc is called. 
    /// </summary>
    override x.ProcessRequest( reqID, reqObject, timebugdetInMs, replyFunc, errorFunc, finishFunc ) = 
        try
            if timebugdetInMs >=0 then 
                match reqObject with 
                | :? 'Request as req -> 
                    match x.ExecutionMode with 
                    | QueryExecutionByFunc func -> 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "execute query %A via QueryExecutionByFunc ..." 
                                                                               reqID ))
                        let reply = func ( reqID, timebugdetInMs, req ) 
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "reply for query %A via QueryExecutionByFunc obtained ..." 
                                                                               reqID ))
                        replyFunc reply
                        finishFunc()
                    | QueryByCallback (processFunc )-> 
                        let wrappedCallback o =
                            replyFunc o
                            finishFunc() 
                        let wrappedError s = 
                            errorFunc s
                            finishFunc() 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "execute query %A via QueryByCallback" 
                                                                               reqID ))
                        processFunc( reqID, timebugdetInMs, req, wrappedCallback, wrappedError )
                    | QueryByTask taskFunc -> 
                        let task = taskFunc( reqID, timebugdetInMs, req )
                        let replyTask (inp:Task<'Reply>) = 
                            replyFunc inp.Result
                            finishFunc()
                        let task1 = task.ContinueWith( replyTask )
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "execute query %A via QueryByTask" 
                                                                               reqID ))
                        // We assume task is already started by the taskFunc
                        // task.Start() 
                    | _ -> 
                        // Unknown execution
                        let msg = sprintf "ServiceInstance.ProcessRequest of serviceID %A received object %A, but has not been assigned with an execution mode" 
                                            x.ServiceID 
                                            reqObject 
                        errorFunc msg
                        finishFunc()
                        ()
                | _ -> 
                    let msgError = sprintf "ServiceInstance.ProcessRequest got request object %A that is not of type %s" reqObject (typeof<'Request>.FullName)
                    Logger.Log( LogLevel.Error, (msgError))
                    errorFunc msgError
                    finishFunc()
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "request for query %A timed out, >timelimit = %d ms" 
                                                                       reqID (-timebugdetInMs) ))
                errorFunc (sprintf "Timeout, timebudget = %d ms" timebugdetInMs )
                finishFunc() 
        with 
        | e -> 
            let msgError = sprintf "ServiceInstance.ProcessRequest encounter exception %A" e 
            Logger.Log( LogLevel.Error, (msgError))
            errorFunc msgError
            finishFunc()
    /// <summary> 
    /// A service function 
    /// </summary> 
    member val ExecutionMode = QueryExecutionMode<'Request,'Reply>.UndefinedQueryExecution with get, set

/// Action to serialize the collection of service 
type EncodeCollectionAction = Action<seq<ServiceInstanceBasic>*Stream>
/// Action to deserialize the collection of service 
type DecodeCollectionFunction = Func< Stream, seq<ServiceInstanceBasic> >
   
/// <summary>
/// This class contains the parameter used to start the back end service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// </summary> 
[<AllowNullLiteral; Serializable>]
type BackEndServiceParam() =
    inherit WorkerRoleInstanceStartParam()
    /// Time Interval to attempt to resolve frontend server
    member val DNSResolveIntervalFrontEndInMS = 10000 with get, set
    /// Timeout value (ms), request that are not served in this time interval will be throw away
    /// If debugging, please set a large timeout value, or set TimeOutRequestInMilliSecond < 0 (infinity timeout), otherwise,
    /// request may be dropped during debugging due to timeout. 
    member val TimeOutRequestInMilliSecond = 30000 with get, set
    /// Statistics period, calculate the request-reply performance statistics of recent X seconds. 
    /// If StatisticsPeriodInSecond<=0, we don't keep statistics information. 
    member val StatisticsPeriodInSecond = 600 with get, set
    /// Front end collection represent a collection of Front End servers.
    member val FrontEndCollection = List<FrontEndServer>() with get
    /// <summary> 
    /// Function to encode an array of service instances. 
    /// </summary> 
    member val EncodeServiceCollectionAction : EncodeCollectionAction = null with get, set
    /// <summary> 
    /// Add a Cluster (represented by clusterName) to the front end server collection. 
    /// </summary>
    member x.AddFrontEndCluster ( clusterName, port ) = 
        x.FrontEndCollection.Add( FrontEndServer.Cluster( clusterName, port) ) 
    /// <summary>
    /// Add one server to the front end server collection. 
    /// </summary> 
    member x.AddOneServer ( serverName, port ) = 
        x.FrontEndCollection.Add( FrontEndServer.Server( serverName, port ) )
    /// <summary>
    /// Add one traffic manager to the front end server collection. see http://azure.microsoft.com/en-us/documentation/services/traffic-manager/ for 
    /// traffic manager setup information. Repeated DNS resolve will be used to resolve the server collection behind the traffic manager, and the BackEnd will
    /// attempt to connect to and serve all front end. 
    /// </summary> 
    member x.AddOneTrafficManager ( serverName, port ) = 
        x.FrontEndCollection.Add( FrontEndServer.TrafficManager( serverName, port ) )

/// Function to excute when the backend starts 
type BackEndOnStartFunction<'StartParamType> = Func< 'StartParamType, bool>
/// Additional message from backend to front end 
type FormMsgToFrontEndFunction = Func< unit, seq< ControllerCommand * MemStream > >
/// Network parser for the backend 
type BackEndParseFunction = Func< (NetworkCommandQueue * NetworkPerformance * ControllerCommand * StreamBase<byte>),( bool * ManualResetEvent ) >

/// <summary>
/// This class represent a backend query instance. The developer needs to extend BackEndInstance class, to implement the missing functions.
/// The following command are reserved:
///     List, Buffer : in the beginning, to list Guids that are used by the current backend instance. 
///     Read, Buffer : request to send a list of missing Guids. 
///     Write, Buffer: send a list of missing guids. 
///     Echo, QueryReply : Keep alive, send by front end periodically
///     EchoReturn, QueryReply: keep alive, send to front end periodically
///     Set, QueryReply: Pack serviceInstance information. 
///     Get, QueryReply: Unpack serviceInstance information 
///     Request, QueryReply : FrontEnd send in a request (reqID, serviceID, payload )
///     Reply, QueryReply : BackEnd send in a reply
///     TimeOut, QueryReply : BackEnd is too heavily loaded, and is unable to serve the request. 
///     NonExist, QueryReply : requested BackEnd service ID doesn't exist. 
/// </summary> 
[<AllowNullLiteral; AbstractClass>]
type BackEndInstance< 'StartParamType 
                    when 'StartParamType :> BackEndServiceParam >() =
    inherit WorkerRoleInstance<'StartParamType>()
    let mutable dnsResolveIntervalFrontEndInMS = 10000
    let lastStatisticsQueueCleanRef = ref (PerfADateTime.UtcNowTicks())
    /// <summary> 
    /// Timeout value, in ticks 
    /// </summary>
    member val internal TimeOutTicks = Int64.MaxValue with get, set
    /// <summary> 
    /// Service collection is filled by the user. These are a set of recognition service that are pending. 
    /// </summary>
    member val ServiceCollection = ConcurrentDictionary< Guid, ConcurrentBag<ServiceInstanceBasic>>() with get

    /// <summary> 
    /// Queue in each service
    /// </summary>
    member val ServiceCapacity = ConcurrentDictionary<_,_>() with get

    /// Primary Queue holds serviceID, it indicates the SecondaryServiceQueue that needs to be looked at. 
    member val internal PrimaryQueue = ConcurrentQueue<_>() with get
    member val internal  EvPrimaryQueue = new ManualResetEvent( false ) with get
    /// Service Queue holds request to be processed.  
    member val internal  SecondaryServiceQueue = ConcurrentDictionary<Guid, _>() with get
    /// Statistics Queue holds statistics of the request that is served. 
    member val StatisticsCollection = ConcurrentDictionary<_,_>() with get
    /// Keep statistics of most recent certain seconds worth of requests
    member val StatisticsPeriodInSecond = 600 with get, set
    member val internal bTerminate = false with get, set
    member val internal  InitialMessage = null with get, set
    /// <summary> tap in to parse command from FrontEnd </summary>
    member val OnParse = List<BackEndParseFunction>()  with get, set
    /// <summary>
    /// Add a single service instance. 
    /// </summary>
    member x.AddServiceInstance( serviceInstance: ServiceInstanceBasic ) =
        let bags = x.ServiceCollection.GetOrAdd( serviceInstance.ServiceID, fun _ -> ConcurrentBag<_>()) 
        let mutable bExist = false
        for item in bags do 
            if Object.ReferenceEquals( item, serviceInstance ) then 
                bExist <- true
        if bExist then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Same class (reference equal) Service ID %A is added multiple times to service collection" serviceInstance.ServiceID ))
        else
            bags.Add( serviceInstance )

    /// Programmer will need to extend BackEndInstance class to fill in OnStartBackEnd. The jobs of OnStartBackEnd are: 
    /// 1. fill in ServiceCollection entries. Note that N parallel thread will be running the Run() operation. However, OnStartBackEnd are called only once.  
    /// 2. fill in BufferCache.Current on CacheableBuffer (constant) that we will expect to store at the server side. 
    /// 3. fill in MoreParseFunc, if you need to extend beyond standard message exchanged between BackEnd/FrontEnd
    ///         Please make sure not to use reserved command (see list in the description of the class BackEndInstance )
    ///         Network health and message integrity check will be enforced. So when you send a new message to the FrontEnd, please use:
    ///             health.WriteHeader (ms)
    ///             ... your own message ...
    ///             health.WriteEndMark (ms )
    /// 4. Setting TimeOutRequestInMilliSecond, if need
    /// 5. Function in EncodeServiceCollectionAction will be called to pack all service collection into a stream to be sent to the front end. 
    member val OnStartBackEnd = List< BackEndOnStartFunction<'StartParamType> >() with get
    /// The function is called to define a set of initial command that will be send to each FrontEnd at start.
    member val OnInitialMsgToFrontEnd = List< FormMsgToFrontEndFunction >() with get
    /// Constant string to export/import contract to retrieve Active FrontEnds
    static member val ContractNameActiveFrontEnds = "ActiveFrontEnds" with get
    /// Constant string to export/import contract of request statistics. 
    static member val ContractNameRequestStatistics = "RequestStatistics" with get
    override x.OnStart param =
        // Put code to initialize gateway here. 
        let mutable bSuccess = true 
        for lst in x.OnStartBackEnd do
            bSuccess <- bSuccess && lst.Invoke( param )     
        BufferCache.TimeOutReleaseCacheableBufferInMs <- Int64.MaxValue // For backend, blob in BufferCache never expires. 
        x.bTerminate <- not bSuccess
        if bSuccess then 
            let seqMsg = x.OnInitialMsgToFrontEnd |> Seq.map ( fun del -> del.Invoke() ) |> Seq.collect ( Operators.id )
            let initialMsg = List<_>( seqMsg )
            x.AddFrontEndEntries param
            dnsResolveIntervalFrontEndInMS <- param.DNSResolveIntervalFrontEndInMS
            x.StatisticsPeriodInSecond <- param.StatisticsPeriodInSecond
            if param.TimeOutRequestInMilliSecond<=0 then 
                x.TimeOutTicks <- Int64.MaxValue // no time out
            else
                x.TimeOutTicks <- int64 param.TimeOutRequestInMilliSecond * TimeSpan.TicksPerMillisecond
            
            let guids = BufferCache.Current.GetGuidCollections()
            if guids.Length > 0 then 
                let ms = new MemStream( 16 * guids.Length + 512 )
                BufferCache.PackGuid( guids, ms ) 
                initialMsg.Add( ControllerCommand( ControllerVerb.List, ControllerNoun.Buffer ), ms ) 
            // Initialize service capacity
            for pair in x.ServiceCollection do 
                let serviceID = pair.Key
                let serviceBags = pair.Value
                let mutable totalCapacity = 0 
                for item in serviceBags do 
                    item.CurrentItems := 0 
                    if item.MaxConcurrentItems <=0 then 
                        item.MaxConcurrentItems <- 1
                    if totalCapacity + item.MaxConcurrentItems < totalCapacity then 
                        // We have an overflow 
                        totalCapacity <- Int32.MaxValue
                    else
                        totalCapacity <- totalCapacity + item.MaxConcurrentItems
                if totalCapacity > 0 then 
                    x.ServiceCapacity.Item( serviceID) <- ServiceCapacity(totalCapacity) 
                    x.SecondaryServiceQueue.Item( serviceID ) <- ConcurrentQueue<_>()
                else
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Service collection %A is removed as it has a capacity of <=0 " serviceID  ))
                    x.ServiceCapacity.TryRemove( serviceID ) |> ignore 
            let collections = x.ServiceCollection.Values |> Seq.collect ( Operators.id )
            // Pack information on service 
            let msCollection = new MemStream() 
            param.EncodeServiceCollectionAction.Invoke( collections, msCollection :> Stream ) 
            initialMsg.Add( ControllerCommand( ControllerVerb.Set, ControllerNoun.QueryReply ), msCollection ) 
            x.InitialMessage <- initialMsg.ToArray()
            // Export backend information
            ContractStore.ExportSeqFunction( BackEndInstance<_>.ContractNameActiveFrontEnds, x.ListActiveFrontEnds, -1, true )
            ContractStore.ExportSeqFunction( BackEndInstance<_>.ContractNameRequestStatistics, x.GetRequestStatistics, -1, true )

        bSuccess 
    /// An Entry of Front end, if the value is -1, the FrontEnd is resolved once to an addr/port, and being try to connect
    /// If the value is 0, the FrontEnd will be resolved repeatedly, 
    member val FrontEndNameEntry = ConcurrentDictionary<_,_>((StringTComparer<int>(StringComparer.OrdinalIgnoreCase))) with get, set
    /// Whether the DNS Resolution thread is in running
    member val internal refRunningDNSThread = ref 0 with get
    /// Blocking DNS Resolution thread
    member val internal EvBlockDNSThread = new ManualResetEvent(false) with get
    /// Data collection of the health of all frontend nodes attached 
    member val FrontEndHealth = ConcurrentDictionary<_, NetworkPerformance >() with get, set
    member internal x.AddFrontEndEntries(param) = 
        for entry in param.FrontEndCollection do 
            match entry with 
            | Cluster (clusterFileName, port) -> 
                let cl = Prajna.Core.Cluster( clusterFileName )
                if Utils.IsNull cl then 
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Failed to read cluster file %s\n" clusterFileName ))
                else
                    for node in cl.Nodes do 
                        let usePort = if port > 0 && port < 65536 then port else node.MachinePort
                        x.FrontEndNameEntry.GetOrAdd( (node.MachineName, usePort), ref -1L) |> ignore
            | Server (machineName, port ) -> 
                x.FrontEndNameEntry.GetOrAdd( (machineName, port), ref -1L) |> ignore
            | TrafficManager (machineName, port ) -> 
                x.FrontEndNameEntry.GetOrAdd( (machineName, port), ref 0L) |> ignore
        if not (x.FrontEndNameEntry.IsEmpty) then 
            x.StartDNSResolve()
    member internal x.StartDNSResolve() = 
        if not x.bTerminate then 
            let count = x.FrontEndNameEntry.Values |> Seq.filter ( fun refV -> (!refV)<=0L ) |> Seq.length
            if count > 0 then 
                // DNS resolve thread is running? 
                if Interlocked.CompareExchange( x.refRunningDNSThread, 1, 0 ) = 0 then 
                    // Start thread
                    ThreadTracking.StartThreadForFunctionWithCancelation (x.CancelAllInstances) 
                        ( fun _ -> sprintf "DNS resolve thread for FrontEndEntries") (x.ContinuousDNSResolve) |> ignore
                else
                    x.EvBlockDNSThread.Set() |> ignore
    /// Synchronous DNS resolution is time consuming. We will start the DNS resolution on a timer, so that 
    member internal x.ContinuousDNSResolve() =
        let mutable bUnResolved = true
        while not x.bTerminate && bUnResolved do
            bUnResolved <- false
            let rnd = Random()
            for pair in x.FrontEndNameEntry do 
                if (!pair.Value) <= 0L then 
                    // Only resolve unresolved FrontEnd entries
                    let machineName, port = pair.Key
                    // Force an actual DNS resolve call, bypass LocalDNS cache
                    try
                        let addr = System.Net.Dns.GetHostAddresses( machineName )
                        // Only use IPV4 Address
                        let addrBytes = addr |> Array.filter ( fun addr -> addr.AddressFamily = Net.Sockets.AddressFamily.InterNetwork ) 
                                             |> Array.map ( fun addr -> addr.GetAddressBytes() )
                        LocalDNS.AddEntry( machineName, addrBytes )
                        if not x.bTerminate && addrBytes.Length > 0 then 
                            let useAddr = addrBytes.[ rnd.Next( addrBytes.Length) ]
                            let remoteSignature = LocalDNS.IPv4AddrToInt64( System.Net.IPAddress(useAddr), port )
                            let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
                            if not ( Utils.IsNull queue) then 
                                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Connect to FrontEnd server %s:%d lead to a remote endpoint that has already been connected. Duplicate FrontEnd entry discovered, no additional connect is established. "
                                                                                   machineName port ))
                            else
                                let health = NetworkPerformance()
                                let healthAdded = x.FrontEndHealth.GetOrAdd( remoteSignature, health ) 
                                if not ( Object.ReferenceEquals( health, healthAdded ))  then 
                                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "connect to FrontEnd server %s:%d find multiple health object, another thread is adding queue? No additional connect is established. "
                                                                               machineName port ))
                                else
                                    let ipAddr = System.Net.IPAddress(useAddr)
                                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "resolve FrontEnd machine %s to (%A:%d)" 
                                                                                       machineName 
                                                                                       ipAddr port ))
                                    let queue = Cluster.Connects.AddConnect( ipAddr, port )
                                    if not ( Utils.IsNull queue ) && not queue.Initialized then 
                                        // Valid connection & not initialized
                                        let newSignature = queue.RemoteEndPointSignature
                                        if newSignature<>remoteSignature then 
                                            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Error in remote signature calculation, the remote signature returned by AddConnect is different from calcuated by IPv4AddrToInt64 %d<>%d for machine %s:%d"
                                                                                       newSignature remoteSignature
                                                                                       machineName port ))
                                        if (!pair.Value) < 0L then 
                                            pair.Value := remoteSignature // Mark this FrontEndServer as resolved
                                        queue.OnConnect.Add( new UnitAction(x.OnConnectQueue queue )) 
                                        queue.OnDisconnect.Add( new UnitAction(x.CloseQueue( remoteSignature )) )
                                        let procItem = (
                                            fun (cmd : NetworkCommand) -> 
                                                if not x.bTerminate then 
                                                    x.ParseFrontEndRequest queue cmd.cmd (cmd.ms)
                                                null
                                        )
                                        queue.AddRecvProc( procItem ) |> ignore
                                        queue.Initialize()

                    with 
                    | e -> 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DNS resolve server %s failes" machineName ))
                    if not bUnResolved then 
                        // There is still server not resolved, which we can resolve again. 
                        bUnResolved <- (!pair.Value) <= 0L
            if bUnResolved then 
                x.EvBlockDNSThread.WaitOne( dnsResolveIntervalFrontEndInMS ) |> ignore
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "All gateway servers resolved, terminate DNS resolve thread" ))
        x.refRunningDNSThread := 0 // Mark the DNS thread as completed.  
    member internal x.OnConnectQueue queue () = 
        // add processing for command 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Send initial connection message from port %d to %s" ((queue.LocalEndPoint :?>IPEndPoint).Port) (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) ))
        x.SendInitialMessage( queue )
    /// Write queue
    member internal x.SendInitialMessage( queue: NetworkCommandQueue ) = 
        if not ( Utils.IsNull x.InitialMessage) then 
            let bExist, health =  x.FrontEndHealth.TryGetValue( queue.RemoteEndPointSignature ) 
            if bExist then 
                for cmd, ms in x.InitialMessage do 
                    // Wrap initial message with health information & end marker
                    let t1 = (PerfADateTime.UtcNowTicks())
                    use msSend = new MemStream( int ms.Length + 128 ) 
                    health.WriteHeader( msSend ) 
                    let buf, pos, length = ms.GetBufferPosLength()
                    msSend.Append(buf, int64 pos, int64 length)
                    health.WriteEndMark( msSend ) 
                    let t2 = (PerfADateTime.UtcNowTicks())
                    queue.ToSend( cmd, msSend )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "send initial message %A of %dB using %f ms (%f ms in preparation)" 
                                                                   cmd msSend.Length (TimeSpan( (PerfADateTime.UtcNowTicks()) - t1 ).TotalMilliseconds) 
                                                                   (TimeSpan( t2-t1).TotalMilliseconds) ))
                    
        
    /// CloseQueue is called if the connection has been abruptly closed (e.g., network failure to FrontEnd.   
    member internal x.CloseQueue( remoteSignature ) () =
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> let ipaddr = LocalDNS.Int64ToIPEndPoint( remoteSignature) 
                                                      sprintf "Remote queue at %s is closed" (LocalDNS.GetShowInfo( ipaddr )) ))
        x.FrontEndHealth.TryRemove( remoteSignature ) |> ignore 
        // Restart DNS resolve of the server. 
        for pair in x.FrontEndNameEntry do 
            if (!pair.Value) = remoteSignature then 
                pair.Value := -1L // Start resolve again
        x.StartDNSResolve()
        let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
        if Utils.IsNotNull queue then 
            // Close queue
            queue.Close()
    override x.Run() = 
        while not x.bTerminate do 
            while not x.bTerminate && x.ExaminePrimaryQueueOnce() do 
                // Continuously process Primary Queue items if there is something to do. 
                ()
            if not x.bTerminate then 
                if x.FrontEndNameEntry.IsEmpty && x.FrontEndHealth.IsEmpty then 
                    // If no gateway, terminates. 
                    Logger.Log( LogLevel.MildVerbose, ("Terminate BackEndInstance as there is no FrontEnd name entry and no active connection"))
                    x.bTerminate <- true
                else
                    x.EvPrimaryQueue.Reset() |> ignore 
                    if x.PrimaryQueue.IsEmpty then 
                        x.EvPrimaryQueue.WaitOne() |> ignore
        x.EvTerminated.Set() |> ignore         
    override x.OnStop() = 
        // Cancel all pending jobs. 
        x.CancelAllInstances()
    override x.IsRunning() = 
        not x.bTerminate
    /// Cancel function, the thread receives a cancellation request, that should terminate all thread in this tasks
    member internal x.CancelAllInstances() = 
        x.bTerminate <- true
        x.EvBlockDNSThread.Set() |> ignore
        // Terminate all queues. 
        for pair in x.FrontEndHealth do 
            let remoteSignature = pair.Key
            let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
            if not (Utils.IsNull queue ) then 
                queue.Terminate()
        x.EvPrimaryQueue.Set() |> ignore 
    /// Parse Receiving command 
    member internal x.ParseFrontEndRequest queue cmd ms = 
        if not x.bTerminate then 
            try
                let remoteSignature = queue.RemoteEndPointSignature
                let health = x.FrontEndHealth.GetOrAdd( remoteSignature, fun _ -> NetworkPerformance() )
                health.ReadHeader( ms ) 
                match cmd.Verb, cmd.Noun with 
                | ControllerVerb.Unknown, _ -> 
                    ()
                | ControllerVerb.Read, ControllerNoun.Buffer -> 
                    let guids = BufferCache.UnPackGuid( ms ) 
                    let items = BufferCache.Current.FindCacheableBufferByGuid( guids ) 
                    use msSend = new MemStream( )
                    health.WriteHeader( msSend ) 
                    BufferCache.PackCacheableBuffers( items, msSend )
                    health.WriteEndMark( msSend ) 
                    queue.ToSend( ControllerCommand(ControllerVerb.Write, ControllerNoun.Buffer ), msSend )
                    ()
                | ControllerVerb.Request, ControllerNoun.QueryReply -> 
                    // x.ParseTimestamp( queue, health, ms )
                    let buf = Array.zeroCreate<_> 16
                    ms.ReadBytes( buf ) |> ignore
                    let reqID = Guid( buf ) 
                    ms.ReadBytes( buf ) |> ignore
                    let serviceID = Guid( buf ) 
                    let requestObject = ms.DeserializeObjectWithTypeName()
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "from %s received request %A %dB, Rtt = %f ms." 
                                                                           (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) 
                                                                           reqID
                                                                           (ms.Length) (health.GetRtt()) ) )
                    x.ProcessRequest( queue, health, reqID, serviceID, requestObject )
                | ControllerVerb.Echo, ControllerNoun.QueryReply ->
                    let t1 = (PerfDateTime.UtcNowTicks())
                    use msSend = new MemStream( 128 )
                    health.WriteHeader( msSend ) 
                    health.WriteEndMark( msSend ) 
                    queue.ToSend( ControllerCommand(ControllerVerb.EchoReturn, ControllerNoun.QueryReply ), msSend )
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "from %s receive echo, Rtt = %.2f ms (last Rtt = %.2f ms ), process in %f ms" (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) (health.GetRtt()) (health.LastRtt) 
                                                                           (TimeSpan((PerfDateTime.UtcNowTicks()) - t1 ).TotalMilliseconds) ) )
                | _ -> 
                    let mutable bParsed = false 
//                    let en = x.OnParse.GetEnumerator()
//                    while not bParsed && en.MoveNext() do 
//                        let del = en.Current
                    for del in x.OnParse do 
                        if not bParsed then 
                            let bUse, ev = del.Invoke( queue, health, cmd, ms ) 
                            if bUse then 
                                bParsed <- bUse
                                if not(Utils.IsNull ev) then 
                                    // Blocked in parse
                                    let mutable bDone = false
                                    while not bDone do
                                        let bUse, ev = del.Invoke( queue, health, cmd, ms ) 
                                        bDone <- Utils.IsNull ev
                    if not bParsed then 
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "from gateway %s received a command %A (%dB) that is not parsed by the service " 
                                                                   (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) 
                                                                   cmd ms.Length))
                let bValid = health.ReadEndMark( ms ) 
                if not bValid then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "from gateway %s received an unrecognized command %A (%dB), the intergrity check fails " 
                                                               (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) 
                                                               cmd ms.Length)                    )
            with 
            | e -> 
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Exception in ParseFrontEndRequest, %A" e ))
    /// Processing Request object
    member x.ProcessRequest( queue, health, reqID, serviceID, requestObject ) =
        let bExist, jobQueue = x.SecondaryServiceQueue.TryGetValue( serviceID ) 
        if bExist then 
            let remoteSignature = queue.RemoteEndPointSignature
            jobQueue.Enqueue( reqID, requestObject, remoteSignature, (PerfADateTime.UtcNowTicks()) ) 
            x.AddPerfStatistics( reqID, serviceID, remoteSignature )
            x.PrimaryQueue.Enqueue( serviceID ) 
            x.EvPrimaryQueue.Set() |> ignore 
        else
            use msError = new MemStream( 1024 ) 
            health.WriteHeader( msError ) 
            msError.WriteBytes( serviceID.ToByteArray() )
            health.WriteEndMark( msError ) 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "query service ID %s is not supported at this backend" (serviceID.ToString()) ))
            queue.ToSend( ControllerCommand( ControllerVerb.NonExist, ControllerNoun.QueryReply ), msError )
    member internal x.AddPerfStatistics( reqID, serviceID, remoteSignature ) = 
        if x.StatisticsPeriodInSecond >= 0 then 
            x.StatisticsCollection.GetOrAdd( reqID, ( (PerfADateTime.UtcNowTicks()), serviceID, remoteSignature, ref None )  ) |> ignore
            x.CleanStatisticsQueue() 
    member x.CleanStatisticsQueue() = 
        let ticksCur = (PerfADateTime.UtcNowTicks())
        let ticksOld = !lastStatisticsQueueCleanRef 
        let ticksClean = ticksCur - TimeSpan.TicksPerSecond * int64 x.StatisticsPeriodInSecond
        if ticksCur - ticksOld >= TimeSpan.TicksPerSecond then 
            if Interlocked.CompareExchange( lastStatisticsQueueCleanRef, ticksCur, ticksOld ) = ticksOld then 
                // Get the lock for cleaning, avoid frequent cleaning operation
                let cntRemoved = ref 0
                let earliestTicks = ref Int64.MaxValue
                for pair in x.StatisticsCollection do 
                    let ticks, _, _, _ = pair.Value
                    if ticks <= ticksClean then 
                        x.StatisticsCollection.TryRemove( pair.Key ) |> ignore
                        cntRemoved := !cntRemoved + 1
                        if !earliestTicks > ticks then 
                            earliestTicks := ticks
                if !cntRemoved > 0 then 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Removed %d query as they timed out, earlied time %s" !cntRemoved (VersionToString(DateTime(!earliestTicks))) ))

    /// Processing primary queue once.
    /// Return: true: we have done something in the loop (need to examine if there is anything to do). 
    ///         false: there is nothing to do in the loop
    member internal x.ExaminePrimaryQueueOnce() = 
        let bAny, serviceID = x.PrimaryQueue.TryDequeue()
        if bAny then 
            let bService, serviceQueue = x.SecondaryServiceQueue.TryGetValue( serviceID ) 
            if bService then 
                let bServiceCapacity, capacity = x.ServiceCapacity.TryGetValue( serviceID )
                if bServiceCapacity then 
                    let bAnyOtherObject = not x.PrimaryQueue.IsEmpty
                    // Ensure ExamineServiceQueueOnce is executed 
                    let bReturn = x.ExamineServiceQueueOnce serviceID serviceQueue capacity
                    bReturn || bAnyOtherObject
                else
                    // Logic error 
                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "When ExaminePrimaryQueueOnce, find service ID %A that has a servce capacity object. Something is wrong!!!" 
                                                               serviceID ))
                    true
            else
                // Service ID doesn't exist, something wrong, as it should not be enqueued in the first place. 
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "When ExaminePrimaryQueueOnce, find service ID %A that doesn't exist in service queue. Something is wrong!!!" 
                                                           serviceID ))

                true
        else
            false    
    /// Examine a certain service queue once
    /// If we dequeued the request, we should return true, to reexamine. 
    ///     We will need to requeue serviceID at the completion of the query, so that waiting query that previously has no slot to be serviced can be examined. 
    /// If we don't do anything, we will return false (no need to reexamine) 
    member internal x.ExamineServiceQueueOnce serviceID serviceQueue capacity = 
        let nItems = Interlocked.Increment( capacity.CurrentItems ) 
        if nItems > capacity.Capacity then 
            // Exceed capacity 
            Interlocked.Decrement( capacity.CurrentItems ) |> ignore
            // Any queue items that is time out? 
            x.ExamineForTimeout serviceQueue 
            false
        else
            let refValue = ref Unchecked.defaultof<_>
            let mutable bFindItem = false
            let numItemsRef = ref 0 
            while not bFindItem && not (serviceQueue.IsEmpty) do 
                let bItem = serviceQueue.TryDequeue( refValue ) 
                if bItem then 
                    let reqID, requestObject, remoteSignature, ticks = !refValue
                    let ticksCur = (PerfDateTime.UtcNowTicks())
                    if x.TimeOutTicks < Int64.MaxValue && ticks < ticksCur - x.TimeOutTicks then 
                        x.TimeoutRequest ticks reqID remoteSignature // timeout, do not send to process
                    else
                        bFindItem <- true                                                              
                        numItemsRef := serviceQueue.Count
            if bFindItem then 
                let bExist, serviceCollection = x.ServiceCollection.TryGetValue( serviceID )
                if not bExist then 
                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "When ExamineServiceQueueOnce, service collection of service ID %A doesn't exist. Something is wrong!!!" 
                                                               serviceID ))
                    Interlocked.Decrement( capacity.CurrentItems ) |> ignore
                    false
                else
                    let serviceSeq = serviceCollection :> seq<_> 
                    let serviceInstance = serviceSeq |> Seq.find( fun service -> service.GrabOneInstance() )
                    if Utils.IsNull serviceInstance then 
                        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "When ExamineServiceQueueOnce, can't find service ID %A in the ConcurrentBag of service collection. Something is wrong!!!" 
                                                                   serviceID ))
                        Interlocked.Decrement( capacity.CurrentItems ) |> ignore
                        false
                    else
                        let reqID, requestObject, remoteSignature, ticks = !refValue
                        let ticksCur = (PerfADateTime.UtcNowTicks())
                        let timeInQueueMS = int (( ticksCur - ticks ) / TimeSpan.TicksPerMillisecond)
                        let timebudgetInMs = if x.TimeOutTicks = Int64.MaxValue then Int32.MaxValue else int (( x.TimeOutTicks - ( ticksCur - ticks ) ) / TimeSpan.TicksPerMillisecond )
                        let numSlotsAvailable = capacity.Capacity - (!capacity.CurrentItems)
                        serviceInstance.ProcessRequest( reqID, requestObject, timebudgetInMs, x.SendReply (!numItemsRef) numSlotsAvailable ticksCur timeInQueueMS reqID remoteSignature, 
                                                            x.ErrorReply reqID remoteSignature, 
                                                            x.FinishProcessRequest reqID serviceInstance capacity
                                                             )
                        true
            else
                Interlocked.Decrement( capacity.CurrentItems ) |> ignore
                false
    /// Always called to ensure proper clean up
    member internal x.FinishProcessRequest reqID serviceInstance capacity () = 
        serviceInstance.ReleaseOneInstance() 
        Interlocked.Decrement( capacity.CurrentItems ) |> ignore
        Interlocked.Increment( capacity.TotalItems ) |> ignore
        // Trigger re-examine of service
        x.PrimaryQueue.Enqueue( serviceInstance.ServiceID ) 
        x.EvPrimaryQueue.Set() |> ignore 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Finished req %A serviceInstance %A, capacity %A" 
                                                                   reqID serviceInstance capacity))
        ()
    member internal x.ExamineForTimeout serviceQueue = 
        if false then 
            // Skip all this code
            if x.TimeOutTicks < Int64.MaxValue then 
                // No timeout examine if x.TimeOutTicks = Int64.MaxValue
                let mutable bDoneExamine = false
                while not bDoneExamine do 
                    bDoneExamine <- true
                    let bItem, tuple = serviceQueue.TryPeek() 
                    if bItem then 
                        let _, _, _, ticks = tuple 
                        let ticksCur = (PerfDateTime.UtcNowTicks())
                        if ticks < ticksCur - x.TimeOutTicks then 
                            // Try remove the item, notice that the item may not be actually timed out due to multithread competition
                            let bItem, tuple = serviceQueue.TryDequeue() 
                            if bItem then 
                                let reqID, requestObject, remoteSignature, ticks = tuple 
                                if ticks < ticksCur - x.TimeOutTicks then 
                                    // Timeout 
                                    x.TimeoutRequest ticks reqID remoteSignature
                                    // Only when we successfully throw away a timeout item, we will need to reexamine another 
                                    bDoneExamine <- false
                                else
                                    // Reexamine, this put the item in lower priority though. 
                                    serviceQueue.Enqueue( tuple )

    member internal x.TimeoutRequest ticks reqID remoteSignature =
        let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
        let bHealth, health = x.FrontEndHealth.TryGetValue( remoteSignature )
        if bHealth && not (Utils.IsNull queue) then 
            let ticksCur = (PerfADateTime.UtcNowTicks())
            let elapse = int( ( ticksCur - ticks )/TimeSpan.TicksPerMillisecond ) 
            use msTimeOut = new MemStream( 1024 ) 
            health.WriteHeader( msTimeOut ) 
            msTimeOut.WriteBytes( reqID.ToByteArray() )
            msTimeOut.WriteInt32( elapse ) // Timeout after (ms) 
            health.WriteEndMark( msTimeOut ) 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "query %s times out at the backend after %d ms" (reqID.ToString()) elapse ))
            queue.ToSend( ControllerCommand( ControllerVerb.TimeOut, ControllerNoun.QueryReply ), msTimeOut )
            if x.StatisticsPeriodInSecond >=0 then 
                let bExist, tuple = x.StatisticsCollection.TryGetValue( reqID )
                let _, _, _, statisticsHolderRef = tuple
                statisticsHolderRef := Some ( "Timeout", elapse, 0 )
    member internal x.SendReply numItems numSlotsAvailable ticks timeInQueueMS reqID remoteSignature (replyObject:Object) =
        let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
        let bHealth, health = x.FrontEndHealth.TryGetValue( remoteSignature )
        if bHealth && not (Utils.IsNull queue) then 
            let ticksCur = (PerfADateTime.UtcNowTicks())
            let timeInProcessingMS = int (( ticksCur - ticks ) / TimeSpan.TicksPerMillisecond)
            let qPerf = SingleQueryPerformance( InQueue = timeInQueueMS, InProcessing = timeInProcessingMS, 
                                                NumSlotsAvailable = numSlotsAvailable, 
                                                NumItemsInQueue = numItems )
            use msReply = new MemStream( ) 
            health.WriteHeader( msReply )         
            msReply.WriteBytes( reqID.ToByteArray() ) 
            SingleQueryPerformance.Pack( qPerf, msReply )
            msReply.SerializeObjectWithTypeName( replyObject ) 
            health.WriteEndMark( msReply ) 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "query %s has been served and returned with a reply of %dB (%s)" (reqID.ToString()) (msReply.Length) (qPerf.BackEndInfo()) ))
            queue.ToSend( ControllerCommand( ControllerVerb.Reply, ControllerNoun.QueryReply ), msReply )
            if x.StatisticsPeriodInSecond >=0 then 
                let bExist, tuple = x.StatisticsCollection.TryGetValue( reqID )
                let _, _, _, statisticsHolderRef = tuple
                statisticsHolderRef := Some ( "Reply", timeInQueueMS, timeInProcessingMS )
    member internal x.ErrorReply reqID remoteSignature (msg:string) =
        let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
        let bHealth, health = x.FrontEndHealth.TryGetValue( remoteSignature )
        if bHealth && not (Utils.IsNull queue) then 
            use msReply = new MemStream( ) 
            health.WriteHeader( msReply )         
            msReply.WriteBytes( reqID.ToByteArray() ) 
            msReply.WriteString( msg ) 
            health.WriteEndMark( msReply ) 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "service of query %s lead to error %s" (reqID.ToString()) (msg) ))
            queue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.QueryReply ), msReply )
            if x.StatisticsPeriodInSecond >=0 then 
                let bExist, tuple = x.StatisticsCollection.TryGetValue( reqID )
                let ticks, _, _, statisticsHolderRef = tuple
                let elapse = int( ( (PerfADateTime.UtcNowTicks()) - ticks )/TimeSpan.TicksPerMillisecond ) 
                statisticsHolderRef := Some ( "Error", elapse, 0 )
    /// <summary>
    /// Return frontend-backend connection statistics in the following forms:
    ///     "backend-frontend", NetworkPerformance 
    /// </summary>
    member x.ListActiveFrontEnds() = 
        x.FrontEndHealth |> Seq.choose( fun pair -> if pair.Value.ConnectionReady() then 
                                                        Some (RemoteExecutionEnvironment.MachineName + "-" + LocalDNS.GetHostInfoInt64( pair.Key), pair.Value.GetRtt() )
                                                    else 
                                                        None )
    /// <summary> 
    /// Return query performance in the following forms:
    ///     frontend server, serviceID, service msg, msInQueue, msInProcessing
    /// service_msg: null (not served yet).
    ///              "Timeout", the request is timed out. 
    ///              "Reply", the request is succesfully served. msInQueue
    /// </summary>
    member x.GetRequestStatistics() = 
        let countRef = ref 0
        let filterByUnavail = ref 0 
        x.CleanStatisticsQueue()
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "GetRequestStatistics called, analyze %d query statistics"
                                                               x.StatisticsCollection.Count ))
        x.StatisticsCollection |> Seq.choose( fun pair ->   let ticks, serviceID, remoteSignature, statisticsHolderRef = pair.Value
                                                            match !statisticsHolderRef with 
                                                            | None -> 
                                                                filterByUnavail := !filterByUnavail + 1
                                                                None 
                                                            | Some tuple -> 
                                                                let msg, msInQueue, msInProcessing = tuple
                                                                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> countRef := ! countRef + 1
                                                                                                              sprintf "Query %d (unavail:%d), %s, inQueue=%d ms, inProc = %d ms"
                                                                                                                       !countRef !filterByUnavail
                                                                                                                       msg 
                                                                                                                       msInQueue msInProcessing ))
                                                                Some ( LocalDNS.GetHostInfoInt64( remoteSignature ), RemoteExecutionEnvironment.ContainerName, serviceID, msg, msInQueue, msInProcessing ) )
/// <summary>
/// Class that are used to get summary statistics of query at backend
/// </summary>
[<Serializable; AllowNullLiteral>]
type BackEndQueryStatistics() = 
    member val internal ReqByFrontEnd = ConcurrentDictionary<_,_>(StringComparer.Ordinal) with get
    member val internal ReqByServiceID = ConcurrentDictionary<_,_>() with get
    member val internal PerfByReply = ConcurrentDictionary<_,_>(StringComparer.Ordinal) with get
    /// Used for the accumulate functional delegate  
    static member FoldQueryStatistics (y:BackEndQueryStatistics) ( tuple: string*Guid*string*int*int) =
        let frontendName, serviceID, replyStatus, msInQueue, msInProcessing = tuple
        let x = if Utils.IsNull y then BackEndQueryStatistics() else y
        x.ReqByFrontEnd.AddOrUpdate( frontendName, (fun _ -> 1), (fun _ value -> value + 1 ) ) |> ignore
        x.ReqByServiceID.AddOrUpdate( serviceID, (fun _ -> 1), (fun _ value -> value + 1 ) ) |> ignore
        x.PerfByReply.AddOrUpdate( replyStatus, ( fun _ -> 1, int64 msInQueue, int64 msInProcessing ), 
                                                ( fun _ ( cnt, sumInQueue, sumInProcessing ) -> ( cnt+1, sumInQueue + int64 msInQueue, sumInProcessing + int64 msInProcessing ) ) ) |> ignore
        x
    /// Used for the aggregation functional delegate 
    static member AggregateQueryStatistics (x:BackEndQueryStatistics) (y:BackEndQueryStatistics) = 
        for pair in y.ReqByFrontEnd do 
            x.ReqByFrontEnd.AddOrUpdate( pair.Key, (fun _ -> pair.Value), ( fun _ cnt -> cnt + pair.Value ) ) |> ignore
        for pair in y.ReqByServiceID do 
            x.ReqByServiceID.AddOrUpdate( pair.Key, (fun _ -> pair.Value), ( fun _ cnt -> cnt + pair.Value ) ) |> ignore
        for pair in y.PerfByReply do 
            x.PerfByReply.AddOrUpdate( pair.Key, (fun _ -> pair.Value), 
                                                 ( fun _ tuple -> let cnt0, sumInQueue0, sumInProcessing0 = tuple
                                                                  let cnt1, sumInQueue1, sumInProcessing1 = pair.Value
                                                                  cnt0+cnt1, sumInQueue0+sumInQueue1, sumInProcessing0+sumInProcessing1 ) ) |> ignore
        x
    /// Show statistics
    member x.ShowStatistics() = 
        for pair in x.ReqByFrontEnd do 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "FrontEnd %s: %d requests" pair.Key pair.Value ) )
        for pair in x.ReqByServiceID do 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "ServiceID %A: %d requests" pair.Key pair.Value ) )
        for pair in x.PerfByReply do 
            Logger.LogF( LogLevel.Info, ( fun _ -> let cnt, sumInQueue, sumInProcessing = pair.Value
                                                   sprintf "Reply %s: %d requests (%d ms, %d ms)" pair.Key cnt (sumInQueue/int64 cnt) (sumInProcessing/int64 cnt) ) )

                                                          
