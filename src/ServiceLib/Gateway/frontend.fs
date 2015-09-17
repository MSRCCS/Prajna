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
        frontend.fs
  
    Description: 
        Front end distributed gateway service. A gateway server receives query
    request (usually through a Web API such as WebGet, then call QueryFrontEnd
    to distributed the request to backend. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jan. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.Gateway

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Network
open Prajna.Core
open Prajna.Service.ServiceEndpoint

open Prajna.Service.FSharp

/// <summary>
/// This class contains information on what remote end has what buffer cache. 
/// In case a certain cacheable buffer is released, it can be used to tracedown what remote endpoint to request the specific buffer cache. 
/// </summary> 
type OwnershipTracking() = 
    static member val Current = OwnershipTracking() with get
    member val RemoteCollection = ConcurrentDictionary<int64,ConcurrentDictionary<Guid, ManualResetEvent>>() with get
    member x.ReceiveBufferFromPeer( remoteSignature, id ) = 
        let collection = x.RemoteCollection.GetOrAdd( remoteSignature, fun _ -> ConcurrentDictionary<_,_>() )
        let bExist, ev = collection.TryGetValue( id ) 
        if bExist && Utils.IsNotNull ev then 
            ev.Set() |> ignore
        collection.Item( id ) <- null
            
    member x.GetCacheableBuffer( id: Guid ) =
        BufferCache.Current.RetrieveCacheableBuffer( id, 
            x.PrivateRetrieve, BufferCache.TimeOutRetrieveCacheableBufferInMs )
    member x.GetCacheableBufferAsync( id: Guid ) =
        BufferCache.Current.RetrieveCacheableBufferAsync( id, 
            x.PrivateRetrieve, BufferCache.TimeOutRetrieveCacheableBufferInMs )
    member internal x.PrivateRetrieve( id ) = 
        let lst = List<_>(x.RemoteCollection.Count)
        let mutable evHolder = null
        for pair in x.RemoteCollection do 
            let dic = pair.Value
            let bExist, ev = dic.TryGetValue( id )
            if bExist then 
                if Utils.IsNull ev then 
                    lst.Add( pair.Key, pair.Value )
                else
                    evHolder <- ev
        if not(Utils.IsNull evHolder) then 
            false, evHolder 
        elif lst.Count > 0 then 
            let rnd = Random()
            let idx = rnd.Next( lst.Count)
            let remoteSignature, dic = lst.[idx]
            let bExist, ev = x.RetrieveCallbackFunc remoteSignature id
            if Utils.IsNotNull ev then 
                dic.Item( id ) <- ev
            false, ev
        else
            false, null
    /// RetrieveCallback to be extended. 
    /// return true if the item is not retrievable. 
    /// return false if the retireve command sends out (thus block retrieve operation) 
    member val internal RetrieveCallbackFunc = fun _ _ -> false, null with get, set

/// Construct a performance statistics for a single query
/// If customized performance tracking is required, please use this function to form a customized performance tracking class 
type SinglePerformanceConstructFunction = Func<unit, SingleQueryPerformance>


/// Construct a performance statistics for a backend node. 
/// If customized performance tracking is required, please use this function to form a customized performance tracking class 
type BackEndPerformanceConstructFunction = Func<unit, BackEndPerformance>

/// <summary> 
/// Backend performance tracking, if the programer intend to track more statistics, additional
/// information may be included. 
/// </summary>
and BackEndPerformance(slot, del: SinglePerformanceConstructFunction) = 
    inherit NetworkPerformance() 
    let totalRequestRef = ref 0L
    let completedReqRef = ref 0L
    let failedReqRef = ref 0L
    let outstandingRequestRef = ref 0
    let mutable avgNetworkRtt=300
    let mutable avgQueuePerSlot = 100
    let mutable avgProcessing = 1000 
    let mutable slotsOnBackend = 0
    member val internal QueryPerformanceCollection = Array.zeroCreate<_> slot with get, set
    /// Maximum number of request that can be served.
    member val MaxSlots = 0 with get, set
    /// Current number of request that is under service 
    member val Curslots = 0 with get, set
    /// Number of completed queries 
    member x.NumCompletedQuery with get() = (!completedReqRef)
    /// Total number of queries issued 
    member x.TotalQuery with get() = (!totalRequestRef)
    /// Number of queries to be serviced. 
    member x.OutstandingRequests with get() = (!outstandingRequestRef)
    /// <summary>
    /// If we send request to this Backend, the expected processing latency (in millisecond)
    /// It is calculated by avgNetworkRtt + avgProcessing + ( avgQueuePerSlot * itemsInQueue )
    /// </summary>
    member val ExpectedLatencyInMS=100 with get, set
    /// <summary> 
    /// This function is called before each request is sent to backend for statistics 
    /// </summary>
    abstract RegisterRequest: unit -> unit
    default x.RegisterRequest() = 
        Interlocked.Increment( outstandingRequestRef ) |> ignore
        x.ExpectedLatencyInMS <- avgNetworkRtt + avgProcessing * ( slotsOnBackend + Math.Max( !outstandingRequestRef, 0) )
    /// Show the expected latency of the backend in string 
    member x.ExpectedLatencyInfo() = 
        sprintf "exp %dms=%d+%d*(%d+%d)"
                    x.ExpectedLatencyInMS
                    avgNetworkRtt avgProcessing 
                    slotsOnBackend !outstandingRequestRef
    /// Show the backend queue status in string 
    member x.QueueInfo() = 
        sprintf "total: %d completed:%d, outstanding: %d, failed:%d" !totalRequestRef !completedReqRef !outstandingRequestRef (!failedReqRef)
    /// <summary> 
    /// This function is called whenever a reply is received. For error/timeout, put a non-empty message in perfQ.Message, and call this function. 
    /// If perQ.Message is not null, the execution fails. 
    /// </summary> 
    abstract DepositReply: SingleQueryPerformance -> unit
    default x.DepositReply( perfQ ) = 
        Interlocked.Increment( totalRequestRef ) |> ignore
        Interlocked.Decrement( outstandingRequestRef ) |> ignore
        if Utils.IsNull perfQ.Message then 
            let items = Interlocked.Increment( completedReqRef ) 
            let idx = int (( items - 1L ) % int64 x.QueryPerformanceCollection.Length)
            x.QueryPerformanceCollection.[idx] <- perfQ
            // Calculate statistics of performance
            let mutable sumNetwork = 0
            let mutable sumQueue = 0 
            let mutable sumProcessing = 0 
            let mutable sumQueueSlots = 0 
            let maxItems = int (Math.Min( items, x.QueryPerformanceCollection.LongLength ) )
            for i = 0 to maxItems - 1 do 
                let pQ = x.QueryPerformanceCollection.[i]
                sumNetwork <- sumNetwork + pQ.InNetwork
                sumQueue <- sumQueue + pQ.InQueue
                sumProcessing <- sumProcessing + pQ.InProcessing
                sumQueueSlots <- sumQueueSlots + Math.Min( 0, pQ.NumItemsInQueue - pQ.NumSlotsAvailable ) 
            avgNetworkRtt <- sumNetwork / maxItems
            avgProcessing <- sumProcessing / maxItems
            avgQueuePerSlot <- sumQueue / Math.Max( sumQueueSlots, 1) // Queue is usually proportional to the # of items in queue. 
            x.MaxSlots <- Math.Max( x.MaxSlots, perfQ.NumSlotsAvailable ) 
            x.Curslots <- perfQ.NumSlotsAvailable - perfQ.NumItemsInQueue - !outstandingRequestRef 
            slotsOnBackend <- Math.Min( 0, perfQ.NumItemsInQueue - perfQ.NumSlotsAvailable ) 
            x.ExpectedLatencyInMS <- avgNetworkRtt + avgProcessing + avgQueuePerSlot * ( slotsOnBackend + Math.Min( !outstandingRequestRef, 0) )
        else
            Interlocked.Increment( failedReqRef ) |> ignore 
    
    /// <summary> 
    /// Override this function or the ConstructionDelegate if you need a customized SingleQueryPerformance
    /// </summary>
    member x.ConstructSingleQueryPerformance() = 
        del.Invoke()
    static member internal DefaultConstructionDelegate() = 
        BackEndPerformanceConstructFunction( 
            fun _ -> BackEndPerformance( NetworkPerformance.RTTSamples, 
                                            SinglePerformanceConstructFunction( fun _ -> SingleQueryPerformance()) ) )

   
/// Function to be executed when the frontend starts 
type FrontEndOnStartFunction<'StartParamType> = Func< 'StartParamType, bool>

/// Function delegate to form messages to frontend 
type OnInitialMsgToBackEndFunction = Func< StreamBase<byte>, seq<ServiceInstanceBasic> >

// type AddServiceInstanceDelegate = delegate of int64*ServiceInstanceBasic -> unit
// type OnDisconnectDelegate = delegate of int64 -> unit
// type FrontEndParseDelegate = delegate of NetworkCommandQueuePeer * NetworkPerformance * ControllerCommand * ms: MemStream -> (bool * ManualResetEvent)
// type OnReplyDelegate = delegate of (SingleQueryPerformance * Object )-> unit



/// <summary>
/// This class captures structure of an in-process request. 
/// </summary>
type RequestHolder() = 
    let numReplySentRef = ref 0 
    /// ID of the service provider to be performed 
    member val ProviderID = Guid.Empty with get, set
    /// ID of the schema to be performed 
    member val SchemaID = Guid.Empty with get, set
    /// ID of the service to be performed 
    member val ServiceID = Guid.Empty with get, set
    /// Guid governs how many backend are involved in servicing the request. 
    member val DistributionPolicy = Guid.Empty with get, set
    /// Guid governs how multiple requests are to be aggregated. 
    member val AggregationPolicy = Guid.Empty with get, set
    /// Request object
    member val ReqObject : Object = null with get, set
    /// Timestamp that identify the request that is sent by the client 
    member val TicksSentByClient = 0L with get, set 
    /// RTT observed by Client (for performance reason) 
    member val RTTSentByClient = 0 with get, set
    /// Address used by client, usualy RemoteEndpointMessageProperty.Address
    member val AddressByClient : string = null with get, set
    /// Port used by Client, usualy RemoteEndpointMessageProperty.Port
    member val PortByClient = 0 with get, set
    /// Timestamp that the request is received. 
    member val TicksRequestReceived = 0L with get, set
    /// Timestamp that the request will timeout 
    member val TicksRequestTimeout = 0L with get, set
    /// Request Collection 
    /// A collection of reply that is received (indexed by Remote Signature )
    member val ReplyCollection = ConcurrentDictionary<int64, (int64* int64 *Object) ref >() with get
    /// <summary>
    /// Request assignment flag, 1 indicates that the request has been assigned and pending replies. We can now start counting on number of replies that received. 
    /// </summary>
    member val RequestAssignedFlag = ref 0 with get
    /// <summary> 
    /// Number of Reply that is sent out to app (via OnReply)
    /// </summary>
    member x.NumReplySent with get() = !numReplySentRef
    /// <summary> 
    /// Max Reply allowed. If MaxReplyAllowed is 1, second reply will be suppressed, and OnReply will garantee not to be called more than once. 
    /// </summary> 
    member val MaxReplyAllowed = 1 with get, set
    /// Trigger action downstream when sufficient number of replys are received from network 
    member val private OnReplyExecutor = ExecuteEveryTrigger<SingleQueryPerformance * Object>(LogLevel.WildVerbose) with get
    /// Trigger action downstream when sufficient number of replys are received from network 
    member x.OnReply(action: Action<SingleQueryPerformance * Object>, infoFunc : unit -> string) =
        x.OnReplyExecutor.Add(action, infoFunc)

    /// Receive a reply from network
    member x.ReceivedReply( perf, replyObject ) = 
        let num = Interlocked.Increment( numReplySentRef ) 
        if num <= x.MaxReplyAllowed then 
            x.OnReplyExecutor.Trigger( (perf, replyObject), fun _ -> "Reply " )

/// <summary> 
/// A distribution Policy delegate select 1 to N backend to service a particular request. 
/// reqHolder:  RequestHolder class that holds information of the current request. 
/// (remoteSignature*BackEndPerformance*ServiceInstanceBasic)[]: an array that contains remote signature, performance, and service information of the backend that can serve the 
///         current request. 
/// Return:
///     1 to N remote signature that is used to serve the current request. 
/// </summary>
type DistributionPolicyFunction = Func<(RequestHolder * ( NetworkCommandQueue * BackEndPerformance * ServiceInstanceBasic ) []) , ( NetworkCommandQueue * BackEndPerformance * ServiceInstanceBasic )[]>
/// <summary> 
/// An aggregation policy aggregates reply received from client, and decide if a report should be sent to the client. 
/// If Object is not null, the replyObject will be sent downstream. If the Object is null, the current object is suppressed. 
/// </summary>
type AggregationPolicyFunction = Func< (RequestHolder * SingleQueryPerformance * Object ), Object>
/// <summary> 
/// A parse function that interpret message at front end 
/// </summary>
type FrontEndParseFunction = Func< (NetworkCommandQueuePeer * BackEndPerformance * ControllerCommand * StreamBase<byte>),( bool * ManualResetEvent ) >


/// <summary>
/// This class contains the parameter used to start the back end service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// </summary> 
[<AllowNullLiteral; Serializable>]
type FrontEndServiceParam() =
    inherit WorkerRoleInstanceStartParam()
    /// Timeout value (ms), request that are not served in this time interval will be throw away
    /// If debugging, please set a large timeout value, or set TimeOutRequestInMilliSecond < 0 (infinity timeout), otherwise,
    /// request may be dropped during debugging due to timeout. 
    member val TimeOutRequestInMilliSecond = 31000 with get, set
    /// <summary> 
    /// Timeout value (ms), release content in BufferCache if it is not used in x second. 
    /// </summary>
    member val TimeOutRetrieveCacheableBufferInMs = 10000 with get, set 
    /// <summary> timeout(ms), if the CacheableBuffer is not used through this interval, it will be released from memory </summary>
    member val TimeOutReleaseCacheableBufferInMs = 466560000L with get, set //466560000L = 0.5year in ms
    /// <summary> time interval, examine BufferCache to clean up </summary>
    member val IntervalExamineBufferCacheInMs = 1000 with get, set
    /// <summary> Keep Alive timer (note that we run one timer for all queues, so it is probable some queue may experience 2x time before a keep alive message is sent out  </summary>
    member val IntervalKeepAliveInMs = 2500 with get, set
    /// <summary> Port that the FrontEnd service will be used to allow BackEnd to connect </summary> 
    member val ServicePort = 81 with get, set
    /// <summary> 
    /// Function to encode an array of service instances. 
    /// </summary> 
    member val DecodeServiceCollectionFunction : DecodeCollectionFunction = null with get, set
    /// <summary> Collection of Distribution Service. Each entry contains
    /// Guid: identify the distribution policy
    /// string: describe the distribution policy (for selection purpose)
    /// A delegate to execute the distributon policy. 
    /// </summary>
    member val DistributionPolicyLists = List<Guid*string*DistributionPolicyFunction>() with get, set
    /// <summary> Collection of Aggregation policy. Each entry contains
    /// Guid: identify the aggregation policy
    /// string: describe the aggregation policy (for selection purpose)
    /// A delegate to execute the aggregation policy. 
    /// </summary>
    member val AggregationPolicyLists = List<Guid*string*AggregationPolicyFunction>() with get, set
    /// <summary>
    /// Client tracking timeout (in milliseconds). Clients that do not appear in the time interval will be removed. 
    /// </summary> 
    member val ClientTrackingTimeOutInMS = 600000 with get, set
    /// Keep statistics of most recent certain seconds worth of requests
    member val StatisticsPeriodInSecond = 600 with get, set


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
[<AllowNullLiteral>]
type FrontEndInstance< 'StartParamType 
                    when 'StartParamType :> FrontEndServiceParam >() =
    inherit WorkerRoleInstance<'StartParamType>()
    let lastStatisticsQueueCleanRef = ref (PerfADateTime.UtcNowTicks())
    let mutable decodeServiceCollectionDelegate = Unchecked.defaultof<_>
    /// <summary> 
    /// Timeout value, in ticks 
    /// </summary>
    member val TimeOutTicks = Int64.MaxValue with get, set
    /// <summary> 
    /// Client collection. Tracking request from client. 
    /// </summary>
    member val ClientTracking = ConcurrentDictionary<_,string*int*int64>(StringTComparer<int>(StringComparer.OrdinalIgnoreCase)) with get
    /// <summary>
    /// Client tracking timeout (in milliseconds). Clients that do not appear in the time interval will be removed. 
    /// </summary> 
    member val ClientTrackingTimeOutInMS = 600000 with get, set
    /// <summary> 
    /// Service collection. map Remote Singature -> serviceID, so that when the remote node leaves, we may be able to remove the corresponding service. 
    /// </summary>
    member val ServiceCollectionByQueueSignature = ConcurrentDictionary<_,ConcurrentQueue<_>>() with get
    /// <summary> 
    /// Service collection by ServiceID 
    /// </summary>
    member val ServiceCollectionByID = ConcurrentDictionary<_,ConcurrentDictionary<_,_>>() with get
    /// Statistics Queue holds statistics of the request that is served. 
    member val StatisticsCollection = ConcurrentDictionary<_,_>() with get
    /// Keep statistics of most recent certain seconds worth of requests
    member val StatisticsPeriodInSecond = 600 with get, set

    /// Requests that are being processed
    member val internal InProcessRequestCollection = ConcurrentDictionary<_,RequestHolder>() with get
    /// Primary Queue holds request to be serviced. 
    member val internal PrimaryQueue = ConcurrentQueue<_>() with get

    member val internal EvPrimaryQueue = new ManualResetEvent( false ) with get

    member val internal bTerminate = false with get, set
    /// Initial message( If any to client)
    member val internal InitialMessage = null with get, set
    /// <summary> tap in to parse command from FrontEnd </summary>
    member val OnParse = List<FrontEndParseFunction>()  with get, set
    /// <summary> tap in when a new service instance is added </summary>
    member val private OnAddServiceInstanceExecutor = ExecuteEveryTrigger<int64*ServiceInstanceBasic>(LogLevel.WildVerbose) with get

    /// <summary> tap in when a new service instance is added </summary>
    member x.OnAddServiceInstance (action : Action<int64*ServiceInstanceBasic>, infoFunc : unit -> string) =
        x.OnAddServiceInstanceExecutor.Add(action, infoFunc)

    /// <summary> tap in when a new service instance is removed </summary>
    member val private OnRemoveServiceInstanceExecutor = ExecuteEveryTrigger<int64*ServiceInstanceBasic>(LogLevel.WildVerbose) with get

    /// <summary> tap in when a new service instance is removed </summary>
    member x.OnRemoveServiceInstance(action : Action<int64*ServiceInstanceBasic>, infoFunc : unit -> string) =
        x.OnRemoveServiceInstanceExecutor.Add(action, infoFunc)

    /// <summary> tap in when a backend connects </summary>
    member val private OnAcceptExecutor = ExecuteEveryTrigger<NetworkCommandQueuePeer>(LogLevel.MildVerbose) with get

    /// <summary> tap in when a backend connects </summary>    
    member x.OnAccept(action : Action<NetworkCommandQueuePeer>, infoFunc : unit -> string) =
        x.OnAcceptExecutor.Add(action, infoFunc)

    /// <summary> tap in when a backend disconnects </summary>
    member val private OnDisconnectExecutor = ExecuteEveryTrigger<int64>(LogLevel.MildVerbose) with get

    /// <summary> tap in when a backend disconnects </summary>
    member x.OnDisconnect(action : Action<int64>, infoFunc : unit -> string) =
        x.OnDisconnectExecutor.Add(action, infoFunc)

    /// <summary> performance tracking for a single backend. If you need to develop additional tracking metrics, override this delegate.
    /// </summary>
    member val BackEndPerformanceConstructionDelegate = BackEndPerformance.DefaultConstructionDelegate() with get, set
    /// Constant string for export/import contract to get the request statistics 
    static member val ContractNameFrontEndRequestStatistics = "FrontEndRequestStatistics" with get

    /// <summary> 
    /// Add a single service instance. 
    /// Derived class should only use AddServiceInstanceRaw to prevent recursive call 
    /// </summary> 
    member x.AddServiceInstanceRaw( remoteSignature:int64, serviceInstance: ServiceInstanceBasic ) =
        x.AddServiceInstanceGuid( remoteSignature, serviceInstance.ServiceID, serviceInstance )
    /// <summary> 
    /// Add a single service instance, allow serviceID to be different than used in serviceInstance (e.g., used in mega services). 
    /// </summary> 
    member x.AddServiceInstanceGuid( remoteSignature, serviceID, serviceInstance ) =
        let oneServiceCollection = x.ServiceCollectionByID.GetOrAdd( serviceID, fun _ -> ConcurrentDictionary<_,_>() )
        // Note: if there is multiple service with the same serviceID from the same remote endpoint, only the first is added, the rest are thrown away
        let addedInstance = oneServiceCollection.GetOrAdd( remoteSignature, serviceInstance ) 
        if Object.ReferenceEquals( addedInstance, serviceInstance ) then 
            let allServiceID = x.ServiceCollectionByQueueSignature.GetOrAdd( remoteSignature, fun _ -> ConcurrentQueue<_>() )
            allServiceID.Enqueue( serviceID )
            true
        else
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "service instance of %A is automatically throw away as there is already another service with the same service ID from %s"
                                                           (serviceInstance.ServiceID)
                                                           (LocalDNS.GetShowInfo( LocalDNS.Int64ToIPEndPoint(remoteSignature) )) ))
            false
    /// <summary>
    /// Add a single service instance. 
    /// </summary>
    member internal x.AddServiceInstance( remoteSignature, serviceInstance ) =
        let bAdded = x.AddServiceInstanceRaw( remoteSignature, serviceInstance )
        if bAdded then 
            x.OnAddServiceInstanceExecutor.Trigger( (remoteSignature, serviceInstance), ( fun _ -> sprintf "Instance %A from %s" (serviceInstance.ServiceID.ToString()) 
                                                                                                    (LocalDNS.GetShowInfo( LocalDNS.Int64ToIPEndPoint(remoteSignature) )) ) )
    /// <summary> 
    /// Enumerate through service Instance collection
    /// </summary> 
    member x.GetServiceCollections() = 
        x.ServiceCollectionByID |> Seq.map ( fun pair -> (pair.Key, pair.Value) )
    /// <summary>
    /// Remove service of a particular backend
    /// </summary>
    member x.RemoveServiceInstance( remoteSignature ) = 
        let bExist, allServiceID = x.ServiceCollectionByQueueSignature.TryRemove( remoteSignature )
        if bExist then 
            for serviceID in allServiceID do 
                let bExist, oneServiceCollection = x.ServiceCollectionByID.TryGetValue( serviceID )
                if bExist then 
                    let bExist, serviceInstance = oneServiceCollection.TryRemove( remoteSignature ) 
                    if bExist then 
                        x.OnRemoveServiceInstanceExecutor.Trigger( (remoteSignature, serviceInstance), ( fun _ -> sprintf "Instance %A from %s" (serviceInstance.ServiceID.ToString()) 
                                                                                                                   (LocalDNS.GetShowInfo( LocalDNS.Int64ToIPEndPoint(remoteSignature) )) ) )

                        /// If after removal, collection is empty remove the upper level serviceID. 
                        if oneServiceCollection.IsEmpty then 
                            x.ServiceCollectionByID.TryRemove( serviceID ) |> ignore
    /// Delegate function that will be called when the instance starts
    member val OnStartFrontEnd = List< FrontEndOnStartFunction<'StartParamType> >() with get
    /// Delegate function that will be called when the instance shutdown
    member val OnStopFrontEnd = List<UnitAction>() with get
    /// The function is called to define a set of initial command that will be send to each FrontEnd at start.
    member val OnInitialMsgToFrontEnd = ConcurrentQueue< FormMsgToFrontEndFunction >() with get
    member val internal TimerBufferCache = null with get, set
    /// Keep Alive timer (note that we run one timer for all queues, so it is probable some queue may experience 2x time before a keep alive message is sent out
    member val IntervalKeepAliveInMs = 2500 with get, set
    /// Request timeout check interval, check whether no reply has received for X ms. 
    member val IntervalRequestTimeoutCheckInMs = 200 with get, set
    member val IntervalCleanupClientTrackingInMs = 1000 with get, set
    /// Keep Alive ticks
    member val IntervalKeepAliveTicks = 0L with get, set

    member val internal TimerKeepAlive = null with get, set
    member val internal TimerRequestTimeout = null with get, set
    member val internal TimerClearClientTracking = null with get, set
    member val internal Listener = null with get, set
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
    override x.OnStart param =
        // Setup distribution policy
        x.RegisterDistributionPolicy( FrontEndInstance<_>.DistributionToFastestBackendGuid, 
                                        "To one fastest backend",
                                        DistributionPolicyFunction(x.DistributionToFastestBackend) )
        x.RegisterDistributionPolicy( FrontEndInstance<_>.DistributionRoundRobinGuid, 
                                        "RoundRobin",
                                        DistributionPolicyFunction(x.DistributionRoundRobin) )

        for entry in param.DistributionPolicyLists do 
            x.RegisterDistributionPolicy( entry )

        // Setup aggregation policy
        x.RegisterAggregationPolicy( FrontEndInstance<_>.AggregationByFirstReplyGuid, 
                                        "Use first reply", 
                                        AggregationPolicyFunction(x.AggregationByFirstReply) )
        for entry in param.AggregationPolicyLists do 
            x.RegisterAggregationPolicy( entry )
        /// Install additional distribution policy
        

        OwnershipTracking.Current.RetrieveCallbackFunc <- x.RetrieveCacheableBufferFromPeer
        // Put code to initialize gateway here. 
        let mutable bSuccess = true 
        for lst in x.OnStartFrontEnd do
            bSuccess <- bSuccess && lst.Invoke( param )     
        // Control front end cache behavior
        decodeServiceCollectionDelegate <- param.DecodeServiceCollectionFunction
        x.StatisticsPeriodInSecond <- param.StatisticsPeriodInSecond
        if Utils.IsNull decodeServiceCollectionDelegate then 
            bSuccess <- false
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Need to install a decode function to parse ServiceCollections" ))
        else
            BufferCache.TimeOutRetrieveCacheableBufferInMs <- param.TimeOutRetrieveCacheableBufferInMs 
            BufferCache.TimeOutReleaseCacheableBufferInMs <- param.TimeOutReleaseCacheableBufferInMs 
            x.TimerBufferCache <- ThreadPoolTimer.TimerWait ( fun _ -> sprintf "Timer for BufferCache cleanup" ) (BufferCache.Current.CleanUp) (param.IntervalExamineBufferCacheInMs) (param.IntervalExamineBufferCacheInMs) 
            x.IntervalKeepAliveInMs <- param.IntervalKeepAliveInMs
            
            if x.IntervalKeepAliveInMs <= 0 then 
                x.IntervalKeepAliveTicks <- Int64.MaxValue
            else
                x.IntervalKeepAliveTicks <- int64 x.IntervalKeepAliveInMs * TimeSpan.TicksPerMillisecond
            x.TimerKeepAlive <- ThreadPoolTimer.TimerWait ( fun _ -> sprintf "Timer for keep alive" ) (x.KeepAlive) x.IntervalKeepAliveInMs x.IntervalKeepAliveInMs                         
            x.TimerRequestTimeout <- ThreadPoolTimer.TimerWait ( fun _ -> sprintf "Timer to request timeout " ) (x.CheckForRequestTimeOut) x.IntervalRequestTimeoutCheckInMs x.IntervalRequestTimeoutCheckInMs                         
            x.ClientTrackingTimeOutInMS <- param.ClientTrackingTimeOutInMS
            x.TimerClearClientTracking <- ThreadPoolTimer.TimerWait ( fun _ -> sprintf "Timer to cleanup ClientTracking " ) (x.CleanupClientTracking) x.IntervalCleanupClientTrackingInMs x.IntervalCleanupClientTrackingInMs                         

            x.bTerminate <- not bSuccess
            if bSuccess then 
                let seqMsg = x.OnInitialMsgToFrontEnd |> Seq.map ( fun del -> del.Invoke() ) |> Seq.collect ( Operators.id )
                x.InitialMessage <- seqMsg |> Seq.toArray
                if param.TimeOutRequestInMilliSecond<=0 then 
                    x.TimeOutTicks <- 1000000L * TimeSpan.TicksPerMillisecond // request timeout in 1000 sec (we need timeout value to remove reqHolder)
                else
                    x.TimeOutTicks <- int64 param.TimeOutRequestInMilliSecond * TimeSpan.TicksPerMillisecond
                if param.ServicePort > 0 && param.ServicePort <= 65536 then 
                    x.Listener <- JobListener.InitializeListenningPort( "", param.ServicePort )  
                    x.Listener.OnAccepted( OnAcceptAction(x.OnAcceptedQueue), fun _ -> sprintf "Accept incoming queue by vHub FrontEnd" ) 
                else
                    // Try to attach to default job port
                    if Utils.IsNotNull RemoteContainer.DefaultJobListener then 
                        RemoteContainer.DefaultJobListener.OnAccepted( OnAcceptAction(x.OnAcceptedQueue), fun _ -> sprintf "Accept incoming queue by vHub FrontEnd" ) 
                ContractStore.ExportSeqFunction( FrontEndInstance<_>.ContractNameFrontEndRequestStatistics, x.GetRequestStatistics, -1, true )
        bSuccess 
    /// Health of backend nodes connected. 
    member val BackEndHealth = ConcurrentDictionary<_, BackEndPerformance >() with get, set
    member internal x.OnAcceptedQueue queue = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "accept a connection from backend server %s." (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) ))
        let procItem = (
            fun (cmd : NetworkCommand) -> 
                if not x.bTerminate then 
                    x.ParseBackEndReply queue cmd.cmd (cmd.ms)
                null
        )
        let remoteSignature = queue.RemoteEndPointSignature
        x.BackEndHealth.GetOrAdd( remoteSignature, fun _ -> x.BackEndPerformanceConstructionDelegate.Invoke() ) |> ignore
        queue.OnDisconnect.Add( new UnitAction(x.CloseQueue( remoteSignature )) )
        queue.GetOrAddRecvProc( "ParseBackEnd", procItem ) |> ignore
        queue.Initialize()
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "add command parser for backend server %s." (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) ))
//        queue.Initialize() Initalized is done after OnAccepted. 
        x.OnAcceptExecutor.Trigger( queue, fun _ -> sprintf "Incoming connection from %s" (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) )
//        for del in x.OnAccept do 
//            del.Invoke( queue )
        x.SendInitialMessage( queue )
    /// Send initial message
    member internal x.SendInitialMessage( queue ) = 
        if not ( Utils.IsNull x.InitialMessage) then 
            let bExist, health =  x.BackEndHealth.TryGetValue( queue.RemoteEndPointSignature ) 
            if bExist then 
                for cmd, ms in x.InitialMessage do 
                    // Wrap initial message with health information & end marker
                    let msSend = new MemStream( int ms.Length + 128 ) 
                    health.WriteHeader( msSend ) 
                    let (ms, pos, len) = ms.GetBufferPosLength()
                    msSend.AppendNoCopy(ms, int64 pos, int64 len)
                    //msSend.WriteBytesWithOffset( ms.GetBufferPosLength() )
                    health.WriteEndMark( msSend ) 
                    queue.ToSend( cmd, msSend )
    /// Send keep alive
    member x.KeepAlive() = 
        if x.IntervalKeepAliveTicks<Int64.MaxValue then 
            for pair in x.BackEndHealth do 
                let remoteSingature = pair.Key
                let health = pair.Value
                let ticksCur = (PerfDateTime.UtcNowTicks())
                let ticks = health.LastSendTicks.Ticks
                if ticks < ticksCur - x.IntervalKeepAliveTicks then 
                    let queue = Cluster.Connects.LookforConnectBySignature( remoteSingature ) 
                    if not (Utils.IsNull queue ) && queue.CanSend then 
                        // send echo message 
                        let t1 = (PerfADateTime.UtcNowTicks())
                        let msEcho = new MemStream( 128 ) 
                        health.WriteHeader( msEcho ) 
                        health.WriteEndMark( msEcho ) 
                        queue.ToSend( ControllerCommand(ControllerVerb.Echo, ControllerNoun.QueryReply ), msEcho )
                        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "send keep alive to server %s (%dB, preparation in %f ms)" 
                                                                           (LocalDNS.GetShowInfo( queue.RemoteEndPoint ))
                                                                           msEcho.Length (TimeSpan((PerfADateTime.UtcNowTicks()) - t1).TotalMilliseconds) ))
    override x.Run() = 
        while not x.bTerminate do 
            while not x.bTerminate && x.ExaminePrimaryQueueOnce() do 
                // Continuously process Primary Queue items if there is something to do. 
                ()
            if not x.bTerminate then 
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
    member x.CancelAllInstances() = 
        x.bTerminate <- true
        // Terminate all queues. 
        for pair in x.BackEndHealth do 
            let remoteSignature = pair.Key
            let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
            if not (Utils.IsNull queue ) then 
                queue.Terminate()
        x.EvPrimaryQueue.Set() |> ignore 
        if not(Utils.IsNull x.TimerBufferCache) then 
            x.TimerBufferCache.Cancel() 
        if not(Utils.IsNull x.TimerKeepAlive) then 
            x.TimerKeepAlive.Cancel() 
    /// Parse Receiving command 
    member internal x.ParseBackEndReply queue cmd ms = 
        if not x.bTerminate then 
            try
                let remoteSignature = queue.RemoteEndPointSignature
                let health = x.BackEndHealth.GetOrAdd( remoteSignature, fun _ -> x.BackEndPerformanceConstructionDelegate.Invoke() )
                health.ReadHeader( ms ) 
                match cmd.Verb, cmd.Noun with 
                | ControllerVerb.Unknown, _ ->
                    ()
                | ControllerVerb.List, ControllerNoun.Buffer -> 
                    let guids = BufferCache.UnPackGuid( ms ) 
                    // Track all guids own by the node, so if later some Cachable buffer is not available, we will know who to ask. 
                    let collection = OwnershipTracking.Current.RemoteCollection.GetOrAdd( remoteSignature, fun _ -> ConcurrentDictionary<_,_>() )
                    guids |> Array.iter ( fun guid -> collection.Item( guid ) <- null )
                    let items = BufferCache.Current.FindMissingGuids( guids ) 
                    let msSend = new MemStream( )
                    health.WriteHeader( msSend ) 
                    BufferCache.PackGuid( items, msSend )
                    health.WriteEndMark( msSend ) 
                    queue.ToSend( ControllerCommand(ControllerVerb.Read, ControllerNoun.Buffer ), msSend )
                | ControllerVerb.Write, ControllerNoun.Buffer -> 
                    let items  = BufferCache.UnPackCacheableBuffers( ms ) 
                    for item in items do 
                        let id = BufferCache.Current.AddCacheableBuffer( item )  
                        OwnershipTracking.Current.ReceiveBufferFromPeer( remoteSignature, id )
                | ControllerVerb.Set, ControllerNoun.QueryReply -> 
                    let serviceCollection = decodeServiceCollectionDelegate.Invoke(ms)
                    let count = ref 0 
                    for serviceInstance in serviceCollection do 
                        x.AddServiceInstance( remoteSignature, serviceInstance )
                        count := !count + 1
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "from %s received %d serviceInstance(Set, QueryReply), Rtt = %f ms." (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) (!count) (health.GetRtt()) ) )
                | ControllerVerb.Reply, ControllerNoun.QueryReply -> 
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "from %s received reply %dB, Rtt = %f ms." (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) (ms.Length) (health.GetRtt()) ) )
                    // x.ParseTimestamp( queue, health, ms )
                    let buf = Array.zeroCreate<_> 16
                    ms.ReadBytes( buf ) |> ignore
                    let reqID = Guid( buf ) 
                    let qPerf = SingleQueryPerformance.Unpack( ms ) 
                    let replyObject = ms.DeserializeObjectWithTypeName()
                    if Utils.IsNull replyObject then 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Reply, Request from %s received Null Object, Rtt = %f ms." (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) (health.GetRtt()) ) )
                        x.ProcessReply( queue, health, reqID, qPerf, replyObject ) 
                    else
                        x.ProcessReply( queue, health, reqID, qPerf, replyObject ) 
                | ControllerVerb.TimeOut, ControllerNoun.QueryReply -> 
                    let buf = Array.zeroCreate<_> 16
                    ms.ReadBytes( buf ) |> ignore
                    let reqID = Guid( buf ) 
                    let elapse = ms.ReadInt32()
                    let msg = sprintf "Server %s timeout after %d milliseconds." (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) elapse
                    x.ProcessReplyBackendError( queue.RemoteEndPointSignature, health, reqID, msg ) 
                | ControllerVerb.Error, ControllerNoun.QueryReply -> 
                    let buf = Array.zeroCreate<_> 16
                    ms.ReadBytes( buf ) |> ignore
                    let reqID = Guid( buf ) 
                    let msg = ms.ReadString()
                    x.ProcessReplyBackendError( queue.RemoteEndPointSignature, health, reqID, msg ) 
                | ControllerVerb.EchoReturn, ControllerNoun.QueryReply ->
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "from %s received echoReturn, Rtt = %.2f ms, Last Rtt = %.2fms" (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) (health.GetRtt()) (health.LastRtt) ) )
                | _ -> 
                    let mutable bParsed = false 
//                    let em = x.OnParse.GetEnumerator()
//                    while not bParsed && (em.MoveNext()) do 
                    for del in x.OnParse do 
//                        let del = em.Current
                        if not bParsed then 
                            let bUse, ev = del.Invoke( (queue, health, cmd, ms) ) 
                            if bUse then 
                                bParsed <- bUse
                                if not(Utils.IsNull ev) then 
                                    // Blocked in parse
                                    let mutable bDone = false
                                    while not bDone do
                                        let bUse, ev = del.Invoke( queue, health, cmd, ms ) 
                                        bDone <- Utils.IsNull ev
                    if not bParsed then 
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "from backend %s received a command %A (%dB) that is not parsed by the frontend service " 
                                                                   (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) 
                                                                   cmd ms.Length))
                let bValid = health.ReadEndMark( ms ) 
                if not bValid then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "from gateway %s received an unrecognized command %A (%dB), the intergrity check fails " 
                                                               (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) 
                                                               cmd ms.Length)                    )
            with 
            | e -> 
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Exception in ParseFrontEndRequest, %A" 
                                                           e ))
    /// <summary> 
    /// Retrieve a guid from a certain peer 
    /// </summary> 
    member x.RetrieveCacheableBufferFromPeer remoteSignature id = 
        let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature ) 
        let bExist, health = x.BackEndHealth.TryGetValue( remoteSignature ) 
        if bExist && Utils.IsNotNull queue then 
            let msSend = new MemStream( )
            health.WriteHeader( msSend ) 
            BufferCache.PackGuid( [| id |], msSend )
            health.WriteEndMark( msSend ) 
            queue.ToSend( ControllerCommand(ControllerVerb.Read, ControllerNoun.Buffer ), msSend )
            false, new ManualResetEvent(false)
        else
            false, null
    /// <summary>
    /// Receive an incoming request. The following information are needed:
    /// reqID: Guid to identify the request. (no request cache is implemented in the moment, but we could cache reply based on guid. 
    /// serviceID: Guid that identify the serviceID that will be requested. 
    /// distributionPolicy: Guid that identify the distribution service to be used
    /// aggregationPolicy: Guid that identify the aggregation service to be used
    /// ticksSentByClient: Client can use the ticks to estimate network RTT of the request. 
    /// RTTSentByClient: network RTT of the request, 
    /// IPAddress, port: IP 
    /// </summary> 
    member x.ReceiveRequest( reqID: Guid, providerID, schemaID, serviceID, distributionPolicy, aggregationPolicy, 
                                reqObject, 
                                ticksSentByClient, 
                                rttSentByClient, 
                                addressByClient, 
                                portByClient, 
                                replyCallback ) = 
        let ticksCur = (PerfADateTime.UtcNowTicks())
        // ReqID is the only one that is not in the reqHolder, it is being used like a key
        let reqHolder = RequestHolder( ProviderID = providerID, 
                                        SchemaID = schemaID, 
                                        ServiceID = serviceID, 
                                        DistributionPolicy = distributionPolicy, 
                                        AggregationPolicy = aggregationPolicy, 
                                        ReqObject = reqObject, 
                                        TicksSentByClient = ticksSentByClient, 
                                        RTTSentByClient = rttSentByClient, 
                                        AddressByClient = addressByClient, 
                                        PortByClient = portByClient, 
                                        TicksRequestReceived = ticksCur, 
                                        TicksRequestTimeout = if x.TimeOutTicks=Int64.MaxValue then DateTime.MaxValue.Ticks else ticksCur + x.TimeOutTicks )
        x.RegisterClient( addressByClient, portByClient, rttSentByClient, "ReceiveRequest" )
        x.ProcessReqeust reqID reqHolder replyCallback (fun _ -> sprintf "Reply that correspond to request from %s:%d" addressByClient portByClient )
    /// Register a received request, 
    /// The request is deposited into the reqHolder. 
    /// When the reply is received, replyCallback will be called. 
    member x.ProcessReqeust reqID reqHolder replyCallback infoReply =
        let existingHolder = x.InProcessRequestCollection.GetOrAdd( reqID, reqHolder ) 
        if Object.ReferenceEquals( reqHolder, existingHolder ) then 
            // An unique request 
            reqHolder.OnReply( replyCallback, infoReply )
            x.PrimaryQueue.Enqueue( reqID )
            x.EvPrimaryQueue.Set() |> ignore
        else
            // Piggyback on an existing request. 
            existingHolder.OnReply( replyCallback, infoReply ) 
    /// <summary> 
    /// Register incoming client 
    /// </summary>
    member x.RegisterClient( addressByClient, portByClient, rttSentByClient, info ) = 
        x.ClientTracking.Item( (addressByClient, portByClient) ) <- (info, rttSentByClient, (PerfADateTime.UtcNowTicks()) ) 
    /// List incoming clients. timePeriodInMS timespan (millisecond) to list for incoming clients, -1: infinity. 
    /// Return:
    ///     seq< client_address, rtt_of_client, ticks_of_client_activity, info > 
    member x.ListClients( timePeriodInMS:int ) = 
        x.CleanupClientTracking()
        let ticks = if timePeriodInMS<=0 then DateTime.MinValue.Ticks else (PerfADateTime.UtcNowTicks()) - TimeSpan.TicksPerMillisecond * int64 timePeriodInMS
        x.ClientTracking |> Seq.choose ( fun kv -> let addressByClient, portByClient = kv.Key
                                                   let info, rttSentByClient, ticksClient = kv.Value
                                                   if ticksClient < ticks then 
                                                        None
                                                   else
                                                        Some ( addressByClient + ":" + portByClient.ToString(), rttSentByClient, ticksClient, info ) )
    /// List incoming clients. timePeriodInMS timespan (millisecond) to list for incoming clients, -1: infinity. 
    /// Return:
    ///     seq< client_address, rtt_of_client, ticks_of_client_activity, info > 
    member x.ListClientsByIP( timePeriodInMS:int ) = 
        x.CleanupClientTracking()
        let ticks = if timePeriodInMS<=0 then DateTime.MinValue.Ticks else (PerfADateTime.UtcNowTicks()) - TimeSpan.TicksPerMillisecond * int64 timePeriodInMS
        x.ClientTracking |> Seq.choose ( fun kv -> let addressByClient, portByClient = kv.Key
                                                   let info, rttSentByClient, ticksClient = kv.Value
                                                   if ticksClient < ticks then 
                                                        None
                                                   else
                                                        Some ( addressByClient, rttSentByClient, ticksClient, info ) )

    /// <summary> 
    /// Clear Clients 
    /// </summary> 
    member x.CleanupClientTracking() = 
        if x.ClientTrackingTimeOutInMS > 0 then 
            let ticksCleanUp = (PerfADateTime.UtcNowTicks()) - TimeSpan.TicksPerMillisecond * int64 x.ClientTrackingTimeOutInMS
            for kv in x.ClientTracking do 
                let _, _, ticksClient = kv.Value
                if ticksClient < ticksCleanUp then 
                    x.ClientTracking.TryRemove( kv.Key ) |> ignore 

    /// <summary> 
    /// Collection of distribution Policy
    /// </summary> 
    member val DistrictionPolicyCollection = ConcurrentDictionary<Guid,string * DistributionPolicyFunction>() with get
    /// Guid of policy to select the backend with the lowest expected latency to serve the request 
    static member val DistributionToFastestBackendGuid = Guid( "5308962E-91F0-439C-AB72-6B2180837433" ) with get
    /// Guid of policy to roundrobinly select a backend 
    static member val DistributionRoundRobinGuid = Guid ("A5000012-4F64-4E9C-B07C-4D984B1D260D" ) with get
    /// Register additional distribution policy 
    member x.RegisterDistributionPolicy( policyGuid, name, del ) = 
        x.DistrictionPolicyCollection.Item( policyGuid ) <- ( name, del )
    member internal x.DistributionToFastestBackend( reqHolder, backendArr ) = 
        let mutable idx = -1
        let mutable score = Int32.MaxValue
        let len = if Utils.IsNull backendArr then 0 else backendArr.Length
        for i = 0 to len - 1 do 
            let signature, perfQ, _ = backendArr.[i]
            if perfQ.ExpectedLatencyInMS < score then 
                score <- perfQ.ExpectedLatencyInMS
                idx <- i
        if idx >=0 then
            [| backendArr.[idx] |]
        else
            [| |]
    member internal x.DistributionRoundRobin( reqHolder, backendArr ) = 
        let useBackEndRef = ref Unchecked.defaultof<_>
        let mutable score = Int32.MaxValue
        let len = if Utils.IsNull backendArr then 0 else backendArr.Length
        let randArr = Array.copy backendArr
        let rnd = System.Random()
        for i = len downto 1 do 
            let idx = rnd.Next( i ) 
            // swap it to back
            let cur = randArr.[idx]
            let signature, perfQ, _ = cur
            if perfQ.OutstandingRequests < score then 
                score <- perfQ.OutstandingRequests
                useBackEndRef := cur
            randArr.[idx] <- randArr.[ i - 1] // Redraw the last item. 
        if score < Int32.MaxValue then
            [| !useBackEndRef |]
        else
            [| |]

    /// Find the distribution policy, if the request distribution policy doesn't exist, the policy DistributionToFastestBackend will be applied. 
    member internal x.FindDistributionPolicy( distributionPolicyID ) = 
        let bExist, tuple = x.DistrictionPolicyCollection.TryGetValue( distributionPolicyID ) 
        if bExist then 
            let _, del = tuple
            del
        else
            DistributionPolicyFunction( x.DistributionRoundRobin )


    /// Processing primary queue once.
    /// Return: true: we have done something in the loop (need to examine if there is anything to do). 
    ///         false: there is nothing to do in the loop
    member internal x.ExaminePrimaryQueueOnce() = 
        let bAny, reqID = x.PrimaryQueue.TryDequeue()
        if bAny then 
            let bExist, reqHolder = x.InProcessRequestCollection.TryGetValue( reqID ) 
            if bExist then 
                // Otherwise, the request is timeout, 
                let serviceID = reqHolder.ServiceID
                let bService, oneServiceCollection = x.ServiceCollectionByID.TryGetValue( serviceID )
                if bService then 
                    let bAnyOtherObject = not x.PrimaryQueue.IsEmpty
                    // Ensure ExamineServiceQueueOnce is executed 
                    let lst = List<_>(oneServiceCollection.Count)
                    for pair in oneServiceCollection do 
                        let remoteSignature = pair.Key
                        let bExist, health = x.BackEndHealth.TryGetValue( remoteSignature ) 
                        let queue = Cluster.Connects.LookforConnectBySignature( remoteSignature )
                        if bExist && not (Utils.IsNull queue) then 
                            lst.Add( queue, health, pair.Value )
                    let examinedService = lst.ToArray()
                    let del = x.FindDistributionPolicy( reqHolder.DistributionPolicy ) 
                    let remoteArr = del.Invoke( reqHolder, examinedService )
                    let len = if Utils.IsNull remoteArr then 0 else remoteArr.Length
                    let mutable bRequestSend = false
                    for i = 0 to len - 1 do 
                        // bRequestSend become true if any of the request is sent 
                        bRequestSend <- bRequestSend || x.SendRequest( reqID, reqHolder, remoteArr.[i] )
                    
                    // Don't find any service
                    if not bRequestSend then 
                        let msg = sprintf "After distribution policy, the service collection %A has 0 availabe service" serviceID
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Req %A %s" 
                                                                           reqID msg ))
                        let perfQ = SingleQueryPerformance( Message = msg )
                        reqHolder.ReceivedReply( perfQ, null )
                        x.AddPerfStatistics( reqID, reqHolder, "NoService", null )
                        x.ServiceCollectionByID.TryRemove( reqID ) |> ignore
                    else
                        reqHolder.RequestAssignedFlag := 1
                    bRequestSend || bAnyOtherObject
                else
                    let perfQ = SingleQueryPerformance( Message = sprintf "Failed to find service collection %A" serviceID )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Req %A has an invalid service ID %A." 
                                                                       reqID serviceID ))
                    reqHolder.ReceivedReply( perfQ, null )
                    x.AddPerfStatistics( reqID, reqHolder, "NoService", null )
                    // Remove request
                    x.ServiceCollectionByID.TryRemove( reqID ) |> ignore
                    true
            else
                // Service ID doesn't exist, something wrong, as it should not be enqueued in the first place. 
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "request ID %A doesn't have a corresponding reqHolder, something is wrong, the request will not be responded!!!" 
                                                           reqID ))
                true
        else
            false    
    /// Send request
    /// Return : true is request is sent
    member internal x.SendRequest( reqID, reqHolder, tuple ) = 
        let queue, health, serviceInstanceBasic = tuple
        let msRequest = new MemStream( ) 
        // Count the request as being sent 
        health.RegisterRequest() 
        health.WriteHeader( msRequest ) 
        msRequest.WriteBytes( reqID.ToByteArray() )
        msRequest.WriteBytes( serviceInstanceBasic.ServiceID.ToByteArray() )
        msRequest.SerializeObjectWithTypeName( reqHolder.ReqObject )
        health.WriteEndMark( msRequest )
        queue.ToSend( ControllerCommand( ControllerVerb.Request, ControllerNoun.QueryReply), msRequest )
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "send req %s to backend %s, to return (%s, %s) " (reqID.ToString()) (LocalDNS.GetShowInfo( queue.RemoteEndPoint)) (health.ExpectedLatencyInfo()) (health.QueueInfo()) ))
        // Timing 
        reqHolder.ReplyCollection.Item( queue.RemoteEndPointSignature) <- ref ( (PerfADateTime.UtcNowTicks()), 0L , null )
        true
    member internal x.AddPerfStatistics( reqID, reqHolder, info, qPerf:SingleQueryPerformance ) = 
        if x.StatisticsPeriodInSecond >= 0 then 
            x.StatisticsCollection.GetOrAdd( reqID, (reqHolder.TicksRequestReceived, reqHolder.ServiceID, info, qPerf ) ) |> ignore
            x.CleanStatisticsQueue() 
    member internal x.CleanStatisticsQueue() = 
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
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Removed %d query from frontend as they timed out, earlied time %s" !cntRemoved (VersionToString(DateTime(!earliestTicks))) ))
    /// <summary> 
    /// Return query performance in the following forms:
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
        x.StatisticsCollection |> Seq.map( fun pair -> let ticks, serviceID, info, qPerf = pair.Value
                                                       pair.Key, ticks, serviceID, info, qPerf ) 

    /// <summary> 
    /// Collection of aggregation Policy
    /// </summary> 
    member val AggregationPolicyCollection = ConcurrentDictionary<Guid,string*AggregationPolicyFunction>() with get
    /// Aggregation by first reply 
    static member val AggregationByFirstReplyGuid  = Guid( "32F1A5FF-CBA7-43B3-A610-893FD3F55ACF" ) with get
    /// Register additional aggregation policy 
    member x.RegisterAggregationPolicy( policyGuid, name, del ) = 
        x.AggregationPolicyCollection.Item( policyGuid ) <- (name, del)
    /// <summary> 
    /// Find the distribution policy, if the request distribution policy doesn't exist, the policy DistributionToFastestBackend will be applied. 
    /// </summary> 
    member internal x.FindAggregationPolicy( aggregationPolicyID ) = 
        let bExist, tuple = x.AggregationPolicyCollection.TryGetValue( aggregationPolicyID ) 
        if bExist then 
            let _, del = tuple
            del
        else
            AggregationPolicyFunction( x.AggregationByFirstReply )
    member internal x.AggregationByFirstReply( reqHolder, perfQ, reqObject ) = 
        reqObject
    /// Has all request been received or timeout has reached
    member internal x.ReplyStatistics (reqHolder:RequestHolder) = 
        if !reqHolder.RequestAssignedFlag > 0 then 
            let mutable nMinReplyTicks = Int64.MaxValue
            let mutable numReplyReceived = 0 
            let mutable bAllReplyReceived = true
            for pair in reqHolder.ReplyCollection do 
                let _, ticksReceived, _ = (!pair.Value)
                if ticksReceived=0L then 
                    bAllReplyReceived <- false
                else 
                    numReplyReceived <- numReplyReceived + 1
                    nMinReplyTicks <- Math.Min( nMinReplyTicks, ticksReceived ) 
            bAllReplyReceived || ((PerfADateTime.UtcNowTicks()) >= reqHolder.TicksRequestTimeout ), numReplyReceived, nMinReplyTicks
        else
            (PerfDateTime.UtcNowTicks()) >= reqHolder.TicksRequestTimeout, 0, DateTime.MinValue.Ticks
    /// <summary> 
    /// This function can be used to implement policy that will wait for the return of 1st reply + num millisecond. Then a second AggregationPolicyFunction function is called to 
    /// resolve the reply object. 
    /// </summary> 
    member internal x.AggregationByFirstReplyPlusN (msToWait:int) (secondAggregation) ( reqHolder, perfQ:SingleQueryPerformance, reqObject:Object )  = 
        let bAllIn, numReplyReceived, nMinReplyTicks = x.ReplyStatistics( reqHolder )
        if bAllIn then 
            secondAggregation( reqHolder, perfQ, reqObject )
        else
            reqHolder.TicksRequestTimeout <- nMinReplyTicks + ( int64 msToWait * TimeSpan.TicksPerMillisecond )
            if (PerfDateTime.UtcNowTicks()) >= reqHolder.TicksRequestTimeout then 
                secondAggregation( reqHolder, perfQ, reqObject )
            else
                null
    /// Processing Request object
    /// The reqHolder is not deallocated after the last reply come in (or request timeout, that way, we can 
    member internal x.ProcessReply( queue, health, reqID, qPerf, replyObject ) =
        let bExist, reqHolder = x.InProcessRequestCollection.TryGetValue( reqID ) 
        if not bExist then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "received a request ID %A from backend %s, but can't find corresponding request holder"
                                                       reqID (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) ))
        else 
            // Throw away if the request is not in processing (e.g., the backend sends in the request too late. 
            // We won't be able to calcualte statistics in such a case. )
            let bSent, refTuple = reqHolder.ReplyCollection.TryGetValue( queue.RemoteEndPointSignature ) 
            if not bSent then 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "received a request ID %A from backend %s that is not in the ReplyCollection (haven't sent out the request). "
                                                           reqID (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) ))
            else 
                let ticksCur = (PerfADateTime.UtcNowTicks())
                let ticksSent, _, _ = !refTuple
                refTuple := ticksSent, ticksCur, replyObject
                qPerf.InAssignment <- int ( (ticksSent - reqHolder.TicksRequestReceived) / TimeSpan.TicksPerMillisecond )
                let rawTotal = int ( (ticksCur-ticksSent ) / TimeSpan.TicksPerMillisecond )
                let rawNetwork = rawTotal - qPerf.InQueue - qPerf.InProcessing
                qPerf.InNetwork <- Math.Min( 0, rawNetwork )
                health.DepositReply( qPerf )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "received reply corresponding to reqID %A from backend %s (%s)"
                                                               reqID (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) (qPerf.BackEndInfo()) ))
                x.PrepareReply( qPerf, reqID, reqHolder, replyObject )
    /// Send out reply 
    member internal x.PrepareReply( qPerf, reqID, reqHolder, replyObject ) =
        // Calculate performance statistics 
        let del = x.FindAggregationPolicy( reqHolder.AggregationPolicy ) 
        let finalReplyObject = del.Invoke( reqHolder, qPerf, replyObject ) 
        if not(Utils.IsNull finalReplyObject)  then 
            reqHolder.ReceivedReply( qPerf, finalReplyObject )
            x.AddPerfStatistics( reqID, reqHolder, "Reply", qPerf )
        else
            let bAllIn, _, _ = x.ReplyStatistics( reqHolder ) 
            if bAllIn then 
                if reqHolder.NumReplySent<=0 then 
                    let msg = sprintf "All %d reply come in, but there is no valid reply" reqHolder.ReplyCollection.Count
                    qPerf.Message <- msg
                    reqHolder.ReceivedReply( qPerf, null )
                    x.AddPerfStatistics( reqID, reqHolder, "Invalid", null )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "all reports coming in for ID %s, request holder removed" (reqID.ToString()) )                    )
                x.InProcessRequestCollection.TryRemove( reqID ) |> ignore 
    /// Check for time out 
    member internal x.CheckForRequestTimeOut() = 
        for pair in x.InProcessRequestCollection do 
            let reqID = pair.Key
            let reqHolder = pair.Value
            let bAllInOrTimeOut, _, _ = x.ReplyStatistics( reqHolder ) 
            if bAllInOrTimeOut then 
                if reqHolder.NumReplySent<=0 then 
                    let msg = sprintf "All %d reply come in, but there is no valid reply" reqHolder.ReplyCollection.Count
                    let qPerf = SingleQueryPerformance( Message = msg ) 
                    reqHolder.ReceivedReply( qPerf, null )
                    x.AddPerfStatistics( reqID, reqHolder, "Invalid", null )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "for request ID %s, all report coming in or timeout, request holder removed" (reqID.ToString()) )                    )
                x.InProcessRequestCollection.TryRemove( reqID ) |> ignore 
    /// Processing Request that is in error 
    member internal x.ProcessReplyBackendError( remoteSignature, health, reqID, msg ) =
        let bExist, reqHolder = x.InProcessRequestCollection.TryGetValue( reqID ) 
        if not bExist then 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "request ID %A received error msg from backend %s, but can't find corresponding request holder"
                                                       reqID (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(remoteSignature))) ))
        else
            // Throw away if the request is not in processing (e.g., the backend sends in the request too late. 
            // We won't be able to calcualte statistics in such a case. )
            let bSent, refTuple = reqHolder.ReplyCollection.TryGetValue( remoteSignature ) 
            if not bSent then 
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "received a request ID %A from backend %s that is not in the ReplyCollection (haven't sent out the request). "
                                                           reqID (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(remoteSignature))) ))
            else 
                let ticksCur = (PerfADateTime.UtcNowTicks())
                let ticksSent, _, _ = !refTuple
                refTuple := ticksSent, ticksCur, null
                let qPerf = SingleQueryPerformance( Message = msg )
                qPerf.InAssignment <- int ( (ticksSent - reqHolder.TicksRequestReceived) / TimeSpan.TicksPerMillisecond )
                qPerf.InNetwork <- int ( (ticksCur-ticksSent ) / TimeSpan.TicksPerMillisecond )
                // Calculate performance statistics 
                health.DepositReply( qPerf )
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "received error msg %s corresponding to reqID %A from backend %s. "
                                                               msg reqID (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(remoteSignature))) ))
                x.PrepareReply( qPerf, reqID, reqHolder, null ) 
    /// CloseQueue is called if the connection has been abruptly closed (e.g., network failure to FrontEnd.   
    member internal x.CloseQueue( remoteSignature ) () =
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> let ipaddr = LocalDNS.Int64ToIPEndPoint( remoteSignature) 
                                                      sprintf "Remote queue at %s is closed" (LocalDNS.GetShowInfo( ipaddr )) ))
        x.RemoveServiceInstance( remoteSignature ) 
        x.BackEndHealth.TryRemove( remoteSignature ) |> ignore 
        // We don't clear item in BufferCache (as it may be used by other instance of other backend, we clear the ownership store 
        let bExist, collection = OwnershipTracking.Current.RemoteCollection.TryRemove( remoteSignature ) 
        if bExist then 
            for pair in collection do 
                let ev = pair.Value
                if Utils.IsNotNull ev then
                    // Unblock all operation waiting for CacheableBuffer 
                    ev.Set() |> ignore                
        // Check out going request. 
        for pair in x.InProcessRequestCollection do 
            let reqID = pair.Key
            let reqHolder = pair.Value
            let bExist, tupleRef = reqHolder.ReplyCollection.TryRemove( remoteSignature ) 
            if bExist then 
                let bAllInOrTimeout, _, _ = x.ReplyStatistics( reqHolder ) 
                if bAllInOrTimeout then 
                    if reqHolder.NumReplySent<=0 then 
                        let msg = sprintf "Remote backend %s has been disconnected. " (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(remoteSignature)))
                        let qPerf = SingleQueryPerformance( Message = msg ) 
                        x.PrepareReply( qPerf, reqID, reqHolder, null )                         
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "queue %s shutdown, for request ID %s, no remaining outstanding report, request holder removed" (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(remoteSignature))) (reqID.ToString()) )                    )
                    x.InProcessRequestCollection.TryRemove( reqID ) |> ignore 
        x.OnDisconnectExecutor.Trigger( remoteSignature, fun _ -> sprintf "Disconnection of %s" (LocalDNS.GetShowInfo(LocalDNS.Int64ToIPEndPoint(remoteSignature))) )
//        for del in x.OnDisconnect do 
//            del.Invoke( remoteSignature ) 

