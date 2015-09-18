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
        dquery.fs
  
    Description: 
        A open distributed gateway / server that support distributed query. This library abstracts
    M global clusters of gateway node (for traffic direction) and N clusters of recognition node. 
    Based on certain performance mapping (specified by the user) the gateway node will redict the 
    incomming traffic to 1 or N backend node for processing. 

        With Prajna, the status of the gateway (query statistics information) & query of the recognition
    service can be realtime access by the user. 
    
        The service library targets web/image/video/audio developer. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jan. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.ServiceEndpoint

open System
open System.Collections.Concurrent
open System.Threading
open Prajna.Core
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp

/// <summary>
/// Network Performance statistics of Request
/// </summary>
[<AllowNullLiteral>]
type NetworkPerformance() = 
    let startTime = (PerfADateTime.UtcNow())
    /// <summary> Number of Rtt samples to statistics </summary>
    static member val RTTSamples = 32 with get, set
    /// <summary> Threshold above which the RTT value is deemed unreliable, and will not be used </summary>
    static member val RTTFilterThreshold = 100000. with get, set
    // Guid written at the end to make sure that the entire blob is currently formatted 
    static member val internal BlobIntegrityGuid = System.Guid("D1742033-5D26-4982-8459-3617C2FF13C4") with get
    member val internal RttArray = Array.zeroCreate<_> NetworkPerformance.RTTSamples with get
    /// Filter out 1st RTT value, as it contains initialization cost, which is not part of network RTT
    member val internal RttCount = ref -2L with get
    /// Last RTT from the remote node. 
    member val LastRtt = 0. with get, set
    member val internal nInitialized = ref 0 with get
    /// Last ticks that the content is sent from this queue
    member val LastSendTicks = DateTime.MinValue with get, set
    member val internal LastRcvdSendTicks = 0L with get, set
    member val internal TickDiffsInReceive = 0L with get, set
    member internal x.RTT with get() = 0
    member internal x.FirstTime() = 
        Interlocked.CompareExchange( x.nInitialized, 1, 0 ) = 0
    member internal x.PacketReport( tickDIffsInReceive:int64, sendTicks ) = 
        x.LastRcvdSendTicks <- sendTicks
        let ticksCur = (PerfADateTime.UtcNowTicks())
        x.TickDiffsInReceive <- ticksCur - sendTicks
        if tickDIffsInReceive<>0L then 
            // Valid ticks 
            let rttTicks = ticksCur - sendTicks + tickDIffsInReceive
            let rtt = TimeSpan( rttTicks ).TotalMilliseconds
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Packet Received, send = %s, diff = %d, diffrcvd: %d"                                                                         
                                                                       (VersionToString(DateTime(sendTicks)))
                                                                       x.TickDiffsInReceive
                                                                       tickDIffsInReceive ) )
            if rtt >= 0. && rtt < NetworkPerformance.RTTFilterThreshold then 
                x.LastRtt <- rtt
                let idx = int (Interlocked.Increment( x.RttCount ))
                if idx >= 0 then 
                    x.RttArray.[idx % NetworkPerformance.RTTSamples ] <- rtt
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "receive packet with unreasonable rtt of %f ms, cur %d, send %d, diff %d thrown away..."
                                                                rtt 
                                                                ticksCur sendTicks tickDIffsInReceive ) )
        else
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "receive packet, unable to calculate RTT as TicksDiff is 0, send = %s, diff = %d" 
                                                                       (VersionToString(DateTime(sendTicks)))
                                                                       x.TickDiffsInReceive
                                                                        ) )
    /// Whether connection receives some valid data
    member x.ConnectionReady() = 
        (!x.RttCount)>=0L
    /// Get the RTT of the connection
    member x.GetRtt() = 
        let sumRtt = Array.sum x.RttArray
        let numRtt = Math.Min( (!x.RttCount)+1L, int64 NetworkPerformance.RTTSamples )
        if numRtt<=0L then 
            1000.
        else
            sumRtt / float numRtt
    member internal x.SendPacket() = 
        x.LastSendTicks <- (PerfADateTime.UtcNow())
    /// The ticks that the connection is initialized. 
    member x.StartTime with get() = startTime
    /// Wrap header for RTT estimation 
    member x.WriteHeader( ms: StreamBase<byte> ) = 
        let diff = x.TickDiffsInReceive
        ms.WriteInt64( diff )
        let curTicks = (PerfADateTime.UtcNowTicks())
        ms.WriteInt64( curTicks )
        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "to send packet, diff = %d" 
                                                                   diff ))
    /// Validate header for RTT estimation 
    member x.ReadHeader( ms: StreamBase<byte> ) = 
        let tickDIffsInReceive = ms.ReadInt64( ) 
        let sendTicks = ms.ReadInt64()
        x.PacketReport( tickDIffsInReceive, sendTicks )
    /// Write end marker 
    member x.WriteEndMark( ms: StreamBase<byte> ) = 
        ms.WriteBytes( NetworkPerformance.BlobIntegrityGuid.ToByteArray() )
        x.SendPacket()
    /// Validate end marker
    member x.ReadEndMark( ms: StreamBase<byte>) = 
        let data = Array.zeroCreate<_> 16
        ms.ReadBytes( data ) |> ignore
        let guid = Guid( data )
        guid = NetworkPerformance.BlobIntegrityGuid

/// <summary>
/// QueryPerformance provides a performance statistics instance for the underlying operation. 
/// Whenever a request is queued, a timestamp is registered in the class. We will wait for the 
/// operation to complete to unqueue the request, and register a finished timestamp of the request. 
/// From both, we can calculate the timespan that the request complete, and compute execution statistics. 
/// </summary>
[<AllowNullLiteral>]
type QueryPerformance(info: unit -> string) = 
    member val internal OperationTimeCollection = ConcurrentDictionary<_,_>()
    member val internal OperationTimeArray = Array.zeroCreate<_> NetworkPerformance.RTTSamples with get
    member val internal nOperation = ref 0L with get
    member val internal nValidOperation = ref -1L with get
    /// <summary> Number of operation queued </summary>
    member x.NumOperations with get() = !x.nOperation
    /// <summary> Number of valid operation completed </summary>
    member x.NumValidOperations with get() = !x.nValidOperation
    /// <summary> This function when the timed operation starts </summary> 
    member x.RegisterStart( reqId: Guid ) = 
        let ticks = (PerfADateTime.UtcNowTicks())
        Interlocked.Increment( x.nOperation ) |> ignore
        x.OperationTimeCollection.GetOrAdd( reqId, ticks ) 
    /// <summary> This function is called when the timed operation ends. bSuccess=true, operation completes. bSuccess=false, operation fails (timeout), and is not timed. </summary> 
    member x.RegisterEnd( reqId, bSuccess ) = 
        let bRemove, ticks = x.OperationTimeCollection.TryRemove( reqId ) 
        if bRemove then 
            if bSuccess then 
                let idx = int (Interlocked.Increment( x.nValidOperation ))
                let ticksCur = (PerfADateTime.UtcNowTicks())
                let elapse = float ( ticksCur - ticks ) / float TimeSpan.TicksPerMillisecond // convert to millisecond
                x.OperationTimeArray.[ idx % NetworkPerformance.RTTSamples ] <- elapse
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "req %A completes successfully in %f ms" reqId elapse))
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  let ticksCur = (PerfADateTime.UtcNowTicks())
                                                               let elapse = float ( ticksCur - ticks ) / float TimeSpan.TicksPerMillisecond 
                                                               sprintf "req %A timedout in %f ms" reqId elapse))
        else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "fail to locate req %A ... " reqId))
            
    /// average operation time in milliseconds. 
    member x.GetAvgOperationTime( ) = 
        let sum = Array.sum x.OperationTimeArray
        let num = Math.Min( (!x.nValidOperation)+1L, int64 NetworkPerformance.RTTSamples )
        if num <= 0L then 
            0.
        else
            sum / float num

/// <summary>
/// SingleQueryPerformance gives out the performance of a single query. 
/// </summary>
[<AllowNullLiteral; Serializable>]
type SingleQueryPerformance() = 
    /// Time spent in assignment stage, before the request is queued to network 
    member val InAssignment = 0 with get, set
    /// Time spent in network (including network stack)
    member val InNetwork = 0 with get, set
    /// Time spent in queue of the query engine 
    member val InQueue = 0 with get, set
    /// Time spent in processing 
    member val InProcessing = 0 with get, set
    /// Number of Pending request in queue
    member val NumItemsInQueue = 0 with get, set
    /// Number of Slots Available
    member val NumSlotsAvailable = 0 with get, set

    /// Additional Message
    member val Message : string = null with get, set
    /// Serialize SingleQueryPerformance
    static member Pack( x:SingleQueryPerformance, ms:StreamBase<byte> ) = 
        let inQueue = if x.InQueue < 0 then 0 else if x.InQueue > 65535 then 65535 else x.InQueue
        let inProc = if x.InProcessing < 0 then 0 else if x.InProcessing > 65535 then 65535 else x.InProcessing
        ms.WriteUInt16( uint16 inQueue )
        ms.WriteUInt16( uint16 inProc )
        ms.WriteVInt32( x.NumItemsInQueue )
        ms.WriteVInt32( x.NumSlotsAvailable )
    /// Deserialize SingleQueryPerformance
    static member Unpack( ms:StreamBase<byte> ) = 
        let inQueue = int (ms.ReadUInt16())
        let inProcessing = int (ms.ReadUInt16())
        let numItems = ms.ReadVInt32()
        let numSlots = ms.ReadVInt32()
        SingleQueryPerformance( InQueue = inQueue, InProcessing = inProcessing, 
                                NumItemsInQueue = numItems, NumSlotsAvailable=numSlots )
    /// Show string that can be used to monitor backend performance 
    abstract BackEndInfo: unit -> string
    override x.BackEndInfo() = 
        sprintf "queue: %dms, proc: %dms, items: %d, slot: %d" x.InQueue x.InProcessing x.NumItemsInQueue x.NumSlotsAvailable
    /// Show string that can be used to monitor frontend performance 
    abstract FrontEndInfo: unit -> string
    override x.FrontEndInfo() = 
        sprintf "assign: %dms, network %dms, queue: %dms, proc: %dms, items: %d, slot: %d" x.InAssignment x.InNetwork x.InQueue x.InProcessing x.NumItemsInQueue x.NumSlotsAvailable
        



/// <summary>
/// QueryProviderQueue represents a connection to/from a recognition hub, and the associated performance information
/// gateway. 
/// </summary>
type internal QueryProviderQueue(info) =
    /// <summary>
    /// The queue associated with the endpoint that is attached to the QueryHub 
    /// </summary>
    member val Queue : NetworkCommandQueue = null with get, set
    /// <summary>
    /// Performance of the endpoint. Each domain is a string, e.g., "DogBreed", "Plant". 
    /// </summary>
    member val NetworkPerf = NetworkPerformance() with get
    /// <summary>
    /// Performance of the endpoint. Each domain is a string, e.g., "DogBreed", "Plant". 
    /// </summary>
    member val QueryPerf = QueryPerformance(info) with get
