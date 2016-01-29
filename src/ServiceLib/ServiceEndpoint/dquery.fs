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
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jan. 2015
        Revised Jan. 2016
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.ServiceEndpoint

open System
open System.Collections.Concurrent
open System.Threading
open Prajna.Core
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp

type NetworkPerformance = Prajna.Core.NetworkPerformance

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
type SingleQueryPerformance = Prajna.Service.SingleRequestPerformance



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
